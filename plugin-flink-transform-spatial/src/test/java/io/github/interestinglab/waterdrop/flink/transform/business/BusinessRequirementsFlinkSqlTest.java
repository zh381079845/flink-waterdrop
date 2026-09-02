package io.github.interestinglab.waterdrop.flink.transform.business;

import io.github.interestinglab.waterdrop.flink.transform.GeometryAreaUdf;
import io.github.interestinglab.waterdrop.flink.transform.GeometryAsTextUdf;
import io.github.interestinglab.waterdrop.flink.transform.GeometryBufferUdf;
import io.github.interestinglab.waterdrop.flink.transform.GeometryBoundsUdf;
import io.github.interestinglab.waterdrop.flink.transform.GeometryDistanceUdf;
import io.github.interestinglab.waterdrop.flink.transform.GeometryFromWktUdf;
import io.github.interestinglab.waterdrop.flink.transform.Predicates;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.github.interestinglab.waterdrop.flink.transform.business.BusinessSampleData.require;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Flink SQL 业务包验收：合规冲突、包络 JOIN、缓冲覆盖、城管落格、危化评估。
 */
public class BusinessRequirementsFlinkSqlTest {

    private static Map<String, BusinessSampleData.Feature> eco;
    private static Map<String, BusinessSampleData.Feature> farm;
    private static Map<String, BusinessSampleData.Feature> project;
    private static Map<String, BusinessSampleData.Feature> facility;
    private static Map<String, BusinessSampleData.Feature> parcel;
    private static Map<String, BusinessSampleData.Feature> zone;
    private static Map<String, BusinessSampleData.Feature> road;

    private static final RowTypeInfo LAYER2 = new RowTypeInfo(
            new TypeInformation[]{BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO},
            new String[]{"id", "geom"});

    @BeforeClass
    public static void load() throws Exception {
        eco = BusinessSampleData.load(BusinessSampleData.ECO);
        farm = BusinessSampleData.load(BusinessSampleData.FARM);
        project = BusinessSampleData.load(BusinessSampleData.PROJECT);
        facility = BusinessSampleData.load(BusinessSampleData.FACILITY);
        parcel = BusinessSampleData.load(BusinessSampleData.PARCEL);
        zone = BusinessSampleData.load(BusinessSampleData.ZONE);
        road = BusinessSampleData.load(BusinessSampleData.ROAD);
    }

    @Test
    public void L_END_01_complianceConflictJoinInSql() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        register(tableEnv);

        List<Row> projects = new ArrayList<Row>();
        projects.add(Row.of("PRJ-CONFLICT", require(project, "PRJ-CONFLICT").wkt));
        projects.add(Row.of("PRJ-OK", require(project, "PRJ-OK").wkt));

        List<Row> constraints = new ArrayList<Row>();
        constraints.add(Row.of("ECO-001", require(eco, "ECO-001").wkt));
        constraints.add(Row.of("FARM-001", require(farm, "FARM-001").wkt));
        constraints.add(Row.of("FARM-002", require(farm, "FARM-002").wkt));

        tableEnv.registerDataStream("project", env.fromCollection(projects, LAYER2), "id, geom");
        tableEnv.registerDataStream("constraint_layer", env.fromCollection(constraints, LAYER2), "id, geom");

        DataStream<Row> result = tableEnv.toAppendStream(
                tableEnv.sqlQuery(
                        "SELECT p.id AS project_id, c.id AS constraint_id, st_area(p.geom) AS area_m2 "
                                + "FROM project p, constraint_layer c "
                                + "WHERE st_xmax(p.geom) >= st_xmin(c.geom) "
                                + "AND st_xmin(p.geom) <= st_xmax(c.geom) "
                                + "AND st_ymax(p.geom) >= st_ymin(c.geom) "
                                + "AND st_ymin(p.geom) <= st_ymax(c.geom) "
                                + "AND st_intersects(p.geom, c.geom)"),
                Row.class);

        Set<String> pairs = new HashSet<String>();
        Iterator<Row> it = DataStreamUtils.collect(result);
        while (it.hasNext()) {
            Row row = it.next();
            pairs.add(row.getField(0) + "/" + row.getField(1));
            assertTrue(((Number) row.getField(2)).doubleValue() > 0);
        }
        assertTrue(pairs.contains("PRJ-CONFLICT/ECO-001"));
        assertTrue(pairs.contains("PRJ-CONFLICT/FARM-001"));
        assertFalse(pairs.contains("PRJ-OK/ECO-001"));
        assertFalse(pairs.contains("PRJ-CONFLICT/FARM-002"));
    }

    @Test
    public void U08_hospitalCoverageJoinInSql() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        register(tableEnv);

        List<Row> facilities = new ArrayList<Row>();
        facilities.add(Row.of("HOSP-001", require(facility, "HOSP-001").wkt));

        List<Row> residentials = new ArrayList<Row>();
        residentials.add(Row.of("RES-001", require(parcel, "RES-001").wkt));
        residentials.add(Row.of("PARCEL-C", require(parcel, "PARCEL-C").wkt));

        tableEnv.registerDataStream("facility", env.fromCollection(facilities, LAYER2), "id, geom");
        tableEnv.registerDataStream("residential", env.fromCollection(residentials, LAYER2), "id, geom");

        DataStream<Row> result = tableEnv.toAppendStream(
                tableEnv.sqlQuery(
                        "SELECT f.id, r.id "
                                + "FROM facility f, residential r "
                                + "WHERE st_intersects(st_buffer(f.geom, CAST(5000 AS DOUBLE)), r.geom)"),
                Row.class);

        Set<String> hits = collectPairs(result);
        assertTrue(hits.contains("HOSP-001/RES-001"));
        assertFalse(hits.contains("HOSP-001/PARCEL-C"));
    }

    @Test
    public void C_END_01_eventGridDispatchInSql() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        register(tableEnv);

        List<Row> events = new ArrayList<Row>();
        events.add(Row.of("EVT-001", require(zone, "EVT-001").wkt));

        List<Row> grids = new ArrayList<Row>();
        grids.add(Row.of("GRID-01", require(zone, "GRID-01").wkt));
        grids.add(Row.of("GRID-02", require(zone, "GRID-02").wkt));

        tableEnv.registerDataStream("event", env.fromCollection(events, LAYER2), "id, geom");
        tableEnv.registerDataStream("grid", env.fromCollection(grids, LAYER2), "id, geom");

        DataStream<Row> result = tableEnv.toAppendStream(
                tableEnv.sqlQuery(
                        "SELECT e.id, g.id FROM event e, grid g WHERE st_within(e.geom, g.geom)"),
                Row.class);

        Set<String> hits = collectPairs(result);
        assertEquals(1, hits.size());
        assertTrue(hits.contains("EVT-001/GRID-01"));
    }

    @Test
    public void S_END_01_hazardResidentImpactInSql() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        register(tableEnv);

        List<Row> hazards = new ArrayList<Row>();
        hazards.add(Row.of("HAZARD-001", require(facility, "HAZARD-001").wkt));

        List<Row> assets = new ArrayList<Row>();
        assets.add(Row.of("RES-001", require(parcel, "RES-001").wkt));
        assets.add(Row.of("PARCEL-C", require(parcel, "PARCEL-C").wkt));

        tableEnv.registerDataStream("hazard", env.fromCollection(hazards, LAYER2), "id, geom");
        tableEnv.registerDataStream("asset", env.fromCollection(assets, LAYER2), "id, geom");

        DataStream<Row> result = tableEnv.toAppendStream(
                tableEnv.sqlQuery(
                        "SELECT h.id, a.id, st_distance(h.geom, a.geom) AS dist_m "
                                + "FROM hazard h, asset a "
                                + "WHERE st_intersects(st_buffer(h.geom, CAST(3000 AS DOUBLE)), a.geom)"),
                Row.class);

        Set<String> hits = new HashSet<String>();
        Iterator<Row> it = DataStreamUtils.collect(result);
        while (it.hasNext()) {
            Row row = it.next();
            hits.add(row.getField(0) + "/" + row.getField(1));
            assertTrue(((Number) row.getField(2)).doubleValue() >= 0);
        }
        assertTrue(hits.contains("HAZARD-001/RES-001"));
        assertFalse(hits.contains("HAZARD-001/PARCEL-C"));
    }

    @Test
    public void M01_roadEcoCrossDetectInSql() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        register(tableEnv);

        List<Row> roads = new ArrayList<Row>();
        roads.add(Row.of("ROAD-001", require(road, "ROAD-001").wkt));
        roads.add(Row.of("ROAD-002", require(road, "ROAD-002").wkt));

        List<Row> ecos = new ArrayList<Row>();
        ecos.add(Row.of("ECO-001", require(eco, "ECO-001").wkt));

        tableEnv.registerDataStream("road", env.fromCollection(roads, LAYER2), "id, geom");
        tableEnv.registerDataStream("eco", env.fromCollection(ecos, LAYER2), "id, geom");

        DataStream<Row> result = tableEnv.toAppendStream(
                tableEnv.sqlQuery(
                        "SELECT r.id, e.id FROM road r, eco e WHERE st_intersects(r.geom, e.geom)"),
                Row.class);

        Set<String> hits = collectPairs(result);
        assertTrue(hits.contains("ROAD-001/ECO-001"));
        assertFalse(hits.contains("ROAD-002/ECO-001"));
    }

    @Test
    public void R01_parcelOverlapDetectInSql() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        register(tableEnv);

        List<Row> parcels = new ArrayList<Row>();
        parcels.add(Row.of("PARCEL-A", require(parcel, "PARCEL-A").wkt));
        parcels.add(Row.of("PARCEL-B", require(parcel, "PARCEL-B").wkt));
        parcels.add(Row.of("PARCEL-C", require(parcel, "PARCEL-C").wkt));

        tableEnv.registerDataStream("p1", env.fromCollection(parcels, LAYER2), "id, geom");
        tableEnv.registerDataStream("p2", env.fromCollection(parcels, LAYER2), "id, geom");

        DataStream<Row> result = tableEnv.toAppendStream(
                tableEnv.sqlQuery(
                        "SELECT a.id, b.id FROM p1 a, p2 b "
                                + "WHERE a.id < b.id AND st_overlaps(a.geom, b.geom)"),
                Row.class);

        Set<String> hits = collectPairs(result);
        assertTrue(hits.contains("PARCEL-A/PARCEL-B"));
        assertFalse(hits.contains("PARCEL-A/PARCEL-C"));
    }

    private static Set<String> collectPairs(DataStream<Row> result) {
        Set<String> hits = new HashSet<String>();
        Iterator<Row> it = DataStreamUtils.collect(result);
        while (it.hasNext()) {
            Row row = it.next();
            hits.add(row.getField(0) + "/" + row.getField(1));
        }
        return hits;
    }

    private void register(StreamTableEnvironment tableEnv) {
        tableEnv.registerFunction("st_buffer", new GeometryBufferUdf());
        tableEnv.registerFunction("st_area", new GeometryAreaUdf());
        tableEnv.registerFunction("st_geomfromwkt", new GeometryFromWktUdf());
        tableEnv.registerFunction("st_astext", new GeometryAsTextUdf());
        tableEnv.registerFunction("st_distance", new GeometryDistanceUdf());
        tableEnv.registerFunction("st_xmin", new GeometryBoundsUdf.XMin());
        tableEnv.registerFunction("st_xmax", new GeometryBoundsUdf.XMax());
        tableEnv.registerFunction("st_ymin", new GeometryBoundsUdf.YMin());
        tableEnv.registerFunction("st_ymax", new GeometryBoundsUdf.YMax());
        tableEnv.registerFunction("st_intersects", new Predicates.ST_Intersects());
        tableEnv.registerFunction("st_contains", new Predicates.ST_Contains());
        tableEnv.registerFunction("st_within", new Predicates.ST_Within());
        tableEnv.registerFunction("st_overlaps", new Predicates.ST_Overlaps());
        tableEnv.registerFunction("st_dwithin", new Predicates.ST_DWithin());
    }
}
