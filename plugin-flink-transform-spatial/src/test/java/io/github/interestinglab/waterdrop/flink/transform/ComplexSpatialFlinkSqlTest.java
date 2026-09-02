package io.github.interestinglab.waterdrop.flink.transform;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Flink SQL 复杂空间链路：多表包络 JOIN、buffer+intersects、多谓词组合。
 */
public class ComplexSpatialFlinkSqlTest {

    private static final RowTypeInfo LAYER_TYPE = new RowTypeInfo(
            new TypeInformation[]{BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO},
            new String[]{"id", "geom"});

    @Test
    public void envelopeJoinFindsOnlyRealConflicts() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        registerAll(tableEnv);

        List<Row> left = new ArrayList<Row>();
        left.add(Row.of("eco",
                "POLYGON((116.30 39.90, 116.30 40.05, 116.50 40.05, 116.50 39.90, 116.30 39.90))"));
        left.add(Row.of("island",
                "POLYGON((110.00 30.00, 110.00 30.10, 110.10 30.10, 110.10 30.00, 110.00 30.00))"));

        List<Row> right = new ArrayList<Row>();
        right.add(Row.of("farm",
                "POLYGON((116.40 39.95, 116.40 40.10, 116.60 40.10, 116.60 39.95, 116.40 39.95))"));
        right.add(Row.of("remote",
                "POLYGON((120.00 31.00, 120.00 31.10, 120.10 31.10, 120.10 31.00, 120.00 31.00))"));

        tableEnv.registerDataStream("left_layer", env.fromCollection(left, LAYER_TYPE), "id, geom");
        tableEnv.registerDataStream("right_layer", env.fromCollection(right, LAYER_TYPE), "id, geom");

        DataStream<Row> result = tableEnv.toAppendStream(
                tableEnv.sqlQuery(
                        "SELECT l.id AS left_id, r.id AS right_id "
                                + "FROM left_layer l, right_layer r "
                                + "WHERE st_xmax(l.geom) >= st_xmin(r.geom) "
                                + "AND st_xmin(l.geom) <= st_xmax(r.geom) "
                                + "AND st_ymax(l.geom) >= st_ymin(r.geom) "
                                + "AND st_ymin(l.geom) <= st_ymax(r.geom) "
                                + "AND st_intersects(l.geom, r.geom)"),
                Row.class);

        Set<String> pairs = new HashSet<String>();
        Iterator<Row> it = DataStreamUtils.collect(result);
        while (it.hasNext()) {
            Row row = it.next();
            pairs.add(row.getField(0) + "-" + row.getField(1));
        }
        assertTrue(pairs.contains("eco-farm"));
        assertFalse(pairs.contains("eco-remote"));
        assertFalse(pairs.contains("island-farm"));
        assertFalse(pairs.contains("island-remote"));
        assertEquals(1, pairs.size());
    }

    @Test
    public void bufferServiceAreaIntersectsParcelsInSql() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        registerAll(tableEnv);

        List<Row> facilities = new ArrayList<Row>();
        facilities.add(Row.of("hospital", "POINT(116.40 39.97)"));
        facilities.add(Row.of("remote", "POINT(117.50 40.80)"));

        List<Row> parcels = new ArrayList<Row>();
        parcels.add(Row.of("eco",
                "POLYGON((116.30 39.90, 116.30 40.05, 116.50 40.05, 116.50 39.90, 116.30 39.90))"));
        parcels.add(Row.of("far",
                "POLYGON((110.00 30.00, 110.00 30.05, 110.05 30.05, 110.05 30.00, 110.00 30.00))"));

        tableEnv.registerDataStream("facility", env.fromCollection(facilities, LAYER_TYPE), "id, geom");
        tableEnv.registerDataStream("parcel", env.fromCollection(parcels, LAYER_TYPE), "id, geom");

        DataStream<Row> result = tableEnv.toAppendStream(
                tableEnv.sqlQuery(
                        "SELECT f.id AS facility_id, p.id AS parcel_id, "
                                + "st_area(p.geom) AS parcel_area_m2 "
                                + "FROM facility f, parcel p "
                                + "WHERE st_intersects(st_buffer(f.geom, CAST(5000 AS DOUBLE)), p.geom)"),
                Row.class);

        Set<String> hits = new HashSet<String>();
        Iterator<Row> it = DataStreamUtils.collect(result);
        while (it.hasNext()) {
            Row row = it.next();
            hits.add(row.getField(0) + "-" + row.getField(1));
            assertTrue(((Number) row.getField(2)).doubleValue() > 1e6);
        }
        assertTrue(hits.contains("hospital-eco"));
        assertFalse(hits.contains("hospital-far"));
        assertFalse(hits.contains("remote-eco"));
        assertFalse(hits.contains("remote-far"));
    }

    @Test
    public void multiPredicatePipelineInOneQuery() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        registerAll(tableEnv);

        List<Row> rows = new ArrayList<Row>();
        rows.add(Row.of(
                "POLYGON((116.30 39.90, 116.30 40.05, 116.50 40.05, 116.50 39.90, 116.30 39.90))",
                "POINT(116.42 39.97)",
                "POINT(117.00 39.50)",
                "LINESTRING(116.20 39.85, 116.45 39.98, 116.65 39.93)"));
        DataStream<Row> stream = env.fromCollection(
                rows,
                new RowTypeInfo(
                        BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.STRING_TYPE_INFO));
        tableEnv.registerDataStream("src", stream, "poly, inside_pt, outside_pt, road");

        DataStream<Row> result = tableEnv.toAppendStream(
                tableEnv.sqlQuery(
                        "SELECT "
                                + "st_contains(poly, inside_pt) AS site_ok, "
                                + "st_contains(poly, outside_pt) AS outside_hit, "
                                + "st_intersects(poly, road) AS road_cross, "
                                + "st_dwithin(inside_pt, outside_pt, CAST(1000 AS DOUBLE)) AS too_close, "
                                + "st_distance(inside_pt, outside_pt) > 10000 AS far_enough, "
                                + "st_area(poly) > 1e7 AS area_ok, "
                                + "st_astext(st_geomfromwkt(inside_pt)) IS NOT NULL AS roundtrip "
                                + "FROM src"),
                Row.class);

        Iterator<Row> it = DataStreamUtils.collect(result);
        assertTrue(it.hasNext());
        Row row = it.next();
        assertTrue((Boolean) row.getField(0));
        assertFalse((Boolean) row.getField(1));
        assertTrue((Boolean) row.getField(2));
        assertFalse((Boolean) row.getField(3));
        assertTrue((Boolean) row.getField(4));
        assertTrue((Boolean) row.getField(5));
        assertTrue((Boolean) row.getField(6));
        assertFalse(it.hasNext());
    }

    private void registerAll(StreamTableEnvironment tableEnv) {
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
        tableEnv.registerFunction("st_dwithin", new Predicates.ST_DWithin());
    }
}
