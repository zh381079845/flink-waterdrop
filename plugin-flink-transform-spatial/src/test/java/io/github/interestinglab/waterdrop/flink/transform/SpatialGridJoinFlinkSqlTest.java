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
 * Sedona 风格：LATERAL st_grid_cells 展开 → grid 等值 JOIN → envelope → intersects 精炼。
 */
public class SpatialGridJoinFlinkSqlTest {

    private static final double CELL = 0.05;

    private static final RowTypeInfo LAYER = new RowTypeInfo(
            new TypeInformation[]{BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO},
            new String[]{"id", "geom"});

    @Test
    public void gridThenEnvelopeThenIntersectsJoin() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        register(tableEnv);

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

        tableEnv.registerDataStream("left_raw", env.fromCollection(left, LAYER), "id, geom");
        tableEnv.registerDataStream("right_raw", env.fromCollection(right, LAYER), "id, geom");

        // 物化包络 + 展开格子
        tableEnv.sqlUpdate(
                "CREATE VIEW left_g AS "
                        + "SELECT l.id, l.geom, "
                        + "st_xmin(l.geom) AS minx, st_xmax(l.geom) AS maxx, "
                        + "st_ymin(l.geom) AS miny, st_ymax(l.geom) AS maxy, "
                        + "g.grid_id AS grid_id "
                        + "FROM left_raw l, "
                        + "LATERAL TABLE(st_grid_cells(l.geom, CAST(" + CELL + " AS DOUBLE))) AS g(grid_id)");

        tableEnv.sqlUpdate(
                "CREATE VIEW right_g AS "
                        + "SELECT r.id, r.geom, "
                        + "st_xmin(r.geom) AS minx, st_xmax(r.geom) AS maxx, "
                        + "st_ymin(r.geom) AS miny, st_ymax(r.geom) AS maxy, "
                        + "g.grid_id AS grid_id "
                        + "FROM right_raw r, "
                        + "LATERAL TABLE(st_grid_cells(r.geom, CAST(" + CELL + " AS DOUBLE))) AS g(grid_id)");

        DataStream<Row> result = tableEnv.toAppendStream(
                tableEnv.sqlQuery(
                        "SELECT l.id AS left_id, r.id AS right_id, "
                                + "st_area(st_intersection(l.geom, r.geom)) AS occupy_m2 "
                                + "FROM left_g l "
                                + "JOIN right_g r ON l.grid_id = r.grid_id "
                                + "AND l.maxx >= r.minx AND l.minx <= r.maxx "
                                + "AND l.maxy >= r.miny AND l.miny <= r.maxy "
                                + "AND st_intersects(l.geom, r.geom)"),
                Row.class);

        Set<String> pairs = new HashSet<String>();
        Iterator<Row> it = DataStreamUtils.collect(result);
        while (it.hasNext()) {
            Row row = it.next();
            String pair = row.getField(0) + "-" + row.getField(1);
            if (pairs.add(pair)) {
                assertTrue(((Number) row.getField(2)).doubleValue() > 0);
            }
        }
        assertTrue(pairs.contains("eco-farm"));
        assertFalse(pairs.contains("eco-remote"));
        assertFalse(pairs.contains("island-farm"));
        assertEquals(1, pairs.size());
    }

    private void register(StreamTableEnvironment tableEnv) {
        tableEnv.registerFunction("st_xmin", new GeometryBoundsUdf.XMin());
        tableEnv.registerFunction("st_xmax", new GeometryBoundsUdf.XMax());
        tableEnv.registerFunction("st_ymin", new GeometryBoundsUdf.YMin());
        tableEnv.registerFunction("st_ymax", new GeometryBoundsUdf.YMax());
        tableEnv.registerFunction("st_intersects", new Predicates.ST_Intersects());
        tableEnv.registerFunction("st_intersection", new GeometryOverlayUdf.Intersection());
        tableEnv.registerFunction("st_area", new GeometryAreaUdf());
        tableEnv.registerFunction("st_geohash", new GeometryGridUdf.GeoHashFn());
        tableEnv.registerFunction("st_grid_id", new GeometryGridUdf.GridId());
        tableEnv.createTemporarySystemFunction("st_grid_cells", new GeometryGridUdf.GridCells());
    }
}
