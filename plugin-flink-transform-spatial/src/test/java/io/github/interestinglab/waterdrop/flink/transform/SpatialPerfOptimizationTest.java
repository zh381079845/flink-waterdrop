package io.github.interestinglab.waterdrop.flink.transform;

import io.github.interestinglab.waterdrop.flink.util.TableUtil;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * 性能优化落地验收：物化、prepared_grid、max_cells、广播 STRtree、simplify。
 */
public class SpatialPerfOptimizationTest {

    private static final RowTypeInfo LAYER2 = new RowTypeInfo(
            new TypeInformation[]{BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO},
            new String[]{"id", "geometry"});

    @Test
    public void materializeSqlContainsBoundsAndOptionalExplode() {
        SpatialMaterializeSqlBuilder.Spec bounds = new SpatialMaterializeSqlBuilder.Spec();
        bounds.sourceTable = "eco_raw";
        bounds.cellSize = 0.05;
        bounds.mode = SpatialMaterializeSqlBuilder.Mode.BOUNDS;
        String sql1 = SpatialMaterializeSqlBuilder.build(bounds);
        assertTrue(sql1.contains("st_xmin"));
        assertTrue(sql1.contains("st_grid_id"));
        assertFalse(sql1.contains("LATERAL"));

        SpatialMaterializeSqlBuilder.Spec explode = new SpatialMaterializeSqlBuilder.Spec();
        explode.sourceTable = "eco_raw";
        explode.cellSize = 0.05;
        explode.maxCells = 100;
        explode.mode = SpatialMaterializeSqlBuilder.Mode.EXPLODE_GRID;
        explode.simplifyTolerance = 0.001;
        String sql2 = SpatialMaterializeSqlBuilder.build(explode);
        assertTrue(sql2.contains("LATERAL TABLE(st_grid_cells"));
        assertTrue(sql2.contains("CAST(100 AS INT)"));
        assertTrue(sql2.contains("st_simplify"));
    }

    @Test
    public void preparedGridJoinSqlSkipsLiveXminAndLateral() {
        SpatialJoinSqlBuilder.Spec spec = new SpatialJoinSqlBuilder.Spec();
        spec.leftTable = "eco_prep";
        spec.rightTable = "farm_prep";
        spec.mode = SpatialJoinSqlBuilder.Mode.PREPARED_GRID;
        spec.intersectionArea = true;
        assertTrue(spec.skipsLiveExpand());
        String sql = SpatialJoinSqlBuilder.build(spec);
        assertFalse(sql.contains("st_xmin"));
        assertFalse(sql.contains("st_xmax"));
        assertFalse(sql.contains("LATERAL"));
        assertFalse(sql.contains("st_grid_cells"));
        assertTrue(sql.contains("l.grid_id = r.grid_id"));
        assertTrue(sql.contains("`minx`") || sql.contains(".`minx`") || sql.contains(" AS minx"));
        assertTrue(sql.contains("st_intersects"));
        assertTrue(sql.contains("st_intersection")); // only in SELECT path
    }

    @Test
    public void materializedFlagsSkipExpandLikePreparedModes() {
        SpatialJoinSqlBuilder.Spec bounds = new SpatialJoinSqlBuilder.Spec();
        bounds.leftTable = "a";
        bounds.rightTable = "b";
        bounds.mode = SpatialJoinSqlBuilder.Mode.GRID;
        bounds.applyMaterializedFlags(true, false);
        assertEquals(SpatialJoinSqlBuilder.Mode.PREPARED, bounds.mode);
        String sqlBounds = SpatialJoinSqlBuilder.build(bounds);
        assertFalse(sqlBounds.contains("st_xmin"));
        assertFalse(sqlBounds.contains("LATERAL"));
        assertFalse(sqlBounds.contains("grid_id"));

        SpatialJoinSqlBuilder.Spec grid = new SpatialJoinSqlBuilder.Spec();
        grid.leftTable = "a";
        grid.rightTable = "b";
        grid.mode = SpatialJoinSqlBuilder.Mode.GRID;
        grid.applyMaterializedFlags(true, true);
        assertEquals(SpatialJoinSqlBuilder.Mode.PREPARED_GRID, grid.mode);
        String sqlGrid = SpatialJoinSqlBuilder.build(grid);
        assertFalse(sqlGrid.contains("st_xmin"));
        assertFalse(sqlGrid.contains("LATERAL"));
        assertTrue(sqlGrid.contains("l.grid_id = r.grid_id"));
    }

    @Test
    public void gridJoinPassesMaxCellsAndPartitionKey() {
        SpatialJoinSqlBuilder.Spec spec = new SpatialJoinSqlBuilder.Spec();
        spec.leftTable = "a";
        spec.rightTable = "b";
        spec.mode = SpatialJoinSqlBuilder.Mode.GRID;
        spec.cellSize = 0.1;
        spec.maxCells = 200;
        spec.leftPartitionKey = "adcode";
        spec.rightPartitionKey = "adcode";
        String sql = SpatialJoinSqlBuilder.build(spec);
        assertTrue(sql.contains("CAST(200 AS INT)"));
        assertTrue(sql.contains("l._part = r._part"));
    }

    @Test
    public void maxCellsCapsExpansion() {
        // 大面 + 极小格子 → 超 maxCells 退化单格
        String big = "POLYGON((116 39, 116 41, 118 41, 118 39, 116 39))";
        List<String> capped = SpatialGrid.cellsCovering(WktUtils.read(big), 0.001, 10);
        assertEquals(1, capped.size());
        List<String> uncapped = SpatialGrid.cellsCovering(WktUtils.read(big), 0.5, 10000);
        assertTrue(uncapped.size() > 1);
    }

    @Test
    public void simplifyReducesCoordinates() {
        GeometrySimplifyUdf simplify = new GeometrySimplifyUdf();
        String dense = "LINESTRING(0 0, 0.001 0.0001, 0.002 0, 0.003 0.0001, 1 0)";
        String out = simplify.eval(dense, 0.01);
        assertNotNull(out);
        assertTrue(WktUtils.read(out).getNumPoints() < WktUtils.read(dense).getNumPoints());
    }

    @Test
    public void materializeThenPreparedGridJoinInFlink() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        SpatialFunctionRegistry.registerAll(tableEnv);

        List<Row> left = new ArrayList<Row>();
        left.add(Row.of("eco",
                "POLYGON((116.30 39.90, 116.30 40.05, 116.50 40.05, 116.50 39.90, 116.30 39.90))"));
        List<Row> right = new ArrayList<Row>();
        right.add(Row.of("farm",
                "POLYGON((116.40 39.95, 116.40 40.10, 116.60 40.10, 116.60 39.95, 116.40 39.95))"));
        right.add(Row.of("remote",
                "POLYGON((120.00 31.00, 120.00 31.10, 120.10 31.10, 120.10 31.00, 120.00 31.00))"));

        tableEnv.registerDataStream("eco_raw", env.fromCollection(left, LAYER2), "id, geometry");
        tableEnv.registerDataStream("farm_raw", env.fromCollection(right, LAYER2), "id, geometry");

        SpatialMaterializeSqlBuilder.Spec mat = new SpatialMaterializeSqlBuilder.Spec();
        mat.cellSize = 0.05;
        mat.maxCells = 5000;
        mat.mode = SpatialMaterializeSqlBuilder.Mode.EXPLODE_GRID;
        mat.sourceTable = "eco_raw";
        tableEnv.createTemporaryView("eco_prep", tableEnv.sqlQuery(SpatialMaterializeSqlBuilder.build(mat)));
        mat.sourceTable = "farm_raw";
        tableEnv.createTemporaryView("farm_prep", tableEnv.sqlQuery(SpatialMaterializeSqlBuilder.build(mat)));

        SpatialJoinSqlBuilder.Spec join = new SpatialJoinSqlBuilder.Spec();
        join.leftTable = "eco_prep";
        join.rightTable = "farm_prep";
        join.mode = SpatialJoinSqlBuilder.Mode.PREPARED_GRID;
        join.intersectionArea = true;
        join.dedup = true;

        Table table = tableEnv.sqlQuery(SpatialJoinSqlBuilder.build(join));
        DataStream<Row> out = TableUtil.tableToDataStream(tableEnv, table, false);
        Set<String> pairs = new HashSet<String>();
        Iterator<Row> it = DataStreamUtils.collect(out);
        while (it.hasNext()) {
            Row row = it.next();
            pairs.add(row.getField(0) + "-" + row.getField(1));
        }
        assertTrue(pairs.contains("eco-farm"));
        assertFalse(pairs.contains("eco-remote"));
        assertEquals(1, pairs.size());
    }

    @Test
    public void broadcastStrTreeFindsIntersectingProbe() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);
        List<Row> build = new ArrayList<Row>();
        build.add(Row.of("eco",
                "POLYGON((116.30 39.90, 116.30 40.05, 116.50 40.05, 116.50 39.90, 116.30 39.90))"));
        List<Row> probe = new ArrayList<Row>();
        probe.add(Row.of("hit", "POINT(116.40 39.97)"));
        probe.add(Row.of("miss", "POINT(120.00 31.00)"));

        DataStream<Row> buildStream = env.fromCollection(build, LAYER2);
        DataStream<Row> probeStream = env.fromCollection(probe, LAYER2);
        BroadcastStream<Row> broadcast = buildStream.broadcast(SpatialBroadcastJoin.BUILD_DESC);

        DataStream<Row> hits = probeStream
                .connect(broadcast)
                .process(new SpatialBroadcastJoin.StrTreeBroadcastProbe())
                .returns(new RowTypeInfo(
                        BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.STRING_TYPE_INFO));

        Set<String> pairs = new HashSet<String>();
        Iterator<Row> it = DataStreamUtils.collect(hits);
        while (it.hasNext()) {
            Row row = it.next();
            pairs.add(row.getField(0) + "-" + row.getField(1));
        }
        assertTrue(pairs.contains("hit-eco"));
        assertFalse(pairs.contains("miss-eco"));
    }
}
