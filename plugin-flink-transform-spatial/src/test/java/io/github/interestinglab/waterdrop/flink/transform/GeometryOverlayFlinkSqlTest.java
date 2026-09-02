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
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Flink SQL：st_intersection + st_area 占用面积；st_distance 线面最近点。
 */
public class GeometryOverlayFlinkSqlTest {

    @Test
    public void occupyAreaAndNearestDistanceInSql() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.registerFunction("st_area", new GeometryAreaUdf());
        tableEnv.registerFunction("st_distance", new GeometryDistanceUdf());
        tableEnv.registerFunction("st_intersection", new GeometryOverlayUdf.Intersection());
        tableEnv.registerFunction("st_difference", new GeometryOverlayUdf.Difference());
        tableEnv.registerFunction("st_intersects", new Predicates.ST_Intersects());

        List<Row> rows = new ArrayList<Row>();
        rows.add(Row.of(
                "POLYGON((116.40 39.90, 116.40 40.00, 116.50 40.00, 116.50 39.90, 116.40 39.90))",
                "POLYGON((116.45 39.95, 116.45 40.05, 116.55 40.05, 116.55 39.95, 116.45 39.95))",
                "LINESTRING(116.50 39.88, 116.55 39.90, 116.60 39.91)",
                "POINT(116.55 39.90)"));
        DataStream<Row> stream = env.fromCollection(
                rows,
                new RowTypeInfo(
                        new TypeInformation[]{
                                BasicTypeInfo.STRING_TYPE_INFO,
                                BasicTypeInfo.STRING_TYPE_INFO,
                                BasicTypeInfo.STRING_TYPE_INFO,
                                BasicTypeInfo.STRING_TYPE_INFO
                        },
                        new String[]{"project", "eco", "pipe", "on_pipe"}));
        tableEnv.registerDataStream("src", stream, "project, eco, pipe, on_pipe");

        DataStream<Row> result = tableEnv.toAppendStream(
                tableEnv.sqlQuery(
                        "SELECT "
                                + "st_intersects(project, eco) AS conflict, "
                                + "st_area(st_intersection(project, eco)) AS occupy_m2, "
                                + "st_area(st_difference(project, eco)) AS net_m2, "
                                + "st_distance(pipe, on_pipe) AS dist_m "
                                + "FROM src"),
                Row.class);

        Iterator<Row> it = DataStreamUtils.collect(result);
        assertTrue(it.hasNext());
        Row row = it.next();
        assertTrue((Boolean) row.getField(0));
        assertTrue(((Number) row.getField(1)).doubleValue() > 1e5);
        assertTrue(((Number) row.getField(2)).doubleValue() > 1e5);
        assertTrue(((Number) row.getField(1)).doubleValue()
                + ((Number) row.getField(2)).doubleValue()
                > ((Number) row.getField(1)).doubleValue());
        assertTrue(((Number) row.getField(3)).doubleValue() < 1.0);
        assertFalse(it.hasNext());
    }
}
