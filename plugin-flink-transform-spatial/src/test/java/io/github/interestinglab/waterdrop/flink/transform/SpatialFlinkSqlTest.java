package io.github.interestinglab.waterdrop.flink.transform;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * 把 UDF 挂进 Flink SQL 跑一遍，确认函数名和 VARCHAR 签名能绑定。
 */
public class SpatialFlinkSqlTest {

    @Test
    public void flinkSqlCanCallSpatialFunctions() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.registerFunction("st_area", new GeometryAreaUdf());
        tableEnv.registerFunction("st_intersects", new GeometryIntersectsUdf());
        tableEnv.registerFunction("st_xmin", new GeometryBoundsUdf.XMin());
        tableEnv.registerFunction("st_xmax", new GeometryBoundsUdf.XMax());
        tableEnv.registerFunction("st_ymin", new GeometryBoundsUdf.YMin());
        tableEnv.registerFunction("st_ymax", new GeometryBoundsUdf.YMax());

        List<Row> input = new ArrayList<Row>();
        input.add(Row.of(
                "POLYGON((116 39, 116 40, 117 40, 117 39, 116 39))",
                "POLYGON((116.2 39.2, 116.2 39.4, 116.4 39.4, 116.4 39.2, 116.2 39.2))"));
        DataStream<Row> stream = env.fromCollection(
                input,
                new RowTypeInfo(BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO));
        tableEnv.registerDataStream("src", stream, "a, b");

        DataStream<Row> result = tableEnv.toAppendStream(
                tableEnv.sqlQuery(
                        "SELECT st_area(a, false) AS planar_area, "
                                + "st_intersects(a, b) AS hit, "
                                + "(st_xmax(a) >= st_xmin(b) AND st_xmin(a) <= st_xmax(b) "
                                + "AND st_ymax(a) >= st_ymin(b) AND st_ymin(a) <= st_ymax(b)) AS env_hit "
                                + "FROM src"),
                Row.class);

        Iterator<Row> iterator = DataStreamUtils.collect(result);
        assertTrue(iterator.hasNext());
        Row row = iterator.next();
        assertEquals(1.0, ((Number) row.getField(0)).doubleValue(), 1e-6);
        assertTrue((Boolean) row.getField(1));
        assertTrue((Boolean) row.getField(2));
        assertFalse(iterator.hasNext());
    }
}
