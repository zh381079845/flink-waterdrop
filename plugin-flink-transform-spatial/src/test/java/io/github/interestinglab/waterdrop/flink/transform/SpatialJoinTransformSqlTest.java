package io.github.interestinglab.waterdrop.flink.transform;

import io.github.interestinglab.waterdrop.flink.util.TableUtil;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * 验证 SpatialJoinSqlBuilder 生成的 SQL 在 Flink 上可执行且结果正确。
 */
public class SpatialJoinTransformSqlTest {

    private static final RowTypeInfo LAYER = new RowTypeInfo(
            new TypeInformation[]{BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO},
            new String[]{"id", "geometry"});

    @Test
    public void generatedGridJoinSqlRunsInFlink() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        SpatialFunctionRegistry.registerAll(tableEnv);

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

        tableEnv.registerDataStream("eco_raw", env.fromCollection(left, LAYER), "id, geometry");
        tableEnv.registerDataStream("farm_raw", env.fromCollection(right, LAYER), "id, geometry");

        SpatialJoinSqlBuilder.Spec spec = new SpatialJoinSqlBuilder.Spec();
        spec.leftTable = "eco_raw";
        spec.rightTable = "farm_raw";
        spec.leftGeom = "geometry";
        spec.rightGeom = "geometry";
        spec.cellSize = 0.05;
        spec.intersectionArea = true;
        spec.dedup = true;

        Table table = tableEnv.sqlQuery(SpatialJoinSqlBuilder.build(spec));
        DataStream<Row> out = TableUtil.tableToDataStream(tableEnv, table, false);

        Map<String, Double> pairs = new HashMap<String, Double>();
        Iterator<Row> it = DataStreamUtils.collect(out);
        while (it.hasNext()) {
            Row row = it.next();
            String pair = row.getField(0) + "-" + row.getField(1);
            pairs.put(pair, ((Number) row.getField(2)).doubleValue());
        }
        assertTrue(pairs.containsKey("eco-farm"));
        assertTrue(pairs.get("eco-farm") > 0);
        assertEquals(1, pairs.size());
    }
}
