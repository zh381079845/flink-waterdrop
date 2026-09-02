package io.github.interestinglab.waterdrop.flink.transform;

import io.github.interestinglab.waterdrop.common.config.CheckConfigUtil;
import io.github.interestinglab.waterdrop.common.config.CheckResult;
import io.github.interestinglab.waterdrop.config.Config;
import io.github.interestinglab.waterdrop.flink.FlinkEnvironment;
import io.github.interestinglab.waterdrop.flink.batch.FlinkBatchTransform;
import io.github.interestinglab.waterdrop.flink.stream.FlinkStreamTransform;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.locationtech.jts.geom.Geometry;

import java.util.List;

/**
 * 小维表广播 + {@link LocalStrTreeIndex} 探测大表（红线 × 海量点）。
 *
 * <pre>
 * SpatialBroadcastJoin {
 *   build_table = "eco_raw"      # 小表
 *   probe_table = "events"       # 大表
 *   build_key = "id"
 *   probe_key = "id"
 *   build_geom = "geometry"
 *   probe_geom = "geometry"
 *   result_table_name = "hits"
 * }
 * </pre>
 *
 * 热路径也可直接调用 {@link #buildIndex(Iterable)} / {@link LocalStrTreeIndex#queryIntersects}
 * （见微基准）。
 */
public class SpatialBroadcastJoin implements FlinkStreamTransform<Row, Row>, FlinkBatchTransform<Row, Row> {

    static final MapStateDescriptor<String, String> BUILD_DESC =
            new MapStateDescriptor<String, String>(
                    "spatial-build-wkt",
                    BasicTypeInfo.STRING_TYPE_INFO,
                    BasicTypeInfo.STRING_TYPE_INFO);

    private Config config;
    private String buildTable;
    private String probeTable;
    private String buildKey = "id";
    private String probeKey = "id";
    private String buildGeom = "geometry";
    private String probeGeom = "geometry";

    /**
     * 小表建 STRtree（与算子广播侧语义一致）。entry: id → WKT。
     */
    public static LocalStrTreeIndex buildIndex(Iterable<? extends java.util.Map.Entry<String, String>> idToWkt) {
        LocalStrTreeIndex index = new LocalStrTreeIndex();
        for (java.util.Map.Entry<String, String> e : idToWkt) {
            Geometry g = WktUtils.read(e.getValue());
            if (g != null) {
                index.insert(e.getKey(), g);
            }
        }
        index.build();
        return index;
    }

    /**
     * 对单条探测几何做 intersects 命中，返回 build_id 列表。
     */
    public static List<String> probeHits(LocalStrTreeIndex index, String probeWkt) {
        Geometry probeGeom = WktUtils.read(probeWkt);
        if (probeGeom == null || index == null) {
            return java.util.Collections.emptyList();
        }
        List<LocalStrTreeIndex.Entry> hits = index.queryIntersects(probeGeom);
        List<String> ids = new java.util.ArrayList<String>(hits.size());
        for (LocalStrTreeIndex.Entry e : hits) {
            ids.add(e.id);
        }
        return ids;
    }

    @Override
    public DataStream<Row> processStream(FlinkEnvironment env, DataStream<Row> dataStream) {
        StreamTableEnvironment tableEnv = env.getStreamTableEnvironment();
        SpatialFunctionRegistry.registerAll(tableEnv);

        RowTypeInfo pairType = new RowTypeInfo(
                new TypeInformation[]{BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO},
                new String[]{"id", "geometry"});

        Table buildT = tableEnv.sqlQuery(
                "SELECT `" + buildKey + "` AS id, `" + buildGeom + "` AS geometry FROM `" + buildTable + "`");
        Table probeT = tableEnv.sqlQuery(
                "SELECT `" + probeKey + "` AS id, `" + probeGeom + "` AS geometry FROM `" + probeTable + "`");

        DataStream<Row> buildStream = tableEnv.toAppendStream(buildT, pairType);
        DataStream<Row> probeStream = tableEnv.toAppendStream(probeT, pairType);

        BroadcastStream<Row> broadcast = buildStream.broadcast(BUILD_DESC);
        return probeStream
                .connect(broadcast)
                .process(new StrTreeBroadcastProbe())
                .returns(new RowTypeInfo(
                        new TypeInformation[]{BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO},
                        new String[]{"probe_id", "build_id"}));
    }

    @Override
    public DataSet<Row> processBatch(FlinkEnvironment env, DataSet<Row> data) {
        throw new UnsupportedOperationException(
                "SpatialBroadcastJoin is stream-oriented; use SpatialJoin for batch SQL path");
    }

    @Override
    public void setConfig(Config config) {
        this.config = config;
    }

    @Override
    public Config getConfig() {
        return config;
    }

    @Override
    public CheckResult checkConfig() {
        return CheckConfigUtil.check(config, "build_table", "probe_table", "result_table_name");
    }

    @Override
    public void prepare(FlinkEnvironment env) {
        buildTable = config.getString("build_table");
        probeTable = config.getString("probe_table");
        if (config.hasPath("build_key")) {
            buildKey = config.getString("build_key");
        }
        if (config.hasPath("probe_key")) {
            probeKey = config.getString("probe_key");
        }
        if (config.hasPath("build_geom")) {
            buildGeom = config.getString("build_geom");
        }
        if (config.hasPath("probe_geom")) {
            probeGeom = config.getString("probe_geom");
        }
        SpatialFunctionRegistry.registerJtsKryoTypes(env.getStreamExecutionEnvironment().getConfig());
    }

    /**
     * 广播端累积 WKT；探测端在维表到达前缓冲，避免空索引漏检。
     */
    public static class StrTreeBroadcastProbe extends BroadcastProcessFunction<Row, Row, Row> {

        private static final long serialVersionUID = 1L;
        private transient LocalStrTreeIndex index;
        private transient boolean indexBuilt;
        private transient java.util.ArrayList<Row> pendingProbes;

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) {
            pendingProbes = new java.util.ArrayList<Row>();
        }

        @Override
        public void processElement(Row probe, ReadOnlyContext ctx, Collector<Row> out) throws Exception {
            ReadOnlyBroadcastState<String, String> state = ctx.getBroadcastState(BUILD_DESC);
            if (!state.immutableEntries().iterator().hasNext()) {
                pendingProbes.add(probe);
                return;
            }
            ensureIndex(state.immutableEntries());
            emitHits(probe, out);
        }

        @Override
        public void processBroadcastElement(Row build, Context ctx, Collector<Row> out) throws Exception {
            BroadcastState<String, String> state = ctx.getBroadcastState(BUILD_DESC);
            String id = String.valueOf(build.getField(0));
            String wkt = String.valueOf(build.getField(1));
            state.put(id, wkt);
            indexBuilt = false;
            index = null;
            ensureIndex(state.entries());

            if (pendingProbes != null && !pendingProbes.isEmpty()) {
                java.util.ArrayList<Row> batch = new java.util.ArrayList<Row>(pendingProbes);
                pendingProbes.clear();
                for (Row probe : batch) {
                    emitHits(probe, out);
                }
            }
        }

        private void emitHits(Row probe, Collector<Row> out) {
            if (index == null) {
                return;
            }
            String probeId = String.valueOf(probe.getField(0));
            Geometry probeGeom = WktUtils.read(String.valueOf(probe.getField(1)));
            if (probeGeom == null) {
                return;
            }
            List<LocalStrTreeIndex.Entry> hits = index.queryIntersects(probeGeom);
            for (LocalStrTreeIndex.Entry e : hits) {
                out.collect(Row.of(probeId, e.id));
            }
        }

        private void ensureIndex(Iterable<? extends java.util.Map.Entry<String, String>> entries) {
            if (indexBuilt && index != null) {
                return;
            }
            index = SpatialBroadcastJoin.buildIndex(entries);
            indexBuilt = true;
        }
    }
}
