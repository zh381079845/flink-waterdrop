package io.github.interestinglab.waterdrop.flink.transform;

import io.github.interestinglab.waterdrop.common.config.CheckConfigUtil;
import io.github.interestinglab.waterdrop.common.config.CheckResult;
import io.github.interestinglab.waterdrop.config.Config;
import io.github.interestinglab.waterdrop.flink.FlinkEnvironment;
import io.github.interestinglab.waterdrop.flink.batch.FlinkBatchTransform;
import io.github.interestinglab.waterdrop.flink.stream.FlinkStreamTransform;
import io.github.interestinglab.waterdrop.flink.util.TableUtil;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * 入库物化 bounds / grid，避免 JOIN 时反复 st_xmin、st_grid_cells。
 *
 * <pre>
 * SpatialMaterialize {
 *   source_table = "eco_raw"   # 或依赖上游；若配置了 source_table 则按表名 SQL
 *   geom = "geometry"
 *   key = "id"
 *   cell_size = 0.05
 *   max_cells = 10000
 *   mode = "bounds"            # bounds | explode_grid
 *   extra_columns = "adcode"
 *   simplify_tolerance = 0.001 # 可选，仅用于算 bounds/grid
 *   result_table_name = "eco_prep"
 * }
 * </pre>
 */
public class SpatialMaterialize implements FlinkStreamTransform<Row, Row>, FlinkBatchTransform<Row, Row> {

    private Config config;
    private String sql;

    @Override
    public DataStream<Row> processStream(FlinkEnvironment env, DataStream<Row> dataStream) {
        StreamTableEnvironment tableEnvironment = env.getStreamTableEnvironment();
        Table table = tableEnvironment.sqlQuery(sql);
        return TableUtil.tableToDataStream(tableEnvironment, table, false);
    }

    @Override
    public DataSet<Row> processBatch(FlinkEnvironment env, DataSet<Row> data) {
        BatchTableEnvironment tableEnvironment = env.getBatchTableEnvironment();
        return TableUtil.tableToDataSet(tableEnvironment, tableEnvironment.sqlQuery(sql));
    }

    @Override
    public void registerFunction(FlinkEnvironment flinkEnvironment) {
        TableEnvironment tableEnv = flinkEnvironment.isStreaming()
                ? flinkEnvironment.getStreamTableEnvironment()
                : flinkEnvironment.getBatchTableEnvironment();
        SpatialFunctionRegistry.registerAll(tableEnv);
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
        return CheckConfigUtil.check(config, "source_table", "result_table_name", "cell_size");
    }

    @Override
    public void prepare(FlinkEnvironment env) {
        SpatialMaterializeSqlBuilder.Spec spec = new SpatialMaterializeSqlBuilder.Spec();
        spec.sourceTable = config.getString("source_table");
        if (config.hasPath("geom")) {
            spec.geomCol = config.getString("geom");
        }
        if (config.hasPath("key")) {
            spec.keyCol = config.getString("key");
        }
        spec.cellSize = config.getDouble("cell_size");
        if (config.hasPath("max_cells")) {
            spec.maxCells = config.getInt("max_cells");
        }
        if (config.hasPath("extra_columns")) {
            spec.extraColumns = config.getString("extra_columns");
        }
        if (config.hasPath("simplify_tolerance")) {
            spec.simplifyTolerance = config.getDouble("simplify_tolerance");
        }
        String mode = config.hasPath("mode") ? config.getString("mode") : "bounds";
        if ("explode_grid".equalsIgnoreCase(mode)) {
            spec.mode = SpatialMaterializeSqlBuilder.Mode.EXPLODE_GRID;
        } else {
            spec.mode = SpatialMaterializeSqlBuilder.Mode.BOUNDS;
        }
        this.sql = SpatialMaterializeSqlBuilder.build(spec);
        SpatialFunctionRegistry.registerJtsKryoTypes(
                env.isStreaming()
                        ? env.getStreamExecutionEnvironment().getConfig()
                        : env.getBatchEnvironment().getConfig());
    }

    public String getGeneratedSql() {
        return sql;
    }
}
