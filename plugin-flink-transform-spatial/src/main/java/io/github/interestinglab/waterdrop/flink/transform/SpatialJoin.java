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
 * 强制模板化空间 JOIN（禁止裸 st_intersects 笛卡尔）。
 * <p>
 * 已物化列时跳过展开：
 * <ul>
 *   <li>{@code mode=prepared} 或 {@code use_materialized_bounds=true} → 读 minx/maxx/miny/maxy</li>
 *   <li>{@code mode=prepared_grid} 或 {@code use_materialized_grid=true} → 再读 grid_id，无 LATERAL</li>
 * </ul>
 */
public class SpatialJoin implements FlinkStreamTransform<Row, Row>, FlinkBatchTransform<Row, Row> {

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
        Table table = tableEnvironment.sqlQuery(sql);
        return TableUtil.tableToDataSet(tableEnvironment, table);
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
        CheckResult base = CheckConfigUtil.check(config, "left_table", "right_table", "result_table_name");
        if (!base.isSuccess()) {
            return base;
        }
        String mode = config.hasPath("mode") ? config.getString("mode") : "grid";
        if ("raw".equalsIgnoreCase(mode) || "none".equalsIgnoreCase(mode)) {
            return new CheckResult(false,
                    "mode=raw/none forbidden: use SpatialJoin mode=grid|envelope|prepared|prepared_grid"
                            + " (or use_materialized_bounds / use_materialized_grid)");
        }
        boolean matGrid = boolOrFalse("use_materialized_grid");
        boolean matBounds = boolOrFalse("use_materialized_bounds");
        boolean needsCellSize = "grid".equalsIgnoreCase(mode) && !matGrid && !matBounds
                && !"prepared".equalsIgnoreCase(mode)
                && !"prepared_grid".equalsIgnoreCase(mode)
                && !"envelope".equalsIgnoreCase(mode);
        if (needsCellSize && !config.hasPath("cell_size")) {
            return new CheckResult(false, "please specify [cell_size] when mode=grid without materialized columns");
        }
        return CheckResult.success();
    }

    @Override
    public void prepare(FlinkEnvironment env) {
        SpatialJoinSqlBuilder.Spec spec = new SpatialJoinSqlBuilder.Spec();
        spec.leftTable = config.getString("left_table");
        spec.rightTable = config.getString("right_table");
        if (config.hasPath("left_geom")) {
            spec.leftGeom = config.getString("left_geom");
        }
        if (config.hasPath("right_geom")) {
            spec.rightGeom = config.getString("right_geom");
        }
        if (config.hasPath("left_key")) {
            spec.leftKey = config.getString("left_key");
        }
        if (config.hasPath("right_key")) {
            spec.rightKey = config.getString("right_key");
        }
        if (config.hasPath("cell_size")) {
            spec.cellSize = config.getDouble("cell_size");
        }
        if (config.hasPath("max_cells")) {
            spec.maxCells = config.getInt("max_cells");
        }
        if (config.hasPath("predicate")) {
            spec.predicate = config.getString("predicate");
        }
        if (config.hasPath("intersection_area")) {
            spec.intersectionArea = config.getBoolean("intersection_area");
        }
        if (config.hasPath("dedup")) {
            spec.dedup = config.getBoolean("dedup");
        }
        if (config.hasPath("left_partition_key")) {
            spec.leftPartitionKey = config.getString("left_partition_key");
        }
        if (config.hasPath("right_partition_key")) {
            spec.rightPartitionKey = config.getString("right_partition_key");
        }
        if (config.hasPath("left_coarse_geom")) {
            spec.leftCoarseGeom = config.getString("left_coarse_geom");
        }
        if (config.hasPath("right_coarse_geom")) {
            spec.rightCoarseGeom = config.getString("right_coarse_geom");
        }
        applyBoundColumnOverrides(spec);

        String mode = config.hasPath("mode") ? config.getString("mode") : "grid";
        if ("envelope".equalsIgnoreCase(mode)) {
            spec.mode = SpatialJoinSqlBuilder.Mode.ENVELOPE;
        } else if ("prepared".equalsIgnoreCase(mode)) {
            spec.mode = SpatialJoinSqlBuilder.Mode.PREPARED;
        } else if ("prepared_grid".equalsIgnoreCase(mode)) {
            spec.mode = SpatialJoinSqlBuilder.Mode.PREPARED_GRID;
        } else {
            spec.mode = SpatialJoinSqlBuilder.Mode.GRID;
        }
        // 显式物化标志覆盖 mode（已物化则跳过展开）
        spec.applyMaterializedFlags(boolOrFalse("use_materialized_bounds"), boolOrFalse("use_materialized_grid"));

        this.sql = SpatialJoinSqlBuilder.build(spec);

        SpatialFunctionRegistry.registerJtsKryoTypes(
                env.isStreaming()
                        ? env.getStreamExecutionEnvironment().getConfig()
                        : env.getBatchEnvironment().getConfig());
    }

    private void applyBoundColumnOverrides(SpatialJoinSqlBuilder.Spec spec) {
        if (config.hasPath("minx_col")) {
            spec.minxCol = config.getString("minx_col");
        }
        if (config.hasPath("maxx_col")) {
            spec.maxxCol = config.getString("maxx_col");
        }
        if (config.hasPath("miny_col")) {
            spec.minyCol = config.getString("miny_col");
        }
        if (config.hasPath("maxy_col")) {
            spec.maxyCol = config.getString("maxy_col");
        }
        if (config.hasPath("grid_id_col")) {
            spec.gridIdCol = config.getString("grid_id_col");
        }
    }

    private boolean boolOrFalse(String path) {
        return config.hasPath(path) && config.getBoolean(path);
    }

    public String getGeneratedSql() {
        return sql;
    }
}
