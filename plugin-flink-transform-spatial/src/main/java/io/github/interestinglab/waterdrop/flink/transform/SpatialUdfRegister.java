package io.github.interestinglab.waterdrop.flink.transform;

import io.github.interestinglab.waterdrop.config.Config;
import io.github.interestinglab.waterdrop.common.config.CheckResult;
import io.github.interestinglab.waterdrop.flink.FlinkEnvironment;
import io.github.interestinglab.waterdrop.flink.batch.FlinkBatchTransform;
import io.github.interestinglab.waterdrop.flink.stream.FlinkStreamTransform;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;

/**
 * 默认注册全部空间 UDF；process 透传。也可由 {@link SpatialJoin} 自行注册。
 */
public class SpatialUdfRegister implements FlinkStreamTransform<Row, Row>, FlinkBatchTransform<Row, Row> {

    private Config config;
    private static final String BUFFER_UDF_NAME = "buffer_udf_name";
    private String bufferUdfName = "st_buffer";

    @Override
    public DataStream<Row> processStream(FlinkEnvironment env, DataStream<Row> dataStream) {
        return dataStream;
    }

    @Override
    public DataSet<Row> processBatch(FlinkEnvironment env, DataSet<Row> data) {
        return data;
    }

    @Override
    public void registerFunction(FlinkEnvironment flinkEnvironment) {
        TableEnvironment tableEnv = flinkEnvironment.isStreaming()
                ? flinkEnvironment.getStreamTableEnvironment()
                : flinkEnvironment.getBatchTableEnvironment();
        SpatialFunctionRegistry.registerAll(tableEnv, bufferUdfName);
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
        return CheckResult.success();
    }

    @Override
    public void prepare(FlinkEnvironment prepareEnv) {
        if (config != null && config.hasPath(BUFFER_UDF_NAME)) {
            bufferUdfName = config.getString(BUFFER_UDF_NAME);
        }
        ExecutionConfig executionConfig = prepareEnv.isStreaming()
                ? prepareEnv.getStreamExecutionEnvironment().getConfig()
                : prepareEnv.getBatchEnvironment().getConfig();
        SpatialFunctionRegistry.registerJtsKryoTypes(executionConfig);
    }
}
