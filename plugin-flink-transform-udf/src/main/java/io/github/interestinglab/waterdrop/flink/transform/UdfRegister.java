package io.github.interestinglab.waterdrop.flink.transform;

import io.github.interestinglab.waterdrop.config.Config;
import io.github.interestinglab.waterdrop.common.config.CheckConfigUtil;
import io.github.interestinglab.waterdrop.common.config.CheckResult;
import io.github.interestinglab.waterdrop.flink.FlinkEnvironment;
import io.github.interestinglab.waterdrop.flink.batch.FlinkBatchTransform;
import io.github.interestinglab.waterdrop.flink.stream.FlinkStreamTransform;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

/**
 * UDF注册类，用于注册自定义UDF函数
 */
public class UdfRegister implements FlinkStreamTransform<Row, Row>, FlinkBatchTransform<Row, Row> {

    private Config config;
    private static final String UDF_NAME = "udf_name";

    @Override
    public DataStream<Row> processStream(FlinkEnvironment env, DataStream<Row> dataStream) {
        // 不改变数据流，只注册UDF
        return dataStream;
    }

    @Override
    public DataSet<Row> processBatch(FlinkEnvironment env, DataSet<Row> data) {
        // 不改变数据集，只注册UDF
        return data;
    }

    @Override
    public void registerFunction(FlinkEnvironment flinkEnvironment) {
        // 获取用户配置的UDF函数名，默认为"str_len"
        String udfName = config.hasPath(UDF_NAME) ? config.getString(UDF_NAME) : "str_len";
        
        // 根据环境类型注册UDF
        if (flinkEnvironment.isStreaming()) {
            flinkEnvironment
                .getStreamTableEnvironment()
                .registerFunction(udfName, new StringLengthUdf());
        } else {
            flinkEnvironment
                .getBatchTableEnvironment()
                .registerFunction(udfName, new StringLengthUdf());
        }
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
        // UDF名称是可选的，没有必须检查的配置项
        return CheckResult.success();

    }

    @Override
    public void prepare(FlinkEnvironment prepareEnv) {
        // 不需要特别的准备工作
    }
} 