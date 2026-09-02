package io.github.interestinglab.waterdrop.flink.transform;

import io.github.interestinglab.waterdrop.config.Config;
import io.github.interestinglab.waterdrop.common.config.CheckConfigUtil;
import io.github.interestinglab.waterdrop.flink.FlinkEnvironment;
import io.github.interestinglab.waterdrop.flink.batch.FlinkBatchTransform;
import io.github.interestinglab.waterdrop.flink.stream.FlinkStreamTransform;
import io.github.interestinglab.waterdrop.flink.util.TableUtil;
import io.github.interestinglab.waterdrop.common.config.CheckResult;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;

public class UdfSql implements FlinkStreamTransform<Row, Row>, FlinkBatchTransform<Row, Row> {

    private String sql;

    private Config config;

    private static final String SQL = "sql";
    private static final String UDF_NAME = "udf_name";

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
    public void setConfig(Config config) {
        this.config = config;
    }

    @Override
    public Config getConfig() {
        return config;
    }


    @Override
    public CheckResult checkConfig() {
       return CheckConfigUtil.check(config,SQL);
    }

    @Override
    public void prepare(FlinkEnvironment env) {
        sql = config.getString("sql");
    }


    @Override
    public void registerFunction(FlinkEnvironment flinkEnvironment) {
        String udfName = config.hasPath(UDF_NAME) ? config.getString(UDF_NAME) : "str_len";
        ScalarFunction udf = resolveUdf(udfName);

        TableEnvironment tableEnv = flinkEnvironment.isStreaming()
                ? flinkEnvironment.getStreamTableEnvironment()
                : flinkEnvironment.getBatchTableEnvironment();
        tableEnv.registerFunction(udfName, udf);
    }

    /**
     * UdfSql 只注册字符串类 UDF。空间函数统一走 SpatialUdfRegister。
     */
    private ScalarFunction resolveUdf(String udfName) {
        switch (udfName.toLowerCase()) {
            case "str_len":
            case "string_len":
                return new StringLenUdf();
            default:
                throw new IllegalArgumentException(
                        "Unsupported udf_name '" + udfName
                                + "'. UdfSql only registers str_len. "
                                + "Use SpatialUdfRegister for st_area / st_buffer / st_intersects / st_geomfromwkt.");
        }
    }
}
