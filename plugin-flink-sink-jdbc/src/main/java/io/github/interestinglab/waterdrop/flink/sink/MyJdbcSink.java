package io.github.interestinglab.waterdrop.flink.sink;

import io.github.interestinglab.waterdrop.config.Config;
import io.github.interestinglab.waterdrop.common.config.CheckConfigUtil;
import io.github.interestinglab.waterdrop.flink.FlinkEnvironment;
import io.github.interestinglab.waterdrop.flink.batch.FlinkBatchSink;
import io.github.interestinglab.waterdrop.flink.stream.FlinkStreamSink;
import io.github.interestinglab.waterdrop.common.config.CheckResult;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.DataSink;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.jdbc.utils.JdbcTypeUtil;
import org.apache.flink.connector.jdbc.utils.JdbcUtils;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;

import javax.annotation.Nullable;
import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;


public class  MyJdbcSink implements FlinkStreamSink<Row, Row>{
    private static final long serialVersionUID = 3677571223952518115L;
    private static final int DEFAULT_BATCH_SIZE = 5000;
    private static final int DEFAULT_MAX_RETRY_TIMES = 3;
    private static final int DEFAULT_INTERVAL_MILLIS = 0;
    private static final String PARALLELISM = "parallelism";
    private Config config;
    private String driverName;
    private String dbUrl;
    private String username;
    private String password;
    private String query;
    private int batchSize = DEFAULT_BATCH_SIZE;
    private long batchIntervalMs = DEFAULT_INTERVAL_MILLIS;
    private int maxRetries = DEFAULT_MAX_RETRY_TIMES;

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
        return CheckConfigUtil.check(config,"driver","url","username","query");
    }

    @Override
    public void prepare(FlinkEnvironment env) {
        driverName = config.getString("driver");
        dbUrl = config.getString("url");
        username = config.getString("username");
        query = config.getString("query");
        if (config.hasPath("password")) {
            password = config.getString("password");
        }
        if (config.hasPath("batch_size")) {
            batchSize = config.getInt("batch_size");
        }
        if (config.hasPath("batch_interval")) {
            batchIntervalMs = config.getLong("batch_interval");
        }
        if (config.hasPath("batch_max_retries")) {
            maxRetries = config.getInt("batch_max_retries");
        }
    }


    public  <T> SinkFunction<T> getSink(String query) {



        return   JdbcSink.sink(query, new JdbcStatementBuilder<T>() {
            @Override
            public void accept(PreparedStatement preparedStatement, T t) throws SQLException {
                try {
                    //获取所有的属性信息
                    Field[] fields = t.getClass().getDeclaredFields();

                    //遍历字段
                    //
                    for (int i = 0; i < fields.length; i++) {

                        //获取字段
                        Field field = fields[i];

                        //设置私有属性可访问
                        field.setAccessible(true);
                        Object value = field.get(t);

                        preparedStatement.setObject(i + 1, value);

                    }

                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                }

            }
        }, new JdbcExecutionOptions.Builder().withBatchSize(batchSize).build(), new JdbcConnectionOptions.JdbcConnectionOptionsBuilder().withDriverName(driverName).withUrl(dbUrl).withPassword(password).withUsername(username).build());

    }

    @Override
    @Nullable
    public DataStreamSink<Row> outputStream(FlinkEnvironment env, DataStream<Row> dataStream) {
        Table table = env.getStreamTableEnvironment().fromDataStream(dataStream);
        TypeInformation<?>[] fieldTypes = table.getSchema().getFieldTypes();

        int[] types = Arrays.stream(fieldTypes).mapToInt(JdbcTypeUtil::typeInformationToSqlType).toArray();
        SinkFunction<Row> sink = org.apache.flink.connector.jdbc.JdbcSink.sink(
                query,
                (st, row) -> JdbcUtils.setRecordToStatement(st, types, row),
                JdbcExecutionOptions.builder()
                        .withBatchSize(batchSize)
                        .withBatchIntervalMs(batchIntervalMs)
                        .withMaxRetries(maxRetries)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(dbUrl)
                        .withDriverName(driverName)
                        .withUsername(username)
                        .withPassword(password)
                        .build());

        if (config.hasPath(PARALLELISM)) {
            return dataStream.addSink(sink).setParallelism(config.getInt(PARALLELISM));
        }
        return dataStream.addSink(sink);

//        DataStream<Row> rowDataStream = TableUtil.tableToDataStream(tableEnvironment, table, true);
//        rowDataStream.addSink(getSink(query));

    }


}
