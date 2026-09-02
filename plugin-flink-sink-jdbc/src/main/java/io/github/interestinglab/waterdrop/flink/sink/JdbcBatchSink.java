package io.github.interestinglab.waterdrop.flink.sink;

import io.github.interestinglab.waterdrop.config.Config;
import io.github.interestinglab.waterdrop.common.config.CheckConfigUtil;
import io.github.interestinglab.waterdrop.flink.FlinkEnvironment;
import io.github.interestinglab.waterdrop.flink.batch.FlinkBatchSink;
import io.github.interestinglab.waterdrop.common.config.CheckResult;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.DataSink;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.utils.JdbcTypeUtil;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;

public class JdbcBatchSink implements FlinkBatchSink<Row, Row> {
    private static final long serialVersionUID = 3677571223952518115L;
    private static final int DEFAULT_BATCH_SIZE = 5000;
    private static final String PARALLELISM = "parallelism";
    private Config config;
    private String driverName;
    private String dbUrl;
    private String username;
    private String password;
    private String query;
    private int batchSize = DEFAULT_BATCH_SIZE;

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
        return CheckConfigUtil.check(config, "driver", "url", "username", "query");
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
    }

    @Override
    public DataSink<Row> outputBatch(FlinkEnvironment env, DataSet<Row> dataSet) {
        // 获取字段类型
        Table table = env.getBatchTableEnvironment().fromDataSet(dataSet);
        TypeInformation<?>[] fieldTypes = table.getSchema().getFieldTypes();
        int[] types = Arrays.stream(fieldTypes).mapToInt(JdbcTypeUtil::typeInformationToSqlType).toArray();
        
        // 创建自定义的 JDBC OutputFormat
        JdbcBatchOutputFormat outputFormat = new JdbcBatchOutputFormat(
            driverName,
            dbUrl,
            username,
            password,
            query,
            types,
            batchSize
        );
        
        // 输出数据
        if (config.hasPath(PARALLELISM)) {
            return dataSet.output(outputFormat).setParallelism(config.getInt(PARALLELISM));
        }
        return dataSet.output(outputFormat);
    }
    
    /**
     * 自定义的 JDBC OutputFormat，用于批处理模式
     */
    private static class JdbcBatchOutputFormat implements OutputFormat<Row> {
        private static final long serialVersionUID = 1L;
        
        private final String driverName;
        private final String dbUrl;
        private final String username;
        private final String password;
        private final String query;
        private final int[] sqlTypes;
        private final int batchSize;
        
        private Connection connection;
        private PreparedStatement statement;
        private int batchCount = 0;
        
        public JdbcBatchOutputFormat(
                String driverName,
                String dbUrl,
                String username,
                String password,
                String query,
                int[] sqlTypes,
                int batchSize) {
            this.driverName = driverName;
            this.dbUrl = dbUrl;
            this.username = username;
            this.password = password;
            this.query = query;
            this.sqlTypes = sqlTypes;
            this.batchSize = batchSize;
        }

        @Override
        public void configure(Configuration parameters) {
            // 配置在 open 方法中处理
        }

        @Override
        public void open(int taskNumber, int numTasks) throws IOException {
            try {
                Class.forName(driverName);
                connection = DriverManager.getConnection(dbUrl, username, password);
                connection.setAutoCommit(false);
                statement = connection.prepareStatement(query);
            } catch (ClassNotFoundException | SQLException e) {
                throw new IOException("无法打开 JDBC 连接", e);
            }
        }

        @Override
        public void writeRecord(Row record) throws IOException {
            try {
                // 设置参数
                for (int i = 0; i < record.getArity(); i++) {
                    Object field = record.getField(i);
                    if (field == null) {
                        statement.setNull(i + 1, sqlTypes[i]);
                    } else {
                        statement.setObject(i + 1, field);
                    }
                }
                
                // 添加到批处理
                statement.addBatch();
                batchCount++;
                
                // 如果达到批处理大小，则执行批处理
                if (batchCount >= batchSize) {
                    statement.executeBatch();
                    connection.commit();
                    batchCount = 0;
                }
            } catch (SQLException e) {
                throw new IOException("写入记录到 JDBC 失败", e);
            }
        }

        @Override
        public void close() throws IOException {
            try {
                // 执行剩余的批处理
                if (batchCount > 0 && statement != null) {
                    statement.executeBatch();
                    connection.commit();
                }
                
                // 关闭资源
                if (statement != null) {
                    statement.close();
                }
                if (connection != null) {
                    connection.close();
                }
            } catch (SQLException e) {
                throw new IOException("关闭 JDBC 连接失败", e);
            }
        }
    }
} 