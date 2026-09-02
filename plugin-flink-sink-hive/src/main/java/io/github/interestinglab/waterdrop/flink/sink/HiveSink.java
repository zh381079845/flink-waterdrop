package io.github.interestinglab.waterdrop.flink.sink;

import io.github.interestinglab.waterdrop.config.Config;
import io.github.interestinglab.waterdrop.common.config.CheckConfigUtil;
import io.github.interestinglab.waterdrop.common.config.CheckResult;
import io.github.interestinglab.waterdrop.flink.FlinkEnvironment;
import io.github.interestinglab.waterdrop.flink.batch.FlinkBatchSink;
import io.github.interestinglab.waterdrop.flink.stream.FlinkStreamSink;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.DataSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sink plugin for writing data to Apache Hive
 */
public class HiveSink implements FlinkStreamSink<Row, Row>, FlinkBatchSink<Row, Row> {

    private static final Logger LOG = LoggerFactory.getLogger(HiveSink.class);

    // 配置项常量
    private static final String HIVE_CONF_DIR = "hive_conf_dir";
    private static final String DATABASE = "database";
    private static final String TABLE = "table";
    private static final String METASTORE_URI = "metastore_uri";
    private static final String PARTITION_FIELDS = "partition_fields";
    private static final String CATALOG_NAME = "catalog_name";
    private static final String PARALLELISM = "parallelism";
    private static final String OVERWRITE = "overwrite";
    private static final String RESULT_TABLE_NAME = "result_table_name";

    private Config config;
    private String catalogName;
    private String hiveConfDir;
    private String metastoreUri;
    private String database;
    private String table;
    private String[] partitionFields;
    private boolean overwrite = false;
    private int parallelism = -1;
    private String resultTableName;

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
        return CheckConfigUtil.check(config, HIVE_CONF_DIR, DATABASE, TABLE, METASTORE_URI);
    }

    @Override
    public void prepare(FlinkEnvironment env) {
        // 读取配置参数
        hiveConfDir = config.getString(HIVE_CONF_DIR);
        database = config.getString(DATABASE);
        table = config.getString(TABLE);
        metastoreUri = config.getString(METASTORE_URI);
        
        if (config.hasPath(CATALOG_NAME)) {
            catalogName = config.getString(CATALOG_NAME);
        } else {
            catalogName = "hive_catalog";
        }
        
        if (config.hasPath(PARTITION_FIELDS)) {
            String partitionFieldsStr = config.getString(PARTITION_FIELDS);
            partitionFields = partitionFieldsStr.split(",");
        }
        
        if (config.hasPath(OVERWRITE)) {
            overwrite = config.getBoolean(OVERWRITE);
        }
        
        if (config.hasPath(PARALLELISM)) {
            parallelism = config.getInt(PARALLELISM);
        }
        
        if (config.hasPath(RESULT_TABLE_NAME)) {
            resultTableName = config.getString(RESULT_TABLE_NAME);
        } else {
            resultTableName = "hive_sink_table_" + System.currentTimeMillis();
        }
        
        LOG.info("HiveSink prepared with database: {}, table: {}", database, table);
    }

    /**
     * 注册Hive Catalog到TableEnvironment
     */
    private void registerHiveCatalog(TableEnvironment tableEnv) {
        try {
            // 创建HiveCatalog
            HiveCatalog hiveCatalog = new HiveCatalog(
                    catalogName,
                    database,
                    hiveConfDir,
                    metastoreUri);
            
            // 注册Catalog
            tableEnv.registerCatalog(catalogName, hiveCatalog);
            
            // 设置当前Catalog和数据库
            tableEnv.useCatalog(catalogName);
            tableEnv.useDatabase(database);
            
            LOG.info("Successfully registered Hive catalog: {}", catalogName);
        } catch (Exception e) {
            LOG.error("Failed to register Hive catalog", e);
            throw new RuntimeException("Failed to register Hive catalog", e);
        }
    }

    @Override
    public DataStreamSink<Row> outputStream(FlinkEnvironment env, DataStream<Row> dataStream) {
        // 获取TableEnvironment
        StreamTableEnvironment tableEnv = env.getStreamTableEnvironment();
        
        // 注册Hive Catalog
        registerHiveCatalog(tableEnv);
        
        // 将DataStream转换为表并注册
        tableEnv.createTemporaryView(resultTableName, dataStream);
        
        // 构建SQL插入语句
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("INSERT ");
        if (overwrite) {
            sqlBuilder.append("OVERWRITE ");
        } else {
            sqlBuilder.append("INTO ");
        }
        
        sqlBuilder.append(catalogName).append(".").append(database).append(".").append(table);
        
        // 处理分区
        if (partitionFields != null && partitionFields.length > 0) {
            sqlBuilder.append(" PARTITION (");
            for (int i = 0; i < partitionFields.length; i++) {
                if (i > 0) {
                    sqlBuilder.append(", ");
                }
                sqlBuilder.append(partitionFields[i]);
            }
            sqlBuilder.append(")");
        }
        
        sqlBuilder.append(" SELECT * FROM ").append(resultTableName);
        
        String sql = sqlBuilder.toString();
        LOG.info("Executing SQL: {}", sql);
        
        // 执行SQL插入
        try {
            tableEnv.executeSql(sql);
        } catch (Exception e) {
            LOG.error("Failed to insert data into Hive table: {}.{}", database, table, e);
            throw new RuntimeException("Failed to insert data into Hive", e);
        }
        
        // 返回一个打印流作为占位，实际写入已通过SQL完成
        if (parallelism > 0) {
            return dataStream.print().setParallelism(parallelism);
        } else {
            return dataStream.print();
        }
    }

    @Override
    public DataSink<Row> outputBatch(FlinkEnvironment env, DataSet<Row> dataSet) {
        // 获取TableEnvironment
        BatchTableEnvironment tableEnv = env.getBatchTableEnvironment();
        
        // 注册Hive Catalog
        registerHiveCatalog(tableEnv);
        
        // 将DataSet转换为表并注册
        Table dataTable = tableEnv.fromDataSet(dataSet);
        tableEnv.registerTable(resultTableName, dataTable);
        
        // 构建SQL插入语句
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("INSERT ");
        if (overwrite) {
            sqlBuilder.append("OVERWRITE ");
        } else {
            sqlBuilder.append("INTO ");
        }
        
        sqlBuilder.append(catalogName).append(".").append(database).append(".").append(table);
        
        // 处理分区
        if (partitionFields != null && partitionFields.length > 0) {
            sqlBuilder.append(" PARTITION (");
            for (int i = 0; i < partitionFields.length; i++) {
                if (i > 0) {
                    sqlBuilder.append(", ");
                }
                sqlBuilder.append(partitionFields[i]);
            }
            sqlBuilder.append(")");
        }
        
        sqlBuilder.append(" SELECT * FROM ").append(resultTableName);
        
        String sql = sqlBuilder.toString();
        LOG.info("Executing SQL: {}", sql);
        
        // 执行SQL插入
        try {
            tableEnv.sqlUpdate(sql);
        } catch (Exception e) {
            LOG.error("Failed to insert data into Hive table: {}.{}", database, table, e);
            throw new RuntimeException("Failed to insert data into Hive", e);
        }
        
        // 返回一个空的DataSink，实际写入通过Table API完成
        return dataSet.output(new DummyOutputFormat<>());
    }
    
    /**
     * 用于丢弃输出的OutputFormat
     */
    private static class DummyOutputFormat<T> implements org.apache.flink.api.common.io.OutputFormat<T> {
        private static final long serialVersionUID = 1L;

        @Override
        public void configure(org.apache.flink.configuration.Configuration parameters) {}

        @Override
        public void open(int taskNumber, int numTasks) {}

        @Override
        public void writeRecord(T record) {}

        @Override
        public void close() {}
    }
} 