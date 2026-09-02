package io.github.interestinglab.waterdrop.flink;

import io.github.interestinglab.waterdrop.config.Config;
import io.github.interestinglab.waterdrop.env.RuntimeEnv;
import io.github.interestinglab.waterdrop.flink.util.ConfigKeyName;
import io.github.interestinglab.waterdrop.flink.util.EnvironmentUtil;
import io.github.interestinglab.waterdrop.common.config.CheckResult;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.TernaryBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class   FlinkEnvironment implements RuntimeEnv {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkEnvironment.class);

    private Config config;

    private StreamExecutionEnvironment environment;

    private StreamTableEnvironment tableEnvironment;

    private ExecutionEnvironment batchEnvironment;

    private BatchTableEnvironment batchTableEnvironment;

    private boolean isStreaming;

    private String jobName = "waterdrop";


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
        return EnvironmentUtil.checkRestartStrategy(config);
    }

    @Override
    public void prepare(Boolean isStreaming) {
        this.isStreaming = isStreaming;
        if (isStreaming) {
            createStreamEnvironment();
            createStreamTableEnvironment();
        } else {
            createExecutionEnvironment();
            createBatchTableEnvironment();
        }
        if (config.hasPath("job.name")) {
            jobName = config.getString("job.name");
        }


//        EnvironmentSettings environmentSettings = envBuilder.build();
//        tableEnvironment =StreamTableEnvironment.create(getStreamExecutionEnvironment(),environmentSettings);
//        environment = StreamExecutionEnvironment.getExecutionEnvironment();
//        tableEnvironment = StreamTableEnvironment.create(environment);

        if (config.hasPath("job.name")){
            jobName = config.getString("job.name");
        }
    }



    public String getJobName() {
        return jobName;
    }

    public boolean isStreaming() {
        return isStreaming;
    }

    public StreamExecutionEnvironment getStreamExecutionEnvironment() {
        return environment;
    }

    public StreamTableEnvironment getStreamTableEnvironment() {
        return tableEnvironment;
    }

    private void createStreamTableEnvironment() {
        // use blink and streammode
        EnvironmentSettings.Builder envBuilder = EnvironmentSettings.newInstance()
                .inStreamingMode();

        if (this.config.hasPath(ConfigKeyName.PLANNER) && "blink".equals(this.config.getString(ConfigKeyName.PLANNER))) {
            envBuilder.useBlinkPlanner();
        } else {
            envBuilder.useOldPlanner();
        }

//        EnvironmentSettings environmentSettings = envBuilder.build();
//        tableEnvironment = StreamTableEnvironment.create(getStreamExecutionEnvironment(), environmentSettings);
//        TableConfig config = tableEnvironment.getConfig();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnvironment = StreamTableEnvironment.create(environment);
        TableConfig config = tableEnvironment.getConfig();
        if (this.config.hasPath(ConfigKeyName.MAX_STATE_RETENTION_TIME) && this.config.hasPath(ConfigKeyName.MIN_STATE_RETENTION_TIME)){
            long max = this.config.getLong(ConfigKeyName.MAX_STATE_RETENTION_TIME);
            long min = this.config.getLong(ConfigKeyName.MIN_STATE_RETENTION_TIME);
            /** 设置状态的最小空闲时间和最大的空闲时间*/
            config.setIdleStateRetentionTime(Time.seconds(min),Time.seconds(max));
        }
    }

    private void createStreamEnvironment() {
        environment = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置摄入时间模式
        setTimeCharacteristic();

        //设置Checkpoint 不同类别
        setCheckpoint();

        //设置失败重启策略
        EnvironmentUtil.setRestartStrategy(config,environment.getConfig());

        //100ms 是 Flink 权衡过的 timeout 默认值，既能保证吞吐，又能保障延迟控制在 100ms 以内
        //触发buffer向Netty Server发送数据 如 TaskManager1 通过Netty Server向 TaskManager2 发送数据
        if (config.hasPath(ConfigKeyName.BUFFER_TIMEOUT_MILLIS)) {
            long timeout = config.getLong(ConfigKeyName.BUFFER_TIMEOUT_MILLIS);
            environment.setBufferTimeout(timeout);
        }

        //设置并行度
        if (config.hasPath(ConfigKeyName.PARALLELISM)) {
            int parallelism = config.getInt(ConfigKeyName.PARALLELISM);
            environment.setParallelism(parallelism);
        }

        //设置最大并行度
        if (config.hasPath(ConfigKeyName.MAX_PARALLELISM)) {
            int max = config.getInt(ConfigKeyName.MAX_PARALLELISM);
            environment.setMaxParallelism(max);
        }
    }


    //获得批处理环境
    public ExecutionEnvironment getBatchEnvironment() {
        return batchEnvironment;
    }

    //BatchTableEnvironment 提供了 DataSet 和 Table 之间相互转换的接口
    public BatchTableEnvironment getBatchTableEnvironment() {
        return batchTableEnvironment;
    }

    //创建环境并设置并行度
    private void createExecutionEnvironment() {
        batchEnvironment = ExecutionEnvironment.getExecutionEnvironment();
        if (config.hasPath(ConfigKeyName.PARALLELISM)) {
            int parallelism = config.getInt(ConfigKeyName.PARALLELISM);
            batchEnvironment.setParallelism(parallelism);
        }
        EnvironmentUtil.setRestartStrategy(config, batchEnvironment.getConfig());
    }

    //创建批处理环境
    private void createBatchTableEnvironment() {
        batchTableEnvironment = BatchTableEnvironment.create(batchEnvironment);
    }

    //设置摄入时间模式
    private void setTimeCharacteristic() {
        if (config.hasPath(ConfigKeyName.TIME_CHARACTERISTIC)) {
            String timeType = config.getString(ConfigKeyName.TIME_CHARACTERISTIC);
            switch (timeType.toLowerCase()) {
                case "event-time":
                    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
                    break;
                case "ingestion-time":
                    environment.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
                    break;
                case "processing-time":
                    environment.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
                    break;
                default:
                    LOG.warn("set time-characteristic failed, unknown time-characteristic [{}],only support event-time,ingestion-time,processing-time", timeType);
                    break;
            }
        }
    }


    //设置Checkpoint 不同类别
    private void setCheckpoint() {
        if (config.hasPath(ConfigKeyName.CHECKPOINT_INTERVAL)) {
            CheckpointConfig checkpointConfig = environment.getCheckpointConfig();
            long interval = config.getLong(ConfigKeyName.CHECKPOINT_INTERVAL);
            environment.enableCheckpointing(interval);

            if (config.hasPath(ConfigKeyName.CHECKPOINT_MODE)) {
                String mode = config.getString(ConfigKeyName.CHECKPOINT_MODE);
                switch (mode.toLowerCase()) {
                    case "exactly-once":
                        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
                        break;
                    case "at-least-once":
                        checkpointConfig.setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);
                        break;
                    default:
                        LOG.warn("set checkpoint.mode failed, unknown checkpoint.mode [{}],only support exactly-once,at-least-once", mode);
                        break;
                }
            }
            //设置超时时间 超时没完成就会被abort掉

            if (config.hasPath(ConfigKeyName.CHECKPOINT_TIMEOUT)) {
                long timeout = config.getLong(ConfigKeyName.CHECKPOINT_TIMEOUT);
                checkpointConfig.setCheckpointTimeout(timeout);
            }

            if (config.hasPath(ConfigKeyName.CHECKPOINT_DATA_URI)) {
                String uri = config.getString(ConfigKeyName.CHECKPOINT_DATA_URI);
                StateBackend fsStateBackend = new FsStateBackend(uri);
                if (config.hasPath(ConfigKeyName.STATE_BACKEND)){
                    String stateBackend = config.getString(ConfigKeyName.STATE_BACKEND);
                    if ("rocksdb".equals(stateBackend.toLowerCase())){
                        StateBackend rocksDBStateBackend = new RocksDBStateBackend(fsStateBackend, TernaryBoolean.TRUE);
                        environment.setStateBackend(rocksDBStateBackend);
                    }
                }else {
                    environment.setStateBackend(fsStateBackend);
                }
            }


            //maxConcurrentCheckpoints用于指定运行中的checkpoint最多可以有多少个
            if (config.hasPath(ConfigKeyName.MAX_CONCURRENT_CHECKPOINTS)) {
                int max = config.getInt(ConfigKeyName.MAX_CONCURRENT_CHECKPOINTS);
                checkpointConfig.setMaxConcurrentCheckpoints(max);
            }

            //enableExternalizedCheckpoints用于开启checkpoints的外部持久化
            //ExternalizedCheckpointCleanup
            if (config.hasPath(ConfigKeyName.CHECKPOINT_CLEANUP_MODE)) {
                boolean cleanup = config.getBoolean(ConfigKeyName.CHECKPOINT_CLEANUP_MODE);
                if (cleanup) {
                    checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);
                } else {
                    checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

                }
            }

            //设置checkpoint 的时间间隔

            if (config.hasPath(ConfigKeyName.MIN_PAUSE_BETWEEN_CHECKPOINTS)) {
                long minPause = config.getLong(ConfigKeyName.MIN_PAUSE_BETWEEN_CHECKPOINTS);
                checkpointConfig.setMinPauseBetweenCheckpoints(minPause);
            }



            //设置setTolerableCheckpointFailureNumber 失败的次数
            if (config.hasPath(ConfigKeyName.FAIL_ON_CHECKPOINTING_ERRORS)) {
                int failNum = config.getInt(ConfigKeyName.FAIL_ON_CHECKPOINTING_ERRORS);
                checkpointConfig.setTolerableCheckpointFailureNumber(failNum);
            }
        }
    }



}
