package io.github.interestinglab.waterdrop.flink.util;

public class ConfigKeyName {
    public final static String TIME_CHARACTERISTIC = "execution.time-characteristic"; //执行时间特征 例如event-time
    public final static String BUFFER_TIMEOUT_MILLIS = "execution.buffer.timeout"; //执行缓存超时时间
    public final static String PARALLELISM = "execution.parallelism"; //执行并行度
    public final static String MAX_PARALLELISM = "execution.max-parallelism"; //执行最大并行度
    public final static String CHECKPOINT_INTERVAL = "execution.checkpoint.interval"; //checkpoint 时间间隔
    public final static String CHECKPOINT_MODE = "execution.checkpoint.mode"; //如exactly-once(精确一次) 或者at-least-once(至少一次)
    public final static String CHECKPOINT_TIMEOUT = "execution.checkpoint.timeout"; //checkpoint 超时时间
    public final static String CHECKPOINT_DATA_URI = "execution.checkpoint.data-uri"; //远端的filesystem uri(一般是HDFS)
    public final static String MAX_CONCURRENT_CHECKPOINTS = "execution.max-concurrent-checkpoints"; //maxConcurrentCheckpoints用于指定运行中的checkpoint最多可以有多少个
    public final static String CHECKPOINT_CLEANUP_MODE = "execution.checkpoint.cleanup-mode";//用于开启checkpoints的外部持久化 true 或者 false
    public final static String MIN_PAUSE_BETWEEN_CHECKPOINTS = "execution.checkpoint.min-pause"; ////确保检查点之间有至少500 ms的间隔(checkpoint最小间隔)
    public final static String FAIL_ON_CHECKPOINTING_ERRORS = "execution.checkpoint.fail-on-error";//checkpoint失败的次数
    public final static String RESTART_STRATEGY = "execution.restart.strategy";//常用的重启策略 1.固定间隔 (Fixed delay) 2.失败率 (Failure rate) 3.无重启 (No restart)
    public final static String RESTART_ATTEMPTS = "execution.restart.attempts";//尝试重启的次数
    public final static String RESTART_DELAY_BETWEEN_ATTEMPTS = "execution.restart.delayBetweenAttempts";//失败重启时间间隔
    public final static String RESTART_FAILURE_INTERVAL = "execution.restart.failureInterval";//衡量失败率的时间段 如 5分钟
    public final static String RESTART_FAILURE_RATE = "execution.restart.failureRate";// 一个时间段内的最大失败次数 3次
    public final static String RESTART_DELAY_INTERVAL = "execution.restart.delayInterval";//连续两次重启尝试间的间隔 5秒
    public final static String MAX_STATE_RETENTION_TIME = "execution.query.state.max-retention";//状态的最小保留时间minRetentionTime和最大保留时间maxRetentionTime
    public final static String MIN_STATE_RETENTION_TIME = "execution.query.state.min-retention";//
    public final static String STATE_BACKEND = "execution.state.backend";//状态存储方式 MemoryStateBackend 内存 FsStateBackend 文件系统 RocksDBStateBackend TaskManager上的KV数据库RocksDB
    public static final String PLANNER = "execution.planner";

}