package io.github.interestinglab.waterdrop.flink.stream;

import io.github.interestinglab.waterdrop.config.Config;
import io.github.interestinglab.waterdrop.env.Execution;
import io.github.interestinglab.waterdrop.flink.FlinkEnvironment;
import io.github.interestinglab.waterdrop.flink.util.TableUtil;
import io.github.interestinglab.waterdrop.common.config.CheckResult;
import io.github.interestinglab.waterdrop.plugin.Plugin;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class FlinkStreamExecution implements Execution<FlinkStreamSource, FlinkStreamTransform, FlinkStreamSink> {

    private Config config;

    private FlinkEnvironment flinkEnvironment;


    public FlinkStreamExecution(FlinkEnvironment streamEnvironment) {
        this.flinkEnvironment = streamEnvironment;
    }

    @Override
    public void start(List<FlinkStreamSource> sources, List<FlinkStreamTransform> transforms, List<FlinkStreamSink> sinks) {
        // 1. 初始化数据流列表
        List<DataStream> data = new ArrayList<>();

        // 2. 处理所有 Source，生成初始数据流
        for (FlinkStreamSource source : sources) {
            DataStream dataStream = source.getStreamData(flinkEnvironment);
            data.add(dataStream);
            registerResultTable(source, dataStream); // 注册临时表
        }

        // 3. 从第一个 Source 的数据流开始作为初始输入
        DataStream input = data.get(0);

        // 4. 处理所有 Transform，串联数据流
        for (FlinkStreamTransform transform : transforms) {
            transform.registerFunction(flinkEnvironment); // 注册自定义函数（如UDF）
            DataStream stream = fromSourceTable(transform); // 尝试从表中获取数据流
            if (stream == null) { // 若未找到表，则使用当前 input
                stream = input;
            }
            input = transform.processStream(flinkEnvironment, stream); // 执行转换
            registerResultTable(transform, input); // 注册转换后的结果表


        }

        // 5. 处理所有 Sink，输出结果
        for (FlinkStreamSink sink : sinks) {
            DataStream stream = fromSourceTable(sink); // 尝试从表中获取数据流
            if (stream == null) { // 若未找到表，则使用最终的 input
                stream = input;
                stream.print(); // 调试输出（可能用于测试）
            }
            sink.outputStream(flinkEnvironment, stream); // 执行输出
        }

        // 6. 启动 Flink 作业
        try {
            flinkEnvironment.getStreamExecutionEnvironment().execute(flinkEnvironment.getJobName());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    private void registerResultTable(Plugin plugin, DataStream dataStream) {
        // 从插件配置中获取配置对象
        Config config = plugin.getConfig();

        // 检查配置中是否包含表名配置项（RESULT_TABLE_NAME）
        if (config.hasPath(RESULT_TABLE_NAME)) {
            // 获取用户指定的表名
            String name = config.getString(RESULT_TABLE_NAME);

            // 获取 Flink 的流式表环境（用于表与流的互操作）
            StreamTableEnvironment tableEnvironment = flinkEnvironment.getStreamTableEnvironment();

            // 检查表是否已存在（避免重复注册）
            if (!TableUtil.tableExists(tableEnvironment, name)) {
                // 若配置中指定了字段名（field_name），则使用该字段名注册表
                if (config.hasPath("field_name")) {
                    String fieldName = config.getString("field_name");
                    // 将 DataStream 注册为临时表，并指定字段名（如：字段名为"value"）
                    tableEnvironment.createTemporaryView(name, dataStream, fieldName);
                } else {
                    // 未指定字段名时，直接注册整个 DataStream 为临时表（默认字段名由类型推断）
                    tableEnvironment.createTemporaryView(name, dataStream);
                }
            }
        }
        // 若未配置表名，则不执行注册（无操作）
    }


    private DataStream fromSourceTable(Plugin plugin) {
        // 从插件配置中获取配置对象
        Config config = plugin.getConfig();
        // 检查配置中是否包含输入表名配置项（SOURCE_TABLE_NAME）
        if (config.hasPath(SOURCE_TABLE_NAME)) {
            // 获取 Flink 的流式表环境（用于表与流的互操作）
            StreamTableEnvironment tableEnvironment = flinkEnvironment.getStreamTableEnvironment();
            // 通过配置中的表名获取对应的 Table 对象
            String tableName = config.getString(SOURCE_TABLE_NAME);
            Table table = tableEnvironment.scan(tableName);
            // 将 Table 转换为 DataStream（使用工具类进行类型转换）
            return TableUtil.tableToDataStream(tableEnvironment, table, true);
        }
        // 若未配置输入表名，则返回 null（表示使用默认输入流）
        return null;
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
        return new CheckResult(true, "");
    }

    @Override
    public void prepare(Void prepareEnv) {
    }
}
