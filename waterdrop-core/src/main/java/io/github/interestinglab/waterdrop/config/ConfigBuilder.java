package io.github.interestinglab.waterdrop.config;

import io.github.interestinglab.waterdrop.common.config.ConfigRuntimeException;
import io.github.interestinglab.waterdrop.env.Execution;
import io.github.interestinglab.waterdrop.env.RuntimeEnv;
import io.github.interestinglab.waterdrop.flink.FlinkEnvironment;
import io.github.interestinglab.waterdrop.flink.batch.FlinkBatchExecution;
import io.github.interestinglab.waterdrop.flink.stream.FlinkStreamExecution;
import io.github.interestinglab.waterdrop.plugin.Plugin;
import io.github.interestinglab.waterdrop.utils.Engine;
import io.github.interestinglab.waterdrop.utils.PluginType;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;

/*
* 创建ConfigBuilder 环境
* @ConfigPackage 得到引擎 return 基础环境
* */
public class ConfigBuilder {

    private static final String PLUGIN_NAME_KEY = "plugin_name";
    private String configFile;
    private Engine engine;
    private ConfigPackage configPackage;
    private Config config;
    private boolean streaming;
    private Config envConfig;
    private RuntimeEnv env;

    public ConfigBuilder(String configFile, Engine engine) {
        this.configFile = configFile;
        this.engine = engine;
        this.configPackage = new ConfigPackage(engine.getEngine());
        this.config = load();
        this.env = createEnv();
    }

    /*
    * 创建configFile
    *
    * */
    public ConfigBuilder(String configFile) {
        this.configFile = configFile;
        this.engine = Engine.NULL;
        this.config = load();
        this.env = createEnv();
    }


    /*
     * 转换configFile文件
     *
     * */

    private Config load() {

        if (configFile.isEmpty()) {
            throw new ConfigRuntimeException("Please specify config file");
        }

        System.out.println("[INFO] Loading config file: " + configFile);

        // variables substitution / variables resolution order:
        //变量替换和变量解析顺序
        // config file --> system environment --> java properties
        Config config = ConfigFactory
                .parseFile(new File(configFile))
                .resolve(ConfigResolveOptions.defaults().setAllowUnresolved(true))
                .resolveWith(ConfigFactory.systemProperties(),
                        ConfigResolveOptions.defaults().setAllowUnresolved(true));

        ConfigRenderOptions options = ConfigRenderOptions.concise().setFormatted(true);
        System.out.println("[INFO] parsed config file: " + config.root().render(options));
        return config;
    }


    public Config getEnvConfigs() {
        return envConfig;
    }

    public RuntimeEnv getEnv() {
        return env;
    }

    private boolean checkIsStreaming() {
        List<? extends Config> sourceConfigList = config.getConfigList(PluginType.SOURCE.getType());

       return sourceConfigList.get(0).getString(PLUGIN_NAME_KEY).toLowerCase().endsWith("stream");
      //return true;
    }

    /**
     * 通过反射api获得完整限定名称,忽略大小写
     * Get full qualified class name by reflection api, ignore case.
     * 这段代码是 Waterdrop 框架中 ConfigBuilder 类的 buildClassFullQualifier 方法，
     * 用于将配置文件中的插件名称（如 "kafka"）转换为全限定类名（如 io.github.interestinglab.waterdrop.source.KafkaSource）。以下是核心解析：
     *
     *
     * 动态扩展性：通过 SPI 机制，无需修改代码即可添加新插件。
     * 配置友好性：允许用户仅配置插件名称（如 "kafka"），框架自动补全类名。
     * 大小写不敏感匹配：配置中的名称（如 "KAFKA"）可匹配实际类名（如 KafkaSource）。
     **/
    private String buildClassFullQualifier(String name, PluginType classType) throws Exception {

        if (name.split("\\.").length == 1) { // 名称无包名（如 "kafka"）
            String packageName = null; // 目标包名
            Iterable<? extends Plugin> plugins = null; // 该类型的所有插件实现类
            // 根据插件类型获取基础包名和实现类列表
            switch (classType) {
                case SOURCE:
                    packageName = configPackage.sourcePackage();
                    Class baseSource = Class.forName(configPackage.baseSourcePackage());
                    //获取其实现子类
                    plugins = ServiceLoader.load(baseSource);
                    break;
                case TRANSFORM:
                    packageName = configPackage.transformPackage();
                    Class baseTransform = Class.forName(configPackage.baseTransformPackage());
                    plugins = ServiceLoader.load(baseTransform);
                    break;
                case SINK:
                    packageName = configPackage.sinkPackage();
                    Class baseSink = Class.forName(configPackage.baseSinkPackage());
                    plugins = ServiceLoader.load(baseSink);
                    break;
                default:
                    break;
            }

            String qualifierWithPackage = packageName + "." + name;
            for (Plugin plugin : plugins) {
                Class serviceClass = plugin.getClass();
                String serviceClassName = serviceClass.getName();
                String clsNameToLower = serviceClassName.toLowerCase();
                if (clsNameToLower.equals(qualifierWithPackage.toLowerCase())) {
                    return serviceClassName;
                }
            }
            return qualifierWithPackage;
        } else {
            return name;
        }
    }


    /**
     * check if config is valid.
     **/
    public void checkConfig() {
        this.createEnv();
        this.createPlugins(PluginType.SOURCE);
        this.createPlugins(PluginType.TRANSFORM);
        this.createPlugins(PluginType.SINK);
    }

    public <T extends Plugin> List<T> createPlugins(PluginType type) {
        List<T> basePluginList = new ArrayList<>(); // 存储插件实例的列表

        List<? extends Config> configList = config.getConfigList(type.getType()); // 获取该类型的所有配置项（如所有 Source 配置）

        configList.forEach(plugin -> { // 遍历每个插件配置
            try {
                // 1. 获取插件全限定类名（如将 "kafka" 转换为具体类名）
                final String className = buildClassFullQualifier(
                        plugin.getString(PLUGIN_NAME_KEY), // 配置中的插件名称（如 "kafka"）
                        type // 插件类型（SOURCE/TRANSFORM/SINK）
                );

                // 2. 通过反射实例化插件类
                T t = (T) Class.forName(className).newInstance();
                t.setConfig(plugin); // 将配置对象注入插件
                basePluginList.add(t); // 添加到列表
            } catch (Exception e) {
                e.printStackTrace(); // 记录错误但继续处理其他插件
            }
        });

        return basePluginList; // 返回所有实例化的插件对象
    }


    private RuntimeEnv createEnv() {
        envConfig = config.getConfig("env");
        //envConfig = config.getConfig("spark");
        streaming = checkIsStreaming();
        RuntimeEnv env = null;
        switch (engine) {
            case SPARK:
                //todo
                break;
            case FLINK:
                env = new FlinkEnvironment();
                break;
            default:
                break;
        }
        env.setConfig(envConfig);
        env.prepare(streaming);
        return env;
    }

    /*开始执行
    * */
    public Execution createExecution() {
        Execution execution = null;
        switch (engine) {
            case SPARK:
                //todo
                break;
            case FLINK:
                FlinkEnvironment flinkEnvironment = (FlinkEnvironment) env;
                if (streaming) {
                    execution = new FlinkStreamExecution(flinkEnvironment);
                } else {
                    execution = new FlinkBatchExecution(flinkEnvironment);
                }
                break;
            default:
                break;
        }
        return execution;
    }


}
