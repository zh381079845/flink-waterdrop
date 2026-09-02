package io.github.interestinglab.waterdrop.apis;


import io.github.interestinglab.waterdrop.env.RuntimeEnv;
import io.github.interestinglab.waterdrop.plugin.Plugin;

/**
 设计目的
 类型安全：确保 BaseTransform 的实现类只能使用 RuntimeEnv 或其子类作为类型参数。
 灵活性：允许不同实现类根据需要指定具体的 RuntimeEnv 子类（例如 FlinkRuntimeEnv），
 从而支持不同运行时环境的适配。
 代码复用：通过泛型统一接口定义，避免硬编码具体类型
 *
 * */
public interface BaseTransform<T extends RuntimeEnv> extends Plugin<T> {

}
