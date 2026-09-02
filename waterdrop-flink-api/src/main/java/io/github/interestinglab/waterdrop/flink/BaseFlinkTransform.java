package io.github.interestinglab.waterdrop.flink;

import io.github.interestinglab.waterdrop.apis.BaseTransform;


///**
// *{@code FlinkEnvironment}的作用是：
// * 绑定具体类型：将父接口的泛型参数 T 固定为 FlinkEnvironment。
// * 确保类型安全：限制接口及其实现类只能操作 FlinkEnvironment 类型的对象。
// *
// * 1. 继承泛型接口时的类型指定
// * 父接口定义：BaseTransform<T extends RuntimeEnv> 是一个泛型接口，
// * 要求其类型参数 T 必须是 RuntimeEnv 或其子类。
// * 子接口指定类型：BaseFlinkTransform 继承 BaseTransform 时，
// * 通过 <FlinkEnvironment> 明确指定 T 的具体类型为 FlinkEnvironment，即：
// */
public interface BaseFlinkTransform extends BaseTransform<FlinkEnvironment> {

    default void registerFunction(FlinkEnvironment flinkEnvironment){
    }

}
