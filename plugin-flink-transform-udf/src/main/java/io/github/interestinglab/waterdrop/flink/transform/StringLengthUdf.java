package io.github.interestinglab.waterdrop.flink.transform;

import org.apache.flink.table.functions.ScalarFunction;

/**
 * 自定义UDF函数，用于计算字符串长度
 */
public class StringLengthUdf extends ScalarFunction {
    
    private static final long serialVersionUID = 1L;
    
    /**
     * 计算字符串长度的方法
     * @param input 输入字符串
     * @return 字符串长度
     */
    public Integer eval(String input) {
        if (input == null) {
            return 0;
        }
        return input.length();
    }
    
    /**
     * 重载eval方法，支持处理Row类型
     * @param input 输入字符串
     * @param defaultValue 默认值
     * @return 字符串长度，如果为null则返回默认值
     */
    public Integer eval(String input, Integer defaultValue) {
        if (input == null) {
            return defaultValue;
        }
        return input.length();
    }
} 