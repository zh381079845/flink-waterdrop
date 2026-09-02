package io.github.interestinglab.waterdrop.common.config;

import io.github.interestinglab.waterdrop.config.Config;

/*
*
* @params
* 对于传入的参数  如果参数为空 返回false
*
* */
public class CheckConfigUtil {

    public static CheckResult check(Config config, String... params) {
        for (String param : params) {
            if (!config.hasPath(param) || config.getAnyRef(param) == null) {
                return new CheckResult(false, "please specify [" + param + "] as non-empty");
            }
        }
        return new CheckResult(true,"");
    }
}
