package com.dinglicom.utils;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 * @author ly
 * @Date Create in 14:22 2021/2/21 0021
 * @Description
 */
public class ConfigUtils {
    public static void main(String[] args) {
        Config load = ConfigFactory.load();
        String string = load.getString("service.redis.host");
        System.out.println(string);
    }
}
