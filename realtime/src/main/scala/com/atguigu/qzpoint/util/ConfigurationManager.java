package com.atguigu.qzpoint.util;

import java.io.InputStream;
import java.util.Properties;

/**
 * @description: 配置文件读取工具类
 * @Author: my.yang
 * @Date: 2019/9/3 4:31 PM
 */
public class ConfigurationManager {

    /**
     * 声明静态对象
     */
    private static Properties prop = new Properties();

    /**
     * 使用静态块进行初始化
     */
    static {
        try {
            InputStream inputStream = ConfigurationManager
                    .class.getClassLoader()
                    .getResourceAsStream("comerce.properties");
            prop.load(inputStream);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取配置项
     *
     * @param key
     * @return
     */
    public static String getProperty(String key) {
        return prop.getProperty(key);
    }

    /**
     * 获取布尔类型的配置项
     *
     * @param key
     * @return
     */
    public static boolean getBoolean(String key) {
        String value = prop.getProperty(key);
        try {
            return Boolean.valueOf(value);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }
}
