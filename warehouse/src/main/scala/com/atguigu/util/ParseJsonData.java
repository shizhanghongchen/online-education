package com.atguigu.util;

import com.alibaba.fastjson.JSONObject;

/**
 * @description: JsonObject转换工具
 * @Author: my.yang
 * @Date: 2019/8/30 7:30 PM
 */
public class ParseJsonData {
    public static JSONObject getJsonData(String data) {
        try {
            return JSONObject.parseObject(data);
        } catch (Exception e) {
            return null;
        }
    }
}
