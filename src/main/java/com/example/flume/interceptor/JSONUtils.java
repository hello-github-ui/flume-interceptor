package com.example.flume.interceptor;

import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;

/**
 * JSON解析工具类
 */
public class JSONUtils {
    public static boolean isJson(String log) {
        // 设置 JSON 解析成功与否
        boolean flag = false;
        // 需要将 log 字符串，通过 FastJson 库解析为一个 json 对象，我们需要判断采集到的log字符串是否为一个JSON对象
        // 怎么判断呢？这里采用如下的 JSONObject.parseObject() 解析，如果报错，说明解析json失败，表示log字符串不是一个JSON对象；反之是
        try {
            JSONObject jsonObject = JSONObject.parseObject(log);
            flag = true;
        } catch (JSONException e) {
            // 默认就是 false
        }
        return flag;
    }
}
