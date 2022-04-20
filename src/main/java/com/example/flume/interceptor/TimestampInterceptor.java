package com.example.flume.interceptor;

import com.alibaba.fastjson.JSONObject;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

/**
 * 时间戳拦截器：用于解决零点漂移问题。
 * 比如 23：59 的业务数据，flume 处理时由于网络延迟关系，导致在落盘 hdfs 时，文件夹名称为次日凌晨了。
 */
public class TimestampInterceptor implements Interceptor {
    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        // 需求：修改 headers 中的时间戳，改为日志数据中的产生时间
        // 提供给 hdfs sink 使用，控制输出文件的文件夹名称
        //  先拿到日志，日志存在 event 的 body 中 （每一条数据过来就是一个 Event）
        byte[] body = event.getBody();
        // 转为 String
        String log = new String(body, StandardCharsets.UTF_8);
        // 此时拿到额 log 一定是一个 JSON 格式的数据，可以解析成 JSON 的，（因为在ETLInterceptor中已经过滤掉非JSON的数据了）
        JSONObject jsonObject = JSONObject.parseObject(log);
        // 根据 json 对象的 key 来拿到对应的 value 值，在我们构造的 log 日志数据中，时间戳的 key 是 ts
        String timestamp = jsonObject.getString("ts");
        // 将 event 中 的 headers 的 timestamp 值进行修改
        Map<String, String> headers = event.getHeaders();
        headers.put("timestamp", timestamp);
        return event;
    }

    /**
     * 真正调用的是 下面这个 intercept 方法
     */
    @Override
    public List<Event> intercept(List<Event> events) {
        for (Event event : events) {
            intercept(event);
        }
        return events;
    }

    @Override
    public void close() {

    }

    /**
     * 静态内部类
     */
    public static class Builder implements Interceptor.Builder{

        @Override
        public Interceptor build() {
            return new TimestampInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
