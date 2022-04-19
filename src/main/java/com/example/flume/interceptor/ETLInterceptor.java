package com.example.flume.interceptor;

import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;

/**
 * 1. 实现接口 Interceptor
 * 2. 重写4个方法
 * 3. 写一个静态内部类 Builder 实现接口 builder
 */
public class ETLInterceptor implements Interceptor {
    // 关于flume生命周期中的初始化代码，目前不需要配置
    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        // 在Flume中一行文本内容会被反序列化成一个event,
        // 一个Event中的header是一个Map<String, String>，
        // 而body是一个字节数组byte[]。但是我们实际使用中真正传输的只有body中的数据，而header传输的数据是不会被sink出去的
        // 所以我们需要通过 body 来获取我们通过 flume 采集到的 log 日志信息
        byte[] body = event.getBody();
        // 将 body 转为 字符串
        String log = new String(body, StandardCharsets.UTF_8);
        // 调用 自定义 json 解析工具类
        boolean flag = JSONUtils.isJson(log);
        return flag ? event : null;
    }

    @Override
    public List<Event> intercept(List<Event> events) {
        Iterator<Event> iterator = events.iterator();
        while (iterator.hasNext()) {
            Event next = iterator.next();
            if (intercept(next) == null) {
                // 如果该 event 不是一个 json 对象，则删除掉该数据
                iterator.remove();
            }
        }
        return events;
    }

    // 关于flume生命周期中的初始化代码，目前不需要配置
    @Override
    public void close() {

    }
}
