import com.alibaba.fastjson.JSONObject;

public class JSONTest {
    public static void main(String[] args) {
        // 解析测试
        JSONObject json = JSONObject.parseObject("aaa");
        // 报错：com.alibaba.fastjson.JSONException
    }
}
