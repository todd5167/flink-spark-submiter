package cn.todd.flink.utils;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.parser.ParserConfig;
import com.alibaba.fastjson.serializer.SerializerFeature;

import java.util.List;

/**
 * Date: 2021/10/1
 *
 * @author todd5167
 */
public class JsonUtils {
    private static final SerializerFeature[] features = {
        SerializerFeature.DisableCircularReferenceDetect,
        SerializerFeature.WriteNullListAsEmpty,
        SerializerFeature.WriteNullStringAsEmpty,
        SerializerFeature.SkipTransientField,
        SerializerFeature.WriteDateUseDateFormat,
        SerializerFeature.WriteMapNullValue,
    };

    static {
        ParserConfig.getGlobalInstance().setSafeMode(true);
    }

    public static String toJSONString(Object obj) {
        return JSONObject.toJSONString(obj, features);
    }

    public static JSONObject parseObject(String jsonStr) {
        if (org.apache.commons.lang.StringUtils.isBlank(jsonStr)) {
            return new JSONObject();
        }
        return JSONObject.parseObject(jsonStr);
    }

    public static <T> T parseJson(String json, Class<T> clazz) {
        return JSONObject.parseObject(json, clazz);
    }

    public static <T> List<T> parseJsonList(String json, Class<T> clazz) {
        return JSONObject.parseArray(json, clazz);
    }

    public static boolean isJson(String jsonStr) {
        if (org.apache.commons.lang.StringUtils.isBlank(jsonStr)) {
            return false;
        }
        try {
            JSONObject jsonObj = JSONObject.parseObject(jsonStr);
            if (jsonObj != null) {
                return true;
            }
        } catch (Exception e) {
            // pls ignore
        }
        return false;
    }
}
