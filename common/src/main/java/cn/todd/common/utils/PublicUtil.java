package cn.todd.common.utils;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;

public class PublicUtil {

	private static ObjectMapper objectMapper = new ObjectMapper();

	static {
	    //允许出现不识别的字段
	    objectMapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		objectMapper.disable(SerializationConfig.Feature.FAIL_ON_EMPTY_BEANS);
    }

	public static <T> T objectToObject(Object params,Class<T> clazz) throws JsonParseException, JsonMappingException, JsonGenerationException, IOException{
		if(params ==null) {return null;}
		return  objectMapper.readValue(objectMapper.writeValueAsBytes(params),clazz);
	}

	public static <T> T mapToObject(Map<String,Object> params,Class<T> clazz) throws JsonParseException, JsonMappingException, JsonGenerationException, IOException{
		return  objectMapper.readValue(objectMapper.writeValueAsBytes(params),clazz);
	}

	public static <T> T jsonStrToObject(String jsonStr, Class<T> clazz) throws JsonParseException, JsonMappingException, JsonGenerationException, IOException{
		return  objectMapper.readValue(jsonStr, clazz);
	}


	@SuppressWarnings("unchecked")
	public static Map<String,Object> objectToMap(Object obj) throws JsonParseException, JsonMappingException, JsonGenerationException, IOException{

		return objectMapper.readValue(objectMapper.writeValueAsBytes(obj), Map.class);
	}

	public static String objToString(Object obj) throws IOException {
		return objectMapper.writeValueAsString(obj);
	}


	public static boolean count(int index,int multiples){
		return index%multiples==0;
	}

	public static Object classConvter(Class<?> clazz, Object obj){
		if(clazz.equals(Integer.class)||int.class.equals(clazz)){
			obj = Integer.parseInt(obj.toString());
		}else if(clazz.equals(Long.class)|| long.class.equals(clazz)){
			obj = Long.parseLong(obj.toString());
		}else if(clazz.equals(Double.class)|| double.class.equals(clazz)){
			obj = Double.parseDouble(obj.toString());
		}else if(clazz.equals(Float.class)|| float.class.equals(clazz)){
			obj = Float.parseFloat(obj.toString());
		}else if(clazz.equals(Byte.class)|| byte.class.equals(clazz)){
			obj = Byte.parseByte(obj.toString());
		}else if(clazz.equals(Short.class)|| short.class.equals(clazz)){
			obj = Short.parseShort(obj.toString());
		}else if(clazz.equals(Boolean.class)||boolean.class.equals(clazz)){
			obj = Boolean.parseBoolean(obj.toString());
		}else if(clazz.equals(String.class)){
			obj = obj.toString();
		}
		return obj;
	}

	public static Properties stringToProperties(String str) throws IOException{
	   Properties properties = new Properties();
	   properties.load(new ByteArrayInputStream(str.getBytes("UTF-8")));
	   return properties;
	}

	public static boolean isJavaBaseType(Class<?> clazz){
		if(Integer.class.equals(clazz) || int.class.equals(clazz)){
			return true;
		}
		if(Long.class.equals(clazz) || long.class.equals(clazz)){
			return true;
		}
		if(Double.class.equals(clazz) || double.class.equals(clazz)){
			return true;
		}
		if(Float.class.equals(clazz) || float.class.equals(clazz)){
			return true;
		}
		if(Byte.class.equals(clazz) || byte.class.equals(clazz)){
			return true;
		}
		if(Short.class.equals(clazz) || short.class.equals(clazz)){
			return true;
		}
		if(clazz.equals(Boolean.class)||boolean.class.equals(clazz)){
			return true;
		}
		if(String.class.equals(clazz)){
			return true;
		}
		return false;
	}


	public static Object ClassConvter(Class<?> clazz,Object obj){
		if(obj ==null) {return null;}
		if(clazz.equals(Integer.class)||int.class.equals(clazz)){
			obj = Integer.parseInt(obj.toString());
		}else if(clazz.equals(Long.class)|| long.class.equals(clazz)){
			obj = Long.parseLong(obj.toString());
		}else if(clazz.equals(Double.class)|| double.class.equals(clazz)){
			obj = Double.parseDouble(obj.toString());
		}else if(clazz.equals(Float.class)|| float.class.equals(clazz)){
			obj = Float.parseFloat(obj.toString());
		}else if(clazz.equals(Byte.class)|| byte.class.equals(clazz)){
			obj = Byte.parseByte(obj.toString());
		}else if(clazz.equals(Short.class)|| short.class.equals(clazz)){
			obj = Short.parseShort(obj.toString());
		}else if(clazz.equals(Boolean.class)||boolean.class.equals(clazz)){
			obj = Boolean.parseBoolean(obj.toString());
		}else if(clazz.equals(String.class)){
			obj = obj.toString();
		}
		return obj;
	}
}
