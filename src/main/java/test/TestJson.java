package test;

import java.io.FileReader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;

import net.sf.json.JSON;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

public class TestJson {
	public static void main(String[] args) {
//		String JsonContext =FileUtil.readTxtFile("src/main/resources/test.json");
//		JSONArray jsonArray = JSONArray.fromObject(JsonContext);
//		int size = jsonArray.size();
//		System.out.println("Size: " + size);
//		for(int  i = 0; i < size; i++){
//			JSONObject jsonObject = jsonArray.getJSONObject(i);
//			System.out.println("[" + i + "]name=" + jsonObject.get("inputTableName"));
//			System.out.println("[" + i + "]package_name=" + jsonObject.get("class"));
//			System.out.println("[" + i + "]check_version=" + jsonObject.get("outputDateFormat"));
//			System.out.println("[" + i + "]check_version=" + jsonObject.get("newColumnName"));
//		}

		
		//		jsonToMap();  
		
		
//		try {
//			JsonParser parser=new JsonParser(); 
//			JsonArray arry=(JsonArray) parser.parse(new FileReader("src/main/resources/test.json"));
//			for(int i=0;i<arry.size();i++){
//                System.out.println("**************");
//                JsonObject subObject=arry.get(i).getAsJsonObject();
//                System.out.println("inputTableName="+subObject.get("inputTableName").getAsString());
//                System.out.println("ouputTableName="+subObject.get("ouputTableName").getAsString());
//                
//                JsonArray arry1=subObject.get("info").getAsJsonArray();    //得到为json的数组
//                for(int j=0;j<arry1.size();j++){
//                    System.out.println("---------------");
//                    JsonObject subObject1=arry1.get(j).getAsJsonObject();
//                    System.out.println("column="+subObject1.get("column").getAsString());
//                    System.out.println("class="+subObject1.get("class").getAsString());
//                    System.out.println("inputDateFormat="+subObject1.get("inputDateFormat").getAsString());
//                    System.out.println("outputDateFormat="+subObject1.get("outputDateFormat").getAsString());
//                    System.out.println("newColumnName="+subObject1.get("newColumnName").getAsString());
//                }
//            }
//			
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
		
//	  try {
//          JsonParser parser=new JsonParser();  //创建JSON解析器
//          JsonObject object=(JsonObject) parser.parse(new FileReader("src/main/resources/test2.json"));  //创建JsonObject对象
//          System.out.println(object.entrySet().iterator().next()); //将json数据转为为String型的数据
//           
//      } catch (Exception e) {
//          e.printStackTrace();
//      }
		
		jsonINfo2();
    }
	
	public static void jsonINfo2(){
		try {
			JsonParser parser=new JsonParser(); 
			JsonArray arry=(JsonArray) parser.parse(new FileReader("src/main/resources/conf/info2.json"));
			for(int i=0;i<arry.size();i++){
                System.out.println("**************");
                JsonObject subObject=arry.get(i).getAsJsonObject();
                System.out.println("inputTableName="+subObject.get("inputTableName").getAsString());
                System.out.println("ouputTableName="+subObject.get("ouputTableName").getAsString());
                
                JsonArray arry1=subObject.get("info").getAsJsonArray();    //得到为json的数组
                for(int j=0;j<arry1.size();j++){
                    System.out.println("---------------");
                    JsonObject subObject1=arry1.get(j).getAsJsonObject();
                    System.out.println("column="+subObject1.get("column").getAsString());
                    JsonArray arry2=subObject1.get("args").getAsJsonArray();  
                    for(int k=0;k<arry2.size();k++){
                    	JsonObject subObject2=arry2.get(k).getAsJsonObject();
                    	System.out.println("type="+subObject2.get("type").getAsString());
                    	if("DateFormatConversion".equals(subObject2.get("type").getAsString())){
                    		 System.out.println("inputDateFormat="+subObject2.get("inputDateFormat"));
                             System.out.println("outputDateFormat="+subObject2.get("outputDateFormat").getAsString());
                             if(subObject2.get("newColumnName")!=null){
                            	 System.out.println("newColumnName="+subObject2.get("newColumnName").getAsString());
                             }
                    	}
                    }
                }
            }
			
        } catch (Exception e) {
            e.printStackTrace();
        }
	}
	
	
	public static final Gson gson = new Gson();
	
	public static JSON mapToJson() {  
        Map<String, String> map = new HashMap<String, String>();  
        map.put( "1", "aaa" );  
        map.put( "2", "bbb" );  
        map.put( "3", "ccc" );  
        map.put( "4", "ddd" );  
        map.put( "5", "eee" );  
        map.put( "6", "fff" );  
        map.put( "7", "ggg" );  
        map.put( "8", "hhh" );  
        JSONObject jsonObject = JSONObject.fromObject(map);  
        return jsonObject;  
   }  

   // json 转 map  
   public static void jsonToMap() {  
       // 得到json  
      JSON json = mapToJson();  
      // 使用谷歌的gson将json转换为map类型    TypeToken<Map<String, String>>()  此格式可以以自己的需求进行调整  
      Map<String, String> mapData = gson.fromJson(json.toString(), new TypeToken<Map<String, String>>(){}.getType());  
      // 循环map  
      for (Entry<String, String> entry : mapData.entrySet()) {  
          System.out.print(entry.getKey() + ":" + entry.getValue() + "\n");  
      }  
      System.out.println(mapData.size());  
   }  
     
}
