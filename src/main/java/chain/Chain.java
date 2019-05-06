package chain;

import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;


/**
 * 通过json配置，组装mapreduce清洗规则
 */
public class Chain {
	public static void main(String [] args) throws Exception {  
		try {
			JsonParser parser=new JsonParser(); 
			JsonArray arry=(JsonArray) parser.parse(new InputStreamReader(Chain.class.getResourceAsStream("/conf/info7.json"), "UTF-8"));
			for(int i=0;i<arry.size();i++){
	            JsonObject subObject=arry.get(i).getAsJsonObject();
	            String inputTableName = subObject.get("inputTableName").getAsString();
	            String ouputTableName = subObject.get("ouputTableName").getAsString();
	            JsonArray arry1=subObject.get("info").getAsJsonArray();    //得到为json的数组
	            Map<String,String> map = new HashMap<String,String>();;//cache dateFormatConversion Info
	    		Map<String,String> map1 = new HashMap<String,String>();;//cache extractLoginId Info
	    		Map<String,String> map2 = new HashMap<String,String>();;//cache urlPattern Info
	    		Map<String,String> map3 = new HashMap<String,String>();;//cache computeFwz Info
	    		Map<String,String> map4 = new HashMap<String,String>();;//cache computeAge Info
	    		Map<String,String> map5 = new HashMap<String,String>();;//cache deleteDirtyData Info
	            for(int j=0;j<arry1.size();j++){
	                JsonObject subObject1=arry1.get(j).getAsJsonObject();
	                String column = subObject1.get("column").getAsString();
	                JsonArray arry2=subObject1.get("args").getAsJsonArray();  
	                for(int k=0;k<arry2.size();k++){
	                	JsonObject subObject2=arry2.get(k).getAsJsonObject();
	                	String type = subObject2.get("type").getAsString();
	                	if("DateFormatConversion".equals(type)){
	                		map.put("column"+j, column);
	                		map.put("inputDateFormat"+j, subObject2.get("inputDateFormat").getAsString());
	                		map.put("outputDateFormat"+j, subObject2.get("outputDateFormat").getAsString());
	                		if(subObject2.get("newColumnName")!=null){
	                			map.put("newColumnName"+j, subObject2.get("newColumnName").getAsString());
	                        }
	                	}else if("extractLoginId".equals(type)){
	                		String newColumnName = subObject2.get("newColumnName").getAsString();
	                		map1.put("column"+j, column);
	                		map1.put("newColumnName"+j, newColumnName);
	                	}else if("urlPattern".equals(type)){
	                		String newColumnName = subObject2.get("newColumnName").getAsString();
	                		map2.put("column"+j, column);
	                		map2.put("newColumnName"+j, newColumnName);
	                	}else if("computeFwz".equals(type)){
	                		String newColumnName = subObject2.get("newColumnName").getAsString();
	                		map3.put("column"+j, column);
	                		map3.put("inputDateFormat"+j, subObject2.get("inputDateFormat").getAsString());
	                		map3.put("newColumnName"+j, newColumnName);
	                		if(subObject2.get("isTimestamp")!=null && subObject2.get("conversionTime")!=null && subObject2.get("isLong")!=null){
	                			map3.put("isTimestamp"+j, subObject2.get("isTimestamp").getAsString());
	                			map3.put("isLong"+j, subObject2.get("isLong").getAsString());
	                			map3.put("conversionTime"+j, subObject2.get("conversionTime").getAsString());
	                        }
	                	}else if("computeAge".equals(type)){
	                		String newColumnName = subObject2.get("newColumnName").getAsString();
	                		map4.put("column"+j, column);
	                		map4.put("newColumnName"+j, newColumnName);
	                	}else if("deleteDirtyData".equals(type)){
	                		map5.put("column"+j, column);
	                	}else{
	                		System.out.println("Class Not Found");
	                	}
	                }
	            }
	            
	            //并行清洗某个表的多个列
	            if(map!=null && map.size()!=0){
	            	System.out.println("========="+inputTableName+"表开始进行字段时间格式转换"+"=========");
	            	map.put("inputTableName", inputTableName);
	            	map.put("ouputTableName", ouputTableName);
	            	map.put("sameOperationColumnSize", arry1.size()+"");
	            	System.out.println(map);
	            	DateFormatConversion.Mymain(map);
	            }
	            if(map1!=null && map1.size()!=0){
	            	System.out.println("========="+inputTableName+"表开始进行loginID字段抽取"+"=========");
	            	map1.put("inputTableName", inputTableName);
            		map1.put("ouputTableName", ouputTableName);
	            	map1.put("sameOperationColumnSize", arry1.size()+"");
	            	System.out.println(map1);
	            	extractLoginId.Mymain(map1);
	            }
	            if(map2!=null && map2.size()!=0){
	            	System.out.println("========="+inputTableName+"表开始进行url字段截取"+"=========");
	            	map2.put("inputTableName", inputTableName);
	            	map2.put("ouputTableName", ouputTableName);
	            	map2.put("sameOperationColumnSize", arry1.size()+"");
	            	System.out.println(map2);
	            	urlPattern.Mymain(map2);
	            }
	            if(map3!=null && map3.size()!=0){
	            	System.out.println("========="+inputTableName+"表开始计算访问周"+"=========");
	            	map3.put("inputTableName", inputTableName);
	            	map3.put("ouputTableName", ouputTableName);
	            	map3.put("sameOperationColumnSize", arry1.size()+"");
	            	System.out.println(map3);
	            	computeFwz.Mymain(map3);
	            }
	            if(map4!=null && map4.size()!=0){
	            	System.out.println("========="+inputTableName+"表开始计算年龄"+"=========");
	            	map4.put("inputTableName", inputTableName);
	            	map4.put("ouputTableName", ouputTableName);
	            	map4.put("sameOperationColumnSize", arry1.size()+"");
	            	System.out.println(map4);
	            	ComputeAge.Mymain(map4);
	            }
	            if(map5!=null && map5.size()!=0){
	            	System.out.println("========="+inputTableName+"表开始清洗不合规数据"+"=========");
	            	map5.put("inputTableName", inputTableName);
	            	map5.put("sameOperationColumnSize", arry1.size()+"");
	            	System.out.println(map5);
	            	deleteDirtyData.Mymain(map5);
	            }
	        }
			
	    } catch (Exception e) {
	        e.printStackTrace();
	    }
    }
}
