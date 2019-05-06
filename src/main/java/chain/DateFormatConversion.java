package chain;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import net.sf.json.JSONObject;

/**
 *  测试样例数据
 *  put 'test:test777','rk0001','info:date','Tue Mar 03 00:00:00 GMT 2015'
    put 'test:test777','rk0002','info:date','Fri Dec 31 00:00:00 GMT 2004'
	put 'test:test777','rk0003','info:date','Fri Dec 31 00:00:00 GMT 2004'
	put 'test:test888','rk0001','info:date','2015-03-03 00:00:00'
	put 'test:test888','rk0002','info:date','2004-12-31 00:00:00'
	put 'test:test888','rk0003','info:date','2004-12-31 00:00:00'
	delete 'test:test777','rk0001','info:newDate'
	delete 'test:test777','rk0002','info:newDate'
	delete 'test:test777','rk0003','info:newDate'
 *
 */
public class DateFormatConversion {
	
	public static class DateFormatConversionMapper extends TableMapper<Text,Text>{
		private static Text formatDate = new Text();
		public static final Gson gson = new Gson();
		private Text rowkey =new Text();
		private HTable table = null;
		private String DateFormatConversionErrorTableName = "DateFormatConversionError";
		private Map<String,String> infoMap = new HashMap<String,String>();
		private int infoColumnJsonMapSize = 0;
		
	    @Override
	    protected void map(ImmutableBytesWritable key, Result value,
	            Context context)
	            throws IOException, InterruptedException {
	    	byte[] bytes = key.get();
	    	rowkey.set(bytes);
	    	for (Cell cell : value.rawCells()){
	    		String FamilyName =  Bytes.toString(CellUtil.cloneFamily(cell));
	            String qualifierName =  Bytes.toString(CellUtil.cloneQualifier(cell));
	        	for(int j=0;j<infoColumnJsonMapSize;j++){
	        		String column = infoMap.get("column"+j);
	        		if(column!=null){
		            	String infoMapFamilyName = column.split("\\:")[0];
		            	String infoMapQualifierName = column.split("\\:")[1];
		            	String infoMapInputTableName = infoMap.get("inputTableName"+j);
		            	String infoMapInputDateFormat = infoMap.get("inputDateFormat"+j);
		            	String infoMapOutputDateFormat = infoMap.get("outputDateFormat"+j);
		            	String newColumnName = infoMap.get("newColumnName"+j);
		            	if(FamilyName!=null && FamilyName.equals(infoMapFamilyName)){
		    	            if(qualifierName!=null && qualifierName.equals(infoMapQualifierName)){
		    	            	String dateValue = Bytes.toString(CellUtil.cloneValue(cell));
		    	            	boolean flag = true;
		    	        		try{
		    	        			SimpleDateFormat inputformatter = new SimpleDateFormat(infoMapInputDateFormat);
		    	        			SimpleDateFormat outputformatter = new SimpleDateFormat(infoMapOutputDateFormat);
		    	        			Date date = inputformatter.parse(dateValue);//判断对应列值是否符合输入时间格式
		    	        			String dateString = outputformatter.format(date);
		    	        			String combinationStr = null;
		    	        			if(newColumnName!=null){
		    	        				combinationStr = dateString+","+newColumnName;
		    	        			}else{
		    	        				combinationStr = dateString+","+column;
		    	        			}
		    	        			formatDate.set(combinationStr);
		    			            context.write(rowkey,formatDate);
		    	        		} catch (ParseException e){
		    	        			flag = false;
		    	        		} finally {
		    	        			if(!flag){
		    	        		        String content = "Table="+infoMapInputTableName+",Rowkey="+rowkey+",Family="+infoMapFamilyName+",Column="+infoMapQualifierName+",Value="+dateValue;  
		    	        		        Date date = new Date();
		    	        		        UUID uuid = UUID.randomUUID();
		    	        		        //把错误信息写入hbase表中
		    	        		        Put put = new Put(Bytes.toBytes(uuid.toString()));
		    	        		        put.add(Bytes.toBytes("info"), Bytes.toBytes("errorMsg"), Bytes.toBytes(content));
		    	        		        put.add(Bytes.toBytes("info"), Bytes.toBytes("date"), Bytes.toBytes(date.toString()));
		    	        		        table.put(put);
		    	        			}
		    	        		}
		    	            }
		                }
	        		}
	        	}
	        }
	    }
	    
	    @Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			super.setup(context);
			Configuration conf = context.getConfiguration();
			String infoJsonMap = conf.get("infoJsonMap");
			infoColumnJsonMapSize = Integer.valueOf(conf.get("infoColumnJsonMapSize"));
			infoMap = gson.fromJson(infoJsonMap, new TypeToken<Map<String, String>>(){}.getType());  
			
	        HBaseAdmin admin = new HBaseAdmin(conf);
	        boolean b = admin.tableExists(Bytes.toBytes(DateFormatConversionErrorTableName));
	        if(!b){
	        	HTableDescriptor table = new HTableDescriptor(TableName.valueOf(DateFormatConversionErrorTableName));
	            table.addFamily(new HColumnDescriptor(Bytes.toBytes("info")));
	            admin.createTable(table);
	            admin.close();
	        }
	        table = new HTable(conf,Bytes.toBytes(DateFormatConversionErrorTableName));
	    }
	    
	    @Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
			super.cleanup(context);
			table.close();
		}
	}
	
	public static class DateFormatConversionReducer extends TableReducer<Text,Text,ImmutableBytesWritable>{
	    @Override
	    protected void reduce(Text key, Iterable<Text> values,
	            Context context)
	            throws IOException, InterruptedException {
	    	for(Text formatDate:values) {
	        	Put put = new Put(Bytes.toBytes(key.toString()));
	        	String[] arry = String.valueOf(formatDate).split(",");
	    		put.add(Bytes.toBytes(arry[1].split("\\:")[0]), Bytes.toBytes(arry[1].split("\\:")[1]), Bytes.toBytes(String.valueOf(arry[0])));
	            context.write(new ImmutableBytesWritable(Bytes.toBytes(key.toString())),put);
	        }
	    }
	}
	
	public static void Mymain(Map<String, String> rowData) throws Exception {
    	String sameOperationColumnSize = rowData.get("sameOperationColumnSize");
        
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","zk1.nszyzx.china:2181,zk2.nszyzx.china:2181,zk3.nszyzx.china:2181");
        JSONObject jsonObject = JSONObject.fromObject(rowData);  
        conf.set("infoJsonMap", jsonObject.toString());
        conf.set("infoColumnJsonMapSize", sameOperationColumnSize);
        Job job = new Job(conf,rowData.get("inputTableName")+"-DateFormat");
        job.setJarByClass(urlPattern.class);
        
        Scan scan = new Scan();
        scan.setCaching(500);
        scan.setCacheBlocks(false);
        TableMapReduceUtil.initTableMapperJob(rowData.get("inputTableName"), scan, DateFormatConversionMapper.class, Text.class, Text.class, job);
        TableMapReduceUtil.initTableReducerJob(rowData.get("ouputTableName"), DateFormatConversionReducer.class, job);
        job.waitForCompletion(true);
    }
}
