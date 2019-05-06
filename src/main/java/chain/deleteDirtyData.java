package chain;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper.Context;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import net.sf.json.JSONObject;


public class deleteDirtyData {
    
    public static class MyMapper extends TableMapper<Text,Text>{
    	public static final Gson gson = new Gson();
    	private Text rowkey =new Text();
    	private Map<String,String> infoMap = new HashMap<String,String>();
    	private int sameOperationColumnSize = 0;
    	private String inputTableName = null;
    	private HTable htable = null;
        
    	@Override
        protected void map(ImmutableBytesWritable key, Result value,
                Context context)
                throws IOException, InterruptedException {
        	byte[] bytes = key.get();
            rowkey.set(bytes);
        	for (Cell cell : value.rawCells()){
        		String FamilyName =  Bytes.toString(CellUtil.cloneFamily(cell));
                String qualifierName =  Bytes.toString(CellUtil.cloneQualifier(cell));
                for(int j=0;j<sameOperationColumnSize;j++){
            		String column = infoMap.get("column"+j);
            		if(column!=null && FamilyName.equals(column.split("\\:")[0]) && qualifierName.equals(column.split("\\:")[1])){
                    	String userID = Bytes.toString(CellUtil.cloneValue(cell));
                    	if(userID!=null&&userID!=""){
                    		String regEx = "^[0-9]*$";
                    		Pattern pattern = Pattern.compile(regEx);
                    		Matcher matcher = pattern.matcher(userID);
                    		boolean rs = matcher.matches();
                    		if(!rs){
                    			htable = new HTable(context.getConfiguration(), inputTableName);  
                    			Delete delete = new Delete(key.get());
                    			delete.deleteColumn(CellUtil.cloneFamily(cell), CellUtil.cloneQualifier(cell));
                    			htable.delete(delete);
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
    		inputTableName = conf.get("inputTableName");
    		String infoJsonMap = conf.get("infoJsonMap");
    		sameOperationColumnSize = Integer.valueOf(conf.get("sameOperationColumnSize"));
    		infoMap = gson.fromJson(infoJsonMap, new TypeToken<Map<String, String>>(){}.getType());  
        }
        
        @Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
			super.cleanup(context);
			htable.close();
		}
    }
    
    public static class MyReducer extends TableReducer<Text,Text,ImmutableBytesWritable>{
        @Override
        protected void reduce(Text key, Iterable<Text> values,
                Context context)
                throws IOException, InterruptedException {
        }
    }
    
    public static void Mymain(Map<String, String> rowData) throws Exception {
    	String sameOperationColumnSize = rowData.get("sameOperationColumnSize");
    	String inputTableName = rowData.get("inputTableName");
        
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","zk1.nszyzx.china:2181,zk2.nszyzx.china:2181,zk3.nszyzx.china:2181");
        JSONObject jsonObject = JSONObject.fromObject(rowData);  
        conf.set("infoJsonMap", jsonObject.toString());
        conf.set("sameOperationColumnSize", sameOperationColumnSize);
        conf.set("inputTableName", inputTableName);
        Job job = new Job(conf,inputTableName+"-deleteDirtyData");
        job.setJarByClass(deleteDirtyData.class);
        
        Scan scan = new Scan();
        scan.setCaching(500);
        scan.setCacheBlocks(false);
        TableMapReduceUtil.initTableMapperJob(inputTableName, scan, MyMapper.class, Text.class, Text.class, job);
        TableMapReduceUtil.initTableReducerJob(inputTableName, MyReducer.class, job);
        job.waitForCompletion(true);
    }
}
