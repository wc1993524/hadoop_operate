package chain;

import java.io.IOException;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
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


public class ComputeAge {
    
    /**
     * TableMapper<Text,Text>  Text:输出的key类型，Text：输出的value类型
     */
    public static class MyMapper extends TableMapper<Text,Text>{
    	private static Text nowage = new Text();
    	public static final Gson gson = new Gson();
    	private Text rowkey =new Text();
    	private Map<String,String> infoMap = new HashMap<String,String>();
    	private int sameOperationColumnSize = 0;
    	
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
            		if(column!=null && column.split("\\:")[0].equals(FamilyName) && column.split("\\:")[1].equals(qualifierName)){
            			String newColumnName = infoMap.get("newColumnName"+j);
            			String csrq = Bytes.toString(CellUtil.cloneValue(cell));
                    	String born = csrq.substring(0,4);
                        int csyear = Integer.parseInt(born);
                        Calendar ca = Calendar.getInstance();
                        int nowyear = ca.get(Calendar.YEAR);
                        int nownl = nowyear - csyear;
                        if(newColumnName!=null){
                        	nowage.set(String.valueOf(nownl)+","+newColumnName);
                        }else{
                        	nowage.set(String.valueOf(nownl)+","+"INFO:NL");
                        }
        	            context.write(rowkey,nowage);
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
    		sameOperationColumnSize = Integer.valueOf(conf.get("sameOperationColumnSize"));
    		infoMap = gson.fromJson(infoJsonMap, new TypeToken<Map<String, String>>(){}.getType());  
        }
    }
    
    public static class MyReducer extends TableReducer<Text,Text,ImmutableBytesWritable>{
        
        @Override
        protected void reduce(Text key, Iterable<Text> values,
                Context context)
                throws IOException, InterruptedException {
            for(Text nownl:values) {
            	Put put = new Put(Bytes.toBytes(key.toString()));
            	String[] arry = String.valueOf(nownl).split(",");
            	put.add(Bytes.toBytes(arry[1].split("\\:")[0]), Bytes.toBytes(arry[1].split("\\:")[1]), Bytes.toBytes(String.valueOf(arry[0])));
	            context.write(new ImmutableBytesWritable(Bytes.toBytes(key.toString())),put);
            }
        }
    }
    
    public static void Mymain(Map<String, String> rowData) throws IOException, ClassNotFoundException, InterruptedException {
    	String sameOperationColumnSize = rowData.get("sameOperationColumnSize");
    	
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","zk1.nszyzx.china:2181,zk2.nszyzx.china:2181,zk3.nszyzx.china:2181");
        JSONObject jsonObject = JSONObject.fromObject(rowData);  
        conf.set("infoJsonMap", jsonObject.toString());
        conf.set("sameOperationColumnSize", sameOperationColumnSize);
        Job job = new Job(conf,rowData.get("inputTableName")+"-computeAge");
        job.setJarByClass(ComputeAge.class);
        
        
        Scan scan = new Scan();
        scan.setCaching(500);
        scan.setCacheBlocks(false);
        TableMapReduceUtil.initTableMapperJob(rowData.get("inputTableName"), scan, MyMapper.class, Text.class, Text.class, job);
        TableMapReduceUtil.initTableReducerJob(rowData.get("ouputTableName"), MyReducer.class, job);
        System.exit(job.waitForCompletion(true)?0:1);
    }
}
