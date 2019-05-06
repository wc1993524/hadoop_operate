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
 *  put 'test:test777','rk0001','info:chain','loginID,hehe1,jaja;aaa1,10:00-21:00,ccc1'
	put 'test:test777','rk0002','info:chain','loginID,hehe2,jaja2;aaa2,11:00-22:00,ccc2'
	put 'test:test777','rk0003','info:chain','loginID,hehe3,jaja3;aaa3,12:00-24:00,ccc3'
	put 'test:test777','rk0001','info:chain1','loginID,hehe21,jaja;aaa21,10:00-21:00,ccc21'
	put 'test:test777','rk0002','info:chain1','loginID,hehe22,jaja2;aaa22,11:00-22:00,ccc22'
	put 'test:test777','rk0003','info:chain1','loginID,hehe23,jaja3;aaa23,12:00-24:00,ccc23'
	delete 'test:test777','rk0001','info:login_id'
	delete 'test:test777','rk0002','info:login_id'
	delete 'test:test777','rk0003','info:login_id'
	delete 'test:test777','rk0001','info:login_id1'
	delete 'test:test777','rk0002','info:login_id1'
	delete 'test:test777','rk0003','info:login_id1'
 *
 */
public class urlPattern {
    
    public static class MyMapper extends TableMapper<Text,Text>{
    	private static Text matchText = new Text();
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
            		if(column!=null && FamilyName.equals(column.split("\\:")[0]) && qualifierName.equals(column.split("\\:")[1])){
            			String newColumnName = infoMap.get("newColumnName"+j);
                    	String urlStr = Bytes.toString(CellUtil.cloneValue(cell));
                    	if(urlStr!=null&&urlStr!=""){
//                    		Pattern p=Pattern.compile("(http\\w?):\\/\\/([^/:]+)|\\?[^/:]+");    
//                            Matcher m=p.matcher(urlStr); 
//                            String matchStr = null;
//                            while(m.find()){    
//                            	matchStr = m.group();    
//                            }
                    		String ragex = "(http\\w?):\\/\\/([^/:]+)|\\?[^/:]+";
                    		urlStr = urlStr.replaceAll(ragex, "");
                            matchText.set(urlStr+","+newColumnName);
                			context.write(rowkey,matchText);
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
    		sameOperationColumnSize = Integer.valueOf(conf.get("sameOperationColumnSize"));
    		infoMap = gson.fromJson(infoJsonMap, new TypeToken<Map<String, String>>(){}.getType());  
        }
    }
    
    public static class MyReducer extends TableReducer<Text,Text,ImmutableBytesWritable>{
        @Override
        protected void reduce(Text key, Iterable<Text> values,
                Context context)
                throws IOException, InterruptedException {
            for(Text matchText:values) {
            	Put put = new Put(Bytes.toBytes(key.toString()));
            	String[] arry = String.valueOf(matchText).split(",");
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
        conf.set("sameOperationColumnSize", sameOperationColumnSize);
        Job job = new Job(conf,rowData.get("inputTableName")+"-urlPattern");
        job.setJarByClass(urlPattern.class);
        
        Scan scan = new Scan();
        scan.setCaching(500);
        scan.setCacheBlocks(false);
        TableMapReduceUtil.initTableMapperJob(rowData.get("inputTableName"), scan, MyMapper.class, Text.class, Text.class, job);
        TableMapReduceUtil.initTableReducerJob(rowData.get("ouputTableName"), MyReducer.class, job);
        job.waitForCompletion(true);
    }
}
