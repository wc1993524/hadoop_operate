package hbase.base;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;

/**
 * 
 * @author Luxh
 *
 */
public class InitData {
    
    public static void main(String[] args) throws Exception {
        //创建一个word表，只有一个列族content
//        HBaseUtil.createTable("word","content");
//    	HBaseUtil.createTable("ods_common:T_COMMON_GTYHXX","INFO");
        
        //获取word表
//        HTable htable = HBaseUtil.getHTable("word");
    	HTable htable = HBaseUtil.getHTable("ods_common:T_COMMON_GTYHXX");
        htable.setAutoFlush(false);
        
        //创建测试数据
       List<Put> puts = new ArrayList<Put>();
       
       /*Put put1 = HBaseUtil.getPut("1","content",null,"The Apache Hadoop software library is a framework");
       Put put2 = HBaseUtil.getPut("2","content",null,"The common utilities that support the other Hadoop modules");
       Put put3 = HBaseUtil.getPut("3","content",null,"Hadoop by reading the documentation");
       Put put4 = HBaseUtil.getPut("4","content",null,"Hadoop from the release page");
       Put put5 = HBaseUtil.getPut("5","content",null,"Hadoop on the mailing list");*/
       
       Put put1 = HBaseUtil.getPut("5","INFO","CSRQ","2003-11-26");
       Put put2 = HBaseUtil.getPut("5","INFO","XBMC","男");
       Put put3 = HBaseUtil.getPut("5","INFO","NL","12");
       Put put4 = HBaseUtil.getPut("5","INFO","XM","赵六");
       Put put5 = HBaseUtil.getPut("5","INFO","NC","zhaoliu");
       
       puts.add(put1);
       puts.add(put2);
       puts.add(put3);
       puts.add(put4);
       puts.add(put5);
       
       //提交测试数据
      htable.put(puts);
      htable.flushCommits();
      htable.close();
        //创建stat表，只有一个列祖result
//      HBaseUtil.createTable("stat","result");
//      HBaseUtil.createTable("ods_common:TEST","INFO");
    }
}
