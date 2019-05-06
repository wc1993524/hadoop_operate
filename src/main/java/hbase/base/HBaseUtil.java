package hbase.base;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseUtil {
    
    /**
     * 初始化HBase的配置文件
     * @return
     */
    public static Configuration getConfiguration(){
        Configuration conf = HBaseConfiguration.create();
        //和hbase-site.xml中配置的一致
        conf.set("hbase.zooker.quorum", "localhost:2181");
        return conf;
    }
    
    /**
     * 实例化HBaseAdmin,HBaseAdmin用于对表的元素据进行操作
     * @return
     * @throws MasterNotRunningException
     * @throws ZooKeeperConnectionException
     */
    public static HBaseAdmin getHBaseAdmin() throws Exception{
        return new HBaseAdmin(getConfiguration());
    }
    
    /**
     * 创建表
     * @param tableName            表名
     * @param columnFamilies    列族
     * @throws IOException
     */
    public static void createTable(String tableName,String...columnFamilies) throws Exception {
        HTableDescriptor htd = new HTableDescriptor(tableName.getBytes());//
        for(String fc : columnFamilies) {
            htd.addFamily(new HColumnDescriptor(fc));
        }
        getHBaseAdmin().createTable(htd);
    }
    
    /**
     * 获取HTableDescriptor
     * @param tableName
     * @return
     * @throws IOException
     */
    public static HTableDescriptor getHTableDescriptor(byte[] tableName) throws Exception{
        return getHBaseAdmin().getTableDescriptor(tableName); 
    }
    
    /**
     * 获取表
     * @param tableName 表名
     * @return
     * @throws IOException
     */
    public static HTable getHTable(String tableName) throws IOException{
        return new HTable(getConfiguration(),tableName);
    }
    
    /**
     * 获取Put,Put是插入一行数据的封装格式
     * @param tableName
     * @param row
     * @param columnFamily
     * @param qualifier
     * @param value
     * @return
     * @throws IOException
     */
    public static Put getPut(String row,String columnFamily,String qualifier,String value) throws IOException{
        Put put = new Put(row.getBytes());
        if(qualifier==null||"".equals(qualifier)) {
            put.add(columnFamily.getBytes(), null, value.getBytes());
        }else {
            put.add(columnFamily.getBytes(), qualifier.getBytes(), value.getBytes());
        }
        return put;
    }
    
    /**
     * 查询某一行的数据
     * @param tableName    表名
     * @param row        行键
     * @return
     * @throws IOException
     */
    public static Result getResult(String tableName,String row) throws IOException {
        Get get = new Get(row.getBytes());
        HTable htable  = getHTable(tableName);
        Result result = htable.get(get);
        htable.close();
        return result;
        
    }
    
    /**
     * 条件查询
     * @param tableName        表名
     * @param columnFamily    列族
     * @param queryCondition    查询条件值
     * @param begin                查询的起始行
     * @param end                查询的终止行
     * @return
     * @throws IOException
     */
    public static ResultScanner getResultScanner(String tableName,String columnFamily,String queryCondition,String begin,String end) throws IOException{
        
        Scan scan = new Scan();
        //设置起始行
        scan.setStartRow(Bytes.toBytes(begin));
        //设置终止行
        scan.setStopRow(Bytes.toBytes(end));
        
        //指定要查询的列族
        scan.addColumn(Bytes.toBytes(columnFamily),null);
        //查询列族中值等于queryCondition的记录
        Filter filter1 = new SingleColumnValueFilter(Bytes.toBytes(columnFamily),null,CompareOp.EQUAL,Bytes.toBytes(queryCondition));
        //Filter filter2 = new SingleColumnValueFilter(Bytes.toBytes(columnFamily),null,CompareOp.EQUAL,Bytes.toBytes("chuliuxiang"));
        
        FilterList filterList = new FilterList(Operator.MUST_PASS_ONE,Arrays.asList(filter1));
        
        scan.setFilter(filterList);
        HTable htable  = getHTable(tableName);
        
        ResultScanner rs = htable.getScanner(scan);
        htable.close();
        return rs;
    }
    
}