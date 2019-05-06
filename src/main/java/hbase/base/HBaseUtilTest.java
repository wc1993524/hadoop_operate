package hbase.base;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;


public class HBaseUtilTest {
    
    //测试创建表
    @Test
    public void testCreateTable() throws Exception {
        HBaseUtil.createTable("student", "info");
    }
    
    //测试插入数据
    @Test
    public void testInsertData() throws IOException {
        HTable htable = HBaseUtil.getHTable("student");
        List<Put> puts = new ArrayList<Put>();;
        Put put1 = HBaseUtil.getPut("rk0001", "info", "username", "wangwu");
        puts.add(put1);
        Put put2 = HBaseUtil.getPut("rk0001", "info", "password", "123321");
        puts.add(put2);
        htable.setAutoFlush(false);
        htable.put(puts);
        htable.flushCommits();
        htable.close();
    }
    
    //测试获取一行数据
    @Test
    public void testGetRow() throws IOException {
        Result result = HBaseUtil.getResult("student", "rk0001");
        byte[] userName = result.getValue("info".getBytes(), "username".getBytes());
        byte[] password = result.getValue("info".getBytes(), "password".getBytes());
        System.out.println("username is: "+Bytes.toString(userName)+" passwd is :"+Bytes.toString(password));
    }
    
    //测试条件查询
    @Test
    public void testScan() throws IOException {
        ResultScanner rs = HBaseUtil.getResultScanner("student", "info","username","rk0001","rk0003");
        for(Result r : rs) {
            System.out.println(Bytes.toString(r.getValue(Bytes.toBytes("info"), Bytes.toBytes("username"))));
        }
    }
}
