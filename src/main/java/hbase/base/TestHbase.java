package hbase.base;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IOUtils;

/**
 * Created by hasee on 2016/9/7.
 */
public class TestHbase {
    public static void main(String[] args) throws Exception {
        //createTab();
        //putDate();
    	getDate();
        //testNameSpace();
        //scanDate();
        //testFilter();
    	//deleteColumn();
    }

    public static void testFilter(){
        HTable table = null;
        try{
            table = getTable();
            Scan scan = new Scan();

            Filter filter = null;
            
            filter = new PrefixFilter(Bytes.toBytes("rk"));
            
            filter = new PageFilter(3);

            ByteArrayComparable comp = null;
            comp = new SubstringComparator("lisi");
            filter = new SingleColumnValueFilter(
                    Bytes.toBytes("info"),
                    Bytes.toBytes("name"),
            CompareFilter.CompareOp.EQUAL,
                    comp
            );

            scan.setFilter(filter);
            ResultScanner rs = table.getScanner(scan);
            for (Result result : rs){
                printResult(result);
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            if(table != null){
                IOUtils.closeStream(table);
            }
        }
    }

    public static void scanDate(){
        HTable table = null;
        try {
            table = getTable();

            Scan scan = new Scan();
            scan.setStartRow(Bytes.toBytes("rk0001"));
            scan.setStopRow(Bytes.toBytes("rk0003"));
            scan.addFamily(Bytes.toBytes("info"));

            scan.setCacheBlocks(false);
            scan.setBatch(2);
            scan.setCaching(2);

            ResultScanner rs = table.getScanner(scan);
            for (Result result:rs){
                printResult(result);
            }

        }catch (Exception e){
            e.printStackTrace();
        }finally {
            if (table !=null){
                org.apache.hadoop.io.IOUtils.closeStream(table);
            }
        }
    }

    public static void printResult(Result rs){
        for(Cell cell:rs.rawCells()){
            System.out.println(Bytes.toString(CellUtil.cloneRow(cell))+":"+
                    Bytes.toString(CellUtil.cloneFamily(cell))+":"+
                    Bytes.toString(CellUtil.cloneQualifier(cell))+":"+
                    Bytes.toString(CellUtil.cloneValue(cell)));
        }
    }

    public static void testNameSpace() throws Exception{
        Configuration conf = HBaseConfiguration.create();
        HBaseAdmin admin = new HBaseAdmin(conf);

        NamespaceDescriptor descriptor = NamespaceDescriptor.create("ns1").build();
        admin.createNamespace(descriptor);

        admin.close();
    }

    public static void getDate() throws Exception{
        HTable table = getTable();
        Get get = new Get(Bytes.toBytes("rk0001"));

        Result result = table.get(get);
//        byte[] value = result.getValue(Bytes.toBytes("info"),Bytes.toBytes("name"));
//        System.out.println(new String(value));

        for (Cell cell : result.rawCells()){
              
//            System.out.println(new String(cell.getFamily())+":"+
//                               new String(cell.getQualifier())+":"+
//                               new String(cell.getValue()));
            System.out.println(Bytes.toString(CellUtil.cloneRow(cell)));
            System.out.println(Bytes.toString(CellUtil.cloneFamily(cell)));
            System.out.println(Bytes.toString(CellUtil.cloneQualifier(cell)));
            System.out.println(Bytes.toString(CellUtil.cloneValue(cell)));
        }
    }

    public static void putDate() throws Exception{
        HTable table = getTable();

        Put put = new Put(Bytes.toBytes("rk0001"));
        put.add(Bytes.toBytes("secret"), Bytes.toBytes("name"), Bytes.toBytes("lisi"));
        put.add(Bytes.toBytes("secret"), Bytes.toBytes("age"), Bytes.toBytes("18"));

        table.put(put);
        table.close();
    }

    public static HTable getTable() throws Exception{
        Configuration conf = HBaseConfiguration.create();
        HTable table = new HTable(conf,Bytes.toBytes("t1"));
        return table;
    }

    public static void createTab() throws Exception{
        Configuration conf = HBaseConfiguration.create();
        HBaseAdmin admin = new HBaseAdmin(conf);
        boolean b = admin.tableExists(Bytes.toBytes("t1"));
        if(b){
            admin.disableTable(Bytes.toBytes("t1"));
            admin.deleteTable("t1");
        }
        HTableDescriptor table = new HTableDescriptor(TableName.valueOf("t1"));

        table.addFamily(new HColumnDescriptor(Bytes.toBytes("info")));
        table.addFamily(new HColumnDescriptor(Bytes.toBytes("secret")));

        admin.createTable(table);

        admin.close();
    }
    
    public static void deleteColumn() throws Exception{
    	Configuration conf = HBaseConfiguration.create();
        HTable table = new HTable(conf,Bytes.toBytes("t1"));
        //穿件删除指定的行
        Delete delete = new Delete(Bytes.toBytes("rk0001"));
        delete.addColumn(Bytes.toBytes("secret"),Bytes.toBytes("name"),3);
        //删除操作
        table.delete(delete);
    }
}
