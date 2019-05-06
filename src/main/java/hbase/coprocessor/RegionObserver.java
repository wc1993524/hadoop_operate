package hbase.coprocessor;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.util.Bytes;

public class RegionObserver extends BaseRegionObserver{

    private static byte[] fixed_rowkey = Bytes.toBytes("Ivy");

    //preGetOp代替preGet
    /*public void preGetOp(ObserverContext<RegionCoprocessorEnvironment> e,
            Get get, List<Cell> results) throws IOException {
        if (Bytes.equals(get.getRow(), fixed_rowkey)) {
            //行键  列族 列
            Cell cell = new KeyValue(get.getRow(), Bytes.toBytes("time"), 
                    Bytes.toBytes("time"));
            results.add(cell);
        }
    }*/

    public void preGet(ObserverContext<RegionCoprocessorEnvironment> c,
            Get get, List<KeyValue> result) throws IOException {
        if (Bytes.equals(get.getRow(), fixed_rowkey)) 
        {

            //行键  列族 列
            KeyValue kv = new KeyValue(get.getRow(), Bytes.toBytes("time"), 
                        Bytes.toBytes("time"),Bytes.toBytes(System.currentTimeMillis()));
            result.add(kv);
        }
    }
}