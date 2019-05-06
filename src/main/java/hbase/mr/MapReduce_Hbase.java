package hbase.mr;

import java.io.IOException;

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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

/**
 * Created by hasee on 2016/9/8.
 */
public class MapReduce_Hbase {
    public static class TabMapper extends TableMapper<Text,Put>{
        private Text rowkey =new Text();

        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
            byte[] bytes = key.get();
            rowkey.set(bytes);

            Put put = new Put(bytes);

            for(Cell cell:value.rawCells()){
                if("info".equals(CellUtil.cloneFamily(cell))){
                    if("name".equals(CellUtil.cloneQualifier(cell))){
                        put.add(cell);
                    }
                }
            }
        }
    }

    public static class TabReduce extends TableReducer<Text,Put,ImmutableBytesWritable>{
        @Override
        protected void reduce(Text key, Iterable<Put> values, Context context) throws IOException, InterruptedException {
            for(Put put:values){
                context.write(null,put);
            }
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration config = HBaseConfiguration.create();

        Job job = Job.getInstance(config);
        job.setJobName("tab2tab");
        job.setJarByClass(MapReduce_Hbase.class);

        Scan scan = new Scan();
        scan.setCaching(500);
        scan.setCacheBlocks(false);

        TableMapReduceUtil.initTableMapperJob(
                "tab1",
                scan,
                TabMapper.class,
                Text.class,
                Put.class,
                job
        );

        TableMapReduceUtil.initTableReducerJob(
                "tab2",
                TabReduce.class,
                job
        );

        job.setNumReduceTasks(1);

        job.waitForCompletion(true);

    }
}


