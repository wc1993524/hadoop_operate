package hbase.mr;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 * Created by hasee on 2016/9/8.
 */
public class HDFS2Tab_Hbase {
    public static class TabMapper extends Mapper<LongWritable,Text,ImmutableBytesWritable,Put> {
        ImmutableBytesWritable rowkey = new ImmutableBytesWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] words = value.toString().split("\t");

            Put put = new Put(Bytes.toBytes(words[0]));
            put.add(Bytes.toBytes("info"),Bytes.toBytes("name"),Bytes.toBytes(words[1]));
            put.add(Bytes.toBytes("info"),Bytes.toBytes("age"),Bytes.toBytes(words[2]));

            rowkey.set(Bytes.toBytes(words[0]));
            context.write(rowkey,put);
        }
    }

//    public static class TabReduce extends TableReducer<Text,Put,ImmutableBytesWritable> {
//        @Override
//        protected void reduce(Text key, Iterable<Put> values, Context context) throws IOException, InterruptedException {
//
//        }
//    }

    public static void main(String[] args) throws Exception{
        //Configuration config = HBaseConfiguration.create();
        Configuration config = new Configuration();
        Job job = Job.getInstance(config);

        job.setJarByClass(HDFS2Tab_Hbase.class);
        FileInputFormat.addInputPath(job,new Path(args[0]));

        job.setMapperClass(TabMapper.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);

        TableMapReduceUtil.initTableReducerJob(
                "user",
                null,
                job
        );

        job.setNumReduceTasks(0);

        job.waitForCompletion(true);

    }
}
