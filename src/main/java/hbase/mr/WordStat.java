package hbase.mr;

import java.io.IOException;
import java.util.StringTokenizer;

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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;


/**
 * @author hbase
 * 单词计数，需要统计的单词存在HBase中的word表，MapReduce执行的时候从word表读取数据，统计结束后将结果写入到HBase的stat表中。
 *
 */
public class WordStat {
    
    /**
     * TableMapper<Text,IntWritable>  Text:输出的key类型，IntWritable：输出的value类型
     */
    public static class MyMapper extends TableMapper<Text,IntWritable>{
        
        private static IntWritable one = new IntWritable(1);
        private static Text word = new Text();
        
        @Override
        protected void map(ImmutableBytesWritable key, Result value,
                Context context)
                throws IOException, InterruptedException {
            //表里面只有一个列族，所以我就直接获取每一行的值
            String words = Bytes.toString(value.list().get(0).getValue());
            StringTokenizer st = new StringTokenizer(words); 
            while (st.hasMoreTokens()) {
                 String s = st.nextToken();
                 word.set(s);
                 context.write(word, one);
            }
        }
    }
    
    /**
     * TableReducer<Text,IntWritable>  Text:输入的key类型，IntWritable：输入的value类型，ImmutableBytesWritable：输出类型
     */
    public static class MyReducer extends TableReducer<Text,IntWritable,ImmutableBytesWritable>{
        
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values,
                Context context)
                throws IOException, InterruptedException {
            
            int sum = 0;
            for(IntWritable val:values) {
                sum+=val.get();
            }
            //添加一行记录，每一个单词作为行键
            Put put = new Put(Bytes.toBytes(key.toString()));
            //在列族result中添加一个标识符num,赋值为每个单词出现的次数
            //String.valueOf(sum)先将数字转化为字符串，否则存到数据库后会变成\x00\x00\x00\x这种形式
            //然后再转二进制存到hbase。
            put.add(Bytes.toBytes("result"), Bytes.toBytes("num"), Bytes.toBytes(String.valueOf(sum)));
            context.write(new ImmutableBytesWritable(Bytes.toBytes(key.toString())),put);
        }
    }
    
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        
        Configuration conf = HBaseConfiguration.create();
        Job job = new Job(conf,"wordstat");
        job.setJarByClass(WordStat.class);
        
        
        Scan scan = new Scan();
        //指定要查询的列族
        scan.addColumn(Bytes.toBytes("content"),null);
        //指定Mapper读取的表为word
        TableMapReduceUtil.initTableMapperJob("word", scan, MyMapper.class, Text.class, IntWritable.class, job);
        //指定Reducer写入的表为stat
        TableMapReduceUtil.initTableReducerJob("stat", MyReducer.class, job);
        System.exit(job.waitForCompletion(true)?0:1);
    }
}
