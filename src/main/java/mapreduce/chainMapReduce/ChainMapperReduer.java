package mapreduce.chainMapReduce;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * 原文地址：https://blog.csdn.net/HANLIPENGHANLIPENG/article/details/52786819
 * hadoop jar hadoop-study.jar mapreduce.chainMapReduce.ChainMapperReduer /hadoop-study/mapreduce/chain /hadoop-study/mapreduce/chain/output
 * @author hadoop
 *
 */
public class ChainMapperReduer {
    /**
     * map1 完成money>10000的过滤
     * @author 韩利鹏
     *
     */
    public static class CMRMap1 extends Mapper<LongWritable, Text, Text, Text>{
        private Text k = new Text();
        private Text v = new Text();
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            String[] strs = value.toString().split(" ");
            int money = Integer.parseInt(strs[1]);
            if(money>10000){
                return;
            }
            k.set(strs[0]);
            v.set(strs[1]);
            context.write(k, v);
        }
    }

    /**
     * map2 money>100的过滤
     * @author 韩利鹏
     *
     */
    public static class CMRMap2 extends Mapper<Text, Text, Text, Text>{
        @Override
        protected void map(Text key, Text value, Mapper<Text, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            int money = Integer.parseInt(value.toString());
            if(money>100){
                return;
            }
            context.write(key, value);
        }
    }
    /**
     * reduce的数据合并
     * @author 韩利鹏
     *
     */
    public static class CMRReduce extends Reducer<Text, Text, Text, Text>{
        private Text v = new Text();
        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for(Text text:values){
                sum+=Integer.parseInt(text.toString());
            }
            v.set(sum+"");
            context.write(key, v);  
        }
    }
    /**
     * map3过滤字长
     * @author 韩利鹏
     *
     */
    public static class CMRMap3 extends Mapper<Text, Text, Text, Text>{
        @Override
        protected void map(Text key, Text value, Mapper<Text, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            if(key.toString().trim().length()>2){
                return;
            }
            context.write(key, value);
        }
    }

    /**
     * main函数启动job
     * @param args
     * @throws Exception 
     */
    public static void main(String[] args) throws Exception {
        Configuration config=new Configuration();
        String[] otherargs = new GenericOptionsParser(config, args).getRemainingArgs();
		if (otherargs.length != 2) {
			System.err.println("Usage ChainMapperReduer <InputPath> <OutPath>");
			System.exit(2);
		}
		
		// 创建文件系统
		FileSystem fileSystem = FileSystem.get(new URI(otherargs[1]), config);
		// 判断输出路径是否存在，如果存在则删除
		if (fileSystem.exists(new Path(otherargs[1]))) {
			fileSystem.delete(new Path(otherargs[1]), true);
		}
		
        Job job=Job.getInstance(config);
        //设置主类
        job.setJarByClass(ChainMapperReduer.class);

        Configuration map1=new Configuration(false);
        ChainMapper.addMapper(job, CMRMap1.class, LongWritable.class, Text.class, Text.class, Text.class, map1);

        Configuration map2=new Configuration(false);
        ChainMapper.addMapper(job, CMRMap2.class, Text.class, Text.class, Text.class, Text.class, map2);

        Configuration reduce1=new Configuration(false);
        ChainReducer.setReducer(job, CMRReduce.class, Text.class, Text.class, Text.class, Text.class, reduce1);

        Configuration map3=new Configuration(true);
        ChainMapper.addMapper(job, CMRMap3.class, Text.class, Text.class, Text.class, Text.class, map3);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(otherargs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherargs[1]));  

        System.exit(job.waitForCompletion(true)?0:1);
    }
}
