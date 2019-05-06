package mapreduce.comparator;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
 

/**
 * 原文地址：https://blog.csdn.net/Xw_Classmate/article/details/50639848?utm_source=blogxgwz0   实例2.2 
 * 
 * 按照总流量从高到低排序(总流量=上行流量+下行流量)
 * hadoop jar hadoop-study.jar mapreduce.comparator.SortMapReduce /hadoop-study/mapreduce/flow.txt /hadoop-study/mapreduce/comparable/output
 */
public class SortMapReduce {
 
	public static class SortMapper extends
			Mapper<LongWritable, Text, FlowBean, NullWritable> {
		@Override
		protected void map(
				LongWritable k1,
				Text v1,
				Mapper<LongWritable, Text, FlowBean, NullWritable>.Context context)
				throws IOException, InterruptedException {
			
			String line = v1.toString();
			String[] fields = StringUtils.split(line, "\t");
 
			// 得到想要的手机号、上行流量、下行流量
			String phoneNB = fields[1];
			long up_flow = Long.parseLong(fields[7]);
			long down_flow = Long.parseLong(fields[8]);
 
			context.write(new FlowBean(phoneNB, up_flow, down_flow),
					NullWritable.get());
		}
	}
 
	public static class SortReducer extends
			Reducer<FlowBean, NullWritable, Text, FlowBean> {
		@Override
		protected void reduce(FlowBean k2, Iterable<NullWritable> v2s,
				Reducer<FlowBean, NullWritable, Text, FlowBean>.Context context)
				throws IOException, InterruptedException {
			String phoneNB = k2.getPhoneNB();
			context.write(new Text(phoneNB), k2);
		}
	}
 
	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
 
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
 
		job.setJarByClass(SortMapReduce.class);
 
		job.setMapperClass(SortMapper.class);
		job.setReducerClass(SortReducer.class);
 
		job.setMapOutputKeyClass(FlowBean.class);
		job.setMapOutputValueClass(NullWritable.class);
 
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);
 
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
 
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
