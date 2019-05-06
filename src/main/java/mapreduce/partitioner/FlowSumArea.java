package mapreduce.partitioner;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
 
/**
 * 原文地址：https://blog.csdn.net/Xw_Classmate/article/details/50639848?utm_source=blogxgwz0   实例2.3
 * 
 * 对流量原始日志进行流量统计，将不同省份的用户统计结果输出到不同文件 需要自定义改造两个机制： 
 * 1、改造分区的逻辑，自定义一个partitioner
 * 2、自定义reduer task的并发任务数
 * 
 * hadoop jar hadoop-study.jar mapreduce.partitioner.FlowSumArea /hadoop-study/mapreduce/flow.txt /hadoop-study/mapreduce/partition/output
 */
public class FlowSumArea {
 
	public static class FlowSumAreaMapper extends
			Mapper<LongWritable, Text, Text, FlowBean> {
 
		@Override
		protected void map(LongWritable k1, Text v1,
				Mapper<LongWritable, Text, Text, FlowBean>.Context context)
				throws IOException, InterruptedException {
			String line = v1.toString();
			String[] fields = StringUtils.split(line, "\t");
 
			String phoneNB = fields[1];
			Long up_flow = Long.parseLong(fields[7]);
			Long down_flow = Long.parseLong(fields[8]);
 
			context.write(new Text(phoneNB), new FlowBean(phoneNB, up_flow,
					down_flow));
		}
	}
 
	public static class FlowSumAreaReducer extends
			Reducer<Text, FlowBean, Text, FlowBean> {
 
		@Override
		protected void reduce(Text k2, Iterable<FlowBean> v2s,
				Reducer<Text, FlowBean, Text, FlowBean>.Context context)
				throws IOException, InterruptedException {
			long up_flow = 0;
			long down_flow = 0;
			for (FlowBean v2 : v2s) {
				up_flow += v2.getUp_flow();
				down_flow += v2.getDown_flow();
			}
			context.write(new Text(k2), new FlowBean(k2.toString(), up_flow,
					down_flow));
		}
	}
 
	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
 
		job.setJarByClass(FlowSumArea.class);
 
		job.setMapperClass(FlowSumAreaMapper.class);
		job.setReducerClass(FlowSumAreaReducer.class);
 
		// 定义分组逻辑类
		job.setPartitionerClass(AreaPartitioner.class);
 
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);
 
		// 设定reducer的任务并发数,应该跟分组的数量保持一致
		job.setNumReduceTasks(6);
 
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}