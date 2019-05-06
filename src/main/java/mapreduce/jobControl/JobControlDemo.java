package mapreduce.jobControl;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
 
/**
 * 原文地址：https://blog.csdn.net/baolibin528/article/details/50754753
 * hadoop jar hadoop-study.jar mapreduce.jobControl.JobControlDemo /hadoop-study/mapreduce/jobControl/input1.txt /hadoop-study/mapreduce/jobControl/input2.txt /hadoop-study/mapreduce/jobControl/output
 * @author hadoop
 *
 */
public class JobControlDemo {
	public static int main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		String[] otherargs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherargs.length != 3) {
			System.err.println("Usage JobControlDemo <InputPath1> <InputPath1> <OutPath>");
			System.exit(2);
		}
 
		// 创建基础作业
		Job job1 = Job.getInstance(conf, JobControlDemo.class.getSimpleName() + "1");
		Job job2 = Job.getInstance(conf, JobControlDemo.class.getSimpleName() + "2");
		Job job3 = Job.getInstance(conf, JobControlDemo.class.getSimpleName() + "3");
 
		// Job1作业参数配置
		job1.setJarByClass(JobControlDemo.class);
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Text.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		job1.setMapperClass(MyMapper1.class);
		job1.setReducerClass(MyReducer1.class);
		job1.setInputFormatClass(TextInputFormat.class);
		job1.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job1, new Path(otherargs[0]));
		FileOutputFormat.setOutputPath(job1, new Path(otherargs[2]+File.separator+"mid1"));
 
		// Job2作业参数配置
		job2.setJarByClass(JobControlDemo.class);
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		job2.setMapperClass(MyMapper2.class);
		job2.setReducerClass(MyReducer2.class);
		job2.setInputFormatClass(TextInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job2, new Path(otherargs[1]));
		FileOutputFormat.setOutputPath(job2, new Path(otherargs[2]+File.separator+"mid2"));
 
		// Job3作业参数配置
		job3.setJarByClass(JobControlDemo.class);
		job3.setMapOutputKeyClass(Text.class);
		job3.setMapOutputValueClass(Text.class);
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(Text.class);
		job3.setMapperClass(MyMapper3.class);
		job3.setReducerClass(MyReducer3.class);
		job3.setInputFormatClass(KeyValueTextInputFormat.class);
		job3.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job3, new Path(otherargs[2]+File.separator+"mid1"));
		FileInputFormat.addInputPath(job3, new Path(otherargs[2]+File.separator+"mid2"));
		FileOutputFormat.setOutputPath(job3, new Path(otherargs[2]+File.separator+"result"));
 
		// 创建受控作业
		ControlledJob cjob1 = new ControlledJob(conf);
		ControlledJob cjob2 = new ControlledJob(conf);
		ControlledJob cjob3 = new ControlledJob(conf);
 
		// 将普通作业包装成受控作业
		cjob1.setJob(job1);
		cjob2.setJob(job2);
		cjob3.setJob(job3);
 
		// 设置依赖关系
		//cjob2.addDependingJob(cjob1);
		cjob3.addDependingJob(cjob1);
		cjob3.addDependingJob(cjob2);
 
		// 新建作业控制器
		JobControl jc = new JobControl("My control job");
 
		// 将受控作业添加到控制器中
		jc.addJob(cjob1);
		jc.addJob(cjob2);
		jc.addJob(cjob3);
 
		/**
		 * hadoop的JobControl类实现了线程Runnable接口。我们需要实例化一个线程来让它启动。直接调用JobControl的run()方法，线程将无法结束。
		 */
		//jc.run();
		
        Thread jcThread = new Thread(jc);  
        jcThread.start();  
        while(true){  
            if(jc.allFinished()){  
                System.out.println(jc.getSuccessfulJobList());  
                jc.stop();  
                return 0;  
            }  
            if(jc.getFailedJobList().size() > 0){  
                System.out.println(jc.getFailedJobList());  
                jc.stop();  
                return 1;  
            }  
        } 
	}
	/**
	 * 第一个Job
	 */
	public static class MyMapper1 extends Mapper<LongWritable, Text, Text, Text>{
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String[] spl1=value.toString().split(" ");
			if(spl1.length==2){
				context.write(new Text(spl1[0].trim()), new Text(spl1[1].trim()));
			}
		}
	}
	public static class MyReducer1 extends Reducer<Text, Text, Text, Text>{
		@Override
		protected void reduce(Text k2, Iterable<Text> v2s, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			for (Text v2 : v2s) {
				context.write(k2, v2);
			}
		}
	}
	/**
	 * 第二个Job
	 */
	public static class MyMapper2 extends Mapper<LongWritable, Text, Text, Text>{
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String[] spl2=value.toString().split(" ");
			if(spl2.length==2){
				context.write(new Text(spl2[0].trim()), new Text(spl2[1].trim()));
			}
		}
	}
	public static class MyReducer2 extends Reducer<Text, Text, Text, Text>{
		@Override
		protected void reduce(Text k3, Iterable<Text> v3s, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			for (Text v3 : v3s) {
				context.write(k3, v3);
			}
		}
	}
	/**
	 * 第三个Job
	 */
	public static class MyMapper3 extends Mapper<Text, Text, Text, Text>{
		@Override
		protected void map(Text key, Text value, Mapper<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			context.write(key, value);
		}
	}
	public static class MyReducer3 extends Reducer<Text,Text, Text, Text>{
		@Override
		protected void reduce(Text k4, Iterable<Text> v4s,Reducer<Text, Text, Text, Text>.Context context) 
				throws IOException, InterruptedException {
			HashSet<String> hashSet=new HashSet<String>();
			for (Text v4 : v4s) {
				hashSet.add(v4.toString().trim());
			}
			if(hashSet.size()>=2){
				context.write(k4, new Text("OK"));
			}
		}
	}
}