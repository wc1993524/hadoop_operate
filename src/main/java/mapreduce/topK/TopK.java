package mapreduce.topK;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * hadoop jar hadoop-study.jar mapreduce.topK.TopK /hadoop-study/mapreduce/topK/ /hadoop-study/mapreduce/topK/output
 * @author hadoop
 *
 */
public class TopK {
	public static class MyMapper extends Mapper<LongWritable, Text, LongWritable, NullWritable> {

		//首先创建一个临时变量，保存一个可存储的最小值：Long.MIN_VALUE=-9223372036854775808
		long temp = Long.MIN_VALUE;
		//Top5存储空间
		long[] tops;
		
		/** 次方法在run中调用，在全部map之前执行一次 **/
		protected void setup(Context context) {
			//初始化数组长度为5
			tops = new long[5];  
		}
		
		//找出最大值
		protected void map(LongWritable k1, Text v1, Context context) throws IOException ,InterruptedException {
			//将文本转数值
			final long val = Long.parseLong(v1.toString());
			//保存在0索引
			if(val>tops[0]){
				tops[0] = val;
			}
			//排序后最大值在最后一个索引，这样从后到前依次减小
			Arrays.sort(tops);
		}
		
		/** ---此方法在全部到map任务结束后执行一次。这时仅输出临时变量到最大值--- **/
		protected void cleanup(Context context) throws IOException ,InterruptedException {
			//保存前5条数据
			for( int i = 0; i < tops.length; i++) {  
				context.write(new LongWritable(tops[i]), NullWritable.get());  
			}
		}
	}
	
	//reduce
	public static class MyReducer extends Reducer<LongWritable, NullWritable, LongWritable, NullWritable> {
		//临时变量
		Long temp = Long.MIN_VALUE;
		//Top5存储空间
		long[] tops;

		/** 次方法在run中调用，在全部map之前执行一次 **/
		protected void setup(Context context) {
			//初始化长度为5
			tops = new long[5];  
		}
		
		//因为每个文件都得到5个值，再次将这些值比对，得到最大的
		protected void reduce(LongWritable k2, Iterable<NullWritable> v2, Context context) throws IOException ,InterruptedException {
			
			long top = Long.parseLong(k2.toString());
			//
			if(top>tops[0]){
				tops[0] = top;
			}
			//
			Arrays.sort(tops);
		}
		
		/** ---此方法在全部到reduce任务结束后执行一次。输出前5个最大值--- **/
		protected void cleanup(Context context) throws IOException, InterruptedException {
			//保存前5条数据
			for( int i = 0; i < tops.length; i++) {  
				context.write(new LongWritable(tops[4-i]), NullWritable.get());  
			} 
		}
	}
	
	public static void main(String[] args) throws Exception {
		  Configuration conf = new Configuration();

		  final FileSystem fileSystem = FileSystem.get(conf);
		  
		  String[] other = new GenericOptionsParser(conf,args).getRemainingArgs();
			if(other.length!=2){
				System.err.println("");
				System.exit(2);
			}
		  fileSystem.delete(new Path(other[1]), true);
		  Job job = new Job(conf, "topK");
		  job.setJarByClass(TopK.class);
		  job.setMapperClass(MyMapper.class);
		  job.setReducerClass(MyReducer.class);


		  job.setOutputKeyClass(LongWritable.class);
		  job.setOutputValueClass(NullWritable.class);

		  FileInputFormat.addInputPath(job, new Path(other[0]));
		  FileOutputFormat.setOutputPath(job, new Path(other[1]));
		  System.exit(job.waitForCompletion(true) ? 0 : 1);
		}
}
