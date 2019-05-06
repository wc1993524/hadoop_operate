package mapreduce.secondarySort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * 参考:https://www.cnblogs.com/codeOfLife/p/5568786.html
 * hadoop jar hadoop-study.jar mapreduce.secondarySort.SecondSort /hadoop-study/mapreduce/secondarySort/ /hadoop-study/mapreduce/secondarySort/result/
 * @author hadoop
 *
 */
public class SecondSort {
	public static void main(String[] args) throws Exception {
		  Configuration conf = new Configuration();
		  
		  final FileSystem fileSystem = FileSystem.get(conf);
		  
		  
		  String[] other = new GenericOptionsParser(conf,args).getRemainingArgs();
			if(other.length!=2){
				System.err.println("");
				System.exit(2);
			}
			
		  fileSystem.delete(new Path(other[1]), true);
		  
		  Job job = new Job(conf, "secondary sort");
		  job.setJarByClass(SecondSort.class);
		  job.setMapperClass(MapClass.class);
		  job.setReducerClass(Reduce.class);

		  job.setGroupingComparatorClass(GroupingComparator.class);

		  job.setMapOutputKeyClass(IntPair.class);
		  job.setMapOutputValueClass(IntWritable.class);

		  job.setOutputKeyClass(Text.class);
		  job.setOutputValueClass(IntWritable.class);

		  FileInputFormat.addInputPath(job, new Path(other[0]));
		  FileOutputFormat.setOutputPath(job, new Path(other[1]));
		  System.exit(job.waitForCompletion(true) ? 0 : 1);
		}
}

