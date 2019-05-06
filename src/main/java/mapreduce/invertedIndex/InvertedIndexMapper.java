package mapreduce.invertedIndex;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
 
public class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, Text> {
 
	private static Text keyInfo = new Text();// 存储单词和 URL 组合
	private static final Text valueInfo = new Text("1");// 存储词频,初始化为1
 
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
 
		String line = value.toString();
		String[] fields = StringUtils.split(line, " ");// 得到字段数组
 
		FileSplit fileSplit = (FileSplit) context.getInputSplit();// 得到这行数据所在的文件切片
		String fileName = fileSplit.getPath().getName();// 根据文件切片得到文件名
 
		for (String field : fields) {
			// key值由单词和URL组成，如“MapReduce:file1”
			keyInfo.set(field + ":" + fileName);
			context.write(keyInfo, valueInfo);
		}
	}
}
