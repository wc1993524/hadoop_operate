package test;

import java.util.Map.Entry;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.TreeMap;

public class TestTreeMap {
	public static void main(String[] args) throws Exception{
		TreeMap<Long, Long> tree = new TreeMap<Long, Long>();
		tree.put(1333333L, 1333333L);
		tree.put(1222222L, 1222222L);
		tree.put(1555555L, 1555555L);
		tree.put(1444444L, 1444444L);
		for (Entry<Long, Long> entry : tree.entrySet()) {
			System.out.println(entry.getKey()+":"+entry.getValue());
		}
		System.out.println(tree.firstEntry().getValue()); //最小值
		System.out.println(tree.lastEntry().getValue()); //最大值
		System.out.println(tree.navigableKeySet());	//从小到大的正序key集合
		System.out.println(tree.descendingKeySet());//从大到小的倒序key集合
		
		long[] tops = new long[5];
		tops[0] = 1;
		tops[1] = 3;
		tops[2] = 2;
		Arrays.sort(tops);
		for( int i = 0; i < tops.length; i++) {  
			System.out.println(tops[i]);
		}
		
		String time = "2018-11-02 00:00:00";
		SimpleDateFormat sDateFormat=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"); 
		Date date=sDateFormat.parse(time);
		System.out.println(date.getTime());
		
		long timestamp = 1540757353465l;
		String str = sDateFormat.format(timestamp);
		System.out.println(str);
	}
}
