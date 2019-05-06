package test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;

public class FwzUtil {
	// 计算年份
	public static String getWeekOfYear(String date, String partten) throws ParseException {

		SimpleDateFormat sdf = new SimpleDateFormat(partten);
		Calendar cl = Calendar.getInstance();
		Date d = sdf.parse(date);
		cl.setTime(d);
		int week = cl.get(Calendar.WEEK_OF_YEAR);
		int y = cl.get(Calendar.YEAR);
		int m = cl.get(Calendar.MONTH);
		String fwz = "";
		// 处理年末的日期属于下一年第一周的情况
		if (m == 11 && week == 1) {
			y += 1;
		}
		// 处理年初的日期属于上一年最后一周的情况
		if (m == 0 && week > 50) {
			y -= 1;
		}
		// 将周数转成2位字符，与年份拼起来
		if (week < 10) {
			fwz += y + "0" + week;
		} else {
			fwz += y + "" + week;
		}
		return fwz;
	}
		
	public static void main(String [] args) throws Exception {  
		System.out.println(FwzUtil.getWeekOfYear("2017-02-16 23:22:22", "yyyy-MM-dd HH:mm:ss"));
		System.out.println(FwzUtil.getWeekOfYear("Tue Mar 03 00:00:00 GMT 2015", "EEE MMM dd HH:mm:ss 'GMT' yyyy"));
	}
}
