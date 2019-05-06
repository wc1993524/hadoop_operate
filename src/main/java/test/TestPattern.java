package test;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TestPattern {
	public static void main(String[] args) {
	    // 要验证的字符串
	    String str = "0";
	    // 邮箱验证规则
	    String regEx = "^[0-9]*$";
	    // 编译正则表达式
	    Pattern pattern = Pattern.compile(regEx);
	    // 忽略大小写的写法
	    // Pattern pat = Pattern.compile(regEx, Pattern.CASE_INSENSITIVE);
	    Matcher matcher = pattern.matcher(str);
	    // 字符串是否与正则表达式相匹配
	    boolean rs = matcher.matches();
	    System.out.println(rs);
	    
	    
//	    String url1 = "http://www.sina.com.cn/video/12dsasd12eq.mp4";
//	    String url2 = "www.sina.com.cn/video/12dsasd12eq.mp4";
//
//	    String regex = "/(http://)?www.sina.com.cn/";
//
//	    System.out.println(url1.replaceAll(regex,""));
//	    System.out.println(url2.replaceAll(regex,""));
	    
	    String str1 ="营业时间： 11:30-21:30 修改 分类标签： 无线上网(8) 可以刷卡(7) 朋友聚餐(5) 家庭聚会(5) 商务宴请(4) 情侣约会(4) 可自带酒水(2)";  
        Pattern p=Pattern.compile("\\d{2}:\\d{2}-\\d{2}:\\d{2}");    
        Matcher m=p.matcher(str1);    
        while(m.find())  
        {    
          System.out.println(m.group());    
        }
        
        String ragex = "(http\\w?):\\/\\/([^/:]+)|\\?[^/:]+";
        String str2 = "https://yftest.admin.wdcloud.cc/ptyyxtpt/page/cpfw/cpgl-rzsh.html?cpid=1100000000000003002&hj=20170522171053861~1100000000000003002&ktid=110000000000000130";
        System.out.println(str2.replaceAll(ragex, "")); 
        
        String date = "1495260310000";
        System.out.println(Long.valueOf(date));
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");  
        System.out.println(sdf.format(new Date(1495709502916l)));
        
        
	}
}
