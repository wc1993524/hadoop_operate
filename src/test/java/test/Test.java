package test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.net.InetAddress;

public class Test {
	public static void main(String[] args) {
//		 获取本机名称和ip
		 InetAddress addr=null;
		 String ip="";
		 String address="";
		 try{
		 addr=InetAddress.getLocalHost();
		 ip=addr.getHostAddress().toString();//获得本机IP
		 address=addr.getHostName().toString();//获得本机名称
		 }catch(Exception e){
		 e.printStackTrace();
		 }
		 System.out.println(ip+"---"+address);

		boolean masterFlag = false;
		boolean regionServerFlag = false;
		boolean zookeeperFlag = false;
//		String[] cmdA = {"/bin/sh", "-c", "echo ${JAVA_HOME}"};
//		String str = execPro(cmdA).toString();
//		System.out.println(str);
		String a = "/home/hadoop/software/jdk1.7.0_79";
		String javaPath =a +"/bin/jps";
//		String[] cmdB = {"/home/hadoop/software/jdk1.7.0_79/bin/jps"};
		String[] cmdB = {javaPath};
		String s = execPro(cmdB).toString();
		System.out.println(s);
		String[] arry = s.split("/");
		
		for(int i=0;i<arry.length;i++){
			String line = arry[i];
			String processName = line.split(" ")[1];
			if("HMaster".equals(processName)){
				masterFlag = true;
			}else if("HRegionServer".equals(processName)){
				regionServerFlag = true;
			}else if("QuorumPeerMain".equals(processName)){
				zookeeperFlag = true;
			}
		}
		System.out.println(masterFlag+"--"+regionServerFlag+"---"+zookeeperFlag);
		
		String javaPath1 = "/home/hadoop/software/hadoop/bin/hadoop";
//		String[] cmdC = {javaPath1,"job","-list"};
		String[] cmdC = {javaPath1,"jar","/home/hadoop/software/practice_file/TOP1.jar","topK.Top","/topN/1.txt","/topN/result"};
		boolean b = execPro1(cmdC);
		System.out.println(b);
		
	}
	
	 private static boolean execPro1(String[] exec) {
	        InputStream in = null;
	        InputStream ein = null;
	        boolean resultFlag = false; //结果标识
	        Process pro = null;
	        try {
	            pro = Runtime.getRuntime().exec(exec);
	            pro.waitFor();
	            in = pro.getInputStream();
	            ein = pro.getErrorStream();
	            BufferedReader read = new BufferedReader(new InputStreamReader(in));
	            String line = "";
	            while ((line = read.readLine()) != null) {
	                System.out.println("标准输出流：" + line);
	                resultFlag = true;
	            }
	            BufferedReader errorRead = new BufferedReader(new InputStreamReader(ein));
	            String errorLine = "";
	            while ((errorLine = errorRead.readLine()) != null) {
	                System.out.println("标准错误流：" + errorLine);
	                resultFlag = false;
	            }
	            ;
	        } catch (InterruptedException e) {
	            e.printStackTrace();
	            if (pro != null) {
	                pro.destroy();
	            }
	        } catch (IOException e) {
	            e.printStackTrace();
	        } finally {
	            try {
	                in.close();
	                ein.close();
	            } catch (IOException e) {
	                e.printStackTrace();
	            }

	        }
	        return resultFlag;
	    }

	/**
	 * 启动第三方进程
	 *
	 * @param exec
	 *            命令行，字符串数组
	 * @return
	 */
	private static Object execPro(String[] exec) {
		InputStream in = null;
		InputStream ein = null;
		boolean resultFlag = false; // 结果标识
		Process pro = null;
		StringBuffer sb = new StringBuffer();
		try {
			pro = Runtime.getRuntime().exec(exec);
			pro.waitFor();
			in = pro.getInputStream();
			ein = pro.getErrorStream();
			BufferedReader read = new BufferedReader(new InputStreamReader(in));
			String line = "";
			while ((line = read.readLine()) != null) {
				//System.out.println("标准输出流：" + line);
//				resultFlag = true;
				sb.append(line).append("/");
			}
			BufferedReader errorRead = new BufferedReader(new InputStreamReader(ein));
			String errorLine = "";
			while ((errorLine = errorRead.readLine()) != null) {
//				System.out.println("标准错误流：" + errorLine);
//				resultFlag = false;
				sb.append(errorLine).append("/");
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
			if (pro != null) {
				pro.destroy();
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				in.close();
				ein.close();
			} catch (IOException e) {
				e.printStackTrace();
			}

		}
		return sb;
	}
}
