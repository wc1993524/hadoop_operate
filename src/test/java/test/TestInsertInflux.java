package test;

import java.net.InetAddress;

import util.InfluxUtil;
import util.ProcessUtil;

public class TestInsertInflux {
	public void getProcess() {
		String masterStatus = "01";
		String regionServerStatus = "01";
		String zookeeperStatus = "01";
		
		//获取本机名称和ip
		InetAddress addr = null;
		String ip = "";
		String hostName = "";
		try {
			addr = InetAddress.getLocalHost();
			ip = addr.getHostAddress().toString();//获得本机IP
			hostName = addr.getHostName().toString();//获得本机名称
		}catch(Exception e) {
			e.printStackTrace();
		}

		//获取进程状态
		boolean masterFlag = false;
		boolean regionServerFlag = false;
		boolean zookeeperFlag = false;
		String[] cmdA = {"/bin/sh", "-c", "echo ${JAVA_HOME}"};
		String path = ProcessUtil.execPro(cmdA).toString();
		String javaPath = path + "/bin/jps";
		String[] cmdB = {javaPath};
		String s = ProcessUtil.execPro(cmdB).toString();
		String[] arry = s.split("/");

		for(int i = 0; i < arry.length; i++) {
			String line = arry[i];
			String processName = line.split(" ")[1];
			if("HMaster".equals(processName)) {
				masterFlag = true;
			}else if("HRegionServer".equals(processName)) {
				regionServerFlag = true;
			}else if("QuorumPeerMain".equals(processName)) {
				zookeeperFlag = true;
			}
		}
		
		if(masterFlag == true){
			masterStatus = "00";
		}
		if(regionServerFlag == true){
			regionServerStatus = "00";
		}
		if(zookeeperFlag == true){
			zookeeperStatus = "00";
		}
		
		//插入influx时序数据库中
		//InfluxUtil.writeToDb("process", hostName, ip, masterStatus, regionServerStatus, zookeeperStatus);
		System.out.println(masterStatus + regionServerStatus + zookeeperStatus);
	}
	
	public static void main(String[] args) {
		try {
			TestInsertInflux pm = new TestInsertInflux();
			for(int i=1;i<10;i++){
				System.out.println(i);
				pm.getProcess();
				Thread.sleep(10 * 1000);
			}
		}catch(Exception e){
			e.printStackTrace();
		}
	}
}
