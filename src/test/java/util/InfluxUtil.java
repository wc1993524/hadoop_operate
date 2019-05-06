package util;

import java.util.concurrent.TimeUnit;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Point;

public class InfluxUtil {
	public static void writeToDb(String tableName,String machineName,String machineIP,
			String masterStatus,String regionServerStatus,String zookeeperStatus){
		InfluxDB influxDB = InfluxDBFactory.connect("http://192.168.6.98:8086", "root", "root");
		String dbName = "hbase";
		
		// Flush every 2000 Points, at least every 100ms
		influxDB.enableBatch(2000, 100, TimeUnit.MILLISECONDS);
	
		Point point1 = Point.measurement(tableName)
                .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
                .addField("hostname", machineName)
                .addField("ip", machineIP)
                .addField("masterStatus", masterStatus)
                .addField("regionServerStatus", regionServerStatus)
                .addField("zookeeperStatus", zookeeperStatus)
                .build();
//		Point point2 = Point.measurement(tableName)
//                .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
//                .addField("hostname", machineName)
//                .addField("ip", machineIP)
//                .addField("type", "regionServerStatus")
//                .addField("value", regionServerStatus)
//                .build();
//		Point point3 = Point.measurement(tableName)
//                .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
//                .addField("hostname", machineName)
//                .addField("ip", machineIP)
//                .addField("type", "zookeeperStatus")
//                .addField("value", zookeeperStatus)
//                .build();
		//autogen代表默认的保留策略
		influxDB.write(dbName, "autogen", point1);
//		influxDB.write(dbName, "autogen", point2);
//		influxDB.write(dbName, "autogen", point3);
		influxDB.close();
	}
}
