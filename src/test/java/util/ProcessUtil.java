package util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class ProcessUtil {
	public static Object execPro(String[] exec) {
		InputStream in = null;
		InputStream ein = null;
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
				sb.append(line).append("/");
			}
			BufferedReader errorRead = new BufferedReader(new InputStreamReader(ein));
			String errorLine = "";
			while ((errorLine = errorRead.readLine()) != null) {
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
