package test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Date;
import java.util.UUID;

public class FileUtil {

	public static String ReadFile(String path) {
		BufferedReader reader = null;
		String laststr = "";
		try {
//			System.out.println(System.getProperty("java.class.path"));
			InputStreamReader inputStreamReader = new InputStreamReader(FileUtil.class.getResourceAsStream(path), "UTF-8");
			reader = new BufferedReader(inputStreamReader);
			String tempString = null;
			while ((tempString = reader.readLine()) != null) {
				laststr += tempString;
			}
			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (reader != null) {
				try {
					reader.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		return laststr;
	}
	
	public static void writeFile(String path,String content){
	     try{
		      File file =new File(path);
	
		      if(!file.exists()){
		    	  file.createNewFile();
		      }

		      FileWriter fw = new FileWriter(file.getAbsoluteFile(),true);
		      BufferedWriter bw = new BufferedWriter(fw);
		      bw.append(content);
		      bw.close();

	     }catch(IOException e){
	    	 e.printStackTrace();
	     }
	}
	
	 public static String readTxtFile(String filePath){
		 BufferedReader reader = null;
		 String laststr = "";
         try {
            String encoding="UTF-8";
            File file=new File(filePath);
            InputStreamReader read = new InputStreamReader(
            new FileInputStream(file),encoding);//考虑到编码格式
            reader = new BufferedReader(read);
            String tempString = null;
			while ((tempString = reader.readLine()) != null) {
				laststr += tempString;
			}
			reader.close();
        } catch (IOException e) {
 			e.printStackTrace();
 		} finally {
 			if (reader != null) {
 				try {
 					reader.close();
 				} catch (IOException e) {
 					e.printStackTrace();
 				}
 			}
 		}
        return laststr;
    }
	
	public static void main(String [] args)   
    {  
//		String path = "/home/hadoop/software/practice_file/test.txt";
//		String content = "test append\n";
//		writeFile(path,content);
//		Date date = new Date();
//		System.out.print(date.toString());
//		 UUID uuid = UUID.randomUUID();
//	        System.out.println(uuid.toString());
		int j = 3;
		for(int i=0;i<=j;i++){
			System.out.println(i);
		}
    } 
}
