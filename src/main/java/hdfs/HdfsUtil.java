package hdfs;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;  
import java.net.URISyntaxException;  
  
import org.apache.hadoop.conf.Configuration;  
import org.apache.hadoop.fs.FSDataInputStream;  
import org.apache.hadoop.fs.FSDataOutputStream;  
import org.apache.hadoop.fs.FileStatus;  
import org.apache.hadoop.fs.FileSystem;  
import org.apache.hadoop.fs.FileUtil;  
import org.apache.hadoop.fs.Path;  
import org.apache.hadoop.io.IOUtils;  
  
  
public class HdfsUtil {  
      
    //在指定位置新建一个文件，并写入字符  
    public static void WriteToHDFS(String file, String words) throws IOException, URISyntaxException  
    {  
        Configuration conf = new Configuration();  
        FileSystem fs = FileSystem.get(URI.create(file), conf);  
        Path path = new Path(file);  
        FSDataOutputStream out = fs.create(path);   //创建文件  
  
        out.write(words.getBytes("UTF-8"));  
          
        out.close();  
    }  
      
    public static void ReadFromHDFS(String file) throws IOException  
    {  
        Configuration conf = new Configuration();  
        FileSystem fs = FileSystem.get(URI.create(file), conf);  
        Path path = new Path(file);  
        FSDataInputStream in = fs.open(path);  
          
        IOUtils.copyBytes(in, System.out, 4096, true);  
        //使用FSDataInoutStream的read方法会将文件内容读取到字节流中并返回  
        /** 
         * FileStatus stat = fs.getFileStatus(path); 
      // create the buffer 
       byte[] buffer = new byte[Integer.parseInt(String.valueOf(stat.getLen()))]; 
       is.readFully(0, buffer); 
       is.close(); 
             fs.close(); 
       return buffer; 
         */  
    }  
      
    public static void DeleteHDFSFile(String file) throws IOException  
    {  
        Configuration conf = new Configuration();  
        FileSystem fs = FileSystem.get(URI.create(file), conf);  
        Path path = new Path(file);  
        //查看fs的delete API可以看到三个方法。deleteonExit实在退出JVM时删除，下面的方法是在指定为目录是递归删除  
        fs.delete(path,true);  
        fs.close();  
    }  
      
    public static void UploadLocalFileHDFS(String src, String dst) throws IOException  
    {  
        Configuration conf = new Configuration();  
        FileSystem fs = FileSystem.get(URI.create(dst), conf);  
        Path pathDst = new Path(dst);  
        Path pathSrc = new Path(src);  
          
        fs.copyFromLocalFile(pathSrc, pathDst);  
        fs.close();  
    }  
      
    public static void ListDirAll(String DirFile) throws IOException  
    {  
        Configuration conf = new Configuration();  
        FileSystem fs = FileSystem.get(URI.create(DirFile), conf);  
        Path path = new Path(DirFile);  
          
        FileStatus[] status = fs.listStatus(path);  
        //方法1    
        for(FileStatus f: status)  
        {  
            System.out.println(f.getPath().toString());    
        }  
        //方法2    
        Path[] listedPaths = FileUtil.stat2Paths(status);    
        for (Path p : listedPaths){   
          System.out.println(p.toString());  
        }  
    }  
     
    //往hdfs文件中追加数据
    public static void append() throws IOException{
    	String hdfs_path = "hdfs://192.168.6.44:9000/tmp/hadoop/FileWrite";//文件路径  
        Configuration conf = new Configuration();  
        conf.setBoolean("dfs.support.append", true);  
  
        String inpath = "/home/hadoop/software/practice_file/user.txt";  
        FileSystem fs = null;  
        try {  
            fs = FileSystem.get(URI.create(hdfs_path), conf);  
            //要追加的文件流，inpath为文件  
            InputStream in = new   
                  BufferedInputStream(new FileInputStream(inpath));  
            OutputStream out = fs.append(new Path(hdfs_path));  
            IOUtils.copyBytes(in, out, 4096, true);  
        } catch (IOException e) {  
            e.printStackTrace();  
        }  
    }
    
    public static void main(String [] args) throws IOException, URISyntaxException  
    {  
        //下面做的是显示目录下所有文件  
//        ListDirAll("hdfs://192.168.6.45:9000/tmp/hadoop");  
          
        String fileWrite = "hdfs://192.168.6.45:9000/tmp/hadoop/FileWrite";  
        String words = "This words is to write into file!\n";  
//        WriteToHDFS(fileWrite, words);  
        //这里我们读取fileWrite的内容并显示在终端  
//        ReadFromHDFS(fileWrite);  
        //这里删除上面的fileWrite文件  
//        DeleteHDFSFile(fileWrite);  
        //假设本地有一个uploadFile，这里上传该文件到HDFS  
//      String LocalFile = "file:///home/kqiao/hadoop/MyHadoopCodes/uploadFile";  
//      UploadLocalFileHDFS(LocalFile, fileWrite    );
        
        //zhui jia shu ju
        append();
    }  
}  
