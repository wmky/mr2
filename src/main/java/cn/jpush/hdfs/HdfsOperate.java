package cn.jpush.hdfs;

/**
 * Created by wmky_kk on 2017-08-23.
 */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HdfsOperate {

    /**
     * hdfs路径下的文件列表
     * @param srcpath
     * @return
     */
    public String[] getFileList(String srcpath){
        Logger logger = Logger.getLogger(HdfsOperate.class);

        try{
            Configuration conf = new Configuration();
            Path path = new Path(srcpath);
            FileSystem fs = path.getFileSystem(conf);  /** Return the FileSystem that owns this Path. */
            List<String> files = new ArrayList<String>();
            if(fs.exists(path) && fs.isDirectory(path)){
                for(FileStatus status : fs.listStatus(path)){
                    files.add(status.getPath().toString());
                }
                return files.toArray(new String[]{});
            }
        }catch(IOException e){
            logger.error("HDFS IOException." + e);
        }catch(Exception e){
            logger.error("HDFS Exception." + e);
        }
        return null;
    }

    public static void main(String[] args){
        HdfsOperate hdfsOperate = new HdfsOperate();
        String path = args[0];
        String[] array = hdfsOperate.getFileList(path);
        for (String s : array) {
            System.out.println(s);
        }
    }
}
