package top.fulsun.filesystem;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class LocalFileSystem {
    public static void main(String[] args) throws IOException {
        // Configuration  配置文件对象    加载配置文件  或设置配置文件
        Configuration conf = new Configuration();
        // hdfs的文件系统对象   想要操作hdfs必须先拿到这个对象
        FileSystem fileSystem = FileSystem.get(conf);
        //org.apache.hadoop.fs.LocalFileSystem@1cbb87f3本地文件系统对象：文件上传和下载都在本地操作的
        System.out.println(fileSystem);

        Path src=new Path("pom.xml");
        Path dst=new Path("pom2.xml");

        //文件操作在本地
        //copyfromlocal(复制) movefromlocal(剪切)
        fileSystem.copyFromLocalFile(src,dst);

        fileSystem.close();
    }
}
