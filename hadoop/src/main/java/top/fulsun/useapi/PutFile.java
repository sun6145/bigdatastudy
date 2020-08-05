package top.fulsun.useapi;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.net.URI;

/**
 * 文件上传
 */
public class PutFile {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        // 指定配置文件
        conf.addResource("hdfs-site.xml");
        // hdfs的文件系统对象
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop01:9000"), conf, "hadoop");

        //文件上传
        Path src = new Path("pom.xml");
        Path dst = new Path("/pom5.xml");
        fileSystem.copyFromLocalFile(src, dst);

        //文件剪切上传
        Path src2 = new Path("pom2.xml");
        Path dst2 = new Path("/pom6.xml");
        fileSystem.moveFromLocalFile(src2, dst2);

        fileSystem.close();
    }
}
