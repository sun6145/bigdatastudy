package top.fulsun.useapi;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.net.URI;

/**
 * @author sfuli
 * @title: QueryFloderinfo
 * @projectName bigdatastudy
 * @description: 查看文件和文件夹的信息
 * @date 2020/8/51:49
 */
public class QueryFileAndFloder {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        // 指定配置文件
        conf.addResource("hdfs-site.xml");
        // hdfs的文件系统对象
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop01:9000"), conf, "hadoop");
        fs.mkdirs(new Path("/aa"));
        Path src = new Path("/pom5.xml");
        //FileStatus  文件或文件夹的状态对象  封装的文件或文件夹的  用户  组   长度  路径
        FileStatus[] listStatus = fs.listStatus(new Path("/"));
        for (FileStatus fss : listStatus) {
            System.out.println("=======================");
            System.out.println(fss.getPath());
            System.out.println(fss.getBlockSize());
            System.out.println(fss.getLen());
            System.out.println(fss.getReplication());
        }
        fs.close();
    }
}
