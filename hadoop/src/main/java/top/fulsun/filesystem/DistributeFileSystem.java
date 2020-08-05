package top.fulsun.filesystem;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * 分布式文件系统需要 集群的连接入口
 *  1） 配置文件指定入口，操作会有权限问题
 *      conf.set("fs.defaultFS", "hdfs://hadoop01:9000");
 *  2） 解决权限问题：指定系统的运行参数 System
 *         System.setProperty("HADOOP_USER_NAME", "hadoop");
 *  3) 创建文件系统时指定入口和用户
 *      FileSystem fs=FileSystem.get(new URI("hdfs://hadoop01:9000"), conf, "hadoop")
 *  4) 指定配置文件
 *     conf.addResource("conf/hdfs-site.xml");
 */
public class DistributeFileSystem {
    public static void main(String[] args) throws Exception {
        // 1. conf指定集群的连接入口 想要获取集群的需要指定集群的连接入口 得到分布式文件系统
//        function1();
        // 2. 解决权限问题
//        function2();
        // 3. 创建文件系统时指定入口和用户
        function3();

    }

    private static void function3() throws URISyntaxException, IOException, InterruptedException {
        Configuration conf = new Configuration();
        // 指定配置文件
        conf.addResource("hdfs-site.xml");
        // hdfs的文件系统对象   想要操作hdfs必须先拿到这个对象
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop01:9000"),conf,"hadoop");
        System.out.println(fileSystem);

        Path src = new Path("pom.xml");
        // Path dst = new Path("/pom3.xml");
        // 加载自己的 dfs-site.xml后，副本个数为2
        Path dst = new Path("/pom4.xml");

        //文件上传
        fileSystem.copyFromLocalFile(src, dst);

        fileSystem.close();
    }

    private static void function2() throws IOException {
        //指定系统的运行参数 System
        System.setProperty("HADOOP_USER_NAME", "hadoop");
        Configuration conf = new Configuration();
        //设置连接入口
        conf.set("fs.defaultFS", "hdfs://hadoop01:9000");
        // hdfs的文件系统对象   想要操作hdfs必须先拿到这个对象
        FileSystem fileSystem = FileSystem.get(conf);
        //DFS[DFSClient[clientName=DFSClient_NONMAPREDUCE_2052129887_1, ugi=sfuli (auth:SIMPLE)]]
        System.out.println(fileSystem);

        Path src = new Path("pom.xml");
        Path dst = new Path("/pom2.xml");

        //文件上传
        fileSystem.copyFromLocalFile(src, dst);
        fileSystem.close();

    }

    private static void function1() throws IOException {
        Configuration conf = new Configuration();
        //在conf对象上设置连接入口 参数1：配置文件的属性名 参数2：配置文件的属性的值
        conf.set("fs.defaultFS", "hdfs://hadoop01:9000");
        FileSystem fileSystem = FileSystem.get(conf);
        //DFS[DFSClient[clientName=DFSClient_NONMAPREDUCE_2052129887_1, ugi=sfuli (auth:SIMPLE)]]
        System.out.println(fileSystem);

        Path src = new Path("pom.xml");
        Path dst = new Path("/pom2.xml");

        //文件上传 报权限问题
        // Exception in thread "main" org.apache.hadoop.security.AccessControlException: Permission denied:
        fileSystem.copyFromLocalFile(src, dst);

        fileSystem.close();
    }
}
