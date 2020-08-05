package top.fulsun.useapi;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.net.URI;

/**
 * @author sfuli
 * @title: DelFile
 * @projectName bigdatastudy
 * @description: 删除操作
 * @date 2020/8/51:34
 */
public class DelFile {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        // 指定配置文件
        conf.addResource("hdfs-site.xml");
        // hdfs的文件系统对象
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop01:9000"), conf, "hadoop");

        Path src = new Path("/pom6.xml");
        //文件删除 delete() 既可以删除文件  也可以删除文件夹的
        // 参数1：需要删除的路径文件或文件夹 参数2：是否需要级联删除  true需要  false  --不需要
        fileSystem.delete(src, false);
        //删除文件夹
        fileSystem.delete(new Path("/ss"),true);

        //判断文件夹/文件是否存在  如果存在  则返回true   否则--false
        boolean ise=fileSystem.exists(new Path("/pom5.xml"));   //mkdir/delete
        System.out.println("pom5.xml文件状态："+ise);

        //判断后删除
        if(fileSystem.exists(new Path("/movie"))){
            fileSystem.delete(new Path("/movie"),false);
        }

        fileSystem.close();
    }
}

