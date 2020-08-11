package top.fulsun.crc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.net.URI;

/**
 * @program: bigdatastudy
 * @description: 测试CRC校验
 * @author: fulsun
 * @create: 2020-08-10 23:25
 **/
public class TestCrc
{
    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();
        // 指定配置文件
        conf.addResource("hdfs-site.xml");
        // hdfs的文件系统对象
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop01:9000"), conf, "hadoop");

        //文件下载
        Path src = new Path("/pom5.xml");
        Path dst = new Path("hadoop/src/main/resources/crc3.xml");

        //java.io.IOException: (null) entry in command string: null chmod 0644 E:\pom.xml
        fileSystem.copyToLocalFile(src, dst);
        //第一个false参数表示不删除源文件，第4个true参数表示使用本地原文件系统，因为这个Demo程序是在Windows系统下运行的。
        // fileSystem.copyToLocalFile(false,src,dst,true);

        /**
         * 文件下载过程中 只要有一个是没有损坏的副本  下载是没有损坏的块  crc校验是可通过的
         *
         * crc校验的时候：校验的内容只是原始文件的偏移量内的内容
         * 只要这部分内容没有发生变化 -> 校验通过
         * 这部分内容  发生变化 -> 校验不通过的
         */
        fileSystem.close();
    }
}
