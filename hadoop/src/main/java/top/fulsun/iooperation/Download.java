package top.fulsun.iooperation;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.FileOutputStream;
import java.net.URI;

/**
 * @program: bigdatastudy
 * @description: 使用IO流下载文件
 * @author: fulsun
 * @create: 2020-08-10 23:10
 **/
public class Download
{
    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop01:9000"), conf, "hadoop");
        //文件下载
        // hdfs -> 读 -> 输入流
        FSDataInputStream in = fs.open(new Path("/pom5.xml"));

        //这个方法将流的指针设置到某一个字节开始读取  参数--偏移量
        in.seek(0L);

        // 本地 -> 写 -> 输出流
        FileOutputStream out = new FileOutputStream("hadoop/src/main/resources/pom5.xml");

        //进行流的复制
        // IOUtils.copyBytes(in, out, 1024, true);
        //参数3是Long类型：读取的字节数  读取指定的字节数 结果文件大小为1K
        IOUtils.copyBytes(in, out, 1024L, true);

        fs.close();
    }
}
