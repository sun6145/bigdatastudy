package top.fulsun.iooperation;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;

/**
 * @program: bigdatastudy
 * @description: 通过IO流方式上传文件
 * @author: fulsun
 * @create: 2020-08-10 22:51
 **/
public class Upload
{
    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop01:9000"), conf, "hadoop");

        // 本地--> 读 --> 输入流
        FileInputStream in = new FileInputStream("pom.xml");
        // hdfs ---> 写 ---> 输出流
        FSDataOutputStream out = fs.create(new Path("/io_pom2.xml"));

        // 传统IO方式
        // operation(in, out);

        //进行读写  IOUtils hadoop进行文件读写的工具类

        //参数1：输入流  参数2：输出流  参数3：缓冲大小
        IOUtils.copyBytes(in, out, 1024);
        //需要手动昂关闭流
        in.close();
        out.close();

        //参数1：输入流  参数2：输出流  参数3：缓冲大小  参数4：执行完成  是否关闭流
        IOUtils.copyBytes(in, out, 1024, true);

        //参数3---long   读取的字节数  读取指定的字节数
        IOUtils.copyBytes(in, out, 2L, true);

        fs.close();

    }

    private static void operation(FileInputStream in, FSDataOutputStream out) throws IOException
    {
        int len = 0;
        byte[] bytes = new byte[1024 * 10]; //10240b = 10kb
        //read()==-1则代表文件读到了末尾
        if ((len = in.read(bytes)) != -1)
        {
            out.write(bytes, 0, len);
        }
        in.close();
        out.close();
    }
}
