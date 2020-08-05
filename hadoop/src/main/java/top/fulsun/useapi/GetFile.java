package top.fulsun.useapi;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.net.URI;

/**
 * 下载
 * public void copyToLocalFile(boolean delSrc,
 *                             Path src,
 *                             Path dst,
 *                             boolean useRawLocalFileSystem)
 *                      throws IOException
 * The src file is under FS, and the dst is on the local disk. Copy it from FS control to the local dst name. delSrc indicates if the src will be removed or not. useRawLocalFileSystem indicates whether to use RawLocalFileSystem as local file system or not. RawLocalFileSystem is non crc file system.So, It will not create any crc files at local.
 * Parameters:
 * delSrc - whether to delete the src
 * src - path
 * dst - path
 * useRawLocalFileSystem - whether to use RawLocalFileSystem as local file system or not.
 * Throws:
 * IOException - - if any IO error
 */
public class GetFile {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        // 指定配置文件
        conf.addResource("hdfs-site.xml");
        // hdfs的文件系统对象
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop01:9000"), conf, "hadoop");

        //文件下载
        Path src = new Path("/pom6.xml");
        Path dst = new Path("E:\\pom.xml");

        //java.io.IOException: (null) entry in command string: null chmod 0644 E:\pom.xml
        //fileSystem.copyToLocalFile(src, dst);
        //第一个false参数表示不删除源文件，第4个true参数表示使用本地原文件系统，因为这个Demo程序是在Windows系统下运行的。
        fileSystem.copyToLocalFile(false,src,dst,true);

        fileSystem.close();
    }
}
