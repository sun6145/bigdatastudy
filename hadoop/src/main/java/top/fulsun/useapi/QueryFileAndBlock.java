package top.fulsun.useapi;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.net.URI;
import java.util.Arrays;

/**
 * @author sfuli
 * @title: QueryFile
 * @projectName bigdatastudy
 * @description: fileSystem.listFiles 只能查看文件的信息
 * @date 2020/8/51:41
 */
public class QueryFileAndBlock {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        // 指定配置文件
        conf.addResource("hdfs-site.xml");
        // hdfs的文件系统对象
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop01:9000"), conf, "hadoop");

        Path src = new Path("/pom5.xml");

        //1.查看文件的信息  指定路径下的文件信息  参数1--需要查看的路径  参数2--是否级联  ls -R
        RemoteIterator<LocatedFileStatus> locatedFileStatusRemoteIterator = fileSystem.listFiles(src, false);
        //封装的文件状态 -- 文件的路径 文件的大小  文件的用户  组  文件的副本
        while (locatedFileStatusRemoteIterator.hasNext()) {
            System.out.println("------------------------------");
            LocatedFileStatus next = locatedFileStatusRemoteIterator.next();
            System.out.println(next.getPath());
            System.out.println("配置文件的分块大小：" + next.getBlockSize() / 1024 / 1024 + 'M');//获取的配置文件的分块大小   128M
            System.out.println("真实大小：" + next.getLen() / 1024 + 'K');//获取文件的真实大小  块的实际大小
            System.out.println("副本数量：" + next.getReplication());
            // BlockLocation   数据块的封装对象
            BlockLocation[] blockLocations = next.getBlockLocations();

            //[起始的偏移量, 当前块的实际大小, 副本存放的位置==> 文件的第一个块]
            System.out.println(Arrays.toString(blockLocations));
        }
        fileSystem.close();
    }
}
