import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
//202308230145 suzhenjiang

public class hdfsCopyFile {
    public static void main(String[] args) {
        String src;
        String dst;
        if (args.length == 2) {
            src = args[0];
            dst = args[1];
        } else {
            src = "/user/hadoop/exercise/test.txt";
            dst = "/user/hadoop/new_test.txt";
        }

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://master:9000");

        try {
            FileSystem fs = FileSystem.get(conf);

            Path srcPath = new Path(src);
            Path dstPath = new Path(dst);

            if (!fs.exists(srcPath)) {
                System.err.println("源文件不存在: " + srcPath);
                System.exit(1);
            }

            Path dstParent = dstPath.getParent();
            if (!fs.exists(dstParent)) {
                fs.mkdirs(dstParent);
                System.out.println("创建目标目录: " + dstParent);
            }

            try (FSDataInputStream in = fs.open(srcPath);
                 FSDataOutputStream out = fs.create(dstPath, true)) {
                byte[] buf = new byte[4096];
                int bytesRead;
                long totalBytes = 0;

                while ((bytesRead = in.read(buf)) > 0) {
                    out.write(buf, 0, bytesRead);
                    totalBytes += bytesRead;
                }

                System.out.println("文件复制成功！从 " + srcPath + " 复制到 " + dstPath + "，共 " + totalBytes + " 字节");
            }

        } catch (IOException e) {
            System.err.println("文件复制失败: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
}