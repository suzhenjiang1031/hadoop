import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.GenericOptionsParser;

public class HDFS {
//    public static void main(String[] args) {
//        try {
//            Configuration conf = new Configuration();
//            conf.set("fs.defaultFS", "hdfs://master:9000");
//            conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
//            FileSystem fs = FileSystem.get(conf);
//            String[] allArgs = (new GenericOptionsParser(conf, args)).getRemainingArgs();
//            String fileName = allArgs[0];
//
//            Path file = new Path(fileName);
//            if (fs.exists(file)) {
//                FSDataInputStream in = fs.open(file);
//                BufferedReader br = new BufferedReader(new InputStreamReader(in));
//                String content = null;
//                while ((content = br.readLine()) != null) {
//                    System.out.println(content);
//                }
//                br.close();
//                in.close();
//                fs.close();
//            } else {
//                InputStream in = new BufferedInputStream(
//                        new FileInputStream("/usr/local/hadoop/README.txt"));
//                FSDataOutputStream out = fs.create(file);
//                IOUtils.copyBytes(in, out, 4096, true);
//                out.close();
//                fs.close();
//                System.out.print("create " + fileName + " successful!");
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }

    public static void FileExists() throws Exception {
        String uri = "hdfs://master:9000/hdfs/word.txt";
        Configuration conf  = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(uri), conf);
        if (fs.exists(new Path(uri))) {
            System.out.println("File exists");
        } else {
            System.out.println("File does not exist");
        }

    }

    public static void main(String[] args) {
        try {
            FileExists();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}