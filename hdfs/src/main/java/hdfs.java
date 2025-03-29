import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import java.io.*;
//202308230145 suzhenjiang

public class hdfs {
    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", "hdfs://master:9000");
            conf.set("fs.hdfs,impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
            FileSystem fs = FileSystem.get(conf);
            String fileName = "hdfs://master:9000/hdfs/hadoop_readme.txt";
            Path file = new Path(fileName);
            if(fs.exists(file)) {
                FSDataInputStream in = fs.open(file);
                BufferedReader br = new BufferedReader(new InputStreamReader(in));
                String content = null;
                while ((content = br.readLine()) != null) {
                    System.out.println(content);
                }
                br.close();
                in.close();
                fs.close();
            } else {
                InputStream in = new BufferedInputStream(new FileInputStream("/usr/local/hadoop/README.txt"));
                FSDataOutputStream out = fs.create(file);
                IOUtils.copyBytes(in, out, 4096, false);
                out.close();
                fs.close();
                System.out.println("create" + fileName + " success!");
            }
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
