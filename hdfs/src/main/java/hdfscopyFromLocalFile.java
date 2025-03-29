import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.IOException;
import java.net.URI;
//202308230145 suzhenjiang

public class hdfscopyFromLocalFile {
    public static void main(String[] args) {
        try {
            String src = "/home/hadoop/Desktop/test.txt";
            String dst = "hdfs://master:9000/user/hadoop/exercise";
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(URI.create(dst), conf);
            fs.copyFromLocalFile(new Path(src), new Path(dst));
            Path path = new Path("hdfs://master:9000/user/hadoop/exercise/test.txt");

            if (fs.exists(path)) {
                System.out.println("Successful added!");
            } else {
                System.out.println("Failed!");
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
