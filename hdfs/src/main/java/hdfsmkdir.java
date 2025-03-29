import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.IOException;
import java.net.URI;
// 202308230145 suzhenjiang

public class hdfsmkdir {
    public static void main(String[] args) throws IOException, InterruptedException {
        try {
            String hdfsUri = "hdfs://master:9000";
            String directoryPath = "/user/hadoop/exercise";
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(URI.create(hdfsUri), conf,"hadoop");
            Path path = new Path(directoryPath);

            if (fs.exists(path)) {
                System.out.println("File already exists:" + path);
            } else {
                boolean isCreated = fs.mkdirs(path);
                if (isCreated) {
                    System.out.println("Directory created:" + path);
                } else {
                    System.out.println("Directory not created:" + path);
                }
            }
            fs.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
