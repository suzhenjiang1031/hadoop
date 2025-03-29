import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import java.io.IOException;
import java.net.URI;
//202308230145 suzhenjiang


public class hdfslook {
    public static void main(String[] args) {
        try {
            String uri = "hdfs://master:9000/user/hadoop/exercise/test.txt";
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(URI.create(uri), conf);
            FSDataInputStream in = null;
            in = fs.open(new Path(uri));
            byte buffer[] = new byte[256];
            int byteRead = 0;
            while ((byteRead = in.read(buffer)) > 0) {
                System.out.println(new String(buffer, 0, byteRead));
            }
            IOUtils.closeStream(in);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
