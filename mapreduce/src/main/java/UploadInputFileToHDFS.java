import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class UploadInputFileToHDFS {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://master:9000");

        try {
            FileSystem fs = FileSystem.get(conf);
            Path hdfsPath = new Path("/input/click_data.txt");

            if (fs.exists(hdfsPath)) {
                fs.delete(hdfsPath, true);
            }

            FSDataOutputStream out = fs.create(hdfsPath);
            String inputData =
                    "1010037\t100\n" +
                            "1010102\t100\n" +
                            "1010152\t97\n" +
                            "1010178\t96\n" +
                            "1010280\t104\n" +
                            "1010320\t103\n" +
                            "1010510\t104\n" +
                            "1010603\t96\n" +
                            "1010637\t97\n";
            out.writeUTF(inputData);
            out.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}

