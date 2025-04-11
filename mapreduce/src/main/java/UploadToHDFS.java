import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class UploadToHDFS {
    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://master:9000");

        FileSystem fs = FileSystem.get(conf);
        Path hdfsPath = new Path("/input/student_scores.txt");

        if (fs.exists(hdfsPath)) {
            fs.delete(hdfsPath, true);
        }

        try (FSDataOutputStream out = fs.create(hdfsPath)) {
            String data = "18001050101\t76\t81\t83\t81\t91\n" +
                    "18001050102\t75\t79\t69\t90\t86\n" +
                    "18001050104\t72\t85\t60\t77\t87\n" +
                    "18001050105\t76\t76\t78\t83\t88\n" +
                    "18001050106\t70\t76\t40\t73\t84\n" +
                    "18001050107\t80\t84\t65\t83\t91\n" +
                    "18001050108\t85\t92\t60\t78\t91\n" +
                    "18001050109\t80\t78\t41\t76\t90\n";

            out.write(data.getBytes("UTF-8"));
            out.flush();
        }

        System.out.println("Data written to HDFS: " + hdfsPath);

        fs.close();
    }
}