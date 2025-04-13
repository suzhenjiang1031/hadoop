import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class StudentAverageScore {
    public static class AverageScoreMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split("\t");
            if (fields.length == 6) {
                String studentId = fields[0];
                try {
                    double sum = 0;
                    for (int i = 1; i < fields.length; i++) {
                        sum += Integer.parseInt(fields[i]);
                    }
                    double average = sum / 5.0;
                    context.write(new Text(studentId), new DoubleWritable(average));
                    context.getCounter("Mapper", "Records Processed").increment(1);
                } catch (NumberFormatException e) {
                    context.getCounter("Mapper", "Invalid Scores").increment(1);
                }
            } else {
                context.getCounter("Mapper", "Malformed Lines").increment(1);
            }
        }
    }

    public static class AverageScoreReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            for (DoubleWritable value : values) {
                context.write(key, value);
            }
        }
    }

    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", "hdfs://master:9000");
            conf.set("mapreduce.framework.name", "yarn");
            conf.set("yarn.resourcemanager.hostname", "master");

            System.out.println("Configuration set: fs.defaultFS=hdfs://master:9000, mapreduce.framework.name=yarn");

            // 测试 HDFS 连接
            org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.get(conf);
            Path inputPath = new Path("hdfs://master:9000/input/student_scores.txt");
            if (!fs.exists(inputPath)) {
                System.err.println("Input file does not exist: " + inputPath);
                System.exit(1);
            } else {
                System.out.println("Input file exists: " + inputPath);
            }

            Job job = Job.getInstance(conf, "Student Average Score");

            job.setJarByClass(StudentAverageScore.class);
            job.setMapperClass(AverageScoreMapper.class);
            job.setReducerClass(AverageScoreReducer.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(DoubleWritable.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(DoubleWritable.class);

            FileInputFormat.addInputPath(job, inputPath);
            FileOutputFormat.setOutputPath(job, new Path("hdfs://master:9000/output"));

            Path outputPath = new Path("hdfs://master:9000/output");
            if (fs.exists(outputPath)) {
                fs.delete(outputPath, true);
                System.out.println("Deleted existing output path: /output");
            }

            System.out.println("Starting job...");
            boolean success = job.waitForCompletion(true);
            if (success) {
                System.out.println("Job completed successfully!");
            } else {
                System.out.println("Job failed!");
            }
            System.exit(success ? 0 : 1);

        } catch (IOException e) {
            System.err.println("IOException occurred: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        } catch (InterruptedException e) {
            System.err.println("InterruptedException occurred: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        } catch (ClassNotFoundException e) {
            System.err.println("ClassNotFoundException occurred: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
}