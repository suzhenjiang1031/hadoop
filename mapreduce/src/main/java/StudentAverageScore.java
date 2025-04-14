import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class StudentAverageScore {

    public static class ScoreMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {
        private Text studentID = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split("\t");

            if (fields.length == 6) {
                studentID.set(fields[0]);
                float sum = 0;
                for (int i = 1; i <= 5; i++) {
                    sum += Float.parseFloat(fields[i]);
                }
                float avg = sum / 5;
                context.write(studentID, new FloatWritable(avg));
            }
        }
    }

    public static class AvgReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
        @Override
        protected void reduce(Text key, Iterable<FloatWritable> values, Context context)
                throws IOException, InterruptedException {
            for (FloatWritable val : values) {
                context.write(key, val);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Student Average Score");

        job.setJarByClass(StudentAverageScore.class);
        job.setMapperClass(ScoreMapper.class);
        job.setReducerClass(AvgReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);

        FileInputFormat.addInputPath(job, new Path("hdfs://master:9000/input/student_scores.txt"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://master:9000/output/student_avg"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
