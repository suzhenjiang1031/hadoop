import javafx.scene.text.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class ClickCountSort {
    public static class ClickCountMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split("\t");
            if (fields.length == 2) {
                String productId = fields[0];
                int clickCount = Integer.parseInt(fields[1]);
                context.write(new IntWritable(clickCount), new Text(productId));
            }
        }

        public static class ClickCountReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
            @Override
            protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
                for (Text value : values) {
                    context.write(key, value);
                }
            }
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Click Count sort");

        job.setJarByClass(ClickCountSort.class);
        job.setMapperClass(ClickCountMapper.class);
        job.setReducerClass(ClickCountMapper.ClickCountReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path("/input/click_data.txt"));
        FileOutputFormat.setOutputPath(job, new Path("/output"));

        Path outputPath = new Path("/output");
        org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.get(conf);

        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        System.exit(job.waitForCompletion(true)?0:1);
    }
}
