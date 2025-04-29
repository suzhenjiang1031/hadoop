import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

public class AverageScore {

    public static class AverageMapper extends TableMapper<ImmutableBytesWritable, DoubleWritable> {
        @Override
        protected void map(ImmutableBytesWritable row, Result value, Context context)
                throws IOException, InterruptedException {
            double sum = 0;
            int count = 0;

            for (int i = 1; i <= 5; i++) {
                String scoreColumn = "score" + i;
                byte[] scoreBytes = value.getValue(Bytes.toBytes("grades"), Bytes.toBytes(scoreColumn));
                if (scoreBytes != null) {
                    double score = Double.parseDouble(Bytes.toString(scoreBytes));
                    sum += score;
                    count++;
                }
            }

            if (count > 0) {
                double average = sum / count;
                context.write(row, new DoubleWritable(average));
            }
        }
    }

    public static class AverageReducer extends TableReducer<ImmutableBytesWritable, DoubleWritable, ImmutableBytesWritable> {
        @Override
        protected void reduce(ImmutableBytesWritable key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {
            for (DoubleWritable value : values) {
                Put put = new Put(key.get());
                put.addColumn(Bytes.toBytes("avg"), Bytes.toBytes("average"),
                        Bytes.toBytes(String.valueOf(value.get())));
                context.write(key, put);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        Job job = Job.getInstance(conf, "AverageScoreJob");
        job.setJarByClass(AverageScore.class);

        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes("grades"));
        TableMapReduceUtil.initTableMapperJob(
                TableName.valueOf("scorelist"),
                scan,
                AverageMapper.class,
                ImmutableBytesWritable.class,
                DoubleWritable.class,
                job);

        TableMapReduceUtil.initTableReducerJob(
                "scoreaverage",
                AverageReducer.class,
                job);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}