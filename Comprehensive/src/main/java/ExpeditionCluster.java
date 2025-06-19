import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartUtils;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.data.category.DefaultCategoryDataset;

import java.io.*;
import java.util.*;

public class ExpeditionCluster {

    // 数据预处理：提取 ID, 年份, 成员数, 死亡数
    public static void preprocessData(String inputFile, String outputFile) throws IOException {
        List<String> processed = new ArrayList<>();
        BufferedReader reader = new BufferedReader(new FileReader(inputFile));
        String line = reader.readLine(); // skip header
        while ((line = reader.readLine()) != null) {
            String[] f = line.split(",");
            if (f.length < 13) continue;

            try {
                int members = Integer.parseInt(f[10]);
                int deaths = Integer.parseInt(f[11]);

                if (members <= 0 || deaths < 0) continue;

                String cleaned = String.join(",", f[0], f[3], String.valueOf(members), String.valueOf(deaths));
                processed.add(cleaned);
            } catch (Exception ignored) {
            }
        }
        reader.close();

        FileWriter writer = new FileWriter(outputFile);
        writer.write("ID,Year,Members,Deaths\n");
        for (String l : processed) writer.write(l + "\n");
        writer.close();
    }

    // Mapper：将每个数据点分配给最近的聚类中心
    public static class KMeansMapper extends Mapper<LongWritable, Text, Text, Text> {
        List<double[]> centroids = new ArrayList<>();
        Random rand = new Random();

        @Override
        protected void setup(Context context) {
            for (int i = 0; i < 3; i++) {
                centroids.add(new double[]{rand.nextDouble() * 20, rand.nextDouble() * 5});
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
            String[] f = value.toString().split(",");
            if (f[0].equals("ID") || f.length < 4) return;

            try {
                double members = Double.parseDouble(f[2]);
                double deaths = Double.parseDouble(f[3]);

                double[] point = {members, deaths};
                int closest = 0;
                double min = Double.MAX_VALUE;
                for (int i = 0; i < centroids.size(); i++) {
                    double[] c = centroids.get(i);
                    double dist = Math.pow(point[0] - c[0], 2) + Math.pow(point[1] - c[1], 2);
                    if (dist < min) {
                        min = dist;
                        closest = i;
                    }
                }

                ctx.write(new Text(String.valueOf(closest)), new Text(members + "," + deaths));
            } catch (Exception ignored) {
            }
        }
    }

    // Reducer：计算每个聚类的平均值
    public static class KMeansReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context ctx) throws IOException, InterruptedException {
            double sumM = 0, sumD = 0;
            int count = 0;

            for (Text val : values) {
                String[] f = val.toString().split(",");
                sumM += Double.parseDouble(f[0]);
                sumD += Double.parseDouble(f[1]);
                count++;
            }

            if (count > 0) {
                double avgM = sumM / count;
                double avgD = sumD / count;
                ctx.write(key, new Text(avgM + "," + avgD));
            }
        }
    }

    // Hadoop 任务执行
    public static void runKMeansJob(String input, String output) throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://master:9000");
        Job job = Job.getInstance(conf, "Expedition Clustering");
        job.setJarByClass(ExpeditionCluster.class);
        job.setMapperClass(KMeansMapper.class);
        job.setReducerClass(KMeansReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(input));
        Path out = new Path(output);
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(out)) fs.delete(out, true);
        FileOutputFormat.setOutputPath(job, out);

        job.waitForCompletion(true);
    }

    // 柱状图绘图 + 控制台输出
    public static void plotAndPrint(String outputPath) throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://master:9000");
        FileSystem fs = FileSystem.get(conf);
        Path outFile = new Path(outputPath + "/part-r-00000");

        if (!fs.exists(outFile)) {
            System.out.println("Output file not found.");
            return;
        }

        BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(outFile)));
        DefaultCategoryDataset dataset = new DefaultCategoryDataset();

        System.out.println("=== 聚类中心输出 ===");
        String line;
        while ((line = reader.readLine()) != null) {
            String[] parts = line.split("\t");
            if (parts.length < 2) continue;
            String cluster = "Cluster " + parts[0];
            String[] vals = parts[1].split(",");
            if (vals.length < 2) continue;

            double members = Double.parseDouble(vals[0]);
            double deaths = Double.parseDouble(vals[1]);

            System.out.printf("%s -> 平均成员数: %.2f, 平均死亡数: %.2f%n", cluster, members, deaths);
            dataset.addValue(members, "Members", cluster);
            dataset.addValue(deaths, "Deaths", cluster);
        }

        JFreeChart chart = ChartFactory.createBarChart(
                "Annapurna II Expedition Clustering",
                "Cluster",
                "Value",
                dataset,
                PlotOrientation.VERTICAL,
                true, true, false
        );

        File image = new File("expedition_cluster_bar.png");
        ChartUtils.saveChartAsPNG(image, chart, 800, 600);
        System.out.println("柱状图已保存为: " + image.getAbsolutePath());
    }

    // 本地文件上传到 HDFS
    public static void uploadToHDFS(String local, String hdfs) throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://master:9000");
        FileSystem fs = FileSystem.get(conf);

        Path src = new Path(local);
        Path dst = new Path(hdfs);
        if (!fs.exists(dst.getParent())) fs.mkdirs(dst.getParent());
        fs.copyFromLocalFile(src, dst);
        fs.close();
        System.out.println("Uploaded to HDFS: " + hdfs);
    }

    // 主程序入口
    public static void main(String[] args) throws Exception {
        String localCsv = "/home/hadoop/Desktop/data/expeditions.csv";
        String cleaned = "cleaned_expeditions.csv";
        String hdfsIn = "hdfs://master:9000/data/expeditions/input.csv";
        String hdfsOut = "hdfs://master:9000/data/expeditions/output";

        preprocessData(localCsv, cleaned);
        uploadToHDFS(cleaned, hdfsIn);
        runKMeansJob(hdfsIn, hdfsOut);
        plotAndPrint(hdfsOut);
    }
}
