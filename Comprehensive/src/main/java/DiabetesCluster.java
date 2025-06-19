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

public class DiabetesCluster {

    // 数据预处理，提取age、bmi、HbA2c_level、blood_glucose_level
    public static void preprocessData(String inputFile, String outputFile) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(inputFile));
        FileWriter writer = new FileWriter(outputFile);

        String line = reader.readLine(); // skip header
        writer.write("age,bmi,HbA1c_level,blood_glucose_level\n");

        while ((line = reader.readLine()) != null) {
            String[] f = line.split(",");
            if (f.length < 9) continue;

            try {
                double age = Double.parseDouble(f[1]);
                double bmi = Double.parseDouble(f[5]);
                double hba1c = Double.parseDouble(f[6]);
                double glucose = Double.parseDouble(f[7]);

                writer.write(age + "," + bmi + "," + hba1c + "," + glucose + "\n");
            } catch (Exception ignored) {}
        }

        reader.close();
        writer.close();
    }

    // Mapper：计算最近聚类中心
    public static class KMeansMapper extends Mapper<LongWritable, Text, Text, Text> {
        List<double[]> centroids = new ArrayList<>();
        Random rand = new Random();

        @Override
        protected void setup(Context context) {
            for (int i = 0; i < 3; i++) {
                centroids.add(new double[]{
                        rand.nextDouble() * 100,  // age
                        rand.nextDouble() * 40,   // bmi
                        rand.nextDouble() * 10,   // HbA1c
                        rand.nextDouble() * 250   // glucose
                });
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
            String[] f = value.toString().split(",");
            if (f[0].equals("age") || f.length < 4) return;

            try {
                double age = Double.parseDouble(f[0]);
                double bmi = Double.parseDouble(f[1]);
                double hba1c = Double.parseDouble(f[2]);
                double glucose = Double.parseDouble(f[3]);

                double[] point = {age, bmi, hba1c, glucose};
                int closest = 0;
                double minDist = Double.MAX_VALUE;

                for (int i = 0; i < centroids.size(); i++) {
                    double[] c = centroids.get(i);
                    double dist = 0;
                    for (int j = 0; j < c.length; j++) {
                        dist += Math.pow(point[j] - c[j], 2);
                    }
                    if (dist < minDist) {
                        minDist = dist;
                        closest = i;
                    }
                }

                ctx.write(new Text(String.valueOf(closest)),
                        new Text(age + "," + bmi + "," + hba1c + "," + glucose));
            } catch (Exception ignored) {}
        }
    }

    // Reducer：更新聚类中心
    public static class KMeansReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context ctx) throws IOException, InterruptedException {
            double[] sum = new double[4];
            int count = 0;

            for (Text val : values) {
                String[] f = val.toString().split(",");
                for (int i = 0; i < 4; i++) {
                    sum[i] += Double.parseDouble(f[i]);
                }
                count++;
            }

            if (count > 0) {
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < 4; i++) {
                    sb.append(String.format("%.2f", sum[i] / count));
                    if (i < 3) sb.append(",");
                }
                ctx.write(key, new Text(sb.toString()));
            }
        }
    }

    // 运行 MapReduce 任务
    public static void runKMeansJob(String input, String output) throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://master:9000");

        Job job = Job.getInstance(conf, "Diabetes Clustering");
        job.setJarByClass(DiabetesCluster.class);
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

    // 可视化
    public static void plotAndPrint(String outputPath) throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://master:9000");
        FileSystem fs = FileSystem.get(conf);
        Path file = new Path(outputPath + "/part-r-00000");

        if (!fs.exists(file)) {
            System.out.println("Output not found.");
            return;
        }

        BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(file)));
        DefaultCategoryDataset dataset = new DefaultCategoryDataset();

        System.out.println("=== 聚类中心 ===");
        String line;
        while ((line = reader.readLine()) != null) {
            String[] parts = line.split("\t");
            if (parts.length < 2) continue;

            String cluster = "Cluster " + parts[0];
            String[] vals = parts[1].split(",");

            dataset.addValue(Double.parseDouble(vals[0]), "Age", cluster);
            dataset.addValue(Double.parseDouble(vals[1]), "BMI", cluster);
            dataset.addValue(Double.parseDouble(vals[2]), "HbA1c", cluster);
            dataset.addValue(Double.parseDouble(vals[3]), "Glucose", cluster);

            System.out.printf("%s -> age=%.2f, bmi=%.2f, HbA1c=%.2f, glucose=%.2f%n",
                    cluster, Double.parseDouble(vals[0]), Double.parseDouble(vals[1]),
                    Double.parseDouble(vals[2]), Double.parseDouble(vals[3]));
        }

        JFreeChart chart = ChartFactory.createBarChart(
                "Diabetes Cluster Centers",
                "Cluster",
                "Value",
                dataset,
                PlotOrientation.VERTICAL,
                true, true, false
        );

        File img = new File("diabetes_cluster.png");
        ChartUtils.saveChartAsPNG(img, chart, 800, 600);
        System.out.println("柱状图已保存: " + img.getAbsolutePath());
    }

    // 上传数据到 HDFS
    public static void uploadToHDFS(String local, String hdfs) throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://master:9000");
        FileSystem fs = FileSystem.get(conf);

        Path src = new Path(local);
        Path dst = new Path(hdfs);
        if (!fs.exists(dst.getParent())) fs.mkdirs(dst.getParent());
        fs.copyFromLocalFile(src, dst);
        fs.close();
        System.out.println("已上传至 HDFS: " + hdfs);
    }

    // 主方法
    public static void main(String[] args) throws Exception {
        String localCsv = "/home/hadoop/Desktop/data/diabetes_prediction_dataset.csv";
        String cleaned = "cleaned_diabetes.csv";
        String hdfsIn = "hdfs://master:9000/data/diabetes/input.csv";
        String hdfsOut = "hdfs://master:9000/data/diabetes/output";

        preprocessData(localCsv, cleaned);
        uploadToHDFS(cleaned, hdfsIn);
        runKMeansJob(hdfsIn, hdfsOut);
        plotAndPrint(hdfsOut);
    }
}
