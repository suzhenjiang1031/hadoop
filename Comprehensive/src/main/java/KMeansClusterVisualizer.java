import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartUtils;
import org.jfree.chart.JFreeChart;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;

public class KMeansClusterVisualizer {

    public static void visualizeResultsAndSaveImage(String outputPath, String imagePath) throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://master:9000");  // 根据你的 HDFS 配置修改

        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(outputPath + "/part-r-00000");

        if (!fs.exists(path)) {
            System.out.println("No result found.");
            return;
        }

        BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)));
        String line;
        XYSeriesCollection dataset = new XYSeriesCollection();

        while ((line = reader.readLine()) != null) {
            String[] parts = line.split("\t");
            if (parts.length < 2) continue;

            String cluster = parts[0];
            String[] values = parts[1].split(",");
            if (values.length < 2) continue;

            double quantity = Double.parseDouble(values[0]);
            double unitPrice = Double.parseDouble(values[1]);

            int clusterIndex = Integer.parseInt(cluster);
            XYSeries series;

            if (dataset.getSeriesCount() <= clusterIndex) {
                // 如果该 clusterIndex 的 series 还不存在，创建新 series
                series = new XYSeries("Cluster " + clusterIndex);
                dataset.addSeries(series);
            } else {
                // 否则，获取已有的 series
                series = dataset.getSeries(clusterIndex);
            }

            series.add(quantity, unitPrice);
        }

        reader.close();
        fs.close();

        JFreeChart scatterPlot = ChartFactory.createScatterPlot(
                "KMeans Cluster Centers",
                "Quantity",
                "UnitPrice",
                dataset
        );

        ChartUtils.saveChartAsPNG(new File(imagePath), scatterPlot, 800, 600);
        System.out.println("聚类中心散点图已保存为: " + imagePath);
    }

    public static void main(String[] args) {
        if (args.length != 2) {
            System.out.println("用法: java KMeansClusterVisualizer <HDFS输出路径> <图像保存路径>");
            System.exit(1);
        }

        String outputPath = args[0];
        String imagePath = args[1];

        try {
            visualizeResultsAndSaveImage(outputPath, imagePath);
        } catch (IOException e) {
            System.err.println("发生错误: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
