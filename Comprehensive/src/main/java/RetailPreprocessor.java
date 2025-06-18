import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;

// 可视化导入
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartUtils;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;

public class RetailPreprocessor {

    static {
        System.setProperty("org.apache.htrace.core.tracer", "false");
    }

    public static void preprocessData(String inputFile, String outputFile) throws IOException {
        List<String> processedLines = new ArrayList<>();
        BufferedReader reader = new BufferedReader(new FileReader(inputFile));
        String line;

        reader.readLine(); // skip header
        while ((line = reader.readLine()) != null) {
            String[] fields = line.split(",");
            if (fields.length < 8) continue;

            String customerID = fields[6].isEmpty() ? "0" : fields[6];
            String quantity = fields[3].isEmpty() ? "0" : fields[3];
            String unitPrice = fields[5].isEmpty() ? "0.0" : fields[5];

            int qty;
            double price;
            try {
                qty = Integer.parseInt(quantity);
                price = Double.parseDouble(unitPrice);
                if (qty < 0 || price < 0) continue;
            } catch (NumberFormatException e) {
                continue;
            }

            String invoiceDate = fields[4];
            try {
                SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy HH:mm");
                sdf.parse(invoiceDate);
            } catch (Exception e) {
                invoiceDate = "01/01/2011 00:00";
            }

            String processedLine = String.join(",", fields[0], fields[1], fields[2], quantity, invoiceDate, unitPrice, customerID, fields[7]);
            processedLines.add(processedLine);
        }
        reader.close();

        try (FileWriter writer = new FileWriter(outputFile)) {
            writer.write("InvoiceNo,StockCode,Description,Quantity,InvoiceDate,UnitPrice,CustomerID,Country\n");
            for (String processedLine : processedLines) {
                writer.write(processedLine + "\n");
            }
        }

        System.out.println("Data preprocessed and saved to: " + outputFile);
    }

    public static void uploadToHDFS(String localPath, String hdfsPath) throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://master:9000");
        FileSystem fs = FileSystem.get(conf);

        Path src = new Path(localPath);
        Path dst = new Path(hdfsPath);
        if (!fs.exists(dst.getParent())) {
            fs.mkdirs(dst.getParent());
        }
        fs.copyFromLocalFile(src, dst);
        fs.close();

        System.out.println("File uploaded to HDFS: " + hdfsPath);
    }

    public static void storeToHBase(String inputFile) throws IOException {
        Configuration hbaseConf = HBaseConfiguration.create();
        hbaseConf.set("hbase.zookeeper.quorum", "master");
        hbaseConf.set("hbase.zookeeper.property.clientPort", "2181");

        Connection connection = ConnectionFactory.createConnection(hbaseConf);
        TableName tableName = TableName.valueOf("RetailData");

        Admin admin = connection.getAdmin();
        if (!admin.tableExists(tableName)) {
            System.err.println("HBase table 'RetailData' does not exist.");
            admin.close();
            connection.close();
            return;
        }
        admin.close();

        Table table = connection.getTable(tableName);
        BufferedReader reader = new BufferedReader(new FileReader(inputFile));
        reader.readLine(); // skip header
        String line;
        int count = 0;
        List<Put> puts = new ArrayList<>();

        while ((line = reader.readLine()) != null) {
            String[] fields = line.split(",");
            if (fields.length < 8) continue;

            String rowKey = fields[0] + "_" + fields[6];
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("StockCode"), Bytes.toBytes(fields[1]));
            put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("Description"), Bytes.toBytes(fields[2]));
            put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("Quantity"), Bytes.toBytes(fields[3]));
            put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("InvoiceDate"), Bytes.toBytes(fields[4]));
            put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("UnitPrice"), Bytes.toBytes(fields[5]));
            put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("CustomerID"), Bytes.toBytes(fields[6]));
            put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("Country"), Bytes.toBytes(fields[7]));

            puts.add(put);
            count++;

            if (count % 1000 == 0) {
                table.put(puts);
                puts.clear();
                System.out.println("Stored " + count + " records to HBase...");
            }
        }

        if (!puts.isEmpty()) {
            table.put(puts);
        }

        table.close();
        connection.close();
        System.out.println("Total records stored to HBase: " + count);
    }

    public static class KMeansMapper extends Mapper<LongWritable, Text, Text, Text> {
        private final List<double[]> centroids = new ArrayList<>();
        private final Random random = new Random();

        @Override
        protected void setup(Context context) {
            for (int i = 0; i < 3; i++) {
                centroids.add(new double[]{random.nextDouble() * 100, random.nextDouble() * 10});
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            if (fields.length < 6 || fields[0].equals("InvoiceNo")) return;

            try {
                double quantity = Double.parseDouble(fields[3]);
                double unitPrice = Double.parseDouble(fields[5]);
                double[] point = {quantity, unitPrice};

                int closest = 0;
                double minDist = Double.MAX_VALUE;
                for (int i = 0; i < centroids.size(); i++) {
                    double[] c = centroids.get(i);
                    double dist = Math.pow(point[0] - c[0], 2) + Math.pow(point[1] - c[1], 2);
                    if (dist < minDist) {
                        minDist = dist;
                        closest = i;
                    }
                }
                context.write(new Text(String.valueOf(closest)), new Text(quantity + "," + unitPrice));
            } catch (Exception ignored) {
            }
        }
    }

    public static class KMeansReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double sumQ = 0, sumP = 0;
            int count = 0;

            for (Text val : values) {
                String[] parts = val.toString().split(",");
                try {
                    sumQ += Double.parseDouble(parts[0]);
                    sumP += Double.parseDouble(parts[1]);
                    count++;
                } catch (Exception ignored) {
                }
            }

            if (count > 0) {
                context.write(key, new Text((sumQ / count) + "," + (sumP / count)));
            }
        }
    }

    public static void runKMeansJob(String input, String output) throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://master:9000");

        Job job = Job.getInstance(conf, "KMeans Clustering");
        job.setJarByClass(RetailPreprocessor.class);
        job.setMapperClass(KMeansMapper.class);
        job.setReducerClass(KMeansReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(input));

        Path outPath = new Path(output);
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outPath)) fs.delete(outPath, true);
        FileOutputFormat.setOutputPath(job, outPath);

        job.waitForCompletion(true);
        System.out.println("KMeans MapReduce job finished.");
    }

    public static void plotClusters(String outputPath) throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://master:9000");
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(outputPath + "/part-r-00000");

        if (!fs.exists(path)) {
            System.out.println("No result found.");
            return;
        }

        BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)));
        XYSeriesCollection dataset = new XYSeriesCollection();
        Map<String, XYSeries> seriesMap = new HashMap<>();

        String line;
        while ((line = reader.readLine()) != null) {
            String[] parts = line.split("\t");
            if (parts.length < 2) continue;

            String clusterId = parts[0];
            String[] values = parts[1].split(",");
            if (values.length < 2) continue;

            try {
                double quantity = Double.parseDouble(values[0]);
                double price = Double.parseDouble(values[1]);

                String seriesKey = "Cluster " + clusterId;
                XYSeries series = seriesMap.get(seriesKey);
                if (series == null) {
                    series = new XYSeries(seriesKey);
                    seriesMap.put(seriesKey, series);
                    dataset.addSeries(series);
                }

                series.add(quantity, price);
            } catch (NumberFormatException ignored) {
            }
        }

        reader.close();
        fs.close();

        JFreeChart chart = ChartFactory.createScatterPlot(
                "KMeans Clustering Result",
                "Quantity",
                "UnitPrice",
                dataset,
                PlotOrientation.VERTICAL,
                true, true, false
        );

        File chartFile = new File("cluster_result.png");
        ChartUtils.saveChartAsPNG(chartFile, chart, 800, 600);
        System.out.println("Cluster result visualized: " + chartFile.getAbsolutePath());
    }

    public static void main(String[] args) throws Exception {
        String localInput = "/home/hadoop/Desktop/data/online_retail.csv";
        String preprocessedOutput = "preprocessed_retail.csv";
        String hdfsInput = "hdfs://master:9000/data/retail/online_retail.csv";
        String hdfsOutput = "hdfs://master:9000/data/retail/output";

        preprocessData(localInput, preprocessedOutput);
        uploadToHDFS(preprocessedOutput, hdfsInput);
        storeToHBase(preprocessedOutput);
        runKMeansJob(hdfsInput, hdfsOutput);
        plotClusters(hdfsOutput);
    }
}
