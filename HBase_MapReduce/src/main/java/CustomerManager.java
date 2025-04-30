import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class CustomerManager {
    public static void createTableAndInsertData(Connection connection) throws Exception {
        Admin admin = connection.getAdmin();
        TableName tableName = TableName.valueOf("customer");

        if (admin.tableExists(tableName)) {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            System.out.println("Table 'customer' already exists, deleted.");
        }

        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableName);
        ColumnFamilyDescriptorBuilder familyDescriptorBuilder =
                ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("info"));
        tableDescriptorBuilder.setColumnFamily(familyDescriptorBuilder.build());

        admin.createTable(tableDescriptorBuilder.build());
        System.out.println("Table 'customer' created successfully.");

        Table table = connection.getTable(tableName);

        // 插入第一行数据：cust102
        Put put1 = new Put(Bytes.toBytes("cust102"));
        put1.addColumn(Bytes.toBytes("info"), Bytes.toBytes("date"), Bytes.toBytes("2010-05-05"));
        put1.addColumn(Bytes.toBytes("info"), Bytes.toBytes("ip"), Bytes.toBytes("117.136.0.172"));
        put1.addColumn(Bytes.toBytes("info"), Bytes.toBytes("status"), Bytes.toBytes("1"));
        table.put(put1);

        // 插入第二行数据：cust103
        Put put2 = new Put(Bytes.toBytes("cust103"));
        put2.addColumn(Bytes.toBytes("info"), Bytes.toBytes("date"), Bytes.toBytes("2010-05-06"));
        put2.addColumn(Bytes.toBytes("info"), Bytes.toBytes("ip"), Bytes.toBytes("114.94.44.230"));
        put2.addColumn(Bytes.toBytes("info"), Bytes.toBytes("status"), Bytes.toBytes("0"));
        table.put(put2);

        System.out.println("Data inserted successfully into 'customer' table.");
        table.close();
    }

    // 查询 ID 为 cust103 的客户信息
    public static void queryCustomerById(Connection connection, String customerId) throws Exception {
        Table table = connection.getTable(TableName.valueOf("customer"));
        Get get = new Get(Bytes.toBytes(customerId));
        Result result = table.get(get);

        if (result.isEmpty()) {
            System.out.println("Customer with ID " + customerId + " not found.");
        } else {
            System.out.println("Customer ID: " + customerId);
            String date = Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("date")));
            String ip = Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("ip")));
            String status = Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("status")));
            System.out.println("Date: " + date);
            System.out.println("IP: " + ip);
            System.out.println("Status: " + status);
        }
        table.close();
    }

    // 查询状态为 1 的客户信息
    public static void queryCustomersByStatus(Connection connection, String status) throws Exception {
        Table table = connection.getTable(TableName.valueOf("customer"));
        Scan scan = new Scan();
        SingleColumnValueFilter filter = new SingleColumnValueFilter(
                Bytes.toBytes("info"),
                Bytes.toBytes("status"),
                CompareFilter.CompareOp.EQUAL,
                Bytes.toBytes(status)
        );
        scan.setFilter(filter);

        ResultScanner scanner = table.getScanner(scan);
        System.out.println("Customers with status " + status + ":");
        for (Result result : scanner) {
            String rowKey = Bytes.toString(result.getRow());
            String date = Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("date")));
            String ip = Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("ip")));
            String statusValue = Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("status")));
            System.out.println("Customer ID: " + rowKey);
            System.out.println("Date: " + date);
            System.out.println("IP: " + ip);
            System.out.println("Status: " + statusValue);
            System.out.println("---");
        }
        scanner.close();
        table.close();
    }

    // main 函数
    public static void main(String[] args) throws Exception {
        // 创建 HBase 配置
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "localhost");
        config.set("hbase.zookeeper.property.clientPort", "2181");

        // 创建 HBase 连接
        Connection connection = ConnectionFactory.createConnection(config);

        // 任务 1：创建表并插入数据
        createTableAndInsertData(connection);

        // 任务 2：查询 ID 为 cust103 的客户信息
        System.out.println("\nQuerying customer with ID 'cust103':");
        queryCustomerById(connection, "cust103");

        // 任务 3：查询状态为 1 的客户信息
        System.out.println("\nQuerying customers with status '1':");
        queryCustomersByStatus(connection, "1");

        // 关闭连接
        connection.close();
    }
}