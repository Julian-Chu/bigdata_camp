import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Arrays;

public class Hbase {
    public static void main(String[] args) throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "127.0.0.1");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        configuration.set("hbase.master", "127.0.0.1:16000");
        Connection connection = ConnectionFactory.createConnection(configuration);
        Admin admin = connection.getAdmin();
        String myNamespace = "julianchu";

        NamespaceDescriptor nsDesc = NamespaceDescriptor.create(myNamespace).build();
        NamespaceDescriptor[] namespaceDescriptors = admin.listNamespaceDescriptors();
        if(Arrays.stream(namespaceDescriptors).noneMatch(desc-> desc.getName().equals(nsDesc.getName()))){
            admin.createNamespace(nsDesc);
        }else{
            System.out.println("namespace exists");
        }

        TableName tableName = TableName.valueOf(myNamespace,"student");
        String colFamily = "info";
        String rowKey = "Tom";

        if (admin.tableExists(tableName)) {
            System.out.println("Table already exists");
        }else{
            HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(colFamily);
            hTableDescriptor.addFamily(hColumnDescriptor);
            admin.createTable(hTableDescriptor);
            System.out.println("Table create successful");
        }

        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes("student_id"), Bytes.toBytes("20210000000001"));
        put.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes("class"), Bytes.toBytes("1"));
        connection.getTable(tableName).put(put);
        System.out.println("Data insert success");
    }
}

