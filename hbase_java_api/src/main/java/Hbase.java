import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

public class Hbase {
    public static void main(String[] args) throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        /// local docker
        configuration.set("hbase.zookeeper.quorum", "127.0.0.1");
        configuration.set("hbase.master", "127.0.0.1:16000");

        /// remote server
//        configuration.set("hbase.zookeeper.quorum", "emr-header-1,emr-worker-1,emr-worker-2");

        configuration.set("hbase.zookeeper.property.clientPort", "2181");
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
        String colFamilyInfo = "info";
        String colFamilyScore = "score";

        if (admin.tableExists(tableName)) {
            System.out.println("Table already exists");
        }else{
            HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
            HColumnDescriptor hColumnDescriptorInfo = new HColumnDescriptor(colFamilyInfo);
            HColumnDescriptor hColumnDescriptorScore = new HColumnDescriptor(colFamilyScore);
            hTableDescriptor.addFamily(hColumnDescriptorInfo);
            hTableDescriptor.addFamily(hColumnDescriptorScore);
            admin.createTable(hTableDescriptor);
            System.out.println("Table create successful");
        }

        ArrayList<Student> students = new ArrayList<>();
        students.add(new Student("Tom", "20210000000001", "1", "75", "82"));
        students.add(new Student("Jerry", "20210000000002", "1", "85", "67"));
        students.add(new Student("Jack", "20210000000003", "2", "80", "80"));
        students.add(new Student("Rose", "20210000000004", "2", "60", "61"));
        students.add(new Student("JulianChu", "G20210607020225", "3", "60", "61"));
        int count = 0;
        for (Student student : students) {
            String rowKey = student.getName();
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(colFamilyInfo), Bytes.toBytes("student_id"), Bytes.toBytes("20210000000001"));
            put.addColumn(Bytes.toBytes(colFamilyInfo), Bytes.toBytes("class"), Bytes.toBytes("1"));
            connection.getTable(tableName).put(put);
            count++;
        }
        System.out.println(count+" rows insert success");
    }
}

