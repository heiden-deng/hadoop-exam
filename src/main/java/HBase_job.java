import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.jruby.RubyProcess;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HBase_job {

    public static void AdminTest() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();
        TableName tbname = TableName.valueOf("testtable");
        HTableDescriptor desc = new HTableDescriptor(tbname);
        HColumnDescriptor coldesc = new HColumnDescriptor(Bytes.toBytes("fam1"));
        desc.addFamily(coldesc);
        admin.createTable(desc);
        boolean avail = admin.isTableAvailable(TableName.valueOf("GoodsOrders"));
        System.out.println(avail);
        HColumnDescriptor cold3 = new HColumnDescriptor(Bytes.toBytes("fam2"));
        desc.addFamily(cold3);
        admin.disableTable(tbname);
        admin.modifyTable(tbname,desc);
        admin.enableTable(tbname);
        admin.close();
        connection.close();
    }

    public static void putTest() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf("testtable"));
        Put put = new Put(Bytes.toBytes("row1"));
        put.addColumn(Bytes.toBytes("fam1"),Bytes.toBytes("qua1"),Bytes.toBytes("val1"));
        put.addColumn(Bytes.toBytes("fam1"),Bytes.toBytes("qua2"),Bytes.toBytes("val2"));
        table.put(put);
        List<Put> puts = new ArrayList<Put>();
        Put put1 = new Put(Bytes.toBytes("row2"));
        put1.addColumn(Bytes.toBytes("fam1"),Bytes.toBytes("qua1"),Bytes.toBytes("val10"));
        puts.add(put1);
        Put put2 = new Put(Bytes.toBytes("row3"));
        put2.addColumn(Bytes.toBytes("fam1"),Bytes.toBytes("qua2"),Bytes.toBytes("val20"));
        puts.add(put2);
        table.put(puts);
        table.close();
        connection.close();
    }

    public static void printCell(Result result){
        for (Cell cell: result.rawCells()){
            System.out.println("行键:" + new String(CellUtil.cloneRow(cell)));
            System.out.println("列族:" + new String(CellUtil.cloneFamily(cell)));
            System.out.println("列:" + new String(CellUtil.cloneQualifier(cell)));
            System.out.println("值:" + new String(CellUtil.cloneValue(cell)));
            System.out.println("时间戳:" + cell.getTimestamp());
        }
    }
    public static void printScan(ResultScanner rs){
        for (Result result: rs){
            System.out.println("========================");
            printCell(result);
        }
    }

    public static void getTest() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf("testtable"));
        Get get = new Get(Bytes.toBytes("row1"));
        get.setMaxVersions(3);
        get.addColumn(Bytes.toBytes("fam1"),Bytes.toBytes("qua1"));
        Result result = table.get(get);
        printCell(result);
        table.close();
        connection.close();
    }

    public static void scanTest() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf("testtable"));
        Scan scan = new Scan();
        scan.setStartRow(Bytes.toBytes("row2"));
        scan.setStopRow(Bytes.toBytes("row8"));
        scan.setMaxVersions(3);
        scan.setCaching(20);
        scan.setBatch(10);
        ResultScanner resultScanner = table.getScanner(scan);
        printScan(resultScanner);
        resultScanner.close();
        table.close();
        connection.close();
    }



    public static void scan2Test() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf("testtable"));
        Scan scan = new Scan();
        Filter filter1 = new RowFilter(CompareOperator.LESS_OR_EQUAL,new BinaryComparator(Bytes.toBytes("row2")));
        scan.setFilter(filter1);

        ResultScanner rs1 = table.getScanner(scan);
        printScan(rs1);
        rs1.close();

        System.out.println("<-------- value = val50 ------------->");
        Filter filter2 = new ValueFilter(CompareOperator.EQUAL,new SubstringComparator("val50"));
        Scan scan2 = new Scan();
        scan2.setFilter(filter2);
        ResultScanner rs2 = table.getScanner(scan2);
        printScan(rs2);
        rs2.close();

        table.close();
        connection.close();
    }

    public static void main(String[] args){
        try {
            //AdminTest();
            //putTest();
            //getTest();
            //scanTest();
            scan2Test();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
