package com.simon.hbasetest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HbaseTest {

    public static Configuration conf;
    public static Connection connection;
    public static HBaseAdmin admin;

    // 获取配置信息
    static {
        conf = HBaseConfiguration.create();
        try {
            // 对表操作需要用HbaseAdmin
            connection = ConnectionFactory.createConnection(conf);
            // 管理表
            admin = (HBaseAdmin)connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // 1. 判断一张表是否存在
    public static boolean isExist(String tbName) throws IOException {
//        return admin.tableExists(tbName);
        return admin.tableExists(TableName.valueOf(tbName));
    }

    // 2. 创建表
    public static void createTable(String tbName, String... columnFamily) throws IOException{
        // 1. 如果表已存在
        if(isExist(tbName)) {
            System.out.println("tbale has been created before!");
        } else {
            // 2. 创建表， 需要创建一个描述器
            HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tbName));

            // 3. 创建列族
            for (String cf: columnFamily) {
                htd.addFamily(new HColumnDescriptor(cf));
            }

            // 4. 创建表
            admin.createTable(htd);
            System.out.println("success");
        }
    }

    // 3. 删除表
    public static void deleteTable(String tbName) throws IOException {
        if(isExist(tbName)) {
            // 1. 需要先指定表不可用
            admin.disableTable(TableName.valueOf(tbName));

            // 2. 删除表
            admin.deleteTable(TableName.valueOf(tbName));

            System.out.println("success");
        } else {
            System.out.println("table is not exist");
        }
    }

    // 4. 添加数据
    public static void addRow(String tbName, String rowkey, String cf, String column, String value) throws IOException {
        // 拿到表对象
        Table t = connection.getTable(TableName.valueOf(tbName));

        // 1. 用put方式加入数据
        Put p = new Put(Bytes.toBytes(rowkey));
        // 2. 加入数据
        p.addColumn(Bytes.toBytes(cf), Bytes.toBytes(column), Bytes.toBytes(value));

        t.put(p);
    }

    // 5. 根据rowkey删除
    public static void deleteRow(String tbName, String rowkey) throws IOException {
        // 拿到表对象
        Table t = connection.getTable(TableName.valueOf(tbName));

        // 1. 根据rowkey删除数据
        Delete d = new Delete(Bytes.toBytes(rowkey));
        t.delete(d);
    }

    // 6. 删除多行rowkey
    public static void deleteAll(String tbName, String... rowkeys) throws IOException {
        Table t = connection.getTable(TableName.valueOf(tbName));

        // 1. 把delete封装到集合
        List<Delete> list = new ArrayList<Delete>();
        // 2. 遍历
        for (String row: rowkeys) {
            Delete d = new Delete(Bytes.toBytes(row));
            list.add(d);
        }
        t.delete(list);
    }

    // 7. 全表扫描
    public static void scanAll(String tbName) throws IOException {
        // 拿到表对象
        Table t = connection.getTable(TableName.valueOf(tbName));

        // 1. 实例scan
        Scan s = new Scan();
        // 2. 拿到Scanner对象
        ResultScanner rs = t.getScanner(s);

        // 3.遍历
        for (Result r: rs) {
            Cell[] cells = r.rawCells();
            for (Cell c: cells) {
                System.out.println("行键为: " + Bytes.toString(CellUtil.cloneRow(c)));
                System.out.println("列族为: " + Bytes.toString(CellUtil.cloneFamily(c)));
                System.out.println("列名为: " + Bytes.toString(CellUtil.cloneQualifier(c)));
                System.out.println("值为: " + Bytes.toString(CellUtil.cloneValue(c)));
            }
        }
    }

    // 8. 扫描指定row
    public static void getRow(String tbName, String rowkey) throws IOException {
        // 拿到表对象
        Table t = connection.getTable(TableName.valueOf(tbName));

        // 1. 实例get对象
        Get g = new Get(Bytes.toBytes(rowkey));

        // 2. 可加过滤条件
        g.addFamily(Bytes.toBytes("info"));

        Result rs = t.get(g);

        Cell[] cells = rs.rawCells();

        // 3. 遍历
        for (Cell c: cells) {
            System.out.println("行键为: " + Bytes.toString(CellUtil.cloneRow(c)));
            System.out.println("列族为: " + Bytes.toString(CellUtil.cloneFamily(c)));
            System.out.println("列名为: " + Bytes.toString(CellUtil.cloneQualifier(c)));
            System.out.println("值为: " + Bytes.toString(CellUtil.cloneValue(c)));
        }
    }

    public static void main(String[] args) throws IOException {
//        System.out.println(isExist("users"));
//        createTable("test", "info", "message");
//        deleteTable("test");
//        addRow("users", "101", "info", "name", "zjn");
//        deleteRow("users", "101");
//        deleteAll("users", "101", "102");
//        scanAll("users");
        getRow("users", "101");

        System.out.println("end");
    }
}
