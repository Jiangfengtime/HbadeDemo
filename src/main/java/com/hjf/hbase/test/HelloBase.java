package com.hjf.hbase.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * @author Jiang锋时刻
 * @create 2020-04-16 16:08
 */
public class HelloBase {

    /**
     * 指定Zookeeper集群
     */
    static final String ZK_CONNECT_KEY = "hbase.zookeeper.quorum";
    static final String ZK_CONNECT_VALUE = "node02:2181,node03:2181,node04:2181";

    private static Connection conn = null;
    private static Admin admin = null;
    private static Table table = null;

    public static void main(String[] args) throws URISyntaxException, IOException {

        getConnection();
        getAdmin();
        /* ************************ 元数据操作 ************************ */
        // 创建表
//        createTable("t1", "cf1", "cf2");

        // 删除表
//        deleteTable("t1");

        // 添加列族
//        addColumnFamily("t1", "cf3", "cf4");

        // 删除列族
//         deleteColumnFamily(tableName, "cf3", "cf4");

        // 修改表
        // modifyTable(cf1,table, tableName);


        /* ************************ 数据库操作 ************************ */

//        putData("t1", "r2", "cf1", "name", "hjf");
        //putData("t1", "r2", "cf1", "name",2222222222222L,"sss");
//        getScann("t1");

//        getData("t1", "r1", "cf1", "name");
        getData("t5", "r1", "cf1", "name", 3);









        close();
    }


    /**
     * 获取连接
     * @return
     */
    public static Connection getConnection(){
        // 创建一个Configuration类, 用于加载需要连接Hbase的各项配置
        Configuration conf = HBaseConfiguration.create();
        // 设置当前的程序去寻找的hbase在哪里
        conf.set(ZK_CONNECT_KEY, ZK_CONNECT_VALUE);
        // 创建Connection
        try {
            conn = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return conn;
    }

    /**
     * 获取Admin类
     * Admin类是HBase API中负责管理建表、改表、删表等元数据操作的管理接口
     * @return
     */
    public static Admin getAdmin() {
        try {
            admin = conn.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return admin;
    }

    /**
     * 判断表是否存在
     * @param table_name
     * @return
     * @throws IOException
     */
    public static boolean isExistsTable(String table_name) throws IOException {
        TableName tableName = TableName.valueOf(table_name);
        if (admin.tableExists(tableName)){
            return true;
        } else {
            return false;
        }
    }

    /**
     * 创建表
     * @param table_name    表名
     * @param cfs           列族列表
     */
    public static void createTable(String table_name, String ... cfs) throws IOException {
        if (isExistsTable(table_name)) {
            System.out.println(table_name + "表已存在!!!");
            return;
        }
        // 用表名的字符串来生成一个TableName的类
        TableName tn = TableName.valueOf(table_name);
        // 生成HTableDescriptor类
        HTableDescriptor table = new HTableDescriptor(tn);
        for (String cf: cfs){
            // 定义列族属性的方法都在HColumnDescriptor类里面
            // 创建HColumnDescriptor类, 用于指定列族
            HColumnDescriptor columnFamily = new HColumnDescriptor(cf);
            // 将列族添加到HTableDescriptor
            table.addFamily(columnFamily);
        }
        TableName tableName = table.getTableName();
        admin.createTable(table);
        System.out.println(tableName + "表创建成功!!!");
    }

    /**
     * 删除表
     * @param table_name
     */
    public static void deleteTable(String table_name) throws IOException {
        if(!isExistsTable(table_name)){
            System.out.println(table_name + "表不存在!!!");
            return;
        }
        TableName tableName = TableName.valueOf(table_name);
        // 禁用当前表, 删除表之前必须先禁用
        admin.disableTable(tableName);
        // 删除当前表
        admin.deleteTable(tableName);
        System.out.println(tableName + "表删除成功!!!");
    }

    /**
     *
     * @param cf1
     * @param table
     * @param tableName
     */
    public static void modifyTable(HColumnDescriptor cf1, HTableDescriptor table, TableName tableName){
        // 修改列族属性:
        // cf1列族的压缩方式设置为GZ
        cf1.setCompactionCompressionType(Compression.Algorithm.GZ);
        // 把cf1的最大版本号修改为ALL_VERSIONS  ==> Integer.MAX_VALUE
        cf1.setMaxVersions(HConstants.ALL_VERSIONS);
        // 把列族的定义更新到表定义里面
        table.modifyFamily(cf1);
        // 此时对表的修改并没有真正执行下去, 只有当调用了Admin类来进行操作的时候对HBase的修改才真正开始执行。
        try {
            admin.modifyTable(tableName, table);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 添加列族
     * @param table_name    表名
     * @param cfs           列族列表
     */
    public static void addColumnFamily(String table_name, String ... cfs) throws IOException {
        if (!isExistsTable(table_name)){
            System.out.println(table_name + "表不存在");
            return;
        }



        TableName tableName = TableName.valueOf(table_name);
        for (String cf: cfs){
            HColumnDescriptor newCf = new HColumnDescriptor(cf);
            admin.addColumn(tableName, newCf);
        }
        System.out.println(table_name + "表添加列族成功!!!");
    }

    /**
     * 删除列族
     * @param table_name    表名
     * @param cfs           列族列表
     * @throws IOException
     */
    public static void deleteColumnFamily(String table_name, String ... cfs) throws IOException {
        if (!isExistsTable(table_name)){
            System.out.println(table_name + "表不存在!!!");
            return;
        }
        TableName tableName = TableName.valueOf(table_name);
        admin.disableTable(tableName);
        for (String cf: cfs){
            admin.deleteColumn(tableName, cf.getBytes("UTF-8"));
        }
        System.out.println("删除列族成功!!!");
    }

    /**
     * 插入数据[没有时间戳]
     * @param table_name    表名
     * @param rowkey        rowkey
     * @param familyName    列族
     * @param columnName    列
     * @param value         值
     */
    public static void putData(String table_name, String rowkey, String familyName, String columnName, String value) throws IOException {

        // 如果表不存在,则先创建表
        if (! isExistsTable(table_name)){
            createTable(table_name, familyName);
        }
        TableName tableName = TableName.valueOf(table_name);
        table = conn.getTable(tableName);
        Put put = new Put(Bytes.toBytes(rowkey));

        put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(columnName), Bytes.toBytes(value));
        // 调用Table接口的put方法把数据真正保存起来：
        table.put(put);
        System.out.println("插入数据成功!!!");
    }

    /**
     * 插入数据[有时间戳]
     * @param table_name
     * @param rowkey
     * @param familyName
     * @param columnName
     * @param timestamp
     * @param value
     * @throws IOException
     */
    public static void putData(String table_name, String rowkey, String familyName, String columnName, long timestamp,
                            String value) throws IOException {
        if (timestamp < 0){
            System.out.println("时间戳不能为负数!!!");
            return;
        }

        // 如果表不存在,则先创建表
        if (! isExistsTable(table_name)){
            createTable(table_name, familyName);
        }

        TableName tableName = TableName.valueOf(table_name);
        table = conn.getTable(tableName);
        Put put = new Put(Bytes.toBytes(rowkey));

        put.addColumn(Bytes.toBytes(familyName),Bytes.toBytes(columnName), timestamp, Bytes.toBytes(ByteBuffer.wrap(value.getBytes("UTF-8"))));
        // 调用Table接口的put方法把数据真正保存起来：
        table.put(put);
        System.out.println("插入数据成功!!!");
    }


    public static void getScann(String table_name) throws IOException {
        if (!isExistsTable(table_name)){
            System.out.println(table_name + "表不存在!!!");
            return;
        }
        TableName tableName = TableName.valueOf(table_name);
        Table table = conn.getTable(tableName);
        Scan scan = new Scan();
        ResultScanner results = table.getScanner(scan);
        for (Result result: results){
            System.out.println(result);
        }
        results.close();

    }

    /**
     * 获取表中rowkey指定列的数据
     * @param table_name        表名
     * @param rowkey            rowkey
     * @param columnFamily      列族
     * @param column            列
     * @throws IOException
     */
    public static void getData(String table_name, String rowkey, String columnFamily, String column) throws IOException {
        if (!isExistsTable(table_name)){
            System.out.println(table_name + "表不存在!!!");
            return;
        }
        TableName tableName = TableName.valueOf(table_name);
        Table table = conn.getTable(tableName);
        Get get = new Get(Bytes.toBytes(rowkey));
        Result result = table.get(get);
        byte[] value = result.getValue(Bytes.toBytes(columnFamily), Bytes.toBytes(column));
        System.out.println(Bytes.toString(value));
    }

    /**
     *
     * @param table_name
     * @param rowkey
     * @param columnFamily
     * @param column
     * @param version           打印版本数量
     * @throws IOException
     */
    public static void getData(String table_name, String rowkey, String columnFamily, String column, int version) throws IOException {
        if (!isExistsTable(table_name)){
            System.out.println(table_name + "表不存在!!!");
            return;
        }
        TableName tableName = TableName.valueOf(table_name);
        Table table = conn.getTable(tableName);
        Get get = new Get(Bytes.toBytes(rowkey));
        // 设置打印的版本数量
        get.setMaxVersions(version);
        // 获取Result对象
        Result result = table.get(get);
        // 用getColumnCells()方法获取到指定列的多个版本值
        List<Cell> cells = result.getColumnCells(Bytes.toBytes(columnFamily), Bytes.toBytes(column));

        for (Cell cell: cells){
            /*
                此处不用getValue()方法直接获取的原因是:
                调用getValue()方法会获取整个Cell的数组备份, 比较耗时.
                如果需要获取cell中的值时, 最好使用CellUtil.cloneValue
             */
            // 调用CellUtil.cloneValue来获取cell中的值
            // byte[] cValue = cell.getValue();     //最好不用
            byte[] cValue = CellUtil.cloneValue(cell);
            System.out.println(Bytes.toString(cValue));
        }

    }














    /**
     * 关闭链接
     */
    public static void close(){
        try {
            admin.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            conn.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }







}
