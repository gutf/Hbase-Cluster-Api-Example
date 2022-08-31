package com.gtf.hbase.util;

import com.gtf.hbase.config.CustomExecutor;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;


/**
 * hbase工具类
 * @author GTF
 */
@Slf4j
@Component
public class HbaseUtil implements InitializingBean {

    @Value("${hbase.conn.quorum}")
    private String quorum;

    public static String QUORUM = "";

    public static Configuration configuration = null;

    /**
     * 要创建的connection
     */
    private static Connection connection = null;


    /**
     * 获取hbase连接 通过线程池的方法获取hbase连接 这样做的好处是在创建的时候就有一定数量
     * 的connection，每次使用的時候直接去線程池中取就可以了，不需要每次都创建connection。connection是一个比较重的对象，
     * 可以节约很多系统的资源
     *
     * @return Connection
     */
    public static Connection getConnection() {
        // 空的时候创建，不为空就直接返回；典型的单例模式
        if (null == connection) {
            synchronized (HbaseUtil.class) {
                if (null == connection) {
                    try {
                        configuration = getConfiguration();
                        ExecutorService pool =
                                new ThreadPoolExecutor(10, 10, 1000L, TimeUnit.MILLISECONDS,
                                        new LinkedBlockingQueue<>(), new CustomExecutor.NameTreadFactory());
                        connection = ConnectionFactory.createConnection(configuration, pool);
                    } catch (IOException e) {
                        log.error("", e);
                    }
                }
            }
        }
        return connection;
    }

    /**
     * 关闭表资源
     *
     * @param table
     */
    public static void closeTable(ResultScanner rs, Table table) {

        try {
            if (rs != null) {
                rs.close();
            }
            if (table != null) {
                table.close();
            }

        } catch (IOException e) {
            log.error("", e);
        }
    }

    /**
     * 关闭表资源
     *
     * @param table 表对象
     */
    public static void closeTable(Table table) {
        try {
            if (table != null) {
                table.close();
            }
        } catch (IOException e) {
            log.error("", e);
        }
    }

    /**
     * 获取hadoop配置信息
     *
     * @return Configuration
     */

    public static Configuration getConfiguration() {
        if (configuration != null) {
            return configuration;
        } else {
            configuration = HBaseConfiguration.create();
            configuration.set("hbase.zookeeper.quorum", QUORUM);
        }
        return configuration;
    }

    /**
     * 获取数据库管理员
     *
     * @return
     */
    public static HBaseAdmin getHbaseAdmin() {
        getConnection();
        if (configuration != null) {
            try {
                return (HBaseAdmin) connection.getAdmin();
            } catch (IOException e) {
                log.error("", e);
            }
        }
        return null;
    }

    /**
     * 根据表前缀获取表名
     *
     * @param tableNamePrefix 表前缀
     * @return
     */
    public static List<String> getTables(String tableNamePrefix) {
        Connection connection = HbaseUtil.getConnection();
        List<String> tables = new LinkedList<>();
        try {
            assert connection != null;
            Admin admin = connection.getAdmin();
            Pattern pattern = Pattern.compile(tableNamePrefix + ".*");
            TableName[] tablesName = admin.listTableNames(pattern);
            if (tablesName.length > 0) {
                for (TableName name : tablesName) {
                    byte[] tableName = name.getName();
                    tables.add(new String(tableName));
                }
            }
        } catch (IOException e) {
            log.error("", e);
        }

        return tables;

    }

    /**
     * 获取md5前缀
     *
     * @return
     */
    public static String getMd5Prefix(String str) {
        return Md5Util.byteToHexString(Objects.requireNonNull(Md5Util.getMd5(str))).substring(0, 8).toUpperCase();
    }


    /**
     * 根据class对象和hbase获取的值集合，将值构建为map
     *
     * @param cla         需要转换成的class对象
     * @param r           hbase获取到的结果集
     * @param familyName 读取的列
     * @return 构建的结果map
     */
    public static Map<String, byte[]> transferResultToEntity(Class cla, Result r, String familyName) {
        // 获取对象所有的字段集合
        Field[] fields = cla.getDeclaredFields();
        Map<String, byte[]> maps = new HashMap<>(fields.length);
        for (Field field : fields) {
            // 如果private修饰的属性允许访问
            field.setAccessible(true);
            // 获取字段名称
            String name = field.getName();
            if ("rowKey".equals(name)) {
                maps.put(name, r.getRow());
                continue;
            }
            // 将原本驼峰标识的字段名称转换为下划线分隔，如：posId
            String resultName = transHumpToUnderline(name);
            // ----> pos_id
            // 获取值
            byte[] value = r.getValue(Bytes.toBytes(familyName), Bytes.toBytes(resultName));
            value = (value == null) ? new byte[0] : value;
            // 存入map
            maps.put(name, value);
        }
        return maps;
    }

    /**
     * 由于字段名称是驼峰标识，在操作数据库时要将这些转换为下划线分隔
     *
     * @param attrName 字段名称
     * @return resultName 转换后的字段名称
     */
    public static String transHumpToUnderline(String attrName) {
        // 根据正则表达式分割字符串
        String[] names = attrName.split("(?<!^)(?=[A-Z])");
        // 初始化返回的属性名称为分割后的第一个字符串
        StringBuilder resultName = new StringBuilder(names[0]);
        if (names.length == 1) {
            return resultName.toString();
        }
        for (int i = 1; i < names.length; i++) {
            // 将首字母转换为小写
            resultName.append("_").append(names[i].substring(0, 1).toLowerCase()).append(names[i].substring(1));
        }
        return resultName.toString();
    }

    /**
     * 将字节rowKey转子字符串
     *
     * @param bytes rowKey字节数组
     */
    public static String getRowKeyStr(byte[] bytes) {
        StringBuilder str = new StringBuilder();
        for (byte b : bytes) {
            str.append(":").append(b);
        }
        return str.toString();
    }

    /**
     * 将字符串rowKey转字节数组
     *
     * @param rowKey rowKey字符串
     */
    public static byte[] getRowKeyBtes(String rowKey) {
        String[] keys = rowKey.split(":");
        byte[] bytes = new byte[keys.length - 1];

        for (int i = 1; i < keys.length; i++) {
            bytes[i - 1] = Byte.parseByte(keys[i]);
        }
        return bytes;

    }

    @Override
    public void afterPropertiesSet() throws Exception {
        QUORUM = quorum;
    }

    /**
     * @param str
     * @return
     * @Description 获取全位的MD5值
     * @version V1.6.2
     */
    public static String getMd5(String str) {
        return Md5Util.byteToHexString(Objects.requireNonNull(Md5Util.getMd5(str))).toUpperCase();
    }


    /**
     * 插入数据（单条）
     *
     * @param tableName    表名
     * @param rowKey       rowKey
     * @param columnFamily 列族
     * @param column       列
     * @param value        值
     * @return true/false
     */
    public boolean putData(String tableName, String rowKey, String columnFamily, String column,
                           String value) {
        return putData(tableName, rowKey, columnFamily, Collections.singletonList(column),
                Collections.singletonList(value));
    }

    /**
     * 插入数据（批量）
     *
     * @param tableName    表名
     * @param rowKey       rowKey
     * @param columnFamily 列族
     * @param columns      列
     * @param values       值
     * @return true/false
     */
    public boolean putData(String tableName, String rowKey, String columnFamily,
                           List<String> columns, List<String> values) {
        try {
            Table table = getConnection().getTable(TableName.valueOf(tableName));
            Put put = new Put(Bytes.toBytes(rowKey));
            for (int i = 0; i < columns.size(); i++) {
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columns.get(i)),
                        Bytes.toBytes(values.get(i)));
            }
            table.put(put);
            table.close();
            return true;
        } catch (IOException e) {
            log.error("", e);
            return false;
        }
    }

    /**
     * 创建表
     *
     * @param tableName    表名
     * @param columnFamily 列族
     * @return true/false
     */
    public static boolean createTable(String tableName, List<String> columnFamily) {
        return createTable(tableName, columnFamily, null);
    }

    /**
     * 预分区创建表
     *
     * @param tableName    表名
     * @param columnFamily 列族
     * @param keys         分区集合
     * @return true/false
     */
    public static boolean createTable(String tableName, List<String> columnFamily, List<String> keys) {
        if (!isExists(tableName)) {
            try {
                TableDescriptorBuilder desc = TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName));
                for (String cf : columnFamily) {
                    ColumnFamilyDescriptor columnFamilyDescriptor = ColumnFamilyDescriptorBuilder.of(cf);
                    desc.setColumnFamily(columnFamilyDescriptor);
                }
                HBaseAdmin admin = getHbaseAdmin();
                if (admin == null) {
                    log.error("HBaseAdmin is null");
                    return false;
                }
                if (keys == null) {
                    admin.createTable(desc.build());
                } else {
                    byte[][] splitKeys = getSplitKeys(keys);
                    admin.createTable(desc.build(), splitKeys);
                }
                return true;
            } catch (IOException e) {
                log.error("", e);
            }
        } else {
            log.error(tableName + "is exists!!!");
            return false;
        }
        return false;
    }

    /**
     * 判断表是否存在
     *
     * @param tableName 表名
     * @return true/false
     */
    public static boolean isExists(String tableName) {
        boolean tableExists = false;
        try {
            TableName table = TableName.valueOf(tableName);
            HBaseAdmin admin = getHbaseAdmin();
            if (admin == null) {
                log.error("HBaseAdmin is null");
                return false;
            }
            tableExists = admin.tableExists(table);
        } catch (IOException e) {
            log.error("", e);
        }
        return tableExists;
    }

    /**
     * 分区【10, 20, 30】 -> ( ,10] (10,20] (20,30] (30, )
     *
     * @param keys 分区集合[10, 20, 30]
     * @return byte二维数组
     */
    private static byte[][] getSplitKeys(List<String> keys) {
        byte[][] splitKeys = new byte[keys.size()][];
        TreeSet<byte[]> rows = new TreeSet<>(Bytes.BYTES_COMPARATOR);
        for (String key : keys) {
            rows.add(Bytes.toBytes(key));
        }
        int i = 0;
        for (byte[] row : rows) {
            splitKeys[i] = row;
            i++;
        }
        return splitKeys;
    }

    /**
     * 删除表
     *
     * @param tableName 表名
     */
    public static void dropTable(String tableName) throws IOException {
        if (isExists(tableName)) {
            TableName table = TableName.valueOf(tableName);
            HBaseAdmin admin = getHbaseAdmin();
            if (admin == null) {
                log.error("HBaseAdmin is null");
                return;
            }
            admin.disableTable(table);
            admin.deleteTable(table);
        }
    }

    /**
     * 获取数据（根据rowKey）
     *
     * @param tableName 表名
     * @param rowKey    rowKey
     * @return map
     */
    public static Map<String, String> getData(String tableName, String rowKey) {
        HBaseAdmin admin = getHbaseAdmin();
        if (admin == null) {
            log.error("HBaseAdmin is null");
            return new LinkedHashMap<>(0);
        }
        HashMap<String, String> map = new LinkedHashMap<>();
        try {
            Table table = admin.getConnection().getTable(TableName.valueOf(tableName));
            Get get = new Get(Bytes.toBytes(rowKey));
            Result result = table.get(get);

            if (result != null && !result.isEmpty()) {
                handleReadHbaseData(map, result,null);
            }
            table.close();
        } catch (IOException e) {
            log.error("", e);
        }
        return map;
    }

    /**
    * 获取读到的Hbase表数据
    */
    private static void handleReadHbaseData(HashMap<String, String> map, Result result,List<String> necessaries) {
        for (Cell cell : result.listCells()) {
            //列族
            String family = Bytes.toString(cell.getFamilyArray(),
                    cell.getFamilyOffset(), cell.getFamilyLength());
            //列
            String qualifier = Bytes.toString(cell.getQualifierArray(),
                    cell.getQualifierOffset(), cell.getQualifierLength());
            //值
            String data = Bytes.toString(cell.getValueArray(),
                    cell.getValueOffset(), cell.getValueLength());

            if (!CollectionUtils.isEmpty(necessaries) && !necessaries.contains(qualifier)){
                continue;
            }
            map.put(family + ":" + qualifier, data);
        }
    }

    /**
     * 获取数据（根据多个rowKey）
     *
     * @param tableName 表名
     * @param rowKeys    rowKey
     * @param necessaries 需返回的字段
     * @return map
     */
    public static List<Map<String,String>> getDataByRowKeys(String tableName, List<String> rowKeys,List<String> necessaries) {
        HBaseAdmin admin = getHbaseAdmin();
        if (admin == null) {
            log.error("HBaseAdmin is null");
            return new LinkedList<>();
        }

        List<Map<String,String>> dataList = new LinkedList<>();
        try {
            Table table = admin.getConnection().getTable(TableName.valueOf(tableName));
            LinkedList<Get> gets = new LinkedList<>();
            for (String rowKey : rowKeys) {
                gets.add(new Get(Bytes.toBytes(rowKey)));
            }
            Result[] results = table.get(gets);
            if (results != null && results.length > 0){
                for (Result result : results) {
                    HashMap<String, String> map = new LinkedHashMap<>();
                    if (result != null && !result.isEmpty()) {
                        handleReadHbaseData(map, result, necessaries);
                    }
                    dataList.add(map);
                }
            }
            table.close();
        } catch (IOException e) {
            log.error("", e);
        }
        return dataList;
    }

    /**
    * 获取表
    * @author GTF
    * @date 2022/8/30 17:16
    * @param tableName 表名
    * @return org.apache.hadoop.hbase.client.Table
    */
    public static Table getTable(String tableName) {
        boolean exists = HbaseUtil.isExists(tableName);
        if (exists) {
            try {
                return connection.getTable(TableName.valueOf(tableName));
            }catch (Exception e) {
                log.error("", e);
            }
        }
        return null;
    }

    /**
     * 获取数据（根据传入的filter）
     *
     * @param tableName 表名
     * @param filter    过滤器
     * @return map
     */
    public List<Map<String, String>> getData(String tableName, Filter filter) {
        HBaseAdmin admin = getHbaseAdmin();
        if (admin == null) {
            log.error("HBaseAdmin is null");
            return new LinkedList<>();
        }
        List<Map<String, String>> list = new ArrayList<>();
        try {
            Table table = admin.getConnection().getTable(TableName.valueOf(tableName));
            Scan scan = new Scan();
            // 添加过滤器
            scan.setFilter(filter);
            ResultScanner resultScanner = table.getScanner(scan);
            for (Result result : resultScanner) {
                HashMap<String, String> map = new LinkedHashMap<>();
                // rowkey
                String row = Bytes.toString(result.getRow());
                map.put("row", row);
                handleReadHbaseData(map, result, null);
                list.add(map);
            }
            table.close();
        } catch (IOException e) {
            log.error("", e);
        }
        return list;
    }
}

