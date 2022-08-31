package com.gtf.hbase.service.impl;

import com.gtf.hbase.service.HbaseService;
import com.gtf.hbase.util.HbaseUtil;
import com.gtf.hbase.util.SnowFlake;
import com.gtf.hbase.vo.User;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

/**
 * Hbase操作表实现类
 *
 * @author : GTF
 * @version : 1.0
 * @date : 2022/8/30 16:07
 */
@Slf4j
@Service
public class HbaseServiceImpl implements HbaseService {

    private static final String TABLE_NAME = "gtf_test2";
    private static final String DEFAULT_FAMILY = "user_info";
    private static final String NAME = "name";
    private static final String PASSWORD = "password";
    private static final String AGE = "age";
    private static final String CREATE_TIME = "create_time";

    @Override
    public String putData(User user) {
        Table table = this.getTable();
        Assert.notNull(table,"创建表失败或表不存在!");
        String rowKey = String.valueOf(SnowFlake.nextId());
        log.error("rowKey=" + rowKey);
        // 处理数据
        Put put = initPutData(user, rowKey);
        try {
            // 插入表
            table.put(put);
            // 关闭表
            table.close();
        } catch (IOException e) {
            log.error("",e);
        }

        return rowKey;
    }

    @Override
    public Map<String, String> getData(String rowkey) {
        Map<String, String> data = HbaseUtil.getData(TABLE_NAME, rowkey);
        return data;
    }

    private Put initPutData(User user,String rowKey){
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(DEFAULT_FAMILY),Bytes.toBytes(NAME),Bytes.toBytes(user.getName()));
        put.addColumn(Bytes.toBytes(DEFAULT_FAMILY),Bytes.toBytes(PASSWORD),Bytes.toBytes(user.getPassword()));
        put.addColumn(Bytes.toBytes(DEFAULT_FAMILY),Bytes.toBytes(AGE),Bytes.toBytes(String.valueOf(user.getAge())));
        put.addColumn(Bytes.toBytes(DEFAULT_FAMILY),Bytes.toBytes(CREATE_TIME),Bytes.toBytes(String.valueOf(System.currentTimeMillis())));
        return put;
    }

    /**
    * 获取表
    * @author GTF
    * @date 2022/8/30 17:21
    * @return org.apache.hadoop.hbase.client.Table
    */
    private Table getTable() {
        Table table = HbaseUtil.getTable(TABLE_NAME);
        if (table != null) {
            return table;
        }
        boolean createTableSuccess = HbaseUtil.createTable(TABLE_NAME, Collections.singletonList(DEFAULT_FAMILY), Arrays.asList(NAME,PASSWORD,AGE,CREATE_TIME));
        return createTableSuccess ? HbaseUtil.getTable(TABLE_NAME) : null;
    }
}
