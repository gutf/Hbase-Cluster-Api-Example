package com.gtf.hbase.service;

import com.gtf.hbase.vo.User;

import java.util.Map;

/**
 * Hbase操作表实现类
 *
 * @author : GTF
 * @version : 1.0
 * @date : 2022/8/30 16:07
 */
public interface HbaseService {

    String putData(User user);


    Map<String,String> getData(String rowkey);
}
