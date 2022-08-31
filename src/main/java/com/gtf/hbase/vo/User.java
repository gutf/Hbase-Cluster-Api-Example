package com.gtf.hbase.vo;

import lombok.Data;

/**
 * 用户信息
 *
 * @author : GTF
 * @version : 1.0
 * @date : 2022/8/30 16:12
 */
@Data
public class User {
    private String name;

    private String password;

    private Integer age;
}
