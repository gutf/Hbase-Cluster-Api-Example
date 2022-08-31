package com.gtf.hbase.controller;

import com.gtf.hbase.service.HbaseService;
import com.gtf.hbase.util.SnowFlake;
import com.gtf.hbase.vo.User;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/**
 * Hbase控制器
 *
 * @author : GTF
 * @version : 1.0
 * @date : 2022/8/30 16:06
 */
@Slf4j
@RestController
@RequestMapping("/hbase")
@RequiredArgsConstructor
public class HbaseController {
    private final HbaseService hbaseService;

    @RequestMapping("/put")
    public String putData(){
        User user = new User();
        user.setName("name" + SnowFlake.nextId());
        user.setPassword("password" + SnowFlake.nextId());

        user.setAge(20);
        return hbaseService.putData(user);
    }

    @RequestMapping("/get")
    public Map<String,String> putData(@RequestParam("rowKey") String rowKey){
        return hbaseService.getData(rowKey);
    }
}
