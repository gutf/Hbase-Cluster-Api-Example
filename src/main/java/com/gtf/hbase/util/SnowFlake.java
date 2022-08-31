package com.gtf.hbase.util;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ObjectUtils;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;

/**
 * 雪花算法
 *
 * @author : GTF
 * @version : 1.0
 * @date : 2022/8/30 16:50
 */
@Slf4j
public class SnowFlake {
    private final static long TWEPOCH = 12888349746579L;
    /**
     * 机器标识位数
     */
    private final static long WORKER_ID_BITS = 5L;
    /**
     * 数据中心标识位数
     */
    private final static long DATA_CENTER_ID_BITS = 5L;
    /**
     * 毫秒内自增位数
     */
    private final static long SEQUENCE_BITS = 12L;
    /**
     * 机器ID偏左移12位
     */
    private final static long WORKER_ID_SHIFT = SEQUENCE_BITS;
    /**
     * 数据中心ID左移17位
     */
    private final static long DATA_CENTER_ID_SHIFT = SEQUENCE_BITS + WORKER_ID_BITS;
    /**
     * 时间毫秒左移22位
     */
    private final static long TIMESTAMP_LEFT_SHIFT = SEQUENCE_BITS + WORKER_ID_BITS + DATA_CENTER_ID_BITS;
    /**
     * sequence掩码，确保sequence不会超出上限
     */
    private final static long SEQUENCE_MASK = ~(-1L << SEQUENCE_BITS);
    /**
     * 上次时间戳
     */
    private static long LAST_TIMESTAMP = -1L;
    /**
     * 序列
     */
    private long sequence = 0L;
    /**
     * 服务器ID
     */
    private long workerId;
    private static final long WORKER_MASK = ~(-1L << WORKER_ID_BITS);
    /**
     * 进程编码
     */
    private long processId;
    private static final long PROCESS_MASK = ~(-1L << DATA_CENTER_ID_BITS);

    private static final SnowFlake SNOW_FLAKE;

    static{
        SNOW_FLAKE = new SnowFlake();
    }

    public static synchronized long nextId(){
        return SNOW_FLAKE.getNextId();
    }

    private SnowFlake() {
        //获取机器编码
        this.workerId=this.getMachineNum();
        //获取进程编码
        RuntimeMXBean runtimeMxBean = ManagementFactory.getRuntimeMXBean();
        this.processId= Long.parseLong(runtimeMxBean.getName().split("@")[0]);

        //避免编码超出最大值
        this.workerId=workerId & WORKER_MASK;
        this.processId=processId & PROCESS_MASK;
    }

    public synchronized long getNextId() {
        //获取时间戳
        long timestamp = timeGen();
        //如果时间戳小于上次时间戳则报错
        if (timestamp < LAST_TIMESTAMP) {
            try {
                throw new Exception("Clock moved backwards.  Refusing to generate id for " + (LAST_TIMESTAMP - timestamp) + " milliseconds");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        //如果时间戳与上次时间戳相同
        if (LAST_TIMESTAMP == timestamp) {
            // 当前毫秒内，则+1，与sequenceMask确保sequence不会超出上限
            sequence = (sequence + 1) & SEQUENCE_MASK;
            if (sequence == 0) {
                // 当前毫秒内计数满了，则等待下一秒
                timestamp = tilNextMillis(LAST_TIMESTAMP);
            }
        } else {
            sequence = 0;
        }
        LAST_TIMESTAMP = timestamp;
        // ID偏移组合生成最终的ID，并返回ID
        return ((timestamp - TWEPOCH) << TIMESTAMP_LEFT_SHIFT) | (processId << DATA_CENTER_ID_SHIFT) | (workerId << WORKER_ID_SHIFT) | sequence;
    }//需要获取资料的朋友请加Q君样：290194256*

    /**
     * 再次获取时间戳直到获取的时间戳与现有的不同
     *
     * @return 下一个时间戳
     */
    private long tilNextMillis(final long lastTimestamp) {
        long timestamp = this.timeGen();
        while (timestamp <= lastTimestamp) {
            timestamp = this.timeGen();
        }
        return timestamp;
    }

    private long timeGen() {
        return System.currentTimeMillis();
    }

    /**
     * 获取机器编码
     */
    private long getMachineNum(){
        long machinePiece;
        StringBuilder sb = new StringBuilder();
        Enumeration<NetworkInterface> e = null;
        try {
            e = NetworkInterface.getNetworkInterfaces();
        } catch (SocketException e1) {
            e1.printStackTrace();
        }
        if (ObjectUtils.isEmpty(e)) {
            return 0L;
        }
        while (e.hasMoreElements()) {
            NetworkInterface ni = e.nextElement();
            sb.append(ni.toString());
        }
        machinePiece = sb.toString().hashCode();
        return machinePiece;
    }
}
