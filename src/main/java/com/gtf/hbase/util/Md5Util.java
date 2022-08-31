package com.gtf.hbase.util;

import lombok.extern.slf4j.Slf4j;

import java.io.FileInputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * MD5Util
 *
 * @author : GTF
 * @version : 1.0
 * @date : 2022/8/30 15:43
 */
@Slf4j
public class Md5Util {
    static char[] hexDigits = {'0', '1', '2', '3', '4', '5', '6', '7', '8',
            '9', 'a', 'b', 'c', 'd', 'e', 'f'};

    /**
     * 根据字符串获取MD5值
     *
     * @param str
     * @return
     */
    public static byte[] getMd5(String str) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            return md.digest(str.getBytes());
        } catch (NoSuchAlgorithmException e) {
            log.error("", e);
            return null;
        }
    }

    /**
     * 根据字节数组获取md5值
     *
     * @param bytes 字节数组
     * @return 异常返回null，正常返回MD5值字节数组
     */
    public static byte[] getMd5(byte[] bytes) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            return md.digest(bytes);
        } catch (NoSuchAlgorithmException e) {
            log.error("", e);
            return null;
        }
    }


    /**
     * 获取文件的MD5值
     *
     * @param fis 文件输入流
     * @return 异常返回null，正常返回MD5值字节数组
     */
    public static byte[] getFileMd5(FileInputStream fis) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] buffer = new byte[2048];
            int length = -1;
            long s = System.currentTimeMillis();
            while ((length = fis.read(buffer)) != -1) {
                md.update(buffer, 0, length);
            }
            return md.digest();
        } catch (NoSuchAlgorithmException | IOException ex) {
            log.error("", ex);
        }
        return null;
    }


    /**
     * 将字节数组打印成16进制字符串
     *
     * @param bytes 字节数组
     * @return 返回16进制字符串
     */
    public static String byteToHexString(byte[] bytes) {
        String s;
        // 用字节表示就是 16 个字节,每个字节用 16 进制表示的话，使用两个字符，所以表示成 16 进制需要 32 个字符
        char[] str = new char[bytes.length * 2];
        // 表示转换结果中对应的字符位置
        int k = 0;
        //从第一个字节开始，对 MD5 的每一个字节
        for (byte byte0 : bytes) {
            // 转换成 16 进制字符的转换
            // 取字节中高 4 位的数字转换,
            str[k++] = hexDigits[byte0 >>> 4 & 0xf];
            // 取字节中低 4 位的数字转换
            str[k++] = hexDigits[byte0 & 0xf];
        }
        // 换后的结果转换为字符串
        s = new String(str);
        return s;
    }

}
