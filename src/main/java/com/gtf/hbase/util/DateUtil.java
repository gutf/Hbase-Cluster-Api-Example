package com.gtf.hbase.util;

import lombok.extern.slf4j.Slf4j;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * TODO
 *
 * @author : GTF
 * @version : 1.0
 * @date : 2022/8/30 15:41
 */
@Slf4j
public class DateUtil {
    public static final String YYYY_MM = "yyyy-MM";

    public static final String YYYY_MM_DD = "yyyy-MM-dd";

    public static final String YYYY_MM_DD_HH = "yyyy-MM-dd HH";

    public static final String YYYY_MM_DD_HH_MM = "yyyy-MM-dd HH:mm";

    public static final String YYYY_MM_DD_HH_MM_SS = "yyyy-MM-dd HH:mm:ss";

    public static final String YYYYMM = "yyyyMM";

    public static final String YYYYMMDDHHMMSS = "yyyyMMddHHmmss";

    /**
     * 每周最大天数
     */
    private static final Integer WEEK_MAX_DAYS = 7;

    private static final Integer TWO = 2;

    private static final Integer TEN = 10;

    private static final Integer THIRTEEN = 13;

    /**
     * 返回当前时间的字符串时间YYYYMMDDHHMMSS
     *
     * @param type 时间格式
     * @return 返回字符串类型时间
     */
    public static String getNowTime(String type) {

        return DateUtil.tranferDateToStr(new Date(), type);

    }

    /**
     * 比较两个日期的年月日是否相等
     *
     * @param date1 日期1
     * @param date2 日期2
     * @return 相等返回true，不相等返回false
     */
    public static boolean compareDate(Date date1, Date date2) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date1);
        int year = calendar.get(Calendar.YEAR);
        int month = calendar.get(Calendar.MONTH);
        int day = calendar.get(Calendar.DATE);

        calendar.setTime(date2);
        int year2 = calendar.get(Calendar.YEAR);
        int month2 = calendar.get(Calendar.MONTH);
        int day2 = calendar.get(Calendar.DATE);

        if (year != year2) {
            return false;
        }
        if (month != month2) {
            return false;
        }
        return day == day2;
    }


    /**
     * 判断两个日期的年月日大小
     *
     * @param date1 日期1
     * @param date2 日期2
     * @return date1>date2则返回正数，相等返回0，否则返回负数
     */
    public static int compareDateForYmd(Date date1, Date date2) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date1);
        int year = calendar.get(Calendar.YEAR);
        int month = calendar.get(Calendar.MONTH);
        int day = calendar.get(Calendar.DATE);

        calendar.setTime(date2);
        int year2 = calendar.get(Calendar.YEAR);
        int month2 = calendar.get(Calendar.MONTH);
        int day2 = calendar.get(Calendar.DATE);

        if (year > year2) {
            return 1;
        } else if (year < year2) {
            return -1;
        }
        if (month > month2) {
            return 1;
        } else if (month < month2) {
            return -1;
        }
        if (day > day2) {
            return 1;
        } else if (day < day2) {
            return -1;
        }
        return 0;
    }

    /**
     * 判断两个日期的年月大小
     *
     * @param date1 日期1
     * @param date2 日期2
     * @return date1>date2则返回整数，相等返回0，否则返回负数
     */
    public static int compareDateForYm(Date date1, Date date2) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date1);
        int year = calendar.get(Calendar.YEAR);
        int month = calendar.get(Calendar.MONTH);

        calendar.setTime(date2);
        int year2 = calendar.get(Calendar.YEAR);
        int month2 = calendar.get(Calendar.MONTH);

        if (year > year2) {
            return 1;
        } else if (year < year2) {
            return -1;
        }
        return Integer.compare(month, month2);
    }

    /**
     * 根据日期判断周几
     *
     * @param date 日期对象
     * @return 周几
     */
    public static int getDay(Date date) {

        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        return cal.get(Calendar.DAY_OF_WEEK);
    }

    /**
     * 计算两个日期之间相差的天数 :只获取整数天数
     *
     * @param smdate 较小的日期
     * @param bdate  较大的日期
     * @return 相差天数
     */
    public static int daysBetween(Date smdate, Date bdate) {

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

        try {
            smdate = sdf.parse(sdf.format(smdate));
            bdate = sdf.parse(sdf.format(bdate));
        } catch (ParseException e) {
            log.error("", e);
        }

        Calendar cal = Calendar.getInstance();
        cal.setTime(smdate);
        long time1 = cal.getTimeInMillis();
        cal.setTime(bdate);
        long time2 = cal.getTimeInMillis();

        long days = (time2 - time1) / (1000 * 3600 * 24);

        return Integer.parseInt(String.valueOf(days));

    }


    /**
     * @param smdate
     * @param bdate
     * @param days
     * @return
     */
    public static int getWeekday(Date smdate, Date bdate, List<Integer> days) {

        if (bdate.compareTo(smdate) < 0) {
            return 0;
        }

        if (days.size() >= WEEK_MAX_DAYS) {
            return 0;
        }

        int total = 0;
        Map<Integer, Integer> weekMaps = new LinkedHashMap<>();
        for (Integer day : days) {
            weekMaps.put(day, day);
        }

        while (true) {

            Integer day = DateUtil.getDay(smdate);
            Object vObject = weekMaps.get(day);

            if (vObject == null) {
                total++;
            }
            boolean bo = DateUtil.compareDate(smdate, bdate);
            if (bo) {
                break;
            }
            smdate.setTime((smdate.getTime() + 1000 * 60 * 60 * 24));
        }

        return total;

    }

    /**
     * 按指定格式将字符串日期转Date类型
     *
     * @param dateStr 字符型日期
     * @param format  格式
     * @return Date 日期
     * @throws ParseException 转换异常
     */
    public static Date transferStrToDate(String dateStr, String format) throws ParseException {

        Date date = null;
        DateFormat dateFormat = new SimpleDateFormat(format);
        date = (Date) dateFormat.parse(dateStr);

        return date;
    }


    /**
     * 按指定格式将Date类型转为字符串
     *
     * @param date   日期类型
     * @param format 日期格式
     * @return String 字符串日期
     */
    public static String tranferDateToStr(Date date, String format) {
        DateFormat dateFormat = new SimpleDateFormat(format);
        return dateFormat.format(date);
    }


    /**
     * 按指定格式将Date类型转为字符串
     *
     * @param date   日期类型
     * @param format 日期格式
     * @return String 字符串日期
     */
    public static String transferDateToStr(Date date, String format) {
        DateFormat dateFormat = new SimpleDateFormat(format);
        return dateFormat.format(date);
    }

    /**
     * 计算两个字符串日期相差的总时间：单位：秒
     *
     * @param maxDateStr 格式：yyyy-MM-DD hh:mm:ss
     * @param minDateStr 格式：yyyy-MM-DD hh:mm:ss
     * @return 相差的总时间，单位：秒
     */
    public static long getSecondsDiffer(String maxDateStr, String minDateStr) {

        Date maxDate;
        Date minDate;

        try {
            maxDate = transferStrToDate(maxDateStr, YYYY_MM_DD_HH_MM_SS);
            minDate = transferStrToDate(minDateStr, YYYY_MM_DD_HH_MM_SS);
        } catch (ParseException e) {
            log.error("", e);
            return -1;
        }

        return (maxDate.getTime() - minDate.getTime()) / 1000;
    }

    /**
     * 计算两个字符串日期相差的总时间：单位：秒
     *
     * @param maxDate 格式：yyyy-MM-DD hh:mm:ss
     * @param minDate 格式：yyyy-MM-DD hh:mm:ss
     * @return 相差的总时间，单位：秒
     */
    public static long getSecondsDiffer(Date maxDate, Date minDate) {
        return (maxDate.getTime() - minDate.getTime()) / 1000;
    }

    /**
     * 向指定日期增加时间
     *
     * @param dateStr 日期
     * @param minutes 单位：分钟
     * @return 返回字符串
     * @throws ParseException 转换异常
     */
    public static String addMinutesToDate(String dateStr, int minutes) throws ParseException {
        Date date = transferStrToDate(dateStr, YYYY_MM_DD_HH_MM_SS);
        date.setTime(date.getTime() + (long) minutes * 60 * 1000);
        return tranferDateToStr(date, YYYY_MM_DD_HH_MM_SS);
    }

    /**
     * 根据起始日期和截止期日获取在这两个日期间的所有日期：包括起始日期和截止日期
     *
     * @param dateStart 起始日期
     * @param dateEnd   截止日期
     * @return 日期集合
     */
    public static List<String> getDateArray(Date dateStart, Date dateEnd) {
        List<String> dates = new LinkedList<>();

        dates.add(tranferDateToStr(dateStart, YYYY_MM_DD_HH_MM_SS));

        while (true) {
            dateStart = addOneDay(dateStart);
            if (compareDateForYmd(dateStart, dateEnd) < 0) {
                dates.add(tranferDateToStr(dateStart, YYYY_MM_DD_HH_MM_SS));
            } else {
                break;
            }
        }
        return dates;
    }


    /**
     * 根据起始日期和截止期日划分出不同年月的日期
     *
     * @param dateStart 起始日期
     * @param dateEnd   截止日期
     * @return 日期集合
     */
    public static List<Date> splitDatesForYm(Date dateStart, Date dateEnd) {

        List<Date> dates = new LinkedList<>();
        dates.add(dateStart);
        Date lastDate = dateStart;
        while (true) {
            dateStart = addOneMonth(dateStart);
            if (compareDateForYm(dateStart, dateEnd) <= 0) {
                dates.add(getLastDay(lastDate));
                dateStart = getFirstDate(dateStart);
                dates.add(dateStart);
                lastDate = dateStart;
            } else {
                break;
            }
        }
        dates.add(dateEnd);

        for (int i = 0; i < dates.size(); i++) {
            if (i == 0 || i == (dates.size() - 1)) {
                continue;
            }
            String time = "";
            if (i % 2 == 1) {
                time = tranferDateToStr(dates.get(i), YYYY_MM_DD) + " 23:59:59";
            }
            if (i % 2 == 0) {
                time = tranferDateToStr(dates.get(i), YYYY_MM_DD) + " 00:00:00";
            }
            Date dealDate = null;
            try {
                dealDate = transferStrToDate(time, YYYY_MM_DD_HH_MM_SS);
                dates.set(i, dealDate);
            } catch (ParseException e) {
                log.error("", e);
            }
        }

        return dates;
    }

    /**
     * 根据起始日期和截止期日划分出不同年月的日期
     *
     * @param startTime 起始日期
     * @param endTime   截止日期
     * @return 日期集合
     */
    public static List<String> splitDatesForYm(String startTime, String endTime) {

        List<String> timeZone = new ArrayList<String>();
        Date startDate = null;
        Date endDate = null;
        try {
            startDate = transferStrToDate(startTime, YYYY_MM_DD_HH_MM_SS);
            endDate = transferStrToDate(endTime, YYYY_MM_DD_HH_MM_SS);
        } catch (ParseException e) {
            log.error("", e);
        }

        List<Date> dates = splitDatesForYm(startDate, endDate);
        for (int i = 0; i < dates.size(); i = i + TWO) {
            String zoneStartTime = tranferDateToStr(dates.get(i), YYYY_MM_DD_HH_MM_SS);
            String zoneEndTime = tranferDateToStr(dates.get(i + 1), YYYY_MM_DD_HH_MM_SS);
            String zone = zoneStartTime + "~" + zoneEndTime;
            timeZone.add(zone);
        }
        return timeZone;
    }


    /**
     * 日期向后推移一天
     *
     * @param date 日期
     * @return 返回日期
     */
    public static Date addOneDay(Date date) {
        Calendar calendar = new GregorianCalendar();
        calendar.setTime(date);

        // 把日期往后增加一天.整数往后推,负数往前移动
        calendar.add(Calendar.DATE, 1);

        // 这个时间就是日期往后推一天的结果
        date = calendar.getTime();
        return date;
    }

    /**
     * 日期向后推移1月
     *
     * @param date 日期
     * @return 返回日期
     */
    public static Date addOneMonth(Date date) {
        Calendar calender = Calendar.getInstance();
        calender.setTime(date);
        calender.add(Calendar.MONTH, 1);
        date = calender.getTime();

        return date;
    }

    /**
     * 日前向前推移1月
     *
     * @param date
     * @return
     */
    public static Date subOneMonth(Date date) {
        Calendar calender = Calendar.getInstance();
        calender.setTime(date);
        calender.add(Calendar.MONTH, -1);
        date = calender.getTime();

        return date;
    }

    /**
     * 日前向前推移1月
     *
     * @param date
     * @return
     */
    public static Date subOneYear(Date date) {
        Calendar calender = Calendar.getInstance();
        calender.setTime(date);
        calender.add(Calendar.YEAR, -1);
        date = calender.getTime();

        return date;
    }

    /**
     * 日期向前推移一天
     *
     * @param date
     * @return
     */
    public static Date subOneDay(Date date) {
        Calendar calendar = new GregorianCalendar();
        calendar.setTime(date);

        //把日期往前减一天.整数往后推,负数往前移动
        calendar.add(Calendar.DATE, -1);

        //这个时间就是日期往前推一天的结果
        date = calendar.getTime();
        return date;
    }


    /**
     * 获取某一月的最后一天的日期
     *
     * @param date 日期
     * @return 返回日期
     */
    public static Date getLastDay(Date date) {

        Calendar calender = Calendar.getInstance();
        calender.setTime(date);

        // 把日期设置为当月第一天
        calender.set(Calendar.DATE, 1);
        // 日期回滚一天，也就是最后一天
        calender.roll(Calendar.DATE, -1);
        date = calender.getTime();
        return date;
    }


    /**
     * 获取某一月的第一天的日期
     *
     * @param date
     * @return 返回日期
     */
    public static Date getFirstDate(Date date) {

        Calendar calender = Calendar.getInstance();
        calender.setTime(date);
        // 把日期设置为当月第一天
        calender.set(Calendar.DATE, 1);
        date = calender.getTime();
        return date;
    }


    /**
     * 判断date是否在(dateStr-timeDiffer)和（dateStr+timeDiffer）之间
     *
     * @param dateStr
     * @param dateStr2
     * @param timeDiffer 单位为s
     * @return
     * @throws ParseException
     */
    public static boolean isBetweenDate(String dateStr, int timeDiffer, String dateStr2) throws ParseException {

        Date date = transferStrToDate(dateStr, YYYY_MM_DD_HH_MM_SS);
        Date date2 = transferStrToDate(dateStr2, YYYY_MM_DD_HH_MM_SS);
        return date2.getTime() > (date.getTime() - timeDiffer * 1000L) && date2.getTime() < (date.getTime() + timeDiffer * 1000L);
    }

    /**
     * 判断dateStr2是否在(dateStr-frontExtend)和（dateStr+backExtend）之间
     *
     * @param dateStr
     * @param dateStr2
     * @param frontExtend 向前扩展时间
     * @param backExtend  向后扩展时间
     * @return 如果在返回true，否则返回false
     * @throws ParseException
     */
    public static boolean isBetweenDate(String dateStr, int frontExtend, int backExtend, String dateStr2) throws ParseException {

        Date date = transferStrToDate(dateStr, YYYY_MM_DD_HH_MM_SS);
        Date date2 = transferStrToDate(dateStr2, YYYY_MM_DD_HH_MM_SS);
        return date2.getTime() > (date.getTime() - frontExtend * 1000L) && date2.getTime() < (date.getTime() + backExtend * 1000L);

    }


    /**
     * 将指定日期往前推timeDiffer 秒
     *
     * @param dateStr
     * @param timeDiffer 单位：秒
     * @return
     * @throws ParseException
     */
    public static String getFrontDate(String dateStr, int timeDiffer) throws ParseException {

        Date date = transferStrToDate(dateStr, YYYY_MM_DD_HH_MM_SS);
        date.setTime(date.getTime() - 1000L * timeDiffer);
        return tranferDateToStr(date, YYYY_MM_DD_HH_MM_SS);
    }

    /**
     * 将指定日期往后推timeDiffer 秒
     *
     * @param dateStr
     * @param timeDiffer 单位秒
     * @return
     * @throws ParseException
     */
    public static String getBackDate(String dateStr, int timeDiffer) throws ParseException {

        Date date = transferStrToDate(dateStr, YYYY_MM_DD_HH_MM_SS);
        date.setTime(date.getTime() + 1000L * timeDiffer);
        return tranferDateToStr(date, YYYY_MM_DD_HH_MM_SS);
    }


    /**
     * 根据时间戳获得秒数
     *
     * @param start
     * @param end
     * @return
     */
    public static String getSecondsByTimeStamp(Long start, Long end) {
        end = end - start;
        end = end / 1000;
        return end + "s";
    }


    /**
     * 获取当天指定时间对应的毫秒数
     *
     * @param time "HH:mm:ss"
     * @return
     */
    public static long getTimeMillis(String time) {

        try {
            DateFormat dateFormat = new SimpleDateFormat("yy-MM-dd HH:mm:ss");
            DateFormat dayFormat = new SimpleDateFormat("yy-MM-dd");
            Date curDate = dateFormat.parse(dayFormat.format(new Date()) + " " + time);
            return curDate.getTime();
        } catch (ParseException e) {
            log.error("", e);
        }
        return 0;
    }

    /**
     * 根据时间获取下一个小时的时间戳
     *
     * @param time
     * @return
     */
    public static Long getSecondsLateHour(Date time) {

        String timeIntoHour = tranferDateToStr(time, YYYY_MM_DD_HH);
        Date tranDate = null;
        try {
            tranDate = transferStrToDate(timeIntoHour + ":00:00", YYYY_MM_DD_HH_MM_SS);
        } catch (ParseException e) {
            log.error("", e);
            return null;
        }
        return tranDate.getTime() + 60 * 60 * 1000;

    }

    /**
     * 获取当天日期的 00:00:00 时间戳
     */
    public static long getCurrentDateTimestamp() {

        Date date = new Date();
        String dateStr = transferDateToStr(date, DateUtil.YYYY_MM_DD);
        try {
            date = transferStrToDate(dateStr, DateUtil.YYYY_MM_DD);
        } catch (ParseException e) {
            log.error("", e);
        }
        return date.getTime();
    }

    /**
     * @return
     * @Description: 获取当前时间，截取到月份
     * @author: liujj
     * @date: 2017年2月23日 下午5:09:17
     * @version: V1.1
     */
    public static String getNowMonthTime() {
        Date date = new Date();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMM");
        return sdf.format(date);
    }

    /**
     * @return
     * @Description: 获取上一个月，截取到月份
     * @author: liujj
     * @date: 2017年2月23日 下午5:48:07
     * @version: V1.1
     */
    public static String getLastMonthTime() {
        Date date = subOneMonth(new Date());
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMM");
        return sdf.format(date);
    }

    /**
     * @return
     * @Description: 获取上一个月第一天的时间戳
     * @author: liujj
     * @date: 2017年2月23日 下午5:26:16
     * @version: V1.1
     */
    public static long getLastFirstDate() {
        Date today = new Date();
        Date lastMonth = subOneMonth(today);
        Date lastFirstDay = getFirstDate(lastMonth);
        return lastFirstDay.getTime();
    }

    /**
     * @param day 向后推的天数
     * @return
     * @Description: 获取上一个月第一天，将其向后推day天
     * @author: liujj
     * @date: 2017年2月23日 下午5:30:35
     * @version: V1.1
     */
    public static long getLastBackDate(int day) {
        Date today = new Date();
        Date lastMonth = subOneMonth(today);
        Date lastFirstDay = getFirstDate(lastMonth);
        lastFirstDay.setTime(lastFirstDay.getTime() + 86400000L * day);
        return lastFirstDay.getTime();
    }

    /**
     * 获取每月第一天指定时间对应的毫秒数
     *
     * @param time 指定时间
     * @return
     * @author: liujj
     * @date: 2017年2月24日 上午11:45:06
     * @version: V1.1
     */
    public static Long getFirstDateTime(String time) {
        Date today = new Date();
        Date firstDay = getFirstDate(today);
        String sFirstDay = tranferDateToStr(firstDay, YYYY_MM_DD);
        sFirstDay = sFirstDay + " " + time;
        Date resultDate = null;
        try {
            resultDate = transferStrToDate(sFirstDay, YYYY_MM_DD_HH_MM_SS);
        } catch (ParseException e) {
            log.error("", e);
            return null;
        }
        return resultDate.getTime();
    }

    /**
     * 获取当月第一天的时间戳
     *
     * @return
     * @author: liujj
     * @date: 2017年2月23日 下午5:26:16
     * @version: V1.1
     */
    public static long getNowFirstDate() {
        Date today = new Date();
        Date firstDay = getFirstDate(today);
        return firstDay.getTime();
    }

    /**
     * 获取当月第一天，将其向后推day天
     *
     * @param day 向后推的天数
     * @return
     * @author: liujj
     * @date: 2017年2月23日 下午5:30:35
     * @version: V1.1
     */
    public static long getNowBackDate(int day) {
        Date today = new Date();
        Date firstDay = getFirstDate(today);
        firstDay.setTime(firstDay.getTime() + 86400000L * day);
        return firstDay.getTime();
    }


    /**
     * 得到输入时间前一个月的开始时间
     * 例如 2017-01-06 00:00:00--->2016-12-01 00:00:00
     *
     * @param dateStr 时间字符串
     * @return
     */
    public static String getLastMonthStarter(String dateStr) {
        int year = Integer.parseInt(dateStr.split("-")[0]);
        int month = Integer.parseInt(dateStr.split("-")[1]) - 1;
        String resultMonth = "";
        String result = "";
        if (month < TEN) {
            resultMonth = "0" + month;
        } else {
            resultMonth = month + "";
        }
        if (month == 0) {
            result = year - 1 + "-" + 12 + "-" + "01" + " 00:00:00";
        } else {
            result = year + "-" + resultMonth + "-" + "01" + " 00:00:00";
        }

        return result;
    }

    /**
     * 获取输入时间下个月时间的开始时间
     * 例如 2016-12-01 00:00:00--->2017-01-01 00:00:00
     *
     * @param dateStr 时间
     * @return
     */
    public static String getNextMonthStarter(String dateStr) {
        int year = Integer.parseInt(dateStr.split("-")[0]);
        int month = Integer.parseInt(dateStr.split("-")[1]) + 1;
        String resultMonth = "";
        String result = "";
        if (month < TEN) {
            resultMonth = "0" + month;
        } else {
            resultMonth = month + "";
        }
        if (month == THIRTEEN) {
            result = year + 1 + "-" + "01-" + "01" + " 00:00:00";
        } else {
            result = year + "-" + resultMonth + "-" + "01" + " 00:00:00";
        }

        return result;
    }


    /**
     * @param timestamp
     * @return
     * @Description: 转换时间戳格式为String格式时间 yyyy-MM-dd hh-mm-ss
     * @author: Akang He
     * @date: Mar 15, 2017 5:36:22 PM
     */
    public static String transFormTimeStampToString(String timestamp) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        long longtime = Long.parseLong(timestamp);
        Date date = new Date(longtime);
        return sdf.format(date);
    }


    /**
     * @param str yyyy-MM-dd HH:mm:ss  型时间字符串
     * @return
     * @Description: 时间字符串转时间戳
     * @author: Akang He
     * @date: Mar 15, 2017 5:46:04 PM
     */
    public static Long tranFormStrToTimestamp(String str) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date = null;
        try {
            date = sdf.parse(str);
        } catch (ParseException e) {
            log.error("", e);
            return null;
        }
        return date.getTime();
    }

    /**
     * 转换String类型Date格式的数据至String类型的yyyy-MM-dd HH:mm:ss格式
     *
     * @param str ex:Mon Jun 13 11:26:47 CST 2016
     * @return sDate   ex:2016-06-13 11:26:47
     * @author: Akang He
     * @date: Mar 16, 2017 11:50:18 AM
     */
    public static String tranFormStringDateToString(String str) {
        SimpleDateFormat sdf1 = new SimpleDateFormat(
                "EEE MMM dd HH:mm:ss Z yyyy", Locale.UK);
        String sDate = "";
        try {
            Date date = sdf1.parse(str);
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            sDate = sdf.format(date);
        } catch (ParseException e) {
            log.error("", e);
        }
        return sDate;
    }

    /**
     * 获取传递时间当天零点的时间
     *
     * @param currentDate
     * @return
     * @author liujj
     * @date 2018年7月4日 上午11:10:43
     * @version V1.6.2
     */
    public static Date getCurrentDateStart(Date currentDate) {
        String currentTime = DateUtil.tranferDateToStr(currentDate, DateUtil.YYYY_MM_DD);
        String startTime = currentTime + " 00:00:00";
        Date startDate = null;
        try {
            startDate = DateUtil.transferStrToDate(startTime, DateUtil.YYYY_MM_DD_HH_MM_SS);
        } catch (ParseException e) {
            log.error("", e);
        }
        return startDate;
    }

    /**
     * 获取传递时间当天24点的时间
     *
     * @param currentDate
     * @return
     * @author liujj
     * @date 2018年7月4日 上午11:10:43
     * @version V1.6.2
     */
    public static Date getCurrentDateEnd(Date currentDate) {
        String currentTime = DateUtil.tranferDateToStr(currentDate, DateUtil.YYYY_MM_DD);
        String endTime = currentTime + " 23:59:59";
        Date endDate = null;
        try {
            endDate = DateUtil.transferStrToDate(endTime, DateUtil.YYYY_MM_DD_HH_MM_SS);
        } catch (ParseException e) {
            log.error("", e);
        }
        return endDate;
    }

    /**
     * 得到一个月前的date
     *
     * @param date 日期
     * @return Date
     * @author: Akang He
     * @date: Apr 16, 2017 9:16:52 PM
     */
    public static Date getOneMonthBeforDate(Date date) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.add(Calendar.DAY_OF_MONTH, -30);
        return calendar.getTime();
    }

}
