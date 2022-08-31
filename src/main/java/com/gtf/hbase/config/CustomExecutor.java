package com.gtf.hbase.config;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nonnull;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 自定义线程池配置类
 *
 * @author hanhuafeng
 * @date 2020/01/13 12:17
 */
@Slf4j
public class CustomExecutor extends ThreadPoolExecutor {
    /**
     * volatile 防止指令重排序
     */
    private static volatile CustomExecutor executor;

    private static final int CORE_POOL_SIZE = 20;

    private static final int MAXIMUM_POOL_SIZE = 40;

    private static final long KEEP_ALIVE_TIME = 120L;

    private static final TimeUnit UNIT = TimeUnit.SECONDS;


    private CustomExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit, BlockingQueue<Runnable> workQueue) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue);
    }

    private CustomExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit, BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory);
    }

    private CustomExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit, BlockingQueue<Runnable> workQueue, RejectedExecutionHandler handler) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, handler);
    }

    private CustomExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit, BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory, RejectedExecutionHandler handler) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory, handler);
    }

    /**
     *  初始化线程池，使用无界队列
     */
    private static void initThreadPoolExecutor(){
        executor = new CustomExecutor(CORE_POOL_SIZE, MAXIMUM_POOL_SIZE, KEEP_ALIVE_TIME, UNIT,
                new LinkedBlockingQueue<>(), new NameTreadFactory(), new MyIgnorePolicy());
    }

    @Override
    public void shutdown() {
        log.error("公共线程池不允许被关闭！");
    }

    @Override
    @Nonnull
    public List<Runnable> shutdownNow() {
        log.error("公共线程池不允许被关闭！");
        return new LinkedList<>();
    }

    /**
     * 线程池提供shutdown方法，以供使用
     */
    public void threadPoolShutdown(){
        super.shutdown();
    }

    /**
     * 线程池提供shutdownNow方法，以供使用
     */
    public void threadPoolShutdownNow(){
        super.shutdownNow();
    }

    /**
     * 自定义线程名称工厂
     *
     * @author hanhuafeng
     */
    public static class NameTreadFactory implements ThreadFactory {
        private final AtomicInteger mThreadNum = new AtomicInteger(1);
        @Override
        public Thread newThread(@NonNull Runnable r) {
            Thread t = new Thread(r, "custom-thread-" + mThreadNum.getAndIncrement());
            log.debug(t.getName() + " has been created");
            return t;
        }
    }

    /**
     * 线程饱和处理策略
     *
     * @author liujijiang
     * @date 2020/6/16 12:29
     */
    static class MyIgnorePolicy implements RejectedExecutionHandler {
        @Override
        public void rejectedExecution(Runnable r, ThreadPoolExecutor e) {
            doLog(r, e);
        }
        private void doLog(Runnable r, ThreadPoolExecutor e) {
            // 可做日志记录等
            log.error(r.toString() + " rejected");
        }
    }

    /**
     * 单例模式获取线程池实例化对象
     *
     * @return 实例化对象
     */
    public static CustomExecutor getThreadPoolExecutorInstance() {
        if (executor == null||executor.isShutdown()){
            synchronized (CustomExecutor.class){
                if (executor == null||executor.isShutdown()){
                    initThreadPoolExecutor();
                }
            }
        }
        return executor;
    }
}
