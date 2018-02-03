package pers.mingshan.curator.api;

import java.util.concurrent.TimeUnit;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 基于Curator的分布式锁
 * 
 * @author mingshan
 *
 */
public class CuratorDistributeLock {
    private static final Logger logger = LoggerFactory.getLogger(CuratorDistributeLock.class);
    // Zookeeper的基本配置
    private static final String ZK_ADDRESS = "127.0.0.1:2181";
    // 命名空间即根路经
    private static final String NAME_SPACE = "zkbase";
    private static final String ZK_PATH = "/zktest";

    public static void main(String[] args) {
        // 使用Fluent风格的Api创建会话
        // RetryPolicy为重试策略,内建有四种重试策略
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        CuratorFramework client =
            CuratorFrameworkFactory.builder()
                    .connectString(ZK_ADDRESS)
                    .sessionTimeoutMs(5000)
                    .connectionTimeoutMs(5000)
                    .retryPolicy(retryPolicy)
                    .namespace(NAME_SPACE)
                    .build();

        // 启动
        client.start();
        logger.info("zk client start successfully!");
        
        Thread t1 = new Thread(() -> {
            doWithLock(client);
        }, "t1");
        Thread t2 = new Thread(() -> {
            doWithLock(client);
        }, "t2");

        t1.start();
        t2.start();
    }

    private static void doWithLock(CuratorFramework client) {
        InterProcessMutex lock = new InterProcessMutex(client, ZK_PATH);
        try {
            if (lock.acquire(10 * 1000, TimeUnit.SECONDS)) {
                logger.info(Thread.currentThread().getName() + " hold lock");
                Thread.sleep(5000L);
                logger.info(Thread.currentThread().getName() + " release lock");
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                lock.release();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
