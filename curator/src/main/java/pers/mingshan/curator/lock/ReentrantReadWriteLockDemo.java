package pers.mingshan.curator.lock;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.framework.recipes.locks.InterProcessReadWriteLock;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;

/**
 * 可重入读写锁
 * 
 * 类似JDK的ReentrantReadWriteLock。一个读写锁管理一对相关的锁。
 * 一个负责读操作，另外一个负责写操作。读操作在写锁没被使用时可同时由多个进程使用，而写锁在使用时不允许读(阻塞)。

 * 此锁是可重入的。一个拥有写锁的线程可重入读锁，但是读锁却不能进入写锁。
 * 这也意味着写锁可以降级成读锁， 比如请求写锁 --->请求读锁--->释放读锁 ---->释放写锁。从读锁升级成写锁是不行的。

 * 可重入读写锁主要由两个类实现：InterProcessReadWriteLock、InterProcessMutex。
 * 使用时首先创建一个InterProcessReadWriteLock实例，然后再根据你的需求得到读锁或者写锁，
 * 读写锁的类型是InterProcessMutex。
 * 
 * @author mingshan
 *
 */
public class ReentrantReadWriteLockDemo {

    private final InterProcessReadWriteLock lock;
    private final InterProcessMutex readLock;
    private final InterProcessMutex writeLock;
    private final FakeLimitedResource resource;
    private final String clientName;

    // Zookeeper的基本配置
    private static final String ZK_ADDRESS = "127.0.0.1:2181";
    // 命名空间即根路经
    private static final String NAME_SPACE = "zkbase";
    private static final String ZK_PATH = "/zktest/locks";
    private static final int QTY = 5;
    private static final int REPETITIONS = QTY;

    public ReentrantReadWriteLockDemo(CuratorFramework client, String lockPath, FakeLimitedResource resource, String clientName) {
        this.resource = resource;
        this.clientName = clientName;
        lock = new InterProcessReadWriteLock(client, lockPath);
        readLock = lock.readLock();
        writeLock = lock.writeLock();
    }

    public void doWork(long time, TimeUnit unit) throws Exception {
        // 注意只能先得到写锁再得到读锁，不能反过来！！！
        if (!writeLock.acquire(time, unit)) {
            throw new IllegalStateException(clientName + " 不能得到写锁");
        }
        System.out.println(clientName + " 已得到写锁");
        if (!readLock.acquire(time, unit)) {
            throw new IllegalStateException(clientName + " 不能得到读锁");
        }
        System.out.println(clientName + " 已得到读锁");
        try {
            resource.use(); // 使用资源
            Thread.sleep(1000);
        } finally {
            System.out.println(clientName + " 释放读写锁");
            readLock.release();
            writeLock.release();
        }
    }

    public static void main(String[] args) throws Exception {
        final FakeLimitedResource resource = new FakeLimitedResource();
        ExecutorService service = Executors.newFixedThreadPool(QTY);

        try {
            for (int i = 0; i < QTY; ++i) {
                final int index = i;
                Callable<Void> task = new Callable<Void>() {
                    @Override
                    public Void call() throws Exception {
                        // 使用Fluent风格的Api创建会话
                        // RetryPolicy为重试策略,内建有四种重试策略
                        RetryPolicy retryPolicy = new ExponentialBackoffRetry(20000, 3);
                        CuratorFramework client =
                                CuratorFrameworkFactory.builder()
                                .connectString(ZK_ADDRESS)
                                .sessionTimeoutMs(5000)
                                .connectionTimeoutMs(5000)
                                .retryPolicy(retryPolicy)
                                .namespace(NAME_SPACE)
                                .build();

                        try {
                            client.start();
                            final ReentrantReadWriteLockDemo example = new ReentrantReadWriteLockDemo(client, ZK_PATH, resource, "Client " + index);
                            for (int j = 0; j < REPETITIONS; ++j) {
                                example.doWork(10, TimeUnit.SECONDS);
                            }
                        } catch (Throwable e) {
                            e.printStackTrace();
                        } finally {
                            CloseableUtils.closeQuietly(client);
                        }
                        return null;
                    }
                };
                service.submit(task);
            }
            service.shutdown();
            service.awaitTermination(10, TimeUnit.MINUTES);
        } finally {

        }
    }
}
