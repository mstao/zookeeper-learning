package pers.mingshan.curator.lock;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;

import com.google.common.collect.Lists;

/**
 * 利用 InterProcessMutex 来实现可重入分布式锁
 * 
 * 
 * @author mingshan
 *
 */
public class InterProcessMutexDemo {
    private InterProcessMutex lock;
    private final FakeLimitedResource resource;
    private final String clientName;

    // Zookeeper的基本配置
    private static final String ZK_ADDRESS = "127.0.0.1:2181";
    // 命名空间即根路经
    private static final String NAME_SPACE = "zkbase";
    private static final String ZK_PATH = "/zktest/locks";
    private static final int QTY = 5;
    private static final int REPETITIONS = QTY;

    public InterProcessMutexDemo(CuratorFramework client, String lockPath, FakeLimitedResource resource, String clientName) {
        this.resource = resource;
        this.clientName = clientName;
        this.lock = new InterProcessMutex(client, lockPath);
    }

    public void doWork(long time, TimeUnit unit) throws Exception {
        if (!lock.acquire(time, unit)) {
            throw new IllegalStateException(clientName + " could not acquire the lock");
        }
        try {
            System.out.println(clientName + " get the lock");
            resource.use(); //access resource exclusively
        } finally {
            System.out.println(clientName + " releasing the lock");
            lock.release(); // always release the lock in a finally block
        }
    }

    public static void main(String[] args) throws Exception {
        final FakeLimitedResource resource = new FakeLimitedResource();
        ExecutorService service = Executors.newFixedThreadPool(QTY);
        List<CuratorFramework> clients = Lists.newArrayList();

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
                            clients.add(client);
                            client.start();
                            final InterProcessMutexDemo example = new InterProcessMutexDemo(client, ZK_PATH, resource, "Client " + index);
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
//            for (CuratorFramework client : clients) {
//                CloseableUtils.closeQuietly(client);
//            }
        }
    }
}
