package pers.mingshan.curator.cache;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pers.mingshan.curator.api.CuratorClient;

/**
 * Path Cache用来监控一个ZNode的子节点. 当一个子节点增加， 更新，删除时， Path Cache会改变它的状态，
 * 会包含最新的子节点， 子节点的数据和状态，而状态的更变将通过PathChildrenCacheListener通知。<br>
 * 
 * 通过下面的构造函数创建Path Cache:<br>
 * <code> public PathChildrenCache(CuratorFramework client, String path, boolean cacheData)</code>
 * 
 * @author mingshan
 *
 */
public class PathCacheDemo {
    private static final Logger logger = LoggerFactory.getLogger(CuratorClient.class);
    // Zookeeper的基本配置
    private static final String ZK_ADDRESS = "127.0.0.1:2181";
    // 命名空间即根路经
    private static final String NAME_SPACE = "zkbase";
    private static final String ZK_PATH = "/zktest";

    public static void main(String[] args) throws Exception {
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

        PathChildrenCache cache = new PathChildrenCache(client, ZK_PATH, true);
        cache.start();
        PathChildrenCacheListener cacheListener = (client1, event) -> {
            logger.info("事件类型：" + event.getType());
            if (null != event.getData()) {
                logger.info("节点数据：" + event.getData().getPath() + " = " + new String(event.getData().getData()));
            }
        };
        cache.getListenable().addListener(cacheListener);
        client.create().creatingParentsIfNeeded().forPath(ZK_PATH + "/test01", "01".getBytes());
        Thread.sleep(10);
        client.create().creatingParentsIfNeeded().forPath(ZK_PATH + "/test02", "02".getBytes());
        Thread.sleep(10);
        client.setData().forPath(ZK_PATH + "/test01", "01_V2".getBytes());
        Thread.sleep(10);
        for (ChildData data : cache.getCurrentData()) {
            logger.info("getCurrentData:" + data.getPath() + " = " + new String(data.getData()));
        }
        client.delete().forPath(ZK_PATH + "/test01");
        Thread.sleep(10);
        client.delete().forPath(ZK_PATH + "/test02");
        Thread.sleep(1000 * 5);
        cache.close();
        client.close();
        logger.info("OK!");
    }
}
