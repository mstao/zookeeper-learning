package pers.mingshan.curator.cache;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pers.mingshan.curator.api.CuratorClient;

/**
 * Node Cache只是监听某一个特定的节点
 * 
 * getCurrentData()将得到节点当前的状态，通过它的状态可以得到当前的值。
 * 
 * 它涉及到下面的三个类：
 *
 *  NodeCache - Node Cache实现类
 *  NodeCacheListener - 节点监听器
 *  ChildData - 节点数据
 * @author mingshan
 *
 */
public class NodeCacheDemo {
    private static final Logger logger = LoggerFactory.getLogger(CuratorClient.class);
    // Zookeeper的基本配置
    private static final String ZK_ADDRESS = "127.0.0.1:2181";
    // 命名空间即根路经
    private static final String NAME_SPACE = "zkbase";
    private static final String ZK_PATH = "/zktest/nodecache";

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

        client.create().creatingParentsIfNeeded().forPath(ZK_PATH);
        final NodeCache cache = new NodeCache(client, ZK_PATH);
        NodeCacheListener listener = () -> {
            ChildData data = cache.getCurrentData();
            if (null != data) {
                System.out.println("节点数据：" + new String(cache.getCurrentData().getData()));
            } else {
                System.out.println("节点被删除!");
            }
        };

        cache.getListenable().addListener(listener);
        cache.start();
        client.setData().forPath(ZK_PATH, "01".getBytes());
        Thread.sleep(100);
        client.setData().forPath(ZK_PATH, "02".getBytes());
        Thread.sleep(100);
        client.delete().deletingChildrenIfNeeded().forPath(ZK_PATH);
        Thread.sleep(1000 * 2);
        cache.close();
        client.close();
        System.out.println("OK!");
    }
}
