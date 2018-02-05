package pers.mingshan.curator.cache;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pers.mingshan.curator.api.CuratorClient;

/**
 * Tree Cache可以监控整个树上的所有节点，类似于PathCache和NodeCache的组合，主要涉及到下面四个类：
 * 
 *  TreeCache - Tree Cache实现类
 *  TreeCacheListener - 监听器类
 *  TreeCacheEvent - 触发的事件类
 *  ChildData - 节点数据
 * 
 * Notice:
 * 
 * TreeCache在初始化(调用start()方法)的时候会回调TreeCacheListener实例一个事TreeCacheEvent，
 * 而回调的TreeCacheEvent对象的Type为INITIALIZED，ChildData为null，
 * 此时event.getData().getPath()很有可能导致空指针异常，这里应该主动处理并避免这种情况。
 * 
 * @author mingshan
 *
 */
public class TreeCacheDemo {
    private static final Logger logger = LoggerFactory.getLogger(CuratorClient.class);
    // Zookeeper的基本配置
    private static final String ZK_ADDRESS = "127.0.0.1:2181";
    // 命名空间即根路经
    private static final String NAME_SPACE = "zkbase";
    private static final String ZK_PATH = "/zktest/treecache";

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
        TreeCache cache = new TreeCache(client, ZK_PATH);
        TreeCacheListener listener = (client1, event) -> System.out.println(
                "事件类型：" + event.getType() + " | 路径：" + (null != event.getData() ? event.getData().getPath() : null));
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
