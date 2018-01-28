package pers.mingshan.curator.api;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Curator的基本Api
 * 
 * 使用 Curator进行连接zookeeper，以及节点的增删改查
 * 
 * @author mingshan
 *
 */
public class CuratorClient {
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

        /*
         * ZNode有以下几种类型
         * 
         * PERSISTENT：持久化
         * PERSISTENT_SEQUENTIAL：持久化并且带序列号
         * EPHEMERAL：临时
         * EPHEMERAL_SEQUENTIAL：临时并且带序列号
         *
         */

        /** 2.Client API test*/
        // 2.1 Create node
        // creatingParentsIfNeeded 自动递归创建父节点
        String data1 = "hello";
        print("create", ZK_PATH, data1);
        client.create()
            .creatingParentsIfNeeded()
            .withMode(CreateMode.EPHEMERAL)
            .forPath(ZK_PATH, data1.getBytes());

        // 2.2 Get node and data
        print("ls", "/");
        print(client.getChildren().forPath("/"));
        print("get", ZK_PATH);

        // 获取到该节点的Stat
        Stat stat = new Stat();
        print(client.getData().storingStatIn(stat).forPath(ZK_PATH));

        // 2.3 Modify data
        String data2 = "world";
        print("set", ZK_PATH, data2);
        client.setData().forPath(ZK_PATH, data2.getBytes());
        print("get", ZK_PATH);
        print(client.getData().forPath(ZK_PATH));

        // 2.4 Remove node
        print("delete", ZK_PATH);
        client.delete().forPath(ZK_PATH);
        print("ls", "/");
        print(client.getChildren().forPath("/"));

        // Check Node is exist
        client.checkExists().forPath(ZK_PATH);
    }

    private static void print(String... cmds) {
        StringBuilder text = new StringBuilder("$ ");
        for (String cmd : cmds) {
            text.append(cmd).append(" ");
        }
        System.out.println(text.toString());
    }

    private static void print(Object result) {
        System.out.println(
                result instanceof byte[]
                    ? new String((byte[]) result)
                    : result);
    }
}
