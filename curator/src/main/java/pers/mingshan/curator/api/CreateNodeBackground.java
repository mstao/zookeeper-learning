package pers.mingshan.curator.api;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 异步接口
 * 
 * {@link BackgroundCallback}接口用于处理异步接口调用之后服务端返回的结果信息
 * <br>
 * 
 * CuratorEventType
 * 
 * <br>
 * <pre>
 * 事件类型    对应CuratorFramework实例的方法
 * 
 *  CREATE  #create()
 *  DELETE  #delete()
 *  EXISTS  #checkExists()
 *  GET_DATA    #getData()
 *  SET_DATA    #setData()
 *  CHILDREN    #getChildren()
 *  SYNC    #sync(String,Object)
 *  GET_ACL     #getACL()
 *  SET_ACL     #setACL()
 *  WATCHED     #Watcher(Watcher)
 *  CLOSING     #close()
 * </pre>
 * 
 * <br>
 * 响应码(#getResultCode())
 * <br>
 * 
 * <pre>
 * 响应码      意义
 * 
 *  0   OK，即调用成功
 *  -4  ConnectionLoss，即客户端与服务端断开连接
 *  -110    NodeExists，即节点已经存在
 *  -112    SessionExpired，即会话过期
 *  
 * </pre>
 * @author mingshan
 *
 */
public class CreateNodeBackground {
    private static final Logger logger = LoggerFactory.getLogger(CuratorClient.class);
    // Zookeeper的基本配置
    private static final String ZK_ADDRESS = "127.0.0.1:2181";
    // 命名空间即根路经
    private static final String NAME_SPACE = "zkbase";
    private static final String ZK_PATH = "/zktest";

    private static CountDownLatch latch = new CountDownLatch(1);
    private static ExecutorService executor = Executors.newFixedThreadPool(2);

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

        // 创建节点
        // 此处传入自定义的Executor 
        // 异步通知事件由自定义的线程池来处理
        client.create()
            .creatingParentContainersIfNeeded()
            .withMode(CreateMode.EPHEMERAL)
            .inBackground(new BackgroundCallback() {

                @Override
                public void processResult(CuratorFramework client, CuratorEvent event) throws Exception {
                    logger.info("event[code: " + event.getResultCode() + ",type: " +
                            event.getType() + "]");

                    logger.info("Thread of processResult: " + Thread.currentThread().getName());
                }
            }, executor).forPath(ZK_PATH,"init".getBytes());

        // 此处没有传入自定义的Executor 
        // 使用ZooKeeper默认的EventThread来处理
        client.create()
            .creatingParentContainersIfNeeded()
            .withMode(CreateMode.EPHEMERAL)
            .inBackground(new BackgroundCallback() {

                @Override
                public void processResult(CuratorFramework client, CuratorEvent event) throws Exception {
                    logger.info("event[code: " + event.getResultCode() + ",type: " +
                            event.getType() + "]");
    
                    logger.info("Thread of processResult: " + Thread.currentThread().getName());
                    latch.countDown();
                }
        }).forPath(ZK_PATH,"init".getBytes());

        latch.await();
        executor.shutdown();
    }
}
