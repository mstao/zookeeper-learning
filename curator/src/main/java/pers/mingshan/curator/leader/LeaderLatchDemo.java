package pers.mingshan.curator.leader;

import java.util.List;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

/**
 * 在zookeeper集群中，leader负责写操作，然后通过Zab协议实现follower的同步，leader或者follower都可以处理读操作。
 * 
 * LeaderLatch是一旦选举出Leader，除非有客户端挂掉重新触发选举，否则不会交出领导权
 * @author mingshan
 *
 */
public class LeaderLatchDemo {
    private static final Logger logger = LoggerFactory.getLogger(LeaderLatchDemo.class);
    // Zookeeper的基本配置
    private static final String ZK_ADDRESS = "127.0.0.1:2181";
    // 命名空间即根路经
    private static final String NAME_SPACE = "zkbase";
    private static final String ZK_PATH = "/zktest";
    private static final int CLIENT_QTY = 10;
    
    public static void main(String[] args) throws Exception {
        List<CuratorFramework> clients = Lists.newArrayList();
        List<LeaderLatch> examples = Lists.newArrayList();

        try {
            for (int i = 0; i < CLIENT_QTY; i++) { 
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

                clients.add(client);

                LeaderLatch latch = new LeaderLatch(client, ZK_PATH, "Client #" + i);
                latch.addListener(new LeaderLatchListener() {

                    @Override
                    public void notLeader() {
                        logger.info("I am Leader");
                    }

                    @Override
                    public void isLeader() {
                        logger.info("I am not Leader");
                    }
                });

                examples.add(latch);
                client.start();
                latch.start();
            }
            
            Thread.sleep(10000);
            LeaderLatch currentLeader = null;
            for (LeaderLatch latch : examples) { 
                if (latch.hasLeadership()) { 
                    currentLeader = latch;
                } 
            } 
            logger.info("current leader is " + currentLeader.getId());
            logger.info("release the leader " + currentLeader.getId());
            currentLeader.close();
            Thread.sleep(5000);
            for (LeaderLatch latch : examples) {
                if (latch.hasLeadership()) { 
                    currentLeader = latch; 
                }
            } 
            logger.info("current leader is " + currentLeader.getId()); 
            logger.info("release the leader " + currentLeader.getId());
        } finally {
            logger.info("Shutting down...");
            for (LeaderLatch latch : examples) { 
                if (null != latch.getState())
                    CloseableUtils.closeQuietly(latch);
            }
            for (CuratorFramework client : clients) { 
                CloseableUtils.closeQuietly(client); 
            }
       }

    }
}
