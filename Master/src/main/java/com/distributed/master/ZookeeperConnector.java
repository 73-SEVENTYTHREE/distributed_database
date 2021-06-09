package com.distributed.master;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class ZookeeperConnector extends Thread{
    private static final String ConnectString = "localhost:2181";
    public ZookeeperConnector(HashMap<String, List<String>> dictionary) throws IOException {
        try {
            getNode(dictionary);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    @Override
    public void run(){
        super.start();
        try {
            getNode(Master.dictionary);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /*
    * 监听Zookeeper集群变化，获取各RegionServer的IP地址和表名
    * Creator: Wei Liu
    * Date: 2021/6/6
    * */
    public static void getNode(HashMap<String, List<String>> dictionary) throws Exception{
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(2000,3);
        CuratorFramework client = CuratorFrameworkFactory.builder().connectString(ConnectString)
                .retryPolicy(retryPolicy)
                .namespace("service")
                .build();
        client.start();
        PathChildrenCache cache = new PathChildrenCache(client, "/", true);
        cache.start();
        cache.getListenable().addListener((c, event) -> {
            System.out.println(event.getType());
            List<String> children = client.getChildren().forPath("/");
            for(String node : children){
                String names = new String(client.getData().forPath("/" + node));
                String[] info = names.split(" ");
                String url = info[0];
                List<String> tableName = new ArrayList<>();
                if(info.length > 1){
                    for(int i = 1; i < info.length; i++) tableName.add(info[i]);
                }
                dictionary.put(url, tableName);
            }
            System.out.println("RegionServer和表的对应关系如下：");
            System.out.println(dictionary);
        });
        Thread.sleep(Integer.MAX_VALUE);
    }


}
