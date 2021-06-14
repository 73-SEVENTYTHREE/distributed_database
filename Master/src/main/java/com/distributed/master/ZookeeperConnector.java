package com.distributed.master;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.io.*;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

public class ZookeeperConnector extends Thread{
    private static final String ConnectString = "localhost:2181";
    private static final int FailurePort =8010;
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
            if(event.getType()== PathChildrenCacheEvent.Type.CHILD_REMOVED)
            {
                String path = event.getData().getPath();
                System.out.println(path+" disconnected!");
                String url= new String("-1");
                List<String> children = client.getChildren().forPath("/");
                for(String node : children){
                    String tables = new String(client.getData().forPath("/" + node));
                    String[] info = tables.split(" ");
                    if(info.length == 1){
                        url = info[0];
                        break;
                    }
                }
                //url = Master.getMostFreeRegionServer();
                //if(Master.dictionary.get(url).size()!=0) url="-1";
                if(!url.equals("-1"))
                {
                    try {
                        Socket socket = new Socket(url, FailurePort);
                        OutputStream outToRegionServer = socket.getOutputStream();
                        DataOutputStream out = new DataOutputStream(outToRegionServer);
                        out.writeUTF(path);
                        InputStream inFromRegionServer = socket.getInputStream();
                        DataInputStream in = new DataInputStream(inFromRegionServer);
                        String result = in.readUTF();
                        socket.close();
                        System.out.println(result);
                    }catch(IOException e)
                    {
                        e.printStackTrace();
                    }
                }
                else{
                    System.out.println("Can not Find Empty RegionServer!");
                }

            }
            List<String> children = client.getChildren().forPath("/");
            dictionary.clear();
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
