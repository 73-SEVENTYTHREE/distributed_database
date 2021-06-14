package ZOOKEEPERMANAGER;

import CATALOGMANAGER.CatalogManager;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;

import java.io.*;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;

public class ZookeeperManager {
    static final String zookeeperServer = "10.181.195.16";
    static CuratorFramework client;
    static ArrayList<String> tableNames;
    static String regionServerIP;
    static String path = "";
    private static final String masterServerName = "10.181.195.16";
    private static final int masterPort = 8002;

    /**
     * @author zzy
     * @date 2021/6/7
     * 连接zookeeper，并且创建一个serv节点
     */
    public static void zookeeperConnect(){
        tableNames = CatalogManager.get_tables();
        System.out.println(tableNames);
        try {
            regionServerIP = String.valueOf(GetLocalIPAddress.getLocalHostLANAddress()).substring(1);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        path = getRegionName();
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(3000,10);
        client = CuratorFrameworkFactory.builder()
                .connectString(zookeeperServer)
                .retryPolicy(retryPolicy)
                .namespace("service")
                .build();
        client.start();
        StringBuilder data = new StringBuilder(regionServerIP);
        for(String t : tableNames){
            data.append(" ");
            data.append(t);
        }
        try {
            Stat isExist = client.checkExists().forPath(path);
            if (isExist != null) {
                client.delete().forPath(path);
            }else{
                FTPConnector.uploadFile("table_catalog");
                FTPConnector.uploadFile("index_catalog");
                for(String t : tableNames){
                    FTPConnector.uploadFile(t);
                    FTPConnector.uploadFile(t+"_index.index");

                }

            }
            client.create().withMode(CreateMode.EPHEMERAL).forPath(path, data.toString().getBytes());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void tableChange(){
        String data = regionServerIP;
        tableNames = CatalogManager.get_tables();
        System.out.println(tableNames);
        for(String t : tableNames){
            data += " ";
            data += t;
        }
        try {
            client.setData().forPath(path, data.getBytes());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void connectClose(){
        client.close();
    }

    public static String getPath(){ return path; }

    public static String getRegionName() {
        try
        {
            Socket client = new Socket(masterServerName, masterPort);
            OutputStream outToServer = client.getOutputStream();
            DataOutputStream out = new DataOutputStream(outToServer);
            out.writeUTF(regionServerIP);
            InputStream inFromServer = client.getInputStream();
            DataInputStream in = new DataInputStream(inFromServer);
            String name = in.readUTF();
            client.close();
            return name;
        }catch(IOException e)
        {
            e.printStackTrace();
            return "error";
        }
    }
}
