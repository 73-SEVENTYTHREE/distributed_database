import CATALOGMANAGER.CatalogManager;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Scanner;

/**
 * @author zzy
 * @date 2021/6/7
 * 从节点服务器，管理客户端连接线程和zookeeper的连接
 */
public class RegionServer {
    static String regionServerIP;
    static final int port = 8001;
    static final String zookeeperServer = "127.0.0.1";
    static CuratorFramework client;
    static ArrayList<String> tableNames;
    static ArrayList<Thread> threads = new ArrayList<>();

    public static void main(String[] args) {
        try {
        ServerSocket serverSocket = new ServerSocket(port);
        API.initial();
        tableNames = CatalogManager.get_tables();
        regionServerIP = String.valueOf(InetAddress.getLocalHost().getHostAddress());
        zookeeperConnect();
        while(true){
            Socket socket = serverSocket.accept();
            Thread regionThread = new RegionServerThread(socket);
            regionThread.start();
            threads.add(regionThread);
        }
        } catch (Exception e) {
            client.close();
            e.printStackTrace();
        }
    }

    @Test
    private static void zookeeperConnect(){
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(3000,10);
        client = CuratorFrameworkFactory.builder()
                .connectString(zookeeperServer)
                .retryPolicy(retryPolicy)
                .namespace("service")
                .build();
        client.start();
        String data = regionServerIP;
        for(String t : tableNames){
            data += " ";
            data += t;
        }
        try {
            client.create().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath("/serv", data.getBytes());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}


/**
 * @author zzy
 * @date 2021/6/7
 * 用来处理客户端连接的线程
 */
class RegionServerThread extends Thread{
    Socket socket = null;

    public RegionServerThread(Socket socket){
        this.socket = socket;
    }

    @Override
    public void run() {
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            Interpreter.interpret(reader);
        } catch (IOException e) {
            System.out.println("101 Run time error : IO exception occurs");
        } catch (Exception e) {
            System.out.println("Default error: " + e.getMessage());
        }
    }
}
