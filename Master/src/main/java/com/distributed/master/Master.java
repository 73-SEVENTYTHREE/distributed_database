package com.distributed.master;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Master extends Thread {
    private final ServerSocket serverSocket;
    //储存IP地址和表的映射关系的哈希Map
    public static HashMap<String, List<String>> dictionary = new HashMap<>();
    public Master(int port) throws IOException
    {
        serverSocket = new ServerSocket(port);
        serverSocket.setSoTimeout(Integer.MAX_VALUE);
    }

    //判断表是否存在
    public String getUrlByTableName(String tableName){
        String url = "-1";
        for(Map.Entry<String, List<String>> entry : dictionary.entrySet()){
            if (entry.getValue().contains(tableName)) {
                url = entry.getKey();
                break;
            }
        }
        return url;
    }

    //寻找存储表最少的从节点
    public String getMostFreeRegionServer(){
        String url = "";
        int min = Integer.MAX_VALUE;
        for(Map.Entry<String, List<String>> entry : dictionary.entrySet()){
            if (entry.getValue().size() < min) {
                url = entry.getKey();
                min = entry.getValue().size();
            }
        }
        return url;
    }

    public void run()
    {
        while(true)
        {
            try
            {
                Socket server = serverSocket.accept();
                DataInputStream in = new DataInputStream(server.getInputStream());

                String response = ""; //回复报文

                String[] tableName = in.readUTF().split(" ");
                System.out.println(Arrays.toString(tableName));
                String url = "";
                if(tableName.length == 1){
                    url = getUrlByTableName(tableName[0]);
                    if(url.equals("-1")) response = "The table does not exist!";
                    else response = url;
                }
                else{
                    url = getUrlByTableName(tableName[1]);
                    if(url.equals("-1")){
                        response = getMostFreeRegionServer();
                    }
                    else response = "The table already exists!";
                }
                System.out.println(response);
                DataOutputStream out = new DataOutputStream(server.getOutputStream());
                out.writeUTF(response);
                server.close();
            }catch(SocketTimeoutException s)
            {
                System.out.println("Socket timed out!");
                break;
            }catch(IOException e)
            {
                e.printStackTrace();
                break;
            }
        }
    }
    public static void main(String[] args)
    {
        try
        {
            Thread t1 = new Master(8000);
            t1.start();
            Thread t2 = new ZookeeperConnector(dictionary);
            t2.start();
        }catch(IOException e)
        {
            e.printStackTrace();
        }
    }
}
