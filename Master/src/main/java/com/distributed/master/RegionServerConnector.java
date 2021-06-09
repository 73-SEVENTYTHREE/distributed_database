/**
 * 监听RegionServer的请求，传回该RegionServer的名字
 * Author: Wei Liu
 * Date: 2021/6/9
 * */

package com.distributed.master;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;

import static com.distributed.master.Master.numberOfRegionServer;
import static com.distributed.master.Master.regionNameTable;

public class RegionServerConnector extends Thread{
    private final ServerSocket serverSocket;
    public RegionServerConnector(int port) throws IOException{
        serverSocket = new ServerSocket(port);
        serverSocket.setSoTimeout(Integer.MAX_VALUE);
    }

    @Override
    public void run()
    {
        while(true)
        {
            try
            {
                Socket server = serverSocket.accept();
                DataInputStream in = new DataInputStream(server.getInputStream());

                String response = ""; //回复报文
                String url = in.readUTF();
                if(regionNameTable.containsKey(url)){
                    System.out.println(url + " reconnected.");
                    response = regionNameTable.get(url);
                }
                else{
                    response = "/server-" + numberOfRegionServer;
                    numberOfRegionServer++;
                    regionNameTable.put(url, response);
                }
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
}
