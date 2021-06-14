import CATALOGMANAGER.CatalogManager;
import RECORDMANAGER.ReturnData;
import ZOOKEEPERMANAGER.FTPConnector;
import ZOOKEEPERMANAGER.ZookeeperManager;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.ArrayList;

/**
 * @author zzy
 * @date 2021/6/7
 * 从节点服务器，管理客户端连接线程和zookeeper的连接
 */
public class RegionServer {
    static final int ClientPort = 8001;
    static final int FailurePort =8010;
    static ArrayList<Thread> threads = new ArrayList<>();

    public static void main(String[] args) {
        try {
            ServerSocket serverSocket = new ServerSocket(ClientPort);
            API.initial();
            boolean ftpSuccess = FTPConnector.FTPConnect();
            ZookeeperManager.zookeeperConnect();
            Thread failureThread = new FailureThread();
            failureThread.start();
            while (true) {
                Socket socket = serverSocket.accept();
                Thread regionThread = new RegionServerThread(socket);
                regionThread.start();
                threads.add(regionThread);
            }
        } catch (Exception e) {
            ZookeeperManager.connectClose();
            e.printStackTrace();
        }
    }

}



/**
 * @author zzy
 * @date 2021/6/7
 * 用来处理客户端连接的线程
 */
class RegionServerThread extends Thread {
    Socket socket = null;
    ObjectOutputStream oout = null;

    public RegionServerThread(Socket socket) {
        this.socket = socket;
    }

    @Override
    public void run() {
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            this.oout = new ObjectOutputStream(socket.getOutputStream());
            String result = reader.readLine();
            ReturnData returnData = Interpreter.interpret(result);
            oout.writeObject(returnData);
            oout.flush();
        } catch (IOException e) {
            ReturnData returnData = new ReturnData(false, "101 Run time error : IO exception occurs");
            try {
                oout.writeObject(returnData);
                oout.flush();
            } catch (IOException ioException) {
                ioException.printStackTrace();
            }
        } catch (Exception e) {
            System.out.println("here3");
            ReturnData returnData = new ReturnData(false, "Default error: " + e.getMessage());
            try {
                oout.writeObject(returnData);
                oout.flush();
            } catch (IOException ioException) {
                ioException.printStackTrace();
            }
        }
    }
}

class FailureThread extends Thread {
    private final ServerSocket serverSocket;
    public FailureThread() throws IOException{
        serverSocket = new ServerSocket(RegionServer.FailurePort);
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

                String response = "Region Transform Fail!";
                String node = in.readUTF();
                System.out.println(node + " need to be reconstruct!");
                if(FTPConnector.downloadAllFiles(node,"")) {
                    response = "Region Transform Success!";
                    try {
                        API.initial();
                        ZookeeperManager.tableChange();
                    }catch (Exception e) {
                        e.printStackTrace();
                    }
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