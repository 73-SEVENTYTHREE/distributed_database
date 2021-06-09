import RECORDMANAGER.ReturnData;
import ZOOKEEPERMANAGER.ZookeeperManager;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;

/**
 * @author zzy
 * @date 2021/6/7
 * 从节点服务器，管理客户端连接线程和zookeeper的连接
 */
public class RegionServer {
    static final int ClientPort = 8001;
    static ArrayList<Thread> threads = new ArrayList<>();

    public static void main(String[] args) {
        try {
            ServerSocket serverSocket = new ServerSocket(ClientPort);
            API.initial();
            ZookeeperManager.zookeeperConnect();
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
