package com.distributed.client;
import java.io.*;
import java.net.Socket;
import java.util.Scanner;

public class Client {
    private static final String ServerName = "127.0.0.1";
    private static final int Port = 8000;
    public static void main(String[] args){
        BufferedReader br;
        String str;
        try {
            br = new BufferedReader(new InputStreamReader(System.in));
            while (true) {
                str = br.readLine();
                if (str.equals("quit"))
                    break;
                String tableName = getTableName(str);
                if(tableName.equals("-1")) continue;
                boolean state = sendData(tableName);
                if(state) System.out.println("Send Data to Master Successfully!");
                else System.out.println("Fail to Send Data to Master");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    /*
    * 发送数据给Master
    * Creator: Wei Liu
    * Date: 2021/6/6
    * */
    public static boolean sendData(String data){
        //建立socket连接
        try
        {
            Socket client = new Socket(ServerName, Port);
            OutputStream outToServer = client.getOutputStream();
            DataOutputStream out = new DataOutputStream(outToServer);
            out.writeUTF(data);
            InputStream inFromServer = client.getInputStream();

            DataInputStream in = new DataInputStream(inFromServer);
            String response = in.readUTF();
            System.out.println("服务器响应： " + response);
            return true;
        }catch(IOException e)
        {
            e.printStackTrace();
            return false;
        }
    }

    /*
    * 解析输入语句，获取表名
    * Creator: Wei Liu
    * Date: 2021/6/6
    * */
    public static String getTableName(String input){
        String result = input.trim().replaceAll("\\s+", " ");
        String[] tokens = result.split(" ");
        //匹配关键字
        switch (tokens[0]) {
            case "create" -> {
                if (tokens.length == 1) {
                    System.out.println("No create object!");
                    return "-1";
                }
                switch (tokens[1]) {
                    case "table", "index" -> {
                        return "create " + tokens[2];
                    }
                    default -> {
                        System.out.println("Wrong Input!");
                        return "-1";
                    }
                }
            }
            case "drop" -> {
                if (tokens.length == 1){
                    System.out.println("No drop object");
                    return "-1";
                }

                switch (tokens[1]) {
                    case "table", "index" -> {
                        return tokens[2];
                    }
                    default -> {
                        System.out.println("Wrong Input!");
                        return "-1";
                    }
                }
            }
            case "select" -> {
                if(tokens.length < 3) {
                    System.out.println("No select object!");
                    return "-1";
                }
                return tokens[3];
            }
            case "show", "insert", "delete" -> {
                if(tokens.length == 1) {
                    System.out.println("No operating object!");
                    return "-1";
                }
                return tokens[2];
            }
            default -> {
                System.out.println("Wrong Input!");
                return "-1";
            }
        }
    }
}
