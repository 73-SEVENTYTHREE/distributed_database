package com.distributed.client;

import RECORDMANAGER.ReturnData;

import java.io.*;
import java.net.Socket;


public class Client {
    private static final String ServerName = "10.162.19.71";
    private static final int MasterPort = 8000;
    private static final int RegionServerPort = 8001;
    public static void main(String[] args){
        String restState = "";
        BufferedReader br;
        try {
            br = new BufferedReader(new InputStreamReader(System.in));
            while (true) {
                StringBuilder str = new StringBuilder();
                while(true){
                    int index;
                    String line = br.readLine();
                    if (line.equals("quit"))
                        System.exit(0);
                    if (line.contains(";")) { //last line
                        index = line.indexOf(";");
                        str.append(line.substring(0, index));
                        restState = line.substring(index + 1); //set reset statement
                        break;
                    } else {
                        str.append(line);
                        str.append(" ");
                    }
                }
                String result = str.toString().trim().replaceAll("\\s+", " ");
//                System.out.println(result);
                String tableName = getTableName(result);
                if(tableName.equals("-1")) continue;
                String state = sendDataToMaster(tableName);
                if(state.charAt(0)>='0'&&state.charAt(0)<='9')
                {
                    System.out.println("Send Data to Master Successfully!");
                    System.out.println("Try to send query to Region Server!");

                }
                else
                {
                    System.out.println(state);
                    continue;
                }
                ReturnData returnData = sendDataToRegionServer(state, result);
                showData(returnData);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    /**
     * @author Wei Liu
     * @date 2021/6/8
     * 将接回的数据展示
     */
    public static void showData(ReturnData returnData){
        int size = returnData.returnData.size();
        if(size > 0){
            for(int k = 0; k < returnData.returnData.get(size - 1).get_attribute_size(); k++){
                System.out.print("-------");
            }
            System.out.println();
            for(int j = 0; j < returnData.returnData.get(size - 1).get_attribute_size(); j++){
                System.out.print("| " + returnData.returnData.get(size - 1).get_attribute_value(j) + " ");
            }
            System.out.println("|");
            for(int i = 0; i < size - 1; i++){
                for(int k = 0; k < returnData.returnData.get(i).get_attribute_size(); k++){
                    System.out.print("-------");
                }
                System.out.println();
                for(int j = 0; j < returnData.returnData.get(i).get_attribute_size(); j++){
                    System.out.print("| " + returnData.returnData.get(i).get_attribute_value(j) + " ");
                }
                System.out.println("|");
            }
            for(int k = 0; k < returnData.returnData.get(0).get_attribute_size(); k++){
                System.out.print("-------");
            }
            System.out.println();
        }
        System.out.println(returnData.info);
    }

    /**
    * 发送数据给Master
    * Creator: Wei Liu
    * Date: 2021/6/6
    */
    public static String sendDataToMaster(String data){
        //建立socket连接
        try
        {
            Socket client = new Socket(ServerName, MasterPort);
            OutputStream outToServer = client.getOutputStream();
            DataOutputStream out = new DataOutputStream(outToServer);
            out.writeUTF(data);
            InputStream inFromServer = client.getInputStream();
            DataInputStream in = new DataInputStream(inFromServer);
            return in.readUTF();
        }catch(IOException e)
        {
            e.printStackTrace();
            return "-1";
        }
    }

    /**
     * 发送数据给RegionServer
     * Creator: Wei Liu
     * Date: 2021/6/6
     */
    public static ReturnData sendDataToRegionServer(String regionServer, String data){
        //建立socket连接
        try
        {
            Socket client = new Socket(regionServer, RegionServerPort);
            OutputStream outToServer = client.getOutputStream();
            PrintWriter pw = new PrintWriter(outToServer);
            pw.println(data);
            pw.flush();
            ObjectInputStream ois=new ObjectInputStream(client.getInputStream());
            ReturnData response = null;
            try {
                response = (ReturnData) ois.readObject();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
            return response;
        }catch(IOException e)
        {
            e.printStackTrace();
            return new ReturnData(false, "Fatal Error: IO error!");
        }
    }

    /**
    * 解析输入语句，获取表名
    * Creator: Wei Liu
    * Date: 2021/6/6
    */
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
