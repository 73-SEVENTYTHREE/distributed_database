package ZOOKEEPERMANAGER;

import java.io.*;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;

import ZOOKEEPERMANAGER.GetLocalIPAddress;
import ZOOKEEPERMANAGER.ZookeeperManager;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;

public class FTPConnector {
    private static final int FTP_port = 21;
    private static final String username = "root";
    private static final String password = "2569535507";
    private static final FTPClient ftpClient = new FTPClient();
    private static final String FTPIP = "10.162.19.71";

    public static boolean FTPConnect() throws Exception{
        ftpClient.connect(FTPIP, FTP_port); //连接ftp服务器
        ftpClient.login(username, password); //登录ftp服务器
        int replyCode = ftpClient.getReplyCode(); //replyCode表示的是返回的状态码。
        return FTPReply.isPositiveCompletion(replyCode);
    }

    public static Boolean downloadFile(String filePath, String fileName, String downloadPath){
        boolean flag = false;
        try {
            // 跳转到文件目录
            ftpClient.changeWorkingDirectory(filePath);
            // 获取目录下文件集合
            ftpClient.enterLocalPassiveMode();
            FTPFile[] files = ftpClient.listFiles();

            //判断文件下载路径是否存在
            File directory = new File(downloadPath);
            if(!directory.exists()) directory.mkdirs();

            for (FTPFile file : files) {
                // 取得指定文件并下载
                if (file.getName().equals(fileName)) {
                    File downFile = new File(downloadPath + File.separator + file.getName());
                    OutputStream out = new FileOutputStream(downFile);
                    // 绑定输出流下载文件,需要设置编码集，不然可能出现文件为空的情况
                    flag = ftpClient.retrieveFile(new String(file.getName().getBytes(StandardCharsets.UTF_8), StandardCharsets.ISO_8859_1), out);
                    // 下载成功删除文件,看项目需求
                    // ftp.deleteFile(new String(fileName.getBytes("UTF-8"),"ISO-8859-1"));
                    out.flush();
                    out.close();
                }
            }

        } catch (Exception e) {
            flag = false;
            System.out.println(e);
        }
        return flag;
    }

    public static Boolean downloadAllFiles(String filePath, String downloadPath){
        boolean flag = false;
        try {
            // 跳转到文件目录
            ftpClient.changeWorkingDirectory(filePath);
            // 获取目录下文件集合
            ftpClient.enterLocalPassiveMode();
            FTPFile[] files = ftpClient.listFiles();

            //判断文件下载路径是否存在
            File directory = new File(downloadPath);
            if(!directory.exists()) directory.mkdirs();

            for (FTPFile file : files) {
                // 取得指定文件并下载
                flag=downloadFile(filePath,file.getName(),downloadPath);
                if(!flag) break;
            }
            return flag;

        } catch (Exception e) {
            flag = false;
            System.out.println(e);
        }
        return flag;
    }
    public static Boolean uploadFile(String fileName){
        String ftpPath = ZookeeperManager.getPath();
        String uploadPath = "";
        boolean flag = false;
        InputStream in = null;
        try {
            // 设置PassiveMode传输
            ftpClient.enterLocalPassiveMode();
            //设置二进制传输，使用BINARY_FILE_TYPE，ASC容易造成文件损坏
            ftpClient.setFileType(FTPClient.BINARY_FILE_TYPE);
            //判断FPT目标文件夹时候存在不存在则创建
            if(!ftpClient.changeWorkingDirectory(ftpPath)){
                ftpClient.makeDirectory(ftpPath);
            }
            //跳转目标目录
            ftpClient.changeWorkingDirectory(ftpPath);

            //上传文件
            File file = new File(uploadPath + fileName);
            in = new FileInputStream(file);
            String tempName = ftpPath + File.separator + file.getName();
            flag = ftpClient.storeFile(new String (tempName.getBytes(StandardCharsets.UTF_8), StandardCharsets.ISO_8859_1),in);
        } catch (Exception e) {
            System.out.println(e);
            e.printStackTrace();
        }finally{
            try {
                in.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return flag;
    }
}
