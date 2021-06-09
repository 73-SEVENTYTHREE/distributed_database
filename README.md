# 分布式数据库使用手册
1. 在本地先安装好[Zookeeper(3.6.3，二进制版本)](https://downloads.apache.org/zookeeper/zookeeper-3.6.3/)。
2. 根据[网上教程](https://www.cnblogs.com/Dcl-Snow/p/11274807.html)配置Zookeeper，到创建数据存放目录那一步即可。
3. 使用IDEA打开该项目，将项目标记为maven项目。
4. 分别运行Master、Region_Server、Client模块的`main`函数。
5. 在Client命令窗口输入相关命令即可在Region_Server端进行数据库的相关操作。

