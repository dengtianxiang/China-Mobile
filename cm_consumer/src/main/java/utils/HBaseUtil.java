package utils;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Bytes;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Iterator;
import java.util.TreeSet;

/**
 * Author: Mr.Deng
 * Date: 2018/6/14
 * Desc: 命名空间 表的创建
 */
public class HBaseUtil {
    private static int regions;

    /**
     * @Description:判断表是否存在
     * @Param: [conf, tableName]
     * @return: boolean
     * @Author: Mr.Deng
     * @Date: 2018/6/14 13:33  
     */ 
    public  static boolean isExistTable(Configuration conf, String tableName) throws IOException {
        // 获取HBase客户端连接
        Connection connection = ConnectionFactory.createConnection(conf);
        // 得到用户
        Admin admin = connection.getAdmin();
        //判断表是否存在的结果
        boolean result = admin.tableExists(TableName.valueOf(tableName));
        //关流
        admin.close();
        connection.close();
        return result;
    }

    /**
     * @Description: 初始化命令空间
     * @Param: [conf, nameSpace]
     * @return: void
     * @Author: Mr.Deng
     * @Date: 2018/6/14 14:11  
     */ 
    public static void initNamespace(Configuration conf,String namespace) throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();
        //命名空间描述器
        NamespaceDescriptor nd = NamespaceDescriptor
                .create(namespace)
                .addConfiguration("CREATE_TIME",String.valueOf(System.currentTimeMillis()))
                .addConfiguration("AUTHOR", "deng")
                .build();
        admin.createNamespace(nd);
        admin.close();
        connection.close();
    }
    
    /**
     * @Description:创建表，创建预分区
     * @Param: [conf, tableName, columnFamily]
     * @return: void
     * @Author: Mr.Deng
     * @Date: 2018/6/14 15:50  
     */ 
    public static void createTable(Configuration conf, String tableName,int regions, String... columnFamily) throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();
        //创建表之前先判断表是否存在
        if(isExistTable(conf, tableName)) return;
        //表描述器
        HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tableName));
        for (String cf: columnFamily) {
            //循环添加列簇
            htd.addFamily(new HColumnDescriptor(cf));
        }
        //带有预分区的创建表
        htd.addCoprocessor("hbase.CalleeWriteObserver");
        admin.createTable(htd,getSplitKeys(regions));
        admin.close();
        connection.close();
    }

    private static byte[][] getSplitKeys(int regions) {
        //定义一个存放分区键的数组
        String[] keys = new String[regions];
        //目前推算region的个数不会超过2位数，所以region分区键格式化为两位数字代表的字符串（例如00,01）
        DecimalFormat df = new DecimalFormat("00");
        for (int i = 0; i < regions; i ++) {
            keys[i] = df.format(i) + "|";
        }
        byte[][] splitKeys = new byte[regions][];
        //生成byte[][]类型的分区键的时候，一定要保证分区键是有序的,下面用hbase自带的比较器
        TreeSet<byte[]> treeSet = new TreeSet<>(Bytes.BYTES_COMPARATOR);
        for(int i = 0; i < regions; i++){
            treeSet.add(Bytes.toBytes(keys[i]));
        }

        Iterator<byte[]> splitKeysIterator = treeSet.iterator();
        int index = 0;
        while(splitKeysIterator.hasNext()){
            byte[] b = splitKeysIterator.next();
            splitKeys[index ++] = b;
        }
        return splitKeys;
    }

    /**
     * @Description: 生成RowKey
     * 格式：regionCode_caller_buildTime_callee_flag_duration
     * @Param: []
     * @return: java.lang.String
     * @Author: Mr.Deng
     * @Date: 2018/6/14 23:46  
     */ 
    public static String getRowkey(String regionCode,String call1,String buildTime,String call2,String flag,String duration){
        StringBuilder sb = new StringBuilder();
        sb.append(regionCode + "_")
                .append(call1 + "_")
                .append(buildTime + "_")
                .append(call2 + "_")
                .append(flag + "_")
                .append(duration);
        return sb.toString();
    }

    /**
     * @Description: 生成分区号
     * @Param: [call1, buildTime, regions]
     * @return: java.lang.String
     * @Author: Mr.Deng
     * @Date: 2018/6/14 23:58  
     */ 
    public static String getRegionCode(String call1, String buildTime, int regions){
        int len = call1.length();
        //取出后4位号码
        String lastPhone = call1.substring(len - 4);
        //取出年月
        String ym = buildTime
                .replaceAll("-", "")
                .replaceAll(":", "")
                .replaceAll(" ", "")
                .substring(0, 6);
        //离散操作1
        Integer x = Integer.valueOf(lastPhone) ^ Integer.valueOf(ym);
        //离散操作2
        int y = x.hashCode();
        //生成分区号
        int regionCode = y % regions;
        //格式化分区号
        DecimalFormat df = new DecimalFormat("00");
        return  df.format(regionCode);
    }

}
