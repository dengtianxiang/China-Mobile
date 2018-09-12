package hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import utils.HBaseUtil;
import utils.PropertiesUtil;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * Author: Administrator
 * Date: 2018/6/14
 * Desc: 操作hbase
 */
public class HBaseDao {

    public static final Configuration conf;
    private int regions;
    private String nameSpace;
    private String tableName;
    private Table table;
    private Connection connection;
    private SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private SimpleDateFormat sdf2 = new SimpleDateFormat("yyyyMMddHHmmss");

    static {
        conf = HBaseConfiguration.create();
    }

    public HBaseDao() {
        try {
            connection = ConnectionFactory.createConnection(conf);
            regions = Integer.valueOf(PropertiesUtil.getProperty("HBase.calllog.regions"));
            nameSpace = PropertiesUtil.getProperty("HBase.calllog.namespace");
            tableName = PropertiesUtil.getProperty("HBase.calllog.tablename");
            table = connection.getTable(TableName.valueOf(tableName));
            if(!HBaseUtil.isExistTable(conf,tableName)){
                HBaseUtil.initNamespace(conf, nameSpace);
                HBaseUtil.createTable(conf,tableName,regions,"f1","f2");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    /**
     * @Description:
     * ori数据格式: 16954485202,15816251623,2017-11-10 23:17:59,1573
     * Rowkey 格式：01_16954485202_20171110231759_1_1573
     * @Param: [ori]
     * @return: void
     * @Author: Mr.Deng
     * @Date: 2018/6/14 23:07  
     */ 

    public void put(String ori){
        try {
            String[] splitOri = ori.split(",");
            String caller = splitOri[0];
            String callee = splitOri[1];
            String buildTime = splitOri[2];
            String duration = splitOri[3];
            String regionCode = HBaseUtil.getRegionCode(caller, buildTime, regions);
            String buildTimeReplace = sdf2.format(sdf1.parse(buildTime));
            String buildTimeTs = String.valueOf(sdf1.parse(buildTime).getTime());
            //生成rowkey
            String rowkey = HBaseUtil.getRowkey(regionCode, caller, buildTimeReplace, callee, "1", duration);
            //向表中插入该条数据
            Put put = new Put(Bytes.toBytes(rowkey));
            put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("call1"), Bytes.toBytes(caller));
            put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("call2"), Bytes.toBytes(callee));
            put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("build_time"), Bytes.toBytes(buildTime));
            put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("build_time_ts"), Bytes.toBytes(buildTimeTs));
            put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("flag"), Bytes.toBytes("1"));
            put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("duration"), Bytes.toBytes(duration));
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }


}
