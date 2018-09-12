package produce;


import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @program: cm
 * @Date: 2018/6/12 18:22
 * @Author: Mr.Deng
 * @Description:
 */
public class ProductLog {
    //通话时间起止范围
    String startTime = "2017-06-01 00:00:00";
    String endTime = "2018-06-01 00:00:00";

    //存放联系人电话与姓名的映射
    public Map<String, String> phoneNameMap = new HashMap<>();
    //存放联系人电话号码
    public List<String> phoneList = new ArrayList<>();

    public void initPhone(){
        phoneList.add("18165534660");
        phoneList.add("18969919377");
        phoneList.add("14047870783");
        phoneList.add("16082000518");
        phoneList.add("19419258139");
        phoneList.add("18891740533");
        phoneList.add("15731969383");
        phoneList.add("16706144241");
        phoneList.add("19527543796");
        phoneList.add("14766788099");
        phoneList.add("19103981532");
        phoneList.add("14397011416");
        phoneList.add("17020420751");
        phoneList.add("15816251623");
        phoneList.add("19983171460");
        phoneList.add("14379070423");
        phoneList.add("15511578338");
        phoneList.add("16954485202");
        phoneList.add("13802601622");
        phoneList.add("19016258082");

        phoneNameMap.put("18165534660","李雁");
        phoneNameMap.put("18969919377","卫艺");
        phoneNameMap.put("14047870783","仰莉");
        phoneNameMap.put("16082000518","陶欣悦");
        phoneNameMap.put("19419258139","施梅梅");
        phoneNameMap.put("18891740533","金虹霖");
        phoneNameMap.put("15731969383","魏明艳");
        phoneNameMap.put("16706144241","华贞");
        phoneNameMap.put("19527543796","华啟倩");
        phoneNameMap.put("14766788099","仲采绿");
        phoneNameMap.put("19103981532","卫丹");
        phoneNameMap.put("14397011416","戚丽红");
        phoneNameMap.put("17020420751","何翠柔");
        phoneNameMap.put("15816251623","钱溶艳");
        phoneNameMap.put("19983171460","钱琳");
        phoneNameMap.put("14379070423","缪静欣");
        phoneNameMap.put("15511578338","焦秋菊");
        phoneNameMap.put("16954485202","吕访琴");
        phoneNameMap.put("13802601622","沈丹");
        phoneNameMap.put("19016258082","褚美丽");
    }

   /**
    * 生成数据的方法
    * 数据格式 ：18165534660,18969919377,2018-06-01 08:30:30,0360
    */ 
    public String product(){
        //主叫
        String caller = null;
        String callerName = null;
        //被叫
        String callee = null;
        String calleeName = null;

        //随机取得主叫电话号码,姓名
        int callerIndex = (int)(Math.random() * phoneList.size());
        caller = phoneList.get(callerIndex);
        callerName = phoneNameMap.get(caller);
        //随机取得被叫电话号码,姓名
        while (true) {
            int calleeIndex = (int)(Math.random() * phoneList.size());
            callee = phoneList.get(calleeIndex);
            calleeName = phoneNameMap.get(callee);
            if (!callee.equals(caller)) {
                break;
            }
        }
        //通话时间
        String buildTime = randomBuildTime(startTime, endTime);
        //通话时长格式 0000
        DecimalFormat df = new DecimalFormat("0000");
        String duration = df.format((long)(30 * 60 * Math.random()));
        //把数据存放到容器
        StringBuilder sb = new StringBuilder();
        sb.append(caller + ",")
          .append(callee + ",")
          .append(buildTime +",")
          .append(duration);
        return sb.toString();
       // System.out.println(caller + "," + callerName + "," +callee + "," +calleeName + "," +buildTime +"," +duration);
    }

    /**
     * 根据传入的时间区间范围，随机产生通话时长
     */ 
    public String randomBuildTime(String startTime,String endTime){
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        try {
            Date startData = sdf.parse(startTime);
            Date endtData = sdf.parse(endTime);
            if (endtData.getTime() <= startData.getTime()){
                return null;
            }
            long randomTS = startData.getTime() +(long)((endtData.getTime()-startData.getTime()) * Math.random());
            Date resultDate = new Date(randomTS);
            String resultTimeString = sdf.format(resultDate);
            return resultTimeString;

        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;
    }

    //把生产的数据写入到日志的方法
    public void writeLog(String filePath){
        try {
            OutputStreamWriter osw = new OutputStreamWriter(new FileOutputStream(filePath),"UTF-8");
            while(true){
            Thread.sleep(500);
            String log = product();
            System.out.println(log);
            osw.write(log +"\n");
            //手动刷新确保每条数据都写入到文件一次
            osw.flush();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    //主方法
      public static void main(String[] args) throws InterruptedException {
        if(args == null || args.length<= 0){
            System.out.println("no agruments");
            return;
        }
        //String logPath = "E:\\calllog.csv";
        ProductLog productLog = new ProductLog();
        productLog.initPhone();
        productLog.writeLog(args[0]);
    }

}
