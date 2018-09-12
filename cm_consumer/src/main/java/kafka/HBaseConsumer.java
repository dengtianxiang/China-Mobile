package kafka;

import hbase.HBaseDao;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import utils.PropertiesUtil;
import java.util.Arrays;

/**
 * Author: Administrator
 * Date: 2018/6/13
 * Desc:该类主要用于读取 kafka 中缓存的数据，然后调用 HBaseAPI，持久化数据
 */
public class HBaseConsumer {
    public static void main(String[] args) {
        //读取配置信息创建消费对象
        KafkaConsumer kafkaConsumer = new KafkaConsumer(PropertiesUtil.properties);
        //发布订阅消息主题
        kafkaConsumer.subscribe(Arrays.asList(PropertiesUtil.getProperty("kafka.topic")));
        //把kafka采集到的数据写入到Hbase
        HBaseDao hd = new HBaseDao();
        //一直在消费
        while (true) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(100);
            for (ConsumerRecord<String,String> cr : records) {
                String orivalue = cr.value();
                System.out.println(orivalue);
                hd.put(orivalue);
            }
        }
    }
}
