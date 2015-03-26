package com.tao.kafka.producer;

import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import com.tao.kafka.bean.Keyword;

public class KeywordProducer {
	public static void main(String[] args)
	{
		/**配置producer必要的参数*/
        Properties props = new Properties();
        props.put("zk.connect", "storm01:2181");
        /**选择用哪个类来进行序列化*/
        props.put("serializer.class", "com.tao.kafka.message.KeywordMessage");
        //props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("zk.connectiontimeout.ms", "6000");
        props.put("metadata.broker.list", "storm01:9092");
        ProducerConfig config=new ProducerConfig(props);
         
        /**制造数据*/
        Keyword keyword=new Keyword();
        keyword.setUser("Chenhui");
        keyword.setId("0");
        keyword.setKeyword("china");
        
        KeyedMessage<String,Keyword> keyedMessage = new KeyedMessage<String, Keyword>("keyword",keyword);
        
        //List<KeyedMessage<String,String>> msg = new ArrayList<KeyedMessage<String,String>>();
       // msg.add(keyedMessage);
         
        /**构造数据发送对象*/
        Producer<String,Keyword> producer=new Producer<String, Keyword>(config);       
        //ProducerData<String,Keyword> data=new ProducerData<String, Keyword>("test", msg);
        producer.send(keyedMessage);
        System.out.println(keyedMessage.message());
	}
}
