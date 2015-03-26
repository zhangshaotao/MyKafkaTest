package com.tao.kafka.producer;

import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import com.tao.kafka.bean.Keyword;

public class KeywordProducer {
	public static void main(String[] args)
	{
		/**����producer��Ҫ�Ĳ���*/
        Properties props = new Properties();
        props.put("zk.connect", "storm01:2181");
        /**ѡ�����ĸ������������л�*/
        props.put("serializer.class", "com.tao.kafka.message.KeywordMessage");
        //props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("zk.connectiontimeout.ms", "6000");
        props.put("metadata.broker.list", "storm01:9092");
        ProducerConfig config=new ProducerConfig(props);
         
        /**��������*/
        Keyword keyword=new Keyword();
        keyword.setUser("Chenhui");
        keyword.setId("0");
        keyword.setKeyword("china");
        
        KeyedMessage<String,Keyword> keyedMessage = new KeyedMessage<String, Keyword>("keyword",keyword);
        
        //List<KeyedMessage<String,String>> msg = new ArrayList<KeyedMessage<String,String>>();
       // msg.add(keyedMessage);
         
        /**�������ݷ��Ͷ���*/
        Producer<String,Keyword> producer=new Producer<String, Keyword>(config);       
        //ProducerData<String,Keyword> data=new ProducerData<String, Keyword>("test", msg);
        producer.send(keyedMessage);
        System.out.println(keyedMessage.message());
	}
}
