package com.tao.kafka.producer;

import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import com.tao.kafka.bean.User;

public class TestProducer {
	public static void main(String[] args) {
		long events = 10;

		Properties props = new Properties();
//		props.put("metadata.broker.list", "broker1:9092,broker2:9092");
		props.put("metadata.broker.list", "172.16.3.64:9092,172.16.3.74:9092,172.16.3.77:9092,172.16.3.87:9092,172.16.3.93:9092");
		props.put("partitioner.class", " com.baihe.hadoop.kafka.SimplePartitionerByUserID");
		props.put("zookeeper.connect", "172.16.3.64:2181,172.16.3.74:2181,172.16.3.77:2181,172.16.3.87:2181,172.16.3.93:2181");
		props.put("request.required.acks", "1");
		
		ProducerConfig config = new ProducerConfig(props);

		Producer<String, String> producer = new Producer<String, String>(config);

		for (long nEvents = 0; nEvents < events; nEvents++) {
			KeyedMessage<String, String> data = new KeyedMessage<String, String>("test", "2222222", "2222222");
			producer.send(data);
		}
		producer.close();
		
		System.out.println("producer is successful .");
	}
}
