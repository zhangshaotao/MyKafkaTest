package com.tao.kafka.producer;

import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import com.tao.kafka.bean.User;

public class UserProducer {
	public static void main(String[] args) {
		long events = 10;

		/*Properties props = new Properties();
//		props.put("metadata.broker.list", "broker1:9092,broker2:9092");
		props.put("metadata.broker.list", "storm01:9092"); // Eclipse 下rs229在本地hosts也要配置，或者写成ip形式也可以
		props.put("serializer.class", "com.tao.kafka.message.UserEncoder"); // 需要修改
		props.put("partitioner.class", "com.tao.kafka.partition.HashSimplePartitioner"); // 需要修改
		props.put("zookeeper.connect", "storm01:2181");
		props.put("request.required.acks", "1");
*/
		Properties props = new Properties();
//		props.put("metadata.broker.list", "broker1:9092,broker2:9092");
		props.put("metadata.broker.list", "172.16.3.64:9092,172.16.3.74:9092,172.16.3.77:9092,172.16.3.87:9092,172.16.3.93:9092");
		props.put("partitioner.class", " com.baihe.hadoop.kafka.SimplePartitionerByUserID");
		props.put("zookeeper.connect", "172.16.3.64:2181,172.16.3.74:2181,172.16.3.77:2181,172.16.3.87:2181,172.16.3.93:2181");
		props.put("request.required.acks", "1");
		
		ProducerConfig config = new ProducerConfig(props);

		Producer<User, User> producer = new Producer<User, User>(config);

		for (long nEvents = 0; nEvents < events; nEvents++) {
			User msg = new User("id00"+nEvents, "name00"+nEvents, "sex"+nEvents%2, "age"+nEvents, "addr00"+nEvents);
			System.out.println(msg);
			KeyedMessage<User, User> data = new KeyedMessage<User, User>("test", msg, msg);
			producer.send(data);
		}
		producer.close();
		
		System.out.println("producer is successful .");
	}
}
