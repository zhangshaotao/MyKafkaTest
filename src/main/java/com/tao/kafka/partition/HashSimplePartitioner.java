package com.tao.kafka.partition;


import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

import com.tao.kafka.bean.User;

/**
 * Kafka�������İ��� SimplePartitioner������������0.8.0�İ汾����0.8.1�İ汾��һ�������Ը����£��㶮�ģ�
 * 
 * @Author ����ͥ
 * @Time 2014-07-18
 * 
 */
public class HashSimplePartitioner implements Partitioner<User> {
	
	public HashSimplePartitioner(VerifiableProperties props) {

	}
	public int partition(User key, int numPartitions) {
		System.out.println("hash partition ---> " + key);
		return key.hashCode() % numPartitions;
	}

}
