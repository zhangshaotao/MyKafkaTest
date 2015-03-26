package com.tao.kafka.partition;


import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

import com.tao.kafka.bean.User;

/**
 * Kafka官网给的案例 SimplePartitioner，官网给的是0.8.0的版本，跟0.8.1的版本不一样，所以改了下，你懂的！
 * 
 * @Author 王扬庭
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
