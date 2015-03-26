package com.tao.kafka.partition;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

public class ProducerPartitioner implements Partitioner<String>{
	
	public ProducerPartitioner(VerifiableProperties props) {

	}
	
	public int partition(String key, int numPartitions)
	{
	   return key.length() % numPartitions;
    }
}
