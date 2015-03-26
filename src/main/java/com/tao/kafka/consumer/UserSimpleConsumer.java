package com.tao.kafka.consumer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;

import com.tao.kafka.bean.User;
import com.tao.kafka.util.BeanUtils;


public class UserSimpleConsumer {
	
	private List<String> replicaBrokers;
	
	public static void main(String args[])
	{
		UserSimpleConsumer example = new UserSimpleConsumer();
		long maxReads = 100;
		String topic = "test-user-002";
		int partition = 1; // 
		List<String> seeds = new ArrayList<String>();
		seeds.add("storm01");
		int port = Integer.parseInt("9092");
		try {
			example.run(maxReads, topic, partition, seeds, port);
		} catch (Exception e) {
			System.out.println("Oops:" + e);
			e.printStackTrace();
		}
	}
	
	public UserSimpleConsumer()
	{
		this.replicaBrokers = new ArrayList<String>();
	}
	
	public void run(long maxReads, String topic, int partition, List<String> seedBrokers, int port)
	{
		PartitionMetadata partitionMetadata = this.findLeader(topic, partition, seedBrokers, port);
		
		String leadBroker = partitionMetadata.leader().host();
		String clientName = "Client_" + topic + "_" + partition;
		
		SimpleConsumer consumer = new SimpleConsumer(leadBroker, port, 100000, 64 * 1024, clientName);
		long readOffset = getLastOffset(consumer, topic, partition, 
							kafka.api.OffsetRequest.EarliestTime(), clientName);
		
		int numErrors = 0;
		while(maxReads > 0)
		{
			kafka.api.FetchRequest fetchRequest = new FetchRequestBuilder().clientId(clientName)
								.addFetch(topic, partition, readOffset, 100000).build();
			FetchResponse fetchResponse = consumer.fetch(fetchRequest);
			
			short code = fetchResponse.errorCode(topic, partition);
			
			if(fetchResponse.hasError())
			{
				numErrors++;
				if(numErrors > 5)
				{
					break;
				}
				if(code == ErrorMapping.OffsetOutOfRangeCode())
				{
					readOffset = getLastOffset(consumer, topic, partition,
							kafka.api.OffsetRequest.LatestTime(), clientName);
					continue;
				}
				consumer.close();
				consumer = null;
				
				leadBroker = findNewLeader(leadBroker,topic,partition,port);
				continue;
			}
			numErrors = 0;
			
			long numRead = 0;
			for(MessageAndOffset messageAndOffset : fetchResponse.messageSet(topic, partition))
			{
				long currentOffset = messageAndOffset.offset();
				if(currentOffset < readOffset)
				{
					continue;
				}
				
				readOffset = messageAndOffset.nextOffset();
				ByteBuffer payload = messageAndOffset.message().payload();
				
				byte[] bytes = new byte[payload.limit()];
				payload.get(bytes);
				
				User user = (User)BeanUtils.bytes2Object(bytes);
				System.out.println(String.valueOf(messageAndOffset.offset()) + ": " + user);
				System.out.println(String.valueOf(messageAndOffset.offset()) + ": " + user);
				
				numRead++;
				maxReads--;
			}
			
			if (numRead == 0)
			{
				try
				{
					Thread.sleep(1000);
				} 
				catch (InterruptedException ie)
				{
				}
			}
		}
		if (consumer != null)
			consumer.close();
	}
	
	private PartitionMetadata findLeader(String topic, int partition, List<String> seedBrokers, int port)
	{
		PartitionMetadata returnPartitionMetadata = null;
		
		for(String seedBroker : seedBrokers)
		{
			SimpleConsumer consumer = null;
			consumer = new SimpleConsumer(seedBroker, port, 100000, 64 * 1024, "leaderLookup");
			List<String> topics = Collections.singletonList(topic);
			TopicMetadataRequest req = new TopicMetadataRequest(topics);
			TopicMetadataResponse resp = consumer.send(req);
			
			List<TopicMetadata> topicsMetadata = resp.topicsMetadata();
			for(TopicMetadata topicMetadata : topicsMetadata)
			{
				for(PartitionMetadata partitionMetadata : topicMetadata.partitionsMetadata())
				{
					if(partition == partitionMetadata.partitionId())
					{
						returnPartitionMetadata = partitionMetadata;
						break;
					}
				}
				
			}
			
			consumer.close();
		}
		return returnPartitionMetadata;
	}
	
	private String findNewLeader(String oldLeader,String topic, int partition, int port)
	{
		for(int i=0;i < 3;i++)
		{
			boolean goToSleep = false;
			PartitionMetadata metadata = findLeader(topic, port,replicaBrokers,partition);
			
			if(metadata == null)
			{
				goToSleep = true;
			}
			else if(metadata.leader() == null)
			{
				goToSleep = true;
			}
			else if(oldLeader.equalsIgnoreCase(metadata.leader().host())&& i == 0)
			{
				goToSleep = true;
			}
			else
			{
				return metadata.leader().host();
			}
			if (goToSleep)
			{
				try
				{
					Thread.sleep(1000);
				} 
				catch (InterruptedException ie)
				{
				}
			}
		}
		return null;
	}
	
	public static long getLastOffset(SimpleConsumer consumer, String topic,
			int partition, long whichTime, String clientName) 
	{
		TopicAndPartition topicAndPartition = new TopicAndPartition(topic,partition);
		Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = 
				new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
		
		requestInfo.put(topicAndPartition,new PartitionOffsetRequestInfo(whichTime,1));
		OffsetRequest request = new OffsetRequest(requestInfo,kafka.api.OffsetRequest.CurrentVersion(),clientName);
		
		OffsetResponse response = consumer.getOffsetsBefore(request);
		
		long[] offset = response.offsets(topic, partition);
		return offset[0];
	}
}
