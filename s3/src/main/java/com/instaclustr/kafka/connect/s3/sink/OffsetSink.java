package com.instaclustr.kafka.connect.s3.sink;
 
import org.apache.kafka.clients.admin.AdminClient; 
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.RetriableException;

import java.io.IOException; 
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry; 
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class OffsetSink {
	private List<String> consumerGroups = new ArrayList<>();
	private final AdminClient adminClient;

	public OffsetSink(AdminClient adminClient) {
		this.adminClient = adminClient;
	}

	public void syncConsumerGroups() {
		try {
			consumerGroups = adminClient.listConsumerGroups().all().get().stream().map(ConsumerGroupListing::groupId)
					.collect(Collectors.toList());

		} catch (InterruptedException | ExecutionException e) {
			throw new RetriableException(e);
		}
	}

	public Map<String, Long> syncOffsets(TopicPartition topicpart) throws IOException {
		boolean error = false;
		Map<String, Long> consumerGroupOffset = new HashMap<>();
		for (String consumerGroup : consumerGroups) {
			try {
				consumerGroupOffset.putAll(syncOffsetsForGroup(consumerGroup, topicpart));
			} catch (IOException e) {
				e.printStackTrace();
				error = true;
			}
		}
		if (error) {
			throw new IOException("syncOffsets() threw an IOException");
		}
		return consumerGroupOffset;
	}

	private Map<String, Long> syncOffsetsForGroup(String consumerGroup, TopicPartition topicpart) throws IOException {
		Iterator<Entry<TopicPartition, OffsetAndMetadata>> topicOffsetsAndMetadata;
		Map<String, Long> consumerGroupOffset = new HashMap<>();
		try {
			topicOffsetsAndMetadata = adminClient.listConsumerGroupOffsets(consumerGroup)
					.partitionsToOffsetAndMetadata().get().entrySet().stream()
					.filter(tp -> tp.getKey().equals(topicpart)).iterator();

			if (topicOffsetsAndMetadata.hasNext()) {
				Entry<TopicPartition, OffsetAndMetadata> entry = topicOffsetsAndMetadata.next(); 
				OffsetAndMetadata offsetAndMetadata = entry.getValue();
				consumerGroupOffset.put(consumerGroup, offsetAndMetadata.offset());
			}

		} catch (InterruptedException | ExecutionException e) {
			throw new RetriableException(e);
		}
		return consumerGroupOffset;
	}

}
