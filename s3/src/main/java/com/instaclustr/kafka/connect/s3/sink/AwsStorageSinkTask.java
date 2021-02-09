package com.instaclustr.kafka.connect.s3.sink;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.instaclustr.kafka.connect.s3.AwsStorageConnectorCommonConfig;
import com.instaclustr.kafka.connect.s3.TransferManagerProvider;
import com.instaclustr.kafka.connect.s3.VersionUtil;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

public class AwsStorageSinkTask extends SinkTask {
    private static Logger logger = LoggerFactory.getLogger(AwsStorageSinkTask.class);
    private AwsStorageSinkWriter sinkWriter;
    private TransferManagerProvider transferManagerProvider;
    private Map<TopicPartition, TopicPartitionBuffer> topicPartitionBuffers = new HashMap<>();
    private Map<TopicPartition, BeginOffsetTotalRecords> beginOffsetTotalRecords = new HashMap<>();
    private OffsetSink offsetSink;
    
    public AwsStorageSinkTask() { //do not remove, kafka connect usage
    }

    public AwsStorageSinkTask(TransferManagerProvider transferManagerProvider, AwsStorageSinkWriter writer) {
        this.transferManagerProvider = transferManagerProvider;
        this.sinkWriter = writer;
    }

    @Override
    public String version() {
        return VersionUtil.getVersion();
    }

    @Override
    public void start(Map<String, String> map) {
        logger.info("Starting S3 sink task");
        map.forEach((k, v) -> logger.debug("st tas {}, {}", k, v));
        if (transferManagerProvider == null) transferManagerProvider = new TransferManagerProvider(map);
        String bucket = map.get(AwsStorageConnectorCommonConfig.BUCKET);
        String keyPrefix = map.get(AwsStorageConnectorCommonConfig.S3_KEY_PREFIX);
        AdminClient adminClient = AdminClient.create(getAdminClientConfig()); 
        offsetSink = new OffsetSink(adminClient);
        if (sinkWriter == null)
            this.sinkWriter = new AwsStorageSinkWriter(transferManagerProvider.get(), bucket, keyPrefix);
    }

    private void putSingleRecord(SinkRecord record) throws IOException, RecordOutOfOrderException, InterruptedException, MaxBufferSizeExceededException {
        TopicPartition topicPartition = new TopicPartition(record.topic(), record.kafkaPartition());
        // actual work
        TopicPartitionBuffer latestBuffer = topicPartitionBuffers.get(topicPartition);
        BeginOffsetTotalRecords beginOffsetTotalRecord = beginOffsetTotalRecords.get(topicPartition);
        if(beginOffsetTotalRecord.getBeginningOffset() == 0L) {
        	beginOffsetTotalRecord.setBeginningOffset(record.kafkaOffset());
        }
        beginOffsetTotalRecord.setTotalRecords(beginOffsetTotalRecord.getTotalRecords()+1L);
        
        try {
            latestBuffer.putRecord(record);
            beginOffsetTotalRecords.put(topicPartition, beginOffsetTotalRecord);
        } catch (MaxBufferSizeExceededException ex) {
            // We need to make a new buffer, so flush the existing one first if there is anything in it
            if (latestBuffer.getStartOffset() > -1) sinkWriter.writeDataSegment(latestBuffer);
            TopicPartitionBuffer newBuffer = new TopicPartitionBuffer(topicPartition);
            topicPartitionBuffers.put(topicPartition, newBuffer);
            writeCounsumerOffset(topicPartition);
            // Further MaxBufferSizeExceededExceptions will be unhandled. This indicates a record too big even by itself
            newBuffer.putRecord(record);
        }
    }

    @Override
    public void put(Collection<SinkRecord> collection) {
        for (SinkRecord record : collection) {
            if (logger.isDebugEnabled()) {
                logger.debug("Record Key : {}", record.key() != null ? record.key().toString() : "Null");
                logger.debug("Record Value :  {}", record.value());
                logger.debug("Key Schema :  {}", record.keySchema() != null ? record.keySchema().type() : "Null");
                logger.debug("Value Schema : {}", record.valueSchema() != null ? record.valueSchema().type() : "Null");
            }
            try {
                putSingleRecord(record);
            } catch (Exception ex) {
                logger.error(String.format("Failed to process record, topic: %s, partition: %d, offset: %d", record.topic(), record.kafkaPartition(), record.kafkaOffset()), ex);
                throw new ConnectException(ex);
            }
        }
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> map) {
        try {
            if (logger.isDebugEnabled()) {
                map.forEach((tp, om) -> logger.debug("flush tp: {}, {}, om: {} {}", tp.topic(), tp.partition(), om.offset(), om.metadata()));
            }
            List<TopicPartition> buffersToBeFlushed = topicPartitionBuffers.
                    entrySet().stream()
                    .filter(entry -> entry.getValue().getStartOffset() > -1)
                    .map(Map.Entry::getKey).collect(Collectors.toList());
	        if(!buffersToBeFlushed.isEmpty()) {
	        	offsetSink.syncConsumerGroups();
	        }
            for (TopicPartition topicPartition : buffersToBeFlushed) {
                final TopicPartitionBuffer topicPartitionBuffer = topicPartitionBuffers.get(topicPartition);
                sinkWriter.writeDataSegment(topicPartitionBuffer);
                topicPartitionBuffers.put(topicPartition, new TopicPartitionBuffer(topicPartition));
                writeCounsumerOffset(topicPartition);
                if (logger.isDebugEnabled()) {
                    logger.debug("actually flushing: {}, {}, {}-{}", topicPartition.topic(), topicPartition.partition(), topicPartitionBuffer.getStartOffset(), topicPartitionBuffer.getEndOffset());
                }
            }
        } catch (IOException ex) {
            map.clear();
            // we can't handle this, but java doesn't allow us to not handle checked exceptions, so propagate
            throw new ConnectException(ex);
        } catch (InterruptedException e) {
            map.clear(); //do not want to commit offsets in the case of an interrupted thread
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void stop() {
        // Close resources here.
        logger.info("Stopping task");
    }

    @Override
    public void open(final Collection<TopicPartition> partitions) {
        super.open(partitions);
        partitions.forEach(tp -> {
            logger.debug("Opening topic {}, partition {}", tp.topic(), tp.partition());
            try {
                topicPartitionBuffers.putIfAbsent(tp, new TopicPartitionBuffer(tp.topic(), tp.partition()));
                beginOffsetTotalRecords.putIfAbsent(tp, new BeginOffsetTotalRecords(0L,0L));
                writeCounsumerOffset(tp);
            } catch (IOException e) {
                // We can't handle this, need to wrap in a runtime exception since the open call doesn't allow checked exceptions
                throw new ConnectException(e);
            }
        });
    }

    @Override
    public void close(final Collection<TopicPartition> partitions) {
        super.close(partitions);
        partitions.forEach(tp -> {
            logger.debug("Closing topic {}, partition {}", tp.topic(), tp.partition());
            topicPartitionBuffers.remove(tp);
        });
    }
    
  private void writeCounsumerOffset(TopicPartition topicPartition) {
	try {	
      	ObjectMapper mapperObj = new ObjectMapper();
    	Map<String,Long> consumer_offset = offsetSink.syncOffsets(topicPartition);
    	consumer_offset.put("beginning_offset", beginOffsetTotalRecords.get(topicPartition).getBeginningOffset());
    	consumer_offset.put("totalrecords", beginOffsetTotalRecords.get(topicPartition).getTotalRecords());
    	String jsonconsumeroffset = mapperObj.writeValueAsString(consumer_offset);
		logger.debug("jsonconsumeroffset::{} TopicPartition:: {}  ",jsonconsumeroffset,topicPartition);
		sinkWriter.writeOffsetData(topicPartition, jsonconsumeroffset, "consumer_offsets");
    } catch (IOException | InterruptedException ex) { 
        throw new ConnectException(ex);
	}
	
  }
  
  private Properties getAdminClientConfig() {
	  Properties adminProps = new Properties(); 
		  try {
				adminProps.load(new FileInputStream(AwsStorageConnectorCommonConfig.CONNECT_DISTRIBUTED_PROPERTIES));
			} catch (IOException  e) {
				throw new ConnectException(e);
			} 
		  return adminProps;
	}
}