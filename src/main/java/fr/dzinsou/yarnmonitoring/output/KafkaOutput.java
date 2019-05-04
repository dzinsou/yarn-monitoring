package fr.dzinsou.yarnmonitoring.output;

import fr.dzinsou.yarnmonitoring.AppConfig;
import org.apache.kafka.clients.producer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class KafkaOutput extends AbstractOutput {
    private Producer<String, String> kafkaProducer;

    private AppConfig appConfig;

    public KafkaOutput(AppConfig appConfig) throws IOException {
        this.appConfig = appConfig;
        this.init();
    }

    private void init() throws IOException {
        try (FileInputStream fis = new FileInputStream(this.appConfig.getOutputKafkaConf())) {
            Properties producerProps = new Properties();
            producerProps.load(fis);
            producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
            producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
            this.kafkaProducer = new KafkaProducer<>(producerProps);
        }
    }

    private Future<RecordMetadata> sendMessage(String topic, String key, String messageValue) {
        return this.kafkaProducer.send(new ProducerRecord<>(topic, key, messageValue), new MyKafkaCallback());
    }

    @Override
    public void close() throws IOException {
        if (this.kafkaProducer != null) {
            this.kafkaProducer.close();
        }
    }

    @Override
    public void bulkSave(List<String> idList, List<String> partitionIdList, List<String> jsonRecordList, boolean async) throws IOException {
        for (int i = 0; i < idList.size(); i++) {
            String partitionId = partitionIdList.get(i);
            String jsonRecord = jsonRecordList.get(i);
            if (async) {
                this.sendMessage(this.appConfig.getOutputKafkaTopic(), partitionId, jsonRecord);
            } else {
                try {
                    this.sendMessage(this.appConfig.getOutputKafkaTopic(), partitionId, jsonRecord).get();
                } catch (InterruptedException | ExecutionException e) {
                    throw new IOException(e);
                }
            }
        }
    }

    /**
     * Kafka callback
     */
    static class MyKafkaCallback implements Callback {
        private final Logger LOGGER = LoggerFactory.getLogger(MyKafkaCallback.class);

        @Override
        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            if (e != null) {
                LOGGER.error(e.getMessage(), e);
            } else {
                LOGGER.debug("Kafka bulk success");
            }
        }
    }

}
