package fr.dzinsou.yarnmonitoring.output;

import org.apache.kafka.clients.producer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.Future;

public class KafkaOutput implements Closeable {
    private Producer<String, String> kafkaProducer;

    private String producerPropertiesFilePath;

    public KafkaOutput(String producerPropertiesFilePath) throws IOException {
        this.producerPropertiesFilePath = producerPropertiesFilePath;
        this.init();
    }

    private void init() throws IOException {
        try (FileInputStream fis = new FileInputStream(this.producerPropertiesFilePath)) {
            Properties producerProps = new Properties();
            producerProps.load(fis);
            producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
            producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
            this.kafkaProducer = new KafkaProducer<>(producerProps);
        }
    }

    public Future<RecordMetadata> sendMessage(String topicName, String messageKey, String messageValue) {
        return this.kafkaProducer.send(new ProducerRecord<>(topicName, messageKey, messageValue), new MyKafkaCallback());
    }

    @Override
    public void close() {
        if (this.kafkaProducer != null) {
            this.kafkaProducer.close();
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
                LOGGER.error(e.getMessage());
            }
        }
    }

}
