package com.mahoni.flink.air_quality.job;

import com.mahoni.flink.schema.AirQualityProcessedSchema;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.UUID;

public class SinkKafka implements SinkFunction<AirQualityProcessedSchema> {
    Properties kafkaProducerProps = new Properties();
    @Override
    public void invoke(AirQualityProcessedSchema value) throws Exception {

        kafkaProducerProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "http://34.128.127.171:9092");
        kafkaProducerProps.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        kafkaProducerProps.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        kafkaProducerProps.setProperty(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://34.128.127.171:8081");
        //kafkaProducerProps.setProperty(KafkaAvroSerializerConfig., "true");
        Producer<String, AirQualityProcessedSchema> kafkaProducer = new KafkaProducer<>(kafkaProducerProps);

        String id = UUID.randomUUID().toString();
        ProducerRecord<String, AirQualityProcessedSchema> record = new ProducerRecord<>("air-quality-processed-topic", id, value);
        kafkaProducer.send(record);
    }
}
