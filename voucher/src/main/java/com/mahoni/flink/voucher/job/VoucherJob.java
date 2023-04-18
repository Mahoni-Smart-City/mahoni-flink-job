package com.mahoni.flink.voucher.job;

import com.mahoni.flink.voucher.schema.VoucherRedeemedSchema;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Properties;

public class VoucherJob {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRestartStrategy(RestartStrategies.noRestart());

        Properties kafkaConsumerProps = new Properties();
        kafkaConsumerProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        kafkaConsumerProps.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        kafkaConsumerProps.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        kafkaConsumerProps.setProperty(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");

        DataStream<VoucherRedeemedSchema> airQualityRaw = env.addSource(new FlinkKafkaConsumer<>("voucher-redeemed-topic", ConfluentRegistryAvroDeserializationSchema.forSpecific(VoucherRedeemedSchema.class, "http://localhost:8081"), kafkaConsumerProps))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5)));

        env.execute("Voucher Job");
    }
}