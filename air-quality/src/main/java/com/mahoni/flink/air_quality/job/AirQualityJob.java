package com.mahoni.flink.air_quality.job;

import com.mahoni.flink.schema.AirQualityProcessedSchema;
import com.mahoni.flink.schema.AirQualityRawSchema;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class AirQualityJob {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRestartStrategy(RestartStrategies.noRestart());

        Properties kafkaConsumerProps = new Properties();

        kafkaConsumerProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "http://34.128.127.171:9092");
        kafkaConsumerProps.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kafkaConsumerProps.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        kafkaConsumerProps.setProperty(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://34.128.127.171:8081");


        DataStream<AirQualityRawSchema> airQualityRaw = env.addSource(new FlinkKafkaConsumer<>("air-quality-raw-topic", ConfluentRegistryAvroDeserializationSchema.forSpecific(AirQualityRawSchema.class, "http://34.128.127.171:8081"), kafkaConsumerProps))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5)))
                .process(new Cleaning.CleanAqi());
        airQualityRaw.print();
        KeyedStream<AirQualityRawSchema, String> keyedAirQualityRaw = airQualityRaw.keyBy((AirQualityRawSchema sensor) -> sensor.getSensorId().toString());

        DataStream<Tuple3<String, Integer, String>> pm10 = keyedAirQualityRaw
                .window(TumblingProcessingTimeWindows.of(Time.minutes(1)))
                .process(new AqiMeasurement.AqiPM10());

        DataStream<Tuple3<String, Integer, String>> pm25 = keyedAirQualityRaw
                .window(TumblingProcessingTimeWindows.of(Time.minutes(1)))
                .process(new AqiMeasurement.AqiPM25());

        DataStream<Tuple3<String, Integer, String>> aqi = pm10.union(pm25)
                .keyBy(value -> value.f0)
                .process(new AqiMeasurement.SearchMaxAqi());


        KeyedStream<Tuple3<String, Integer, String>, String> keyedAqi = aqi.keyBy(value -> value.f0);

        DataStream<Tuple2<AirQualityRawSchema,String>> airQualityProcessed = keyedAirQualityRaw
                .intervalJoin(keyedAqi)
                .between(Time.minutes(-1), Time.minutes(0)).process(new EnrichmentClass.RenewAqi());

        DataStream<AirQualityProcessedSchema> resultStream =
                AsyncDataStream.orderedWait(airQualityProcessed, new EnrichmentClass.EnrichmentAsync(), 2000, TimeUnit.MILLISECONDS, 1000);

        resultStream.addSink(new SinkFunction());

        env.execute("Air Quality Job");
    }
}
