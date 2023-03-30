package com.mahoni.flink;

import com.mahoni.schema.AirQualityRawSchema;

import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import com.mahoni.flink.AqiMeasurement;

import java.time.Duration;
import java.util.Properties;

public class AirQualityJob {
    public static void main(String[] args) throws Exception{

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRestartStrategy(RestartStrategies.noRestart());

        Properties kafkaProps = new Properties();
        kafkaProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        kafkaProps.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        kafkaProps.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        kafkaProps.setProperty(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");

        AqiMeasurement aqiMeasurement = new AqiMeasurement();

        DataStream<AirQualityRawSchema> airQualityRaw= env.addSource(new FlinkKafkaConsumer<>("air-quality-raw-topic", ConfluentRegistryAvroDeserializationSchema.forSpecific(AirQualityRawSchema.class,"http://localhost:8081"), kafkaProps))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5)));

        //contoh penghitungan aqi dengan o3
        DataStream<Tuple3<String,Double,Integer>> o3 = airQualityRaw
                .keyBy((AirQualityRawSchema sensor) -> sensor.getSensorId())
                .window(TumblingProcessingTimeWindows.of(Time.minutes(1)))
                        .process(new AqiMeasurement.AqiO3());

        o3.print(); // hasil (o3 rata2 selama 1 menit, aqi yang didapatkan dari 1 menit)

        DataStream<Tuple3<String,Double,Integer>> so2 = airQualityRaw
                .keyBy((AirQualityRawSchema sensor) -> sensor.getSensorId())
                .window(TumblingProcessingTimeWindows.of(Time.minutes(1)))
                .process(new AqiMeasurement.AqiSO2());

        so2.print(); // hasil (so2 rata2 selama 1 menit, aqi yang didapatkan dari 1 menit)

        DataStream<Tuple3<String,Double,Integer>> no2 = airQualityRaw
                .keyBy((AirQualityRawSchema sensor) -> sensor.getSensorId())
                .window(TumblingProcessingTimeWindows.of(Time.minutes(1)))
                .process(new AqiMeasurement.AqiNO2());

        no2.print(); // hasil (no2 rata2 selama 1 menit, aqi yang didapatkan dari 1 menit)

        DataStream<Tuple3<String,Double,Integer>> co = airQualityRaw
                .keyBy((AirQualityRawSchema sensor) -> sensor.getSensorId())
                .window(TumblingProcessingTimeWindows.of(Time.minutes(8)))
                .process(new AqiMeasurement.AqiCO());

        co.print(); // hasil (co rata2 selama 8 menit, aqi yang didapatkan dari 8 menit)

        DataStream<Tuple3<String,Double,Integer>> pm10 = airQualityRaw
                .keyBy((AirQualityRawSchema sensor) -> sensor.getSensorId())
                .window(TumblingProcessingTimeWindows.of(Time.minutes(8)))
                .process(new AqiMeasurement.AqiPM10());

        pm10.print(); // hasil (pm10 rata2 selama 8 menit, aqi yang didapatkan dari 8 menit)

        DataStream<Tuple3<String,Double,Integer>> pm25 = airQualityRaw
                .keyBy((AirQualityRawSchema sensor) -> sensor.getSensorId())
                .window(TumblingProcessingTimeWindows.of(Time.minutes(8)))
                .process(new AqiMeasurement.AqiPM25());

        pm25.print(); // hasil (pm25 rata2 selama 8 menit, aqi yang didapatkan dari 8 menit)

        env.execute("Air Quality Job");
    }
}
