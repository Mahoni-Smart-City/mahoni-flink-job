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
        //kafkaConsumerProps.setProperty(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true");

        DataStream<AirQualityRawSchema> airQualityRaw = env.addSource(new FlinkKafkaConsumer<>("air-quality-raw-topic", ConfluentRegistryAvroDeserializationSchema.forSpecific(AirQualityRawSchema.class, "http://34.128.127.171:8081"), kafkaConsumerProps))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5)))
                .process(new Cleaning.CleanAqi());

        KeyedStream<AirQualityRawSchema, String> keyedAirQualityRaw = airQualityRaw.keyBy((AirQualityRawSchema sensor) -> sensor.getSensorId().toString());

        //contoh penghitungan aqi dengan o3
        /*
        DataStream<Tuple3<String, Integer, String>> o3 = keyedAirQualityRaw
                .window(TumblingProcessingTimeWindows.of(Time.minutes(1)))
                .process(new AqiMeasurement.AqiO3());

        //o3.print(); // hasil (o3 rata2 selama 1 menit, aqi yang didapatkan dari 1 menit)

        DataStream<Tuple3<String, Integer, String>> so2 = keyedAirQualityRaw
                .window(TumblingProcessingTimeWindows.of(Time.minutes(1)))
                .process(new AqiMeasurement.AqiSO2());

        //so2.print(); // hasil (so2 rata2 selama 1 menit, aqi yang didapatkan dari 1 menit)

        DataStream<Tuple3<String, Integer, String>> no2 = keyedAirQualityRaw
                .window(TumblingProcessingTimeWindows.of(Time.minutes(1)))
                .process(new AqiMeasurement.AqiNO2());

        //no2.print(); // hasil (no2 rata2 selama 1 menit, aqi yang didapatkan dari 1 menit)

        DataStream<Tuple3<String, Integer, String>> co = keyedAirQualityRaw
                .window(TumblingProcessingTimeWindows.of(Time.minutes(8)))
                .process(new AqiMeasurement.AqiCO());

        //co.print(); // hasil (co rata2 selama 8 menit, aqi yang didapatkan dari 8 menit)

         */

        DataStream<Tuple3<String, Integer, String>> pm10 = keyedAirQualityRaw
                .window(TumblingProcessingTimeWindows.of(Time.minutes(1)))
                .process(new AqiMeasurement.AqiPM10());

        //pm10.print(); // hasil (pm10 rata2 selama 8 menit, aqi yang didapatkan dari 8 menit)

        DataStream<Tuple3<String, Integer, String>> pm25 = keyedAirQualityRaw
                .window(TumblingProcessingTimeWindows.of(Time.minutes(1)))
                .process(new AqiMeasurement.AqiPM25());

        //pm25.print(); // hasil (pm25 rata2 selama 8 menit, aqi yang didapatkan dari 8 menit)
        /*
        DataStream<Tuple3<String, Integer, String>> aqi = o3.union(so2, no2, co, pm10, pm25)
                .keyBy(value -> value.f0)
                .process(new AqiMeasurement.SearchMaxAqi());

         */

        DataStream<Tuple3<String, Integer, String>> aqi = pm10.union(pm25)
                .keyBy(value -> value.f0)
                .process(new AqiMeasurement.SearchMaxAqi());

        //aqi.print(); //hasil pencarian AQI Max dari semua indikator yang digunakan

        KeyedStream<Tuple3<String, Integer, String>, String> keyedAqi = aqi.keyBy(value -> value.f0);

        DataStream<Tuple2<AirQualityRawSchema,String>> airQualityProcessed = keyedAirQualityRaw
                .intervalJoin(keyedAqi)
                .between(Time.minutes(-1), Time.minutes(0)).process(new EnrichmentClass.RenewAqi());

        DataStream<AirQualityProcessedSchema> resultStream =
                AsyncDataStream.orderedWait(airQualityProcessed, new EnrichmentClass.EnrichmentAsync(), 2000, TimeUnit.MILLISECONDS, 1000);
        resultStream.print();

        resultStream.addSink(new SinkFunction());

        env.execute("Air Quality Job");
    }
}
