package com.mahoni.flink;

import com.mahoni.schema.AirQualityRawSchema;

import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.tuple.Tuple2;
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

        DataStream<AirQualityRawSchema> airQualityRaw= env.addSource(new FlinkKafkaConsumer<>("air-quality-raw-topic", ConfluentRegistryAvroDeserializationSchema.forSpecific(AirQualityRawSchema.class,"http://localhost:8081"), kafkaProps))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5)));

        //contoh penghitungan aqi dengan o3
        DataStream<Tuple2<Double,Integer>> o3 = airQualityRaw
                .keyBy((AirQualityRawSchema sensor) -> sensor.getSensorId())
                .window(TumblingProcessingTimeWindows.of(Time.minutes(1)))
                        .process(new SearchO3());

        o3.print(); // hasil (o3 rata2 selama 1 menit, aqi yang didapatkan dari 1 menit)
        env.execute("Air Quality Job");
    }

    public static class SearchO3
            extends ProcessWindowFunction<AirQualityRawSchema, Tuple2<Double,Integer>, String, TimeWindow>{
        @Override
        public void process(
                String key,
                Context context,
                Iterable<AirQualityRawSchema> airQuality,
                Collector<Tuple2<Double,Integer>> out) throws Exception {
            double o3sum = 0.0;
            int count = 0;
            for( AirQualityRawSchema air : airQuality){
                double o3 = Math.floor(air.getO3() * 1000) / 1000; //truncate 3 decimal
                o3sum += o3;
                count ++;
            }
            double o3avg = Math.floor(o3sum/count * 1000) / 1000;
            int aqi = 0;
            if( o3avg >= 0.125 && o3avg <= 0.164 ){
                aqi = (int)Math.floor(((150-101)/(0.164-0.125))*(o3avg-0.125) + 101);
            } else if (o3avg >= 0.165 && o3avg <= 0.204) {
                aqi = (int)Math.floor(((200-151)/(0.204-0.165))*(o3avg-0.165) + 151);
            } else if (o3avg >= 0.205 && o3avg <= 0.404) {
                aqi = (int)Math.floor(((300-201)/(0.404-0.205))*(o3avg-0.205) + 201);
            } else if (o3avg >= 0.405 && o3avg <= 0.504) {
                aqi = (int)Math.floor(((400-301)/(0.504-0.405))*(o3avg-0.405) + 301);
            } else if (o3avg >= 0.505 && o3avg <= 0.604) {
                aqi = (int)Math.floor(((500-401)/(0.604-0.505))*(o3avg-0.505) + 401);
            } else {
                aqi = 605;
            }
            out.collect(Tuple2.of(o3avg,aqi));
        }
    }
}
