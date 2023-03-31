package com.mahoni.flink;

import com.mahoni.schema.AirQualityRawSchema;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroSerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.producer.ProducerConfig;
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
        DataStream<Tuple3<String,Integer,String>> o3 = airQualityRaw
                .keyBy((AirQualityRawSchema sensor) -> sensor.getSensorId())
                .window(TumblingProcessingTimeWindows.of(Time.minutes(1)))
                        .process(new AqiMeasurement.AqiO3());

        //o3.print(); // hasil (o3 rata2 selama 1 menit, aqi yang didapatkan dari 1 menit)

        DataStream<Tuple3<String,Integer,String>> so2 = airQualityRaw
                .keyBy((AirQualityRawSchema sensor) -> sensor.getSensorId())
                .window(TumblingProcessingTimeWindows.of(Time.minutes(1)))
                .process(new AqiMeasurement.AqiSO2());

        //so2.print(); // hasil (so2 rata2 selama 1 menit, aqi yang didapatkan dari 1 menit)

        DataStream<Tuple3<String,Integer,String>> no2 = airQualityRaw
                .keyBy((AirQualityRawSchema sensor) -> sensor.getSensorId())
                .window(TumblingProcessingTimeWindows.of(Time.minutes(1)))
                .process(new AqiMeasurement.AqiNO2());

        //no2.print(); // hasil (no2 rata2 selama 1 menit, aqi yang didapatkan dari 1 menit)

        DataStream<Tuple3<String,Integer,String>> co = airQualityRaw
                .keyBy((AirQualityRawSchema sensor) -> sensor.getSensorId())
                .window(TumblingProcessingTimeWindows.of(Time.minutes(8)))
                .process(new AqiMeasurement.AqiCO());

        //co.print(); // hasil (co rata2 selama 8 menit, aqi yang didapatkan dari 8 menit)

        DataStream<Tuple3<String,Integer,String>> pm10 = airQualityRaw
                .keyBy((AirQualityRawSchema sensor) -> sensor.getSensorId())
                .window(TumblingProcessingTimeWindows.of(Time.minutes(8)))
                .process(new AqiMeasurement.AqiPM10());

        //pm10.print(); // hasil (pm10 rata2 selama 8 menit, aqi yang didapatkan dari 8 menit)

        DataStream<Tuple3<String,Integer,String>> pm25 = airQualityRaw
                .keyBy((AirQualityRawSchema sensor) -> sensor.getSensorId())
                .window(TumblingProcessingTimeWindows.of(Time.minutes(8)))
                .process(new AqiMeasurement.AqiPM25());

        //pm25.print(); // hasil (pm25 rata2 selama 8 menit, aqi yang didapatkan dari 8 menit)

        DataStream<Tuple3<String,Integer,String>> aqi = o3.union(so2,no2,co,pm10,pm25)
                .keyBy(value -> value.f0)
                .process(new AqiMeasurement.SearchMaxAqi());

        aqi.print(); //hasil pencarian AQI Max dari semua indikator yang digunakan

        //DataStream<AirQualityRawSchema> newaqi = airQualityRaw.join(aqi)



        DataStream<AirQualityRawSchema> renewAqi = airQualityRaw
                .keyBy((AirQualityRawSchema sensor) -> sensor.getSensorId())
                        .intervalJoin(aqi.keyBy(value -> value.f0))
                                .between(Time.minutes(-1),Time.minutes(0)).process( new RenewAqi());

        //renewAqi.print();
        Properties kafkaProd = new Properties();
        kafkaProd.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        kafkaProd.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        kafkaProd.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        kafkaProd.setProperty(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        FlinkKafkaProducer<AirQualityRawSchema> producer = new FlinkKafkaProducer<>("air-quality-processed", ConfluentRegistryAvroSerializationSchema.forSpecific(AirQualityRawSchema.class,"air-quality-processed","http://localhost:8081"), kafkaProd);
        renewAqi.addSink(producer);

        env.execute("Air Quality Job");
    }

    public static class RenewAqi extends ProcessJoinFunction<AirQualityRawSchema,Tuple3<String,Integer,String>,AirQualityRawSchema>{
        @Override
        public void processElement(AirQualityRawSchema airQualityRawSchema,
                                   Tuple3<String, Integer, String > aqi,
                                   Context context,
                                   Collector<AirQualityRawSchema> out) throws Exception {
            if (aqi != null) {
                int newAqi = aqi.f1;
                airQualityRawSchema.setAqi((double) newAqi );
            }
            out.collect(airQualityRawSchema);
        }
    }
}
