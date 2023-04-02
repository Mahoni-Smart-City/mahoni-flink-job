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
import org.apache.flink.streaming.api.functions.ProcessFunction;
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

        Properties kafkaConsumerProps = new Properties();
        kafkaConsumerProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        kafkaConsumerProps.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        kafkaConsumerProps.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        kafkaConsumerProps.setProperty(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");

        DataStream<AirQualityRawSchema> airQualityRaw= env.addSource(new FlinkKafkaConsumer<>("air-quality-raw-topic", ConfluentRegistryAvroDeserializationSchema.forSpecific(AirQualityRawSchema.class,"http://localhost:8081"), kafkaConsumerProps))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5)))
                .process(new CleanAqi());

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

        DataStream<AirQualityRawSchema> renewAqi = airQualityRaw
                .keyBy((AirQualityRawSchema sensor) -> sensor.getSensorId())
                .intervalJoin(aqi.keyBy(value -> value.f0))
                .between(Time.minutes(-1),Time.minutes(0)).process( new RenewAqi());

        //renewAqi.print();

        Properties kafkaProducerProps = new Properties();
        kafkaProducerProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        kafkaProducerProps.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        kafkaProducerProps.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        kafkaProducerProps.setProperty(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");

        FlinkKafkaProducer<AirQualityRawSchema> producer = new FlinkKafkaProducer<>("air-quality-processed", ConfluentRegistryAvroSerializationSchema.forSpecific(AirQualityRawSchema.class,"air-quality-processed","http://localhost:8081"), kafkaProducerProps);

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

    public static class CleanAqi extends ProcessFunction<AirQualityRawSchema,AirQualityRawSchema>{
        @Override
        public void processElement(AirQualityRawSchema airQualityRawSchema,
                                   Context context,
                                   Collector<AirQualityRawSchema> out) throws Exception {
            double co = Math.floor(airQualityRawSchema.getCo() * 100) / 100;
            airQualityRawSchema.setCo(co);

            double no = Math.floor(airQualityRawSchema.getNo() * 100) / 100;
            airQualityRawSchema.setNo(no);

            double no2 = Math.floor(airQualityRawSchema.getNo2() * 100) / 100;
            airQualityRawSchema.setNo2(no2);

            double o3 = Math.floor(airQualityRawSchema.getO3() * 100) / 100;
            airQualityRawSchema.setO3(o3);

            double so2 = Math.floor(airQualityRawSchema.getSo2() * 100) / 100;
            airQualityRawSchema.setSo2(so2);

            double pm25 = Math.floor(airQualityRawSchema.getPm25() * 100) / 100;
            airQualityRawSchema.setPm25(pm25);

            double pm10 = Math.floor(airQualityRawSchema.getPm10() * 100) / 100;
            airQualityRawSchema.setPm10(pm10);

            double pm1 = Math.floor(airQualityRawSchema.getPm1() * 100) / 100;
            airQualityRawSchema.setPm1(pm1);

            double nh3 = Math.floor(airQualityRawSchema.getNh3() * 100) / 100;
            airQualityRawSchema.setNh3(nh3);

            double pressure = Math.floor(airQualityRawSchema.getPressure() * 100) / 100;
            airQualityRawSchema.setPressure(pressure);

            double humidity = Math.floor(airQualityRawSchema.getHumidity() * 100) / 100;
            airQualityRawSchema.setHumidity(humidity);

            out.collect(airQualityRawSchema);
        }
    }
}
