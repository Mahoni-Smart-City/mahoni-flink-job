package com.mahoni.flink.air_quality.job;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.mahoni.flink.schema.AirQualityProcessedSchema;
import com.mahoni.flink.schema.AirQualityRawSchema;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Properties;

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
        //airQualityRaw.keyBy((AirQualityRawSchema sensor) -> sensor.getSensorId()).print();
        //airQualityRaw.print();

        //contoh penghitungan aqi dengan o3
        DataStream<Tuple3<String, Integer, String>> o3 = airQualityRaw
                .keyBy((AirQualityRawSchema sensor) -> sensor.getSensorId().toString())
                .window(TumblingProcessingTimeWindows.of(Time.minutes(1)))
                .process(new AqiMeasurement.AqiO3());

        //o3.print(); // hasil (o3 rata2 selama 1 menit, aqi yang didapatkan dari 1 menit)

        DataStream<Tuple3<String, Integer, String>> so2 = airQualityRaw
                .keyBy((AirQualityRawSchema sensor) -> sensor.getSensorId().toString())
                .window(TumblingProcessingTimeWindows.of(Time.minutes(1)))
                .process(new AqiMeasurement.AqiSO2());

        //so2.print(); // hasil (so2 rata2 selama 1 menit, aqi yang didapatkan dari 1 menit)

        DataStream<Tuple3<String, Integer, String>> no2 = airQualityRaw
                .keyBy((AirQualityRawSchema sensor) -> sensor.getSensorId().toString())
                .window(TumblingProcessingTimeWindows.of(Time.minutes(1)))
                .process(new AqiMeasurement.AqiNO2());

        //no2.print(); // hasil (no2 rata2 selama 1 menit, aqi yang didapatkan dari 1 menit)

        DataStream<Tuple3<String, Integer, String>> co = airQualityRaw
                .keyBy((AirQualityRawSchema sensor) -> sensor.getSensorId().toString())
                .window(TumblingProcessingTimeWindows.of(Time.minutes(8)))
                .process(new AqiMeasurement.AqiCO());

        //co.print(); // hasil (co rata2 selama 8 menit, aqi yang didapatkan dari 8 menit)

        DataStream<Tuple3<String, Integer, String>> pm10 = airQualityRaw
                .keyBy((AirQualityRawSchema sensor) -> sensor.getSensorId().toString())
                .window(TumblingProcessingTimeWindows.of(Time.minutes(8)))
                .process(new AqiMeasurement.AqiPM10());

        //pm10.print(); // hasil (pm10 rata2 selama 8 menit, aqi yang didapatkan dari 8 menit)

        DataStream<Tuple3<String, Integer, String>> pm25 = airQualityRaw
                .keyBy((AirQualityRawSchema sensor) -> sensor.getSensorId().toString())
                .window(TumblingProcessingTimeWindows.of(Time.minutes(8)))
                .process(new AqiMeasurement.AqiPM25());

        //pm25.print(); // hasil (pm25 rata2 selama 8 menit, aqi yang didapatkan dari 8 menit)

        DataStream<Tuple3<String, Integer, String>> aqi = o3.union(so2, no2, co, pm10, pm25)
                .keyBy(value -> value.f0)
                .process(new AqiMeasurement.SearchMaxAqi());

        //aqi.print(); //hasil pencarian AQI Max dari semua indikator yang digunakan

        DataStream<AirQualityProcessedSchema> airQualityProcessed = airQualityRaw
                .keyBy((AirQualityRawSchema sensor) -> sensor.getSensorId().toString())
                .intervalJoin(aqi.keyBy(value -> value.f0))
                .between(Time.minutes(-1), Time.minutes(0)).process(new RenewAqi());

        airQualityProcessed.print(); //hasil data final berupa enrichmen dan hasil AQI yang sudah dicari

        airQualityProcessed.addSink(new SinkKafka());
        //airQualityProcessed.addSink(new SinkInflux());

        env.execute("Air Quality Job");
    }

    public static class RenewAqi extends ProcessJoinFunction<AirQualityRawSchema, Tuple3<String, Integer, String>, AirQualityProcessedSchema> {
        private static final String CASSANDRA_KEYSPACE = "mahoni";
        private static final String CASSANDRA_TABLE = "air_sensor";
        private static final String CASSANDRA_HOST = "34.101.66.113";
        private static final int CASSANDRA_PORT = 9042;

        private transient CqlSession session;

        Long idLocation;
        String nameLocation;
        String district;
        String subDistrict;
        AirQualityProcessedSchema result;

        @Override
        public void open(Configuration config) {

            session = CqlSession.builder()
                    .addContactPoint(InetSocketAddress.createUnresolved(CASSANDRA_HOST, CASSANDRA_PORT))
                    .withLocalDatacenter("asia-southeast2")
                    .withKeyspace(CASSANDRA_KEYSPACE)
                    .build();

        }
        @Override
        public void processElement(AirQualityRawSchema airQualityRawSchema,
                                   Tuple3<String, Integer, String> aqi,
                                   Context context,
                                   Collector<AirQualityProcessedSchema> out) throws Exception {
            if (aqi != null) {
                int newAqi = aqi.f1;
                airQualityRawSchema.setAqi((double) newAqi);
            }
            System.out.println(airQualityRawSchema.getSensorId());
            ResultSet airSensorDetail = session.execute("SELECT * FROM air_sensors WHERE id=" + airQualityRawSchema.getSensorId());
            for(Row rowAirSensor: airSensorDetail){
                idLocation = rowAirSensor.getLong("location_id");
                nameLocation = rowAirSensor.getString("location_name");
            }

            ResultSet locationDetail = session.execute("SELECT * FROM locations WHERE id=" + idLocation);
            for(Row rowLocation: locationDetail){
                district = rowLocation.getString("district");
                subDistrict = rowLocation.getString("sub_district");
            }
            //System.out.print(airQualityRawSchema.getSensorId().toString());
            //System.out.print(subDistrict);
            result = new AirQualityProcessedSchema(
                    airQualityRawSchema.getEventId(),
                    Long.parseLong(airQualityRawSchema.getSensorId().toString()),
                    airQualityRawSchema.getTimestamp(),
                    airQualityRawSchema.getAqi(),
                    aqi.f2,
                    airQualityRawSchema.getCo(),
                    airQualityRawSchema.getNo(),
                    airQualityRawSchema.getNo2(),
                    airQualityRawSchema.getO3(),
                    airQualityRawSchema.getSo2(),
                    airQualityRawSchema.getPm25(),
                    airQualityRawSchema.getPm10(),
                    airQualityRawSchema.getPm1(),
                    airQualityRawSchema.getNh3(),
                    airQualityRawSchema.getPressure(),
                    airQualityRawSchema.getHumidity(),
                    airQualityRawSchema.getTemperature(),
                    nameLocation,
                    idLocation,
                    subDistrict,
                    district
            );

            out.collect(result);
        }
    }
}
