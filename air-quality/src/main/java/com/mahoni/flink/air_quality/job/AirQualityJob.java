package com.mahoni.flink.air_quality.job;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.mahoni.flink.schema.AirQualityProcessedSchema;
import com.mahoni.flink.schema.AirQualityRawSchema;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.influxdb.InfluxDBPoint;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;

public class AirQualityJob {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRestartStrategy(RestartStrategies.noRestart());

        Properties kafkaConsumerProps = new Properties();
        kafkaConsumerProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "34.128.127.171:9092");
        kafkaConsumerProps.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        kafkaConsumerProps.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        kafkaConsumerProps.setProperty(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://34.128.127.171:8081");

        DataStream<AirQualityRawSchema> airQualityRaw = env.addSource(new FlinkKafkaConsumer<>("air-quality-raw-topic", ConfluentRegistryAvroDeserializationSchema.forSpecific(AirQualityRawSchema.class, "http://34.128.127.171:8081"), kafkaConsumerProps))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5)))
                .process(new CleanAqi());

        //contoh penghitungan aqi dengan o3
        DataStream<Tuple3<String, Integer, String>> o3 = airQualityRaw
                .keyBy((AirQualityRawSchema sensor) -> sensor.getSensorId())
                .window(TumblingProcessingTimeWindows.of(Time.minutes(1)))
                .process(new AqiMeasurement.AqiO3());

        //o3.print(); // hasil (o3 rata2 selama 1 menit, aqi yang didapatkan dari 1 menit)

        DataStream<Tuple3<String, Integer, String>> so2 = airQualityRaw
                .keyBy((AirQualityRawSchema sensor) -> sensor.getSensorId())
                .window(TumblingProcessingTimeWindows.of(Time.minutes(1)))
                .process(new AqiMeasurement.AqiSO2());

        //so2.print(); // hasil (so2 rata2 selama 1 menit, aqi yang didapatkan dari 1 menit)

        DataStream<Tuple3<String, Integer, String>> no2 = airQualityRaw
                .keyBy((AirQualityRawSchema sensor) -> sensor.getSensorId())
                .window(TumblingProcessingTimeWindows.of(Time.minutes(1)))
                .process(new AqiMeasurement.AqiNO2());

        //no2.print(); // hasil (no2 rata2 selama 1 menit, aqi yang didapatkan dari 1 menit)

        DataStream<Tuple3<String, Integer, String>> co = airQualityRaw
                .keyBy((AirQualityRawSchema sensor) -> sensor.getSensorId())
                .window(TumblingProcessingTimeWindows.of(Time.minutes(8)))
                .process(new AqiMeasurement.AqiCO());

        //co.print(); // hasil (co rata2 selama 8 menit, aqi yang didapatkan dari 8 menit)

        DataStream<Tuple3<String, Integer, String>> pm10 = airQualityRaw
                .keyBy((AirQualityRawSchema sensor) -> sensor.getSensorId())
                .window(TumblingProcessingTimeWindows.of(Time.minutes(8)))
                .process(new AqiMeasurement.AqiPM10());

        //pm10.print(); // hasil (pm10 rata2 selama 8 menit, aqi yang didapatkan dari 8 menit)

        DataStream<Tuple3<String, Integer, String>> pm25 = airQualityRaw
                .keyBy((AirQualityRawSchema sensor) -> sensor.getSensorId())
                .window(TumblingProcessingTimeWindows.of(Time.minutes(8)))
                .process(new AqiMeasurement.AqiPM25());

        //pm25.print(); // hasil (pm25 rata2 selama 8 menit, aqi yang didapatkan dari 8 menit)

        DataStream<Tuple3<String, Integer, String>> aqi = o3.union(so2, no2, co, pm10, pm25)
                .keyBy(value -> value.f0)
                .process(new AqiMeasurement.SearchMaxAqi());

        //aqi.print(); //hasil pencarian AQI Max dari semua indikator yang digunakan

        DataStream<AirQualityProcessedSchema> airQualityProcessed = airQualityRaw
                .keyBy((AirQualityRawSchema sensor) -> sensor.getSensorId())
                .intervalJoin(aqi.keyBy(value -> value.f0))
                .between(Time.minutes(-1), Time.minutes(0)).process(new RenewAqi());

        airQualityProcessed.print(); //hasil data final berupa enrichmen dan hasil AQI yang sudah dicari

        airQualityProcessed.addSink(new SinkKafka());
        airQualityProcessed.addSink(new SinkInflux());

        env.execute("Air Quality Job");
    }

    public static class RenewAqi extends ProcessJoinFunction<AirQualityRawSchema, Tuple3<String, Integer, String>, AirQualityProcessedSchema> {
        private static final String CASSANDRA_KEYSPACE = "mahoni";
        private static final String CASSANDRA_TABLE = "air_sensor";
        private static final String CASSANDRA_HOST = "34.101.176.46";
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
            ResultSet airSensorDetail = session.execute("SELECT * FROM air_sensor WHERE id=" + airQualityRawSchema.getSensorId());
            for(Row rowAirSensor: airSensorDetail){
                idLocation = rowAirSensor.getLong("id_location");
                nameLocation = rowAirSensor.getString("name_location");
            }

            ResultSet locationDetail = session.execute("SELECT * FROM location WHERE id=" + Long.toString(idLocation));
            for(Row rowLocation: locationDetail){
                district = rowLocation.getString("district");
                subDistrict = rowLocation.getString("subDistrict");
            }
            result = new AirQualityProcessedSchema(
                    airQualityRawSchema.getEventId(),
                    Long.parseLong(airQualityRawSchema.getSensorId()),
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
                    nameLocation,
                    idLocation,
                    district,
                    subDistrict
            );

            out.collect(result);
        }
    }

    public static class CleanAqi extends ProcessFunction<AirQualityRawSchema, AirQualityRawSchema> {
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
