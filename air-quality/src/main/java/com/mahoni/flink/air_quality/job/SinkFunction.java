package com.mahoni.flink.air_quality.job;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.WriteApiBlocking;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import com.mahoni.flink.schema.AirQualityProcessedSchema;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.UUID;

public class SinkFunction implements org.apache.flink.streaming.api.functions.sink.SinkFunction<AirQualityProcessedSchema> {
    Properties kafkaProducerProps = new Properties();
    @Override
    public void invoke(AirQualityProcessedSchema airQualityProcessedSchema) throws Exception {

        kafkaProducerProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "http://34.128.127.171:9092");
        kafkaProducerProps.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        kafkaProducerProps.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        kafkaProducerProps.setProperty(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://34.128.127.171:8081");

        Producer<String, AirQualityProcessedSchema> kafkaProducer = new KafkaProducer<>(kafkaProducerProps);

        String id = UUID.randomUUID().toString();
        ProducerRecord<String, AirQualityProcessedSchema> record = new ProducerRecord<>("air-quality-processed-topic", id, airQualityProcessedSchema);
        kafkaProducer.send(record);

        InfluxDBClient influxDBClient = InfluxDBClientFactory.create("http://34.101.46.116:8086/", "awgmOZ1qYzHHevqtvKTnz6jZlgj9qO2QKeW7n7yB5hrTY-HZaV3tEPP9f6PkGz_915aN52CyumKsupbTvPnbtw==".toCharArray(), "mahoni_v1", "air_sensor_v1");
        WriteApiBlocking writeApi = influxDBClient.getWriteApiBlocking();

        Point point = Point.measurement("air_sensor_new")
                .addTag("nameLocation",airQualityProcessedSchema.getNameLocation().toString())
                .addTag("district",airQualityProcessedSchema.getDistrict().toString())
                .addField("aqi",airQualityProcessedSchema.getAqi())
                .addField("co",airQualityProcessedSchema.getCo())
                .addField("no",airQualityProcessedSchema.getNo())
                .addField("no2",airQualityProcessedSchema.getNo2())
                .addField("o3",airQualityProcessedSchema.getO3())
                .addField("so2",airQualityProcessedSchema.getSo2())
                .addField("pm25",airQualityProcessedSchema.getPm25())
                .addField("pm10",airQualityProcessedSchema.getPm10())
                .addField("pm1",airQualityProcessedSchema.getPm1())
                .addField("nh3",airQualityProcessedSchema.getNh3())
                .addField("pressure",airQualityProcessedSchema.getPressure())
                .addField("humidity",airQualityProcessedSchema.getHumidity())
                .addField("temperature",airQualityProcessedSchema.getTemperature())
                .time(airQualityProcessedSchema.getTimestamp(), WritePrecision.MS);
        writeApi.writePoint(point);
    }
}
