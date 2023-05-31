package com.mahoni.flink.air_quality.job;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.WriteApiBlocking;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import com.mahoni.flink.schema.AirQualityProcessedSchema;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.UUID;

public class SinkFunction extends RichSinkFunction<AirQualityProcessedSchema>{
    private transient Producer<String, AirQualityProcessedSchema> kafkaProducer;
    private transient WriteApiBlocking writeApi;

    @Override
    public void open(Configuration config){
        Properties kafkaProducerProps = new Properties();
        kafkaProducerProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "http://34.128.127.171:9092");
        kafkaProducerProps.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        kafkaProducerProps.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        kafkaProducerProps.setProperty(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://34.128.127.171:8081");
        kafkaProducer = new KafkaProducer<>(kafkaProducerProps);

        InfluxDBClient influxDBClient = InfluxDBClientFactory.create("http://34.101.97.78:8086", "sFTppz2pO6_iaWRvl0yxcilS5XsREBzwZf0g7eEgyNdKdlsr8Y_0H3-OGnIwpLjZari0WILir5N2EQmiGbd9Zw==".toCharArray(), "mahoni", "mahoni_analysis");
        writeApi = influxDBClient.getWriteApiBlocking();
    }
    @Override
    public void invoke(AirQualityProcessedSchema airQualityProcessedSchema){
        String id = UUID.randomUUID().toString();
        ProducerRecord<String, AirQualityProcessedSchema> record = new ProducerRecord<>("air-quality-processed-topic", id, airQualityProcessedSchema);
        kafkaProducer.send(record);

        Point point = Point.measurement("air_sensor")
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
