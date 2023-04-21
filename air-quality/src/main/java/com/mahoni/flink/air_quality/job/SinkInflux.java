package com.mahoni.flink.air_quality.job;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.WriteApiBlocking;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import com.mahoni.flink.schema.AirQualityProcessedSchema;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

public class SinkInflux implements SinkFunction<AirQualityProcessedSchema> {

    @Override
    public void invoke(AirQualityProcessedSchema airQualityProcessedSchema) throws Exception {

        InfluxDBClient influxDBClient = InfluxDBClientFactory.create("http://localhost:8086", "ovGwppoZfaI6FAWWFl5MBUna0I8vjH0rOc-jGOgH-vS0z2Im3FU2Gq-CoyAWMNp2qmIP8Llrb_uq1E-qGuuGgA==".toCharArray(), "mahoni", "air_quality");

        WriteApiBlocking writeApi = influxDBClient.getWriteApiBlocking();

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
                .time(airQualityProcessedSchema.getTimestamp(), WritePrecision.MS);
        writeApi.writePoint(point);
    }
}
