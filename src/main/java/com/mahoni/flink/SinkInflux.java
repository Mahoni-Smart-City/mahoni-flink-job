package com.mahoni.flink;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.WriteApiBlocking;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import com.mahoni.schema.AirQualityProcessedSchema;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.influxdb.InfluxDB;
//import org.influxdb.dto.Point;

import java.util.concurrent.TimeUnit;

public class SinkInflux implements SinkFunction<AirQualityProcessedSchema> {

    @Override
    public void invoke(AirQualityProcessedSchema airQualityProcessedSchema) throws Exception {

        //create(url,token,org,bucket)
        InfluxDBClient influxDBClient = InfluxDBClientFactory.create("http://localhost:8086", "ovGwppoZfaI6FAWWFl5MBUna0I8vjH0rOc-jGOgH-vS0z2Im3FU2Gq-CoyAWMNp2qmIP8Llrb_uq1E-qGuuGgA==".toCharArray(), "mahoni", "air_quality");
        //
        // Write data
        //
        WriteApiBlocking writeApi = influxDBClient.getWriteApiBlocking();

        //
        // Write by Data Point
        //
        Point point = Point.measurement("air_sensor")
                .addTag("nameLocation",airQualityProcessedSchema.getNameLocation().toString())
                .addTag("district",airQualityProcessedSchema.getDistrict().toString())
                .addField("aqi",airQualityProcessedSchema.getAqi())
                .time(airQualityProcessedSchema.getTimestamp(), WritePrecision.MS);
        writeApi.writePoint(point);
    }
}
