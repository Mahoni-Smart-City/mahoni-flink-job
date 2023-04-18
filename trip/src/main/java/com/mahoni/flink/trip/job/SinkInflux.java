package com.mahoni.flink.trip.job;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.WriteApiBlocking;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import com.mahoni.flink.trip.schema.TripEnrichment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

public class SinkInflux implements SinkFunction<TripEnrichment> {

    @Override
    public void invoke(TripEnrichment tripEnrichment) throws Exception {
        //belum disesuaikan, jadi masih dicomment
/*
        InfluxDBClient influxDBClient = InfluxDBClientFactory.create("http://localhost:8086", "ovGwppoZfaI6FAWWFl5MBUna0I8vjH0rOc-jGOgH-vS0z2Im3FU2Gq-CoyAWMNp2qmIP8Llrb_uq1E-qGuuGgA==".toCharArray(), "mahoni", "air_quality");

        WriteApiBlocking writeApi = influxDBClient.getWriteApiBlocking();

        Point point = Point.measurement("air_sensor")
                .addTag("nameLocation",airQualityProcessedSchema.getNameLocation().toString())
                .addTag("district",airQualityProcessedSchema.getDistrict().toString())
                .addField("aqi",airQualityProcessedSchema.getAqi())
                .time(airQualityProcessedSchema.getTimestamp(), WritePrecision.MS);
        writeApi.writePoint(point);

 */
    }
}
