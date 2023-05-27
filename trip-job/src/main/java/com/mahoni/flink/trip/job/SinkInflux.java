package com.mahoni.flink.trip.job;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.WriteApiBlocking;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import com.mahoni.schema.TripEnrichment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

public class SinkInflux implements SinkFunction<TripEnrichment> {

    @Override
    public void invoke(TripEnrichment tripEnrichment) throws Exception {
        //belum disesuaikan, jadi masih dicomment

        InfluxDBClient influxDBClient = InfluxDBClientFactory.create("http://34.101.97.78:8086", "sFTppz2pO6_iaWRvl0yxcilS5XsREBzwZf0g7eEgyNdKdlsr8Y_0H3-OGnIwpLjZari0WILir5N2EQmiGbd9Zw==".toCharArray(), "mahoni", "trip");

        WriteApiBlocking writeApi = influxDBClient.getWriteApiBlocking();

        Point point = Point.measurement("trips")
                .addTag("sex",tripEnrichment.getSex().toString())
                .addTag("location_name",tripEnrichment.getLocationName().toString())
                .addTag("type",tripEnrichment.getType().toString())
                .addField("age",tripEnrichment.getAge())
                .addField("user_id",tripEnrichment.getTripId().toString())
                .addField("qr_id",tripEnrichment.getQrId().toString())
                .addField("scan",tripEnrichment.getScan().toString())
                .addField("status",tripEnrichment.getStatus().toString())
                .time(tripEnrichment.getTimestamp(), WritePrecision.MS);
        writeApi.writePoint(point);


    }
}
