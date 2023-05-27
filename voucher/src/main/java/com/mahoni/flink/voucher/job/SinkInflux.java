package com.mahoni.flink.voucher.job;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.WriteApiBlocking;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import com.mahoni.schema.VoucherMerchantEnrichment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

public class SinkInflux implements SinkFunction<VoucherMerchantEnrichment> {

    @Override
    public void invoke(VoucherMerchantEnrichment voucherMerchantEnrichment) throws Exception {

        InfluxDBClient influxDBClient = InfluxDBClientFactory.create("http://34.101.97.78:8086", "sFTppz2pO6_iaWRvl0yxcilS5XsREBzwZf0g7eEgyNdKdlsr8Y_0H3-OGnIwpLjZari0WILir5N2EQmiGbd9Zw==".toCharArray(), "mahoni", "voucher-merchant");

        WriteApiBlocking writeApi = influxDBClient.getWriteApiBlocking();

        Point point = Point.measurement("voucher_merchant")
                .addTag("sex",voucherMerchantEnrichment.getSex().toString())
                .addTag("voucherName",voucherMerchantEnrichment.getVoucherName().toString())
                .addTag("typeVoucher",voucherMerchantEnrichment.getVoucherType().toString())
                .addTag("merchantName",voucherMerchantEnrichment.getMerchantName().toString())
                .addField("age",voucherMerchantEnrichment.getAge())
                .addField("voucherId",voucherMerchantEnrichment.getVoucherId().toString())
                .addField("userId", voucherMerchantEnrichment.getUserId().toString())
                .time(voucherMerchantEnrichment.getTimestamp(), WritePrecision.MS);
        writeApi.writePoint(point);
    }
}
