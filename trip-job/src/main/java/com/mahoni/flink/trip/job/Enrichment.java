package com.mahoni.flink.trip.job;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.mahoni.schema.TripEnrichment;
import com.mahoni.schema.TripSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.net.InetSocketAddress;
import java.time.LocalDate;
import java.util.Collections;

public class Enrichment extends RichAsyncFunction<TripSchema, TripEnrichment> {

    private static final String CASSANDRA_KEYSPACE = "mahoni";
    private static final String CASSANDRA_HOST = "34.101.176.46";
    private static final int CASSANDRA_PORT = 9042;

    String sex;
    int sex_decode;
    int age;
    String nameLocation;
    String type;
    String scanId;
    String scan;

    TripEnrichment result;
    private transient CqlSession session;

    @Override
    public void open(Configuration config) {

        session = CqlSession.builder()
                .addContactPoint(InetSocketAddress.createUnresolved(CASSANDRA_HOST, CASSANDRA_PORT))
                .withLocalDatacenter("asia-southeast2")//asia-southeast2
                .withKeyspace(CASSANDRA_KEYSPACE)
                .build();


    }
    @Override
    public void asyncInvoke(TripSchema tripSchema, ResultFuture<TripEnrichment> resultFuture) throws Exception {
        ResultSet userDetail = session.execute("SELECT * FROM users WHERE id=" + tripSchema.getUserId());
        for(Row rowUser: userDetail){
            sex_decode = rowUser.getShort("sex");
            if (sex_decode==1){
                sex = "Male";
            } else if (sex_decode==2){
                sex = "Female";
            } else{
                sex = "Unknown";
            }
            age = LocalDate.now().getYear() - (int)rowUser.getLong("year_of_birth");
        }

        if (tripSchema.getScanInPlaceId()!=null && tripSchema.getScanOutPlaceId()==null){
            scanId = tripSchema.getScanInPlaceId();
            scan = "IN";
        } else if (tripSchema.getScanInPlaceId()!=null && tripSchema.getScanOutPlaceId()!=null) {
            scanId = tripSchema.getScanOutPlaceId();
            scan = "OUT";
        }
        ResultSet qrDetail = session.execute("SELECT * FROM qr_generators WHERE id=" + scanId);
        for(Row rowQr: qrDetail){
            nameLocation = rowQr.getString("location");
            type = rowQr.getString("type");
        }

        result = new TripEnrichment(
                tripSchema.getEventId(),
                tripSchema.getTimestamp(),
                tripSchema.getTripId(),
                tripSchema.getUserId(),
                scanId,
                scan,
                tripSchema.getStatus(),
                sex,
                age,
                nameLocation,
                type);
        resultFuture.complete(Collections.singleton(result));
    }
}
