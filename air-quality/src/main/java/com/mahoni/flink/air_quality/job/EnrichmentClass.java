package com.mahoni.flink.air_quality.job;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.mahoni.flink.schema.AirQualityProcessedSchema;
import com.mahoni.flink.schema.AirQualityRawSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.util.Collector;

import java.net.InetSocketAddress;
import java.util.Collections;

public class EnrichmentClass {
    public static class RenewAqi extends ProcessJoinFunction<AirQualityRawSchema, Tuple3<String, Integer, String>, Tuple2<AirQualityRawSchema,String>> {
        Tuple2<AirQualityRawSchema,String> result;

        @Override
        public void processElement(AirQualityRawSchema airQualityRawSchema, Tuple3<String, Integer, String> aqi, ProcessJoinFunction<AirQualityRawSchema, Tuple3<String, Integer, String>, Tuple2<AirQualityRawSchema, String>>.Context context, Collector<Tuple2<AirQualityRawSchema, String>> out) throws Exception {
            if (aqi != null) {
                int newAqi = aqi.f1;
                airQualityRawSchema.setAqi((double) newAqi);
            }
            result = Tuple2.of(airQualityRawSchema,aqi.f2);
            out.collect(result);
        }
    }

    public static class EnrichmentAsync extends RichAsyncFunction<Tuple2<AirQualityRawSchema,String>, AirQualityProcessedSchema> {

        private static final String CASSANDRA_KEYSPACE = "mahoni";
        private static final String CASSANDRA_TABLE = "air_sensor";
        private static final String CASSANDRA_HOST = "34.101.176.46";
        private static final int CASSANDRA_PORT = 9042;

        private transient CqlSession session1;
        private transient CqlSession session2;

        Long idLocation;
        String nameLocation;
        String district;
        String subDistrict;
        AirQualityProcessedSchema result;

        @Override
        public void open(Configuration config) {

            session1 = CqlSession.builder()
                    .addContactPoint(InetSocketAddress.createUnresolved(CASSANDRA_HOST, CASSANDRA_PORT))
                    .withLocalDatacenter("asia-southeast2")//asia-southeast2
                    .withKeyspace(CASSANDRA_KEYSPACE)
                    .build();
            session2 = CqlSession.builder()
                    .addContactPoint(InetSocketAddress.createUnresolved(CASSANDRA_HOST, CASSANDRA_PORT))
                    .withLocalDatacenter("asia-southeast2")
                    .withKeyspace(CASSANDRA_KEYSPACE)
                    .build();
        }
        @Override
        public void asyncInvoke(Tuple2<AirQualityRawSchema,String> input, ResultFuture<AirQualityProcessedSchema> resultFuture) throws Exception {
            AirQualityRawSchema airQualityRawSchema = input.f0;
            String categoryAqi = input.f1;
            ResultSet airSensorDetail = session1.execute("SELECT location_id, location_name FROM air_sensors WHERE id=" + airQualityRawSchema.getSensorId());

            Row rowAirSensorDetail = airSensorDetail.one();
            if (rowAirSensorDetail != null){
                idLocation = rowAirSensorDetail.getLong("location_id");
                nameLocation = rowAirSensorDetail.getString("location_name");
            }

            ResultSet locationDetail = session2.execute("SELECT district, sub_district FROM locations WHERE id=" + idLocation);

            Row rowLocationDetail = locationDetail.one();
            if (rowLocationDetail != null){
                district = rowLocationDetail.getString("district");

                subDistrict = rowLocationDetail.getString("sub_district");
            }


            result = new AirQualityProcessedSchema(
                    airQualityRawSchema.getEventId(),
                    Long.parseLong(airQualityRawSchema.getSensorId().toString()),
                    airQualityRawSchema.getTimestamp(),
                    airQualityRawSchema.getAqi(),
                    categoryAqi,
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
            resultFuture.complete(Collections.singleton(result));
        }
    }
}
