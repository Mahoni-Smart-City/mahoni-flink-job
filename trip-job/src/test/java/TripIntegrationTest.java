import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.mahoni.flink.trip.job.Enrichment;
import com.mahoni.flink.trip.job.SinkInflux;
import com.mahoni.schema.TripEnrichment;
import com.mahoni.schema.TripSchema;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertTrue;

public class TripIntegrationTest {
    @ClassRule
    public static MiniClusterWithClientResource flinkCluster =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberSlotsPerTaskManager(2)
                            .setNumberTaskManagers(1)
                            .build());

    public static TripSchema tripFromKafka;
    public static TripEnrichment result;
    public static InfluxDBClient influxDBClient;
    public static String tripId;

    @BeforeClass
    public static void setUp() {

        String eventId = UUID.randomUUID().toString();
        tripId = UUID.randomUUID().toString();
        String kafkaId = UUID.randomUUID().toString();
        long now = System.currentTimeMillis();
        String userId = "e00ddf0d-1141-4bb1-897d-7d162547b581";
        String placeId = "52d49e42-fedb-4454-a66c-ae224f2dcf34";
        tripFromKafka = TripSchema.newBuilder()
                .setEventId(eventId)
                .setTimestamp(now)
                .setTripId(tripId)
                .setUserId(userId)
                .setScanInPlaceId(placeId)
                .setScanInTimestamp(now)
                .setScanOutPlaceId(null)
                .setScanOutTimestamp(null)
                .setStatus("ACTIVE")
                .setAqi(null)
                .setPoint(null)
                .build();

        influxDBClient = InfluxDBClientFactory.create("http://34.101.97.78:8086", "sFTppz2pO6_iaWRvl0yxcilS5XsREBzwZf0g7eEgyNdKdlsr8Y_0H3-OGnIwpLjZari0WILir5N2EQmiGbd9Zw==".toCharArray(), "mahoni", "mahoni_analysis");
    }


    @Test
    public void EndToEndTripJob() throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<TripSchema> trip = env.fromElements(tripFromKafka);
        DataStream<TripEnrichment> tripEnrichment = AsyncDataStream.orderedWait(trip, new Enrichment(), 2000, TimeUnit.MILLISECONDS, 1000);
        tripEnrichment.addSink(new CollectSink());
        tripEnrichment.addSink(new SinkInflux());
        env.execute("test");
        System.out.println(tripId);

        String query = "from(bucket: \"mahoni_analysis\")\n" +
                "  |> range(start: -5m)\n" +
                "  |> filter(fn: (r) => r[\"_measurement\"] == \"trips\")\n" +
                "  |> filter(fn: (r) => r[\"_field\"] == \"trip_id\" and r[\"_value\"] == \"" + tripId + "\" )";

        Long fluxResultCount = influxDBClient.getQueryApi().query(query).stream().count();

        assertEquals(tripFromKafka.getTripId(),result.getTripId());
        assertTrue(fluxResultCount>0);
    }

    private static class CollectSink extends RichSinkFunction<TripEnrichment> {
        @Override
        public void invoke(TripEnrichment value, SinkFunction.Context context) throws Exception {
            result = value;
        }
    }
}
