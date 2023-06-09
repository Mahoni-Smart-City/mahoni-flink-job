import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.mahoni.flink.air_quality.job.EnrichmentClass;
import com.mahoni.flink.air_quality.job.SinkFunction;
import com.mahoni.flink.schema.AirQualityProcessedSchema;
import com.mahoni.flink.schema.AirQualityRawSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertTrue;

public class AirQualityIntegrationTest {
    @ClassRule
    public static MiniClusterWithClientResource flinkCluster =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberSlotsPerTaskManager(2)
                            .setNumberTaskManagers(1)
                            .build());
    private static final String INFLUXDB_URL = "http://34.101.97.78:8086";
    private static final String INFLUXDB_BUCKET = "mahoni_analysis";
    private static final String INFLUXDB_ORG = "mahoni";
    public static InfluxDBClient influxDBClient;
    public static AirQualityRawSchema airQualityFromKafka;
    public static AirQualityProcessedSchema result;

    @BeforeClass
    public static void setUp(){
        String eventId = UUID.randomUUID().toString();
        String sensorId = "40905";
        LocalDateTime datetime = LocalDateTime.now();
        LocalDateTime rounded = datetime.minusMinutes(datetime.getMinute()).minusSeconds(datetime.getSecond());
        Long timestamp = datetime.toEpochSecond(ZoneId.systemDefault().getRules().getOffset(rounded)) * 1000L;
        long now = System.currentTimeMillis();

        airQualityFromKafka = AirQualityRawSchema.newBuilder()
                .setEventId(eventId)
                .setSensorId(sensorId)
                .setTimestamp(timestamp)
                .setAqi(0)
                .setCo(0)
                .setNo(0)
                .setNo2(0)
                .setO3(0)
                .setSo2(0)
                .setPm25(0)
                .setPm10(0)
                .setPm1(0)
                .setNh3(0)
                .setPressure(0)
                .setHumidity(0)
                .setTemperature(0)
                .build();

        influxDBClient = InfluxDBClientFactory.create(INFLUXDB_URL, "sFTppz2pO6_iaWRvl0yxcilS5XsREBzwZf0g7eEgyNdKdlsr8Y_0H3-OGnIwpLjZari0WILir5N2EQmiGbd9Zw==".toCharArray(), INFLUXDB_ORG, INFLUXDB_BUCKET);
    }

    @Test
    public void EndToEndAirQualityJob() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<AirQualityRawSchema,String>> airQualityProcessed = env.fromElements(Tuple2.of(airQualityFromKafka,"GOOD"));

        DataStream<AirQualityProcessedSchema> resultStream =
                AsyncDataStream.orderedWait(airQualityProcessed, new EnrichmentClass.EnrichmentAsync(), 2000, TimeUnit.MILLISECONDS, 1000);

        resultStream.addSink(new SinkFunction());
        resultStream.addSink(new CollectSink());

        env.execute("testing Air Quality");

        String query = "from(bucket: \"mahoni_analysis\")\n" +
                "  |> range(start: -5m)\n" +
                "  |> filter(fn: (r) => r[\"_measurement\"] == \"air_sensor\")\n" +
                "  |> filter(fn: (r) => r[\"nameLocation\"] == \"Jalan Haji R. Rasuna Said\")";

        Long fluxResultCount = influxDBClient.getQueryApi().query(query).stream().count();

        assertEquals("Jalan Haji R. Rasuna Said",result.getNameLocation());
        assertTrue(fluxResultCount>0);
    }

    private static class CollectSink extends RichSinkFunction<AirQualityProcessedSchema> {
        @Override
        public void invoke(AirQualityProcessedSchema value, org.apache.flink.streaming.api.functions.sink.SinkFunction.Context context) throws Exception {
            result = value;
        }
    }
}
