import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.mahoni.flink.voucher.job.Enrichment;
import com.mahoni.flink.voucher.job.SinkInflux;
import com.mahoni.schema.VoucherMerchantEnrichment;
import com.mahoni.schema.VoucherRedeemedSchema;
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

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertTrue;

public class VoucherIntegrationTest {
    @ClassRule
    public static MiniClusterWithClientResource flinkCluster =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberSlotsPerTaskManager(2)
                            .setNumberTaskManagers(1)
                            .build());
    public static VoucherRedeemedSchema voucherFromKafka;
    public static InfluxDBClient influxDBClient;
    public static VoucherMerchantEnrichment result;
    public static String voucherId;

    @BeforeClass
    public static void setUp() {
        String eventId = UUID.randomUUID().toString();
        long now = System.currentTimeMillis();
        String userId = "e00ddf0d-1141-4bb1-897d-7d162547b581";
        voucherId = "25f91fd5-fc5d-4b44-8445-552aa0e044f3";
        voucherFromKafka = VoucherRedeemedSchema.newBuilder()
                .setEventId(eventId)
                .setTimestamp(now)
                .setVoucherId(voucherId)
                .setUserId(userId)
                .setCode("abc")
                .setPoint(10)
                .setExpiredAt(now)
                .setRedeemedAt(now)
                .build();

        influxDBClient = InfluxDBClientFactory.create("http://34.101.97.78:8086", "sFTppz2pO6_iaWRvl0yxcilS5XsREBzwZf0g7eEgyNdKdlsr8Y_0H3-OGnIwpLjZari0WILir5N2EQmiGbd9Zw==".toCharArray(), "mahoni", "mahoni_analysis");
    }

    @Test
    public void EndToEndVoucherJob() throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<VoucherRedeemedSchema> trip = env.fromElements(voucherFromKafka);
        DataStream<VoucherMerchantEnrichment> tripEnrichment = AsyncDataStream.orderedWait(trip, new Enrichment(), 2000, TimeUnit.MILLISECONDS, 1000);
        tripEnrichment.addSink(new CollectSink());
        tripEnrichment.addSink(new SinkInflux());
        env.execute("testVoucher");

        String query = "from(bucket: \"mahoni_analysis\")\n" +
                "  |> range(start: -5m)\n" +
                "  |> filter(fn: (r) => r[\"_measurement\"] == \"voucher_merchant\")\n" +
                "  |> filter(fn: (r) => r[\"_field\"] == \"voucherId\" and r[\"_value\"] == \"" + voucherId + "\" )";

        Long fluxResultCount = influxDBClient.getQueryApi().query(query).stream().count();

        assertEquals(voucherFromKafka.getVoucherId(),result.getVoucherId());
        assertTrue(fluxResultCount>0);
    }

    private static class CollectSink extends RichSinkFunction<VoucherMerchantEnrichment> {
        @Override
        public void invoke(VoucherMerchantEnrichment value, SinkFunction.Context context) throws Exception {
            result = value;
        }
    }
}
