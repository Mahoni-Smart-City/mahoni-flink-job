package com.mahoni.flink.voucher.job;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.mahoni.flink.voucher.schema.VoucherEnrichment;
import com.mahoni.flink.voucher.schema.VoucherRedeemedSchema;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.time.LocalDate;
import java.util.Properties;

public class VoucherJob {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRestartStrategy(RestartStrategies.noRestart());

        Properties kafkaConsumerProps = new Properties();
        kafkaConsumerProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        kafkaConsumerProps.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        kafkaConsumerProps.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        kafkaConsumerProps.setProperty(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");

        DataStream<VoucherRedeemedSchema> voucher = env.addSource(new FlinkKafkaConsumer<>("voucher-redeemed-topic", ConfluentRegistryAvroDeserializationSchema.forSpecific(VoucherRedeemedSchema.class, "http://localhost:8081"), kafkaConsumerProps))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5)));
        DataStream<VoucherEnrichment> voucherEnrichment = voucher.process(new VoucherProcess());
        voucherEnrichment.addSink(new SinkInflux());
        env.execute("Voucher Job");
    }
    public static class VoucherProcess extends ProcessFunction<VoucherRedeemedSchema, VoucherEnrichment>{
        private static final String CASSANDRA_KEYSPACE_1 = "voucher";
        private static final String CASSANDRA_KEYSPACE_2 = "user";
        private static final String CASSANDRA_HOST = "localhost";
        private static final int CASSANDRA_PORT = 9042;
        boolean sex;
        int age;
        String nameVoucher;
        String typeVoucher;
        VoucherEnrichment result;

        private transient CqlSession session_1;
        private transient CqlSession session_2;
        @Override
        public void open(Configuration config) {
            session_1 = CqlSession.builder()
                    .addContactPoint(InetSocketAddress.createUnresolved(CASSANDRA_HOST, CASSANDRA_PORT))
                    .withLocalDatacenter("datacenter1")
                    .withKeyspace(CASSANDRA_KEYSPACE_1)
                    .build();
            session_2 = CqlSession.builder()
                    .addContactPoint(InetSocketAddress.createUnresolved(CASSANDRA_HOST, CASSANDRA_PORT))
                    .withLocalDatacenter("datacenter1")
                    .withKeyspace(CASSANDRA_KEYSPACE_2)
                    .build();
        }
        @Override
        public void processElement(VoucherRedeemedSchema voucherRedeemedSchema,
                                   Context context,
                                   Collector<VoucherEnrichment> out) throws Exception {
            ResultSet userDetail = session_2.execute("SELECT * FROM user WHERE id=" + voucherRedeemedSchema.getUserId());
            for(Row rowUser: userDetail){
                sex = rowUser.getBoolean("sex");
                age = LocalDate.now().getYear() - rowUser.getInt("year_of_birth");
            }
            ResultSet voucherDetail = session_1.execute("SELECT * FROM voucher WHERE id=" + voucherRedeemedSchema.getVoucherId());
            for(Row rowVoucher: voucherDetail){
                nameVoucher = rowVoucher.getString("name");
                typeVoucher = rowVoucher.getString("type");
            }

            result = new VoucherEnrichment(voucherRedeemedSchema.getTimestamp(),
                    voucherRedeemedSchema.getVoucherId(),
                    voucherRedeemedSchema.getUserId(),
                    sex,
                    age,
                    nameVoucher,
                    typeVoucher);
            out.collect(result);

        }
    }
}