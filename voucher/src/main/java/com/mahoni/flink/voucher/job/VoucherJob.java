package com.mahoni.flink.voucher.job;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.mahoni.schema.VoucherMerchantEnrichment;
import com.mahoni.schema.VoucherRedeemedSchema;
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
        kafkaConsumerProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "34.128.127.171:9092");
        kafkaConsumerProps.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        kafkaConsumerProps.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        kafkaConsumerProps.setProperty(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://34.128.127.171:8081");

        DataStream<VoucherRedeemedSchema> voucher = env.addSource(new FlinkKafkaConsumer<>("voucher-redeemed-topic", ConfluentRegistryAvroDeserializationSchema.forSpecific(VoucherRedeemedSchema.class, "http://34.128.127.171:8081"), kafkaConsumerProps))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5)));
        voucher.print();
        DataStream<VoucherMerchantEnrichment> voucherEnrichment = voucher.process(new VoucherProcess());
        voucherEnrichment.print();
        voucherEnrichment.addSink(new SinkInflux());
        env.execute("Voucher Job");
    }

    public static class VoucherProcess extends ProcessFunction<VoucherRedeemedSchema, VoucherMerchantEnrichment>{
        private static final String CASSANDRA_KEYSPACE = "mahoni";
        private static final String CASSANDRA_HOST = "34.101.176.46";
        private static final int CASSANDRA_PORT = 9042;
        String sex;
        int sex_decode;
        int age;
        String nameVoucher;
        String typeVoucher;

        String merchantId;
        String nameMerchant;
        VoucherMerchantEnrichment result;

        private transient CqlSession session;
        @Override
        public void open(Configuration config) {

            session = CqlSession.builder()
                    .addContactPoint(InetSocketAddress.createUnresolved(CASSANDRA_HOST, CASSANDRA_PORT))
                    .withLocalDatacenter("asia-southeast2")
                    .withKeyspace(CASSANDRA_KEYSPACE)
                    .build();

        }
        @Override
        public void processElement(VoucherRedeemedSchema voucherRedeemedSchema,
                                   Context context,
                                   Collector<VoucherMerchantEnrichment> out) throws Exception {
            ResultSet userDetail = session.execute("SELECT * FROM users WHERE id=" + voucherRedeemedSchema.getUserId());
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
            ResultSet voucherDetail = session.execute("SELECT * FROM vouchers WHERE id=" + voucherRedeemedSchema.getVoucherId());
            for(Row rowVoucher: voucherDetail){
                nameVoucher = rowVoucher.getString("name");
                typeVoucher = rowVoucher.getString("type");
                merchantId = rowVoucher.getString("merchant_id");
            }

            ResultSet merchantDetail = session.execute("SELECT * FROM merchants WHERE id=" + merchantId);
            for(Row rowMerchant: merchantDetail){
                nameMerchant = rowMerchant.getString("name");
            }

            result = new VoucherMerchantEnrichment(
                    voucherRedeemedSchema.getEventId(),
                    voucherRedeemedSchema.getTimestamp(),
                    voucherRedeemedSchema.getVoucherId(),
                    voucherRedeemedSchema.getUserId(),
                    sex,
                    age,
                    nameVoucher,
                    typeVoucher,
                    nameMerchant
            );
            out.collect(result);

        }
    }
}