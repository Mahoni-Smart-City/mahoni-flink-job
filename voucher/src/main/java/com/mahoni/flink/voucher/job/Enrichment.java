package com.mahoni.flink.voucher.job;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.mahoni.schema.VoucherMerchantEnrichment;
import com.mahoni.schema.VoucherRedeemedSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.net.InetSocketAddress;
import java.time.LocalDate;
import java.util.Collections;

public class Enrichment extends RichAsyncFunction<VoucherRedeemedSchema, VoucherMerchantEnrichment> {

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
                .withLocalDatacenter("asia-southeast2")//asia-southeast2
                .withKeyspace(CASSANDRA_KEYSPACE)
                .build();


    }
    @Override
    public void asyncInvoke(VoucherRedeemedSchema voucherRedeemedSchema, ResultFuture<VoucherMerchantEnrichment> resultFuture) throws Exception {

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
        System.out.println(voucherRedeemedSchema.getVoucherId());
        ResultSet voucherDetail = session.execute("SELECT * FROM vouchers WHERE id=" + voucherRedeemedSchema.getVoucherId());
        for(Row rowVoucher: voucherDetail){
            nameVoucher = rowVoucher.getString("name");
            typeVoucher = rowVoucher.getString("type");
            merchantId = rowVoucher.getString("merchant_id");
        }
        System.out.println(nameVoucher);
        System.out.println(typeVoucher);
        System.out.println(merchantId);

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
        resultFuture.complete(Collections.singleton(result));
    }
}
