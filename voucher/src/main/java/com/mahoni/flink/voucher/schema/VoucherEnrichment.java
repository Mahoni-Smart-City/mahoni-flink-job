package com.mahoni.flink.voucher.schema;

public class VoucherEnrichment {
    private long timestamp;
    private String voucherId;
    private String userId;
    private boolean sex;
    private int age;
    private String nameVoucher;
    private String typeVoucher;

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String getVoucherId() {
        return voucherId;
    }

    public void setVoucherId(String voucherId) {
        this.voucherId = voucherId;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public boolean isSex() {
        return sex;
    }

    public void setSex(boolean sex) {
        this.sex = sex;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public String getNameVoucher() {
        return nameVoucher;
    }

    public void setNameVoucher(String nameVoucher) {
        this.nameVoucher = nameVoucher;
    }

    public String getTypeVoucher() {
        return typeVoucher;
    }

    public void setTypeVoucher(String typeVoucher) {
        this.typeVoucher = typeVoucher;
    }

    public VoucherEnrichment(long timestamp, String voucherId, String userId, boolean sex, int age, String nameVoucher, String typeVoucher) {
        this.timestamp = timestamp;
        this.voucherId = voucherId;
        this.userId = userId;
        this.sex = sex;
        this.age = age;
        this.nameVoucher = nameVoucher;
        this.typeVoucher = typeVoucher;
    }
}
