package com.mahoni.flink.trip.schema;

public class TripEnrichment {
    private long timestamp;
    private String tripId;
    private String userId;
    private String scan;
    private String status;
    private String scanId;
    private boolean sex;
    private int age;
    private String nameLocation;
    private String type;

    public TripEnrichment(long timestamp, String tripId, String userId, String scan, String status, String scanId, boolean sex, int age, String nameLocation, String type) {
        this.timestamp = timestamp;
        this.tripId = tripId;
        this.userId = userId;
        this.scan = scan;
        this.status = status;
        this.scanId = scanId;
        this.sex = sex;
        this.age = age;
        this.nameLocation = nameLocation;
        this.type = type;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String getTripId() {
        return tripId;
    }

    public void setTripId(String tripId) {
        this.tripId = tripId;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getScanId() {
        return scanId;
    }

    public void setScanId(String scanId) {
        this.scanId = scanId;
    }

    public String getScan() {
        return scan;
    }

    public void setScan(String scan) {
        this.scan = scan;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
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

    public String getNameLocation() {
        return nameLocation;
    }

    public void setNameLocation(String nameLocation) {
        this.nameLocation = nameLocation;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }


}
