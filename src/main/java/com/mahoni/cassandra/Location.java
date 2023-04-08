package com.mahoni.cassandra;
import com.datastax.driver.mapping.annotations.Table;
import com.datastax.driver.mapping.annotations.Column;

@Table(keyspace = "air_quality", name = "location")
public class Location {
    @Column(name = "id")
    private Long id;
    @Column(name = "district")
    private String district;
    @Column(name = "subDistrict")
    private String subDistrict;
    @Column(name = "village")
    private String village;
    @Column(name = "longtitude")
    private String longtitude;
    @Column(name = "latitude")
    private String latitude;

    public Long getId(){
        return id;
    }
    public void setId(Long id){
        this.id = id;
    }

    public String getDistrict(){
        return district;
    }
    public void setDistrict(String district){
        this.district = district;
    }

    public String getSubdistrict(){
        return subDistrict;
    }
    public void setSubDistrict(String subDistrict){
        this.subDistrict = subDistrict;
    }

    public String getVillage(){
        return village;
    }
    public void setVillage(String village){
        this.village = village;
    }

    public String getLongtitude(){
        return longtitude;
    }
    public void setLongtitude(String longtitude){
        this.longtitude = longtitude;
    }

    public String getLatitude(){
        return latitude;
    }
    public void setLatitude(String latitude){
        this.latitude = latitude;
    }
}
