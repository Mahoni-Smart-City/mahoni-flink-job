package com.mahoni.cassandra;

import com.datastax.driver.mapping.annotations.Table;
import com.datastax.driver.mapping.annotations.Column;

@Table(keyspace = "air_quality", name = "air_sensor")
public class AirSensor {
    @Column(name = "id")
    private Long id;

    @Column(name = "name_location")
    private String nameLocation;

    @Column(name = "id_location")
    private Long idLocation;

    public Long getId(){
        return id;
    }

    public void setId(Long id){
        this.id = id;
    }

    public String getNameLocation(){
        return nameLocation;
    }

    public void setNameLocation(String nameLocation){
        this.nameLocation = nameLocation;
    }

    public Long getIdLocation(){
        return idLocation;
    }

    public void setIdLocation(Long idLocation){
        this.idLocation = idLocation;
    }
}
