package it.polito.bigdata.spark.example;

import java.io.Serializable;
import java.sql.Timestamp;

public class registerClass implements Serializable {
    private Integer station;
    private Timestamp timestamp;
    private Integer used_slots;
    private Integer free_slots;

    public registerClass() {}

    public Integer getStation(){
        return this.station;
    }

    public void setStation(Integer station){
        this.station = station;
    }

    public Timestamp getTimestamp(){
        return this.timestamp;
    }

    public void setTimestamp(Timestamp timestamp){
        this.timestamp = timestamp;
    }

    public Integer getUsed_Slots(){
        return this.used_slots;
    }

    public void setUsed_Slots(Integer used_slots){
        this.used_slots = used_slots;
    }

    public Integer getFree_Slots(){
        return this.free_slots;
    }

    public void setFree_Slots(Integer free_slots){
        this.free_slots = free_slots;
    }
}
