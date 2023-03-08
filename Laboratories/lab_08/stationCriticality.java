package it.polito.bigdata.spark.example;

import java.io.Serializable;

public class stationCriticality implements Serializable{
    private Integer station;
    private String dayofweek;
    private Integer hour;
    private Double criticality;

    public Integer getStation() {
        return this.station;
    }

    public void setStation(Integer station) {
        this.station = station;
    }

    public String getDayofweek() {
        return this.dayofweek;
    }

    public void setDayofweek(String dayofweek) {
        this.dayofweek = dayofweek;
    }

    public Integer getHour() {
        return this.hour;
    }

    public void setHour(Integer hour) {
        this.hour = hour;
    }

    public Double getCriticality() {
        return this.criticality;
    }

    public void setCriticality(Double criticality) {
        this.criticality = criticality;
    }
}
