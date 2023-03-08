package it.polito.bigdata.spark.example;

public class stationCoordinates {
    private Integer station;
    private String dayofweek;
    private Integer hour;
    private Double criticality;
    private Double longitude;
    private Double latitude;

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

    public Double getLongitude() {
        return this.longitude;
    }

    public void setLongitude(Double longitude) {
        this.longitude = longitude;
    }

    public Double getLatitude() {
        return this.latitude;
    }

    public void setLatitude(Double latitude) {
        this.latitude = latitude;
    }
}
