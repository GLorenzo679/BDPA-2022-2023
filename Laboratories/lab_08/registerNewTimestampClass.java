package it.polito.bigdata.spark.example;

public class registerNewTimestampClass extends registerClass{
    private String dayofweek;
    private Integer hour;
    private Integer status;

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

    public Integer getStatus() {
        return this.status;
    }

    public void setStatus(Integer status) {
        this.status = status;
    }
}
