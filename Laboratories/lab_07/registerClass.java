package it.polito.bigdata.spark.example;

import java.io.Serializable;

public class registerClass implements Serializable {
    private Integer station;
    private String date;
    private String hour;
    private String timeslot;
    private Integer used_slots;
    private Integer free_slots;

    public registerClass(){};

    public registerClass(Integer station, String date, String hour, Integer used_slots, Integer free_slots){
        this.station = station;
        this.date = date;
        this.hour = hour;
        this.timeslot = DateTool.DayOfTheWeek(date) + "_" + hour.split(":")[0];
        this.used_slots = used_slots;
        this.free_slots = free_slots;
    };

    public Integer getStation(){
        return this.station;
    }

    public void setStation(Integer station){
        this.station = station;
    }

    public String getDate(){
        return this.date;
    }

    public void setDate(String date){
        this.date = date;
    }

    public String getHour(){
        return this.hour;
    }

    public void setHour(String hour){
        this.hour = hour;
    }

   public String getTimeslot(){
        return this.timeslot;
    }

    public Integer getUsedSlots(){
        return this.used_slots;
    }

    public void setUsedSlots(Integer used_slots){
        this.used_slots = used_slots;
    }

    public Integer getFreeSlots(){
        return this.free_slots;
    }

    public void setFreeSlots(Integer free_slots){
        this.free_slots = free_slots;
    }

    @Override
    public String toString(){
        return Integer.toString(this.station) + "\t"
                + date + " " + hour + "\t"
                + Integer.toString(this.used_slots) + "\t"
                + Integer.toString(this.free_slots);
    }
}
