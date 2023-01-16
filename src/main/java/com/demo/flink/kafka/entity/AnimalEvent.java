package com.demo.flink.kafka.entity;

/**
 * Copyright：dp.com
 * Author: SongXiaoGuang
 * Date: 2023/1/14.
 * Description:
 */
public class AnimalEvent {
    private Integer id;
    private String clientTimeStr;
    private Long clientTime;
    private String animal;

    public AnimalEvent(Integer id, String clientTimeStr, Long clientTime, String animal) {
        this.id = id;
        this.clientTimeStr = clientTimeStr;
        this.clientTime = clientTime;
        this.animal = animal;
    }

    public Integer getId() {
        return id;
    }

    public AnimalEvent setId(Integer id) {
        this.id = id;
        return this;
    }

    public String getAnimal() {
        return animal;
    }

    public AnimalEvent setAnimal(String animal) {
        this.animal = animal;
        return this;
    }

    public Long getClientTime() {
        return clientTime;
    }

    public AnimalEvent setClientTime(Long clientTime) {
        this.clientTime = clientTime;
        return this;
    }

    public String getClientTimeStr() {
        return clientTimeStr;
    }

    public AnimalEvent setClientTimeStr(String clientTimeStr) {
        this.clientTimeStr = clientTimeStr;
        return this;
    }

    @Override
    public String toString() {
        return "AnimalEvent{" +
                "id=" + id +
                ", animal='" + animal + '\'' +
                ", clientTimeStr='" + clientTimeStr + '\'' +
                ", clientTime=" + clientTime +
                '}';
    }
}
