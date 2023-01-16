package com.demo.flink.kafka.entity;

/**
 * Copyright：dp.com
 * Author: SongXiaoGuang
 * Date: 2023/1/3.
 * Description:
 */
public class KafkaEvent {
    private String distinct_id;
    private String event;
    private Long time;

    public String getDistinct_id() {
        return distinct_id;
    }

    public KafkaEvent setDistinct_id(String distinct_id) {
        this.distinct_id = distinct_id;
        return this;
    }

    public String getEvent() {
        return event;
    }

    public KafkaEvent setEvent(String event) {
        this.event = event;
        return this;
    }

    public Long getTime() {
        return time;
    }

    public KafkaEvent setTime(Long time) {
        this.time = time;
        return this;
    }

    @Override
    public String toString() {
        return "KafkaEvent{" +
                "distinct_id='" + distinct_id + '\'' +
                ", event='" + event + '\'' +
                ", time=" + time +
                '}';
    }
}
