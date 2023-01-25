package com.demo.flink.state.broadcast;

/**
 * Copyright：gome.com.cn
 * Author: SongXiaoGuang
 * Date: 2023/1/24.
 * Description:
 */
public class Action {
    public Long uid;
    public String action;

    public Action(Long uid, String action) {
        this.uid = uid;
        this.action = action;
    }

    public Long getUid() {
        return uid;
    }

    public Action setUid(Long uid) {
        this.uid = uid;
        return this;
    }

    public String getAction() {
        return action;
    }

    public Action setAction(String action) {
        this.action = action;
        return this;
    }

    @Override
    public String toString() {
        return "Action{" +
                "uid=" + uid +
                ", action='" + action + '\'' +
                '}';
    }
}
