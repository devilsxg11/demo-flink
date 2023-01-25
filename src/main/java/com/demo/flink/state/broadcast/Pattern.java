package com.demo.flink.state.broadcast;

/**
 * Copyright：gome.com.cn
 * Author: SongXiaoGuang
 * Date: 2023/1/24.
 * Description:
 */
public class Pattern {
    private String prevAction;
    private String nextAction;

    public Pattern(String previousAction, String nextAction) {
        this.prevAction = previousAction;
        this.nextAction = nextAction;
    }

    public String getPrevAction() {
        return prevAction;
    }

    public Pattern setPrevAction(String prevAction) {
        this.prevAction = prevAction;
        return this;
    }

    public String getNextAction() {
        return nextAction;
    }

    public Pattern setNextAction(String nextAction) {
        this.nextAction = nextAction;
        return this;
    }

    @Override
    public String toString() {
        return "Pattern{" +
                "previousAction='" + prevAction + '\'' +
                ", nextAction='" + nextAction + '\'' +
                '}';
    }
}
