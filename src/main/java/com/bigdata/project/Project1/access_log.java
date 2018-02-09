package com.bigdata.project.Project1;

import java.io.Serializable;

public class access_log implements Serializable {
    private long upFlow;
    private long downFlow;
    private long timeStamp;

    public access_log(){}

    public access_log(long upFlow, long downFlow, long timeStamp) {
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.timeStamp = timeStamp;
    }

    @Override
    public String toString() {
        return "access_log{" +
                "upFlow=" + upFlow +
                ", downFlow=" + downFlow +
                ", timeStamp='" + timeStamp + '\'' +
                '}';
    }

    public long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(long downFlow) {
        this.downFlow = downFlow;
    }

    public long getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(long timeStamp) {
        this.timeStamp = timeStamp;
    }
}
