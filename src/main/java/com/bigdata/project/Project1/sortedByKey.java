package com.bigdata.project.Project1;

import scala.math.Ordered;

import java.io.Serializable;

public class sortedByKey implements Serializable,Ordered<sortedByKey> {

    private long upFlow;
    private long downFlow;
    private long timeStamp;

    public sortedByKey(long upFlow, long downFlow, long timeStamp) {
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.timeStamp = timeStamp;
    }


    @Override
    public int compare(sortedByKey that) {
        if (this.upFlow - that.upFlow != 0){
            return (int) (this.upFlow - that.upFlow);
        }else if (this.downFlow - that.downFlow != 0){
            return (int) (this.downFlow - that.downFlow);
        }else if (this.timeStamp - that.timeStamp != 0){
            return (int) (this.timeStamp - that.timeStamp);
        }
        return 0;
    }

    @Override
    public boolean $less(sortedByKey that) {
        if (this.upFlow < that.upFlow){
            return true;
        }else if (this.upFlow == that.upFlow && this.downFlow < that.downFlow){
            return true;
        }else if (this.upFlow == that.upFlow && this.downFlow == that.downFlow && this.timeStamp < that.timeStamp){
            return true;
        }
        return false;
    }

    @Override
    public boolean $greater(sortedByKey that) {
        if (this.upFlow > that.upFlow){
            return true;
        }else if (this.upFlow == that.upFlow && this.downFlow > that.downFlow){
            return true;
        }else if (this.upFlow == that.upFlow && this.downFlow == that.downFlow && this.timeStamp > that.timeStamp){
            return true;
        }
        return false;
    }

    @Override
    public boolean $less$eq(sortedByKey that) {
        if (this.upFlow <= that.upFlow){
            return true;
        }else if (this.upFlow > that.upFlow && this.downFlow <= that.downFlow){
            return true;
        }else if (this.upFlow > that.upFlow && this.downFlow > that.downFlow && this.timeStamp <= that.timeStamp){
            return true;
        }
        return false;
    }

    @Override
    public boolean $greater$eq(sortedByKey that) {
        if (this.upFlow >= that.upFlow){
            return true;
        }else if (this.upFlow < that.upFlow && this.downFlow >= that.downFlow){
            return true;
        }else if (this.upFlow < that.upFlow && this.downFlow < that.downFlow && this.timeStamp >= that.timeStamp){
            return true;
        }
        return false;
    }

    @Override
    public int compareTo(sortedByKey that) {
        if (this.upFlow - that.upFlow != 0){
            return (int) (this.upFlow - that.upFlow);
        }else if (this.downFlow - that.downFlow != 0){
            return (int) (this.downFlow - that.downFlow);
        }else if (this.timeStamp - that.timeStamp != 0){
            return (int) (this.timeStamp - that.timeStamp);
        }
        return 0;
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        sortedByKey that = (sortedByKey) o;

        if (upFlow != that.upFlow) return false;
        if (downFlow != that.downFlow) return false;
        return timeStamp == that.timeStamp;
    }

    @Override
    public int hashCode() {
        int result = (int) (upFlow ^ (upFlow >>> 32));
        result = 31 * result + (int) (downFlow ^ (downFlow >>> 32));
        result = 31 * result + (int) (timeStamp ^ (timeStamp >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "sortedByKey{" +
                "upFlow=" + upFlow +
                ", downFlow=" + downFlow +
                ", timeStamp=" + timeStamp +
                '}';
    }
}
