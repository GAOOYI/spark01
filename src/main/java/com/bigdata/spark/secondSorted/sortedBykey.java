package com.bigdata.spark.secondSorted;

import scala.math.Ordered;

import java.io.Serializable;

public class sortedBykey implements Ordered<sortedBykey>, Serializable {

    //定义需要排序的数据
    private int first;
    private int second;

    public sortedBykey(int first, int second) {
        this.first = first;
        this.second = second;
    }

    @Override
    public int compare(sortedBykey that) {
        if (this.first - that.getFirst() != 0){
            return this.first - that.first;
        }else {
            return this.second - that.getSecond();
        }
    }

    @Override
    public boolean $less(sortedBykey that) {
        if (this.first < that.getFirst()){
            return true;
        }else if (this.first == that.getFirst() && this.second < that.getSecond()){
            return true;
        }
        return false;
    }

    @Override
    public boolean $greater(sortedBykey that) {
        if (this.first > that.getFirst()){
            return true;
        }else if (this.first == that.getFirst() && this.second > that.getSecond()){
            return true;
        }
        return false;
    }

    @Override
    public boolean $less$eq(sortedBykey that) {
        if (this.$less(that)){
            return true;
        }else if (this.first == that.getFirst() && this.second == that.getSecond()){
            return true;
        }
        return false;
    }

    @Override
    public boolean $greater$eq(sortedBykey that) {
        if (this.$greater(that)){
            return true;
        }else if (this.first == that.getFirst() && this.second == that.getSecond()){
            return true;
        }
        return false;
    }

    @Override
    public int compareTo(sortedBykey that) {
        if (this.first - that.getFirst() != 0){
            return this.first - that.first;
        }else {
            return this.second - that.getSecond();
        }
    }


    //手动实现getter setter，hashcode，equals 方法


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        sortedBykey that = (sortedBykey) o;

        if (first != that.first) return false;
        return second == that.second;
    }

    @Override
    public int hashCode() {
        int result = first;
        result = 31 * result + second;
        return result;
    }

    public int getFirst() {
        return first;
    }

    public void setFirst(int first) {
        this.first = first;
    }

    public int getSecond() {
        return second;
    }

    public void setSecond(int second) {
        this.second = second;
    }
}
