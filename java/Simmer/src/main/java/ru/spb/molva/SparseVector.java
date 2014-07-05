package ru.spb.molva;

import org.apache.commons.math3.linear.RealVector;

import java.util.List;
import java.util.Map;

/**
 * Created by lonlylocly on 06.07.14.
 */
public class SparseVector {
    Map<Long,Double> vector;
    List<Long> keys;

    public SparseVector(Map<Long, Double> vector, List<Long> keys) {
        this.vector = vector;
        this.keys = keys;
    }

    RealVector getRealVector() {
        return Simmer.getRealVector(vector, keys);
    }

    public Map<Long, Double> getVector() {
        return vector;
    }

    public void setVector(Map<Long, Double> vector) {
        this.vector = vector;
    }

    public List<Long> getKeys() {
        return keys;
    }

    public void setKeys(List<Long> keys) {
        this.keys = keys;
    }
}
