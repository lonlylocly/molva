package ru.spb.molva.align;

import java.util.Comparator;

/**
 * Created by lonlylocly on 4/19/15.
 */
public class VertexComparator implements Comparator<Vertex> {
    public int compare(Vertex o1, Vertex o2) {
        return Double.valueOf(o1.getPair().getDistance()).compareTo(Double.valueOf(o2.getPair().getDistance()));
    }
}
