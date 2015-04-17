package ru.spb.molva.align;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by lonlylocly on 4/16/15.
 */
public class Vertex {
    public static final int MAX_DIST = Integer.MAX_VALUE;

    private Pair pair;
    private Vertex parent;
    int depth;
    int distance = MAX_DIST;

    private List<Vertex> next = new ArrayList<Vertex>();

    public Vertex(Pair pair, int depth) {
        this.pair = pair;
        this.depth = depth;
    }

    public Vertex(Pair pair, int depth, int distance) {
        this.pair = pair;
        this.depth = depth;
        this.distance = distance;
    }

    public Pair getPair() {
        return pair;
    }

    public void setPair(Pair pair) {
        this.pair = pair;
    }

    public Vertex getParent() {
        return parent;
    }

    public void setParent(Vertex parent) {
        this.parent = parent;
    }

    public int getDistance() {
        return distance;
    }

    public void setDistance(int distance) {
        this.distance = distance;
    }

    public List<Vertex> getNext() {
        return next;
    }

    public void setNext(List<Vertex> next) {
        this.next = next;
    }

    public int getDepth() {
        return depth;
    }

    public void setDepth(int depth) {
        this.depth = depth;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Vertex vertex = (Vertex) o;

        if (depth != vertex.depth) return false;
        return !(pair != null ? !pair.equals(vertex.pair) : vertex.pair != null);

    }

    @Override
    public int hashCode() {
        int result = pair != null ? pair.hashCode() : 0;
        result = 31 * result + depth;
        return result;
    }
}
