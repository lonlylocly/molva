package ru.spb.molva.align;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.PriorityQueue;

/**
 * Created by lonlylocly on 4/16/15.
 */
public class Vertex {
    public static final double MAX_DIST = Double.MAX_VALUE;
    public static final VertexComparator cmp = new VertexComparator();

    private Pair pair;
    private Vertex parent;
    int depth;
    double distance = MAX_DIST;

    private List<Vertex> next = new ArrayList<Vertex>();

    private PriorityQueue<Vertex> nextQueue = new PriorityQueue<Vertex>(cmp);

    public Vertex(Pair pair, int depth) {
        this.pair = pair;
        this.depth = depth;
    }

    public Vertex(Pair pair, int depth, double distance) {
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

    public double getDistance() {
        return distance;
    }

    public void setDistance(double distance) {
        this.distance = distance;
    }

    public Collection<Vertex> getNext() {
        return next;
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

    @Override
    public String toString() {
        return "Vertex{" +
                "distance=" + distance +
                ", pair=" + pair +
                ", parent=" + parent +
                ", depth=" + depth +
                '}';
    }
}
