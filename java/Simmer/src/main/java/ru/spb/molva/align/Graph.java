package ru.spb.molva.align;

import java.util.*;

/**
 * Created by lonlylocly on 4/16/15.
 */
public class Graph {


    final Pair mockPair = new Pair() {
        @Override
        public double getDistance() {
            return 0;
        }
    };

    final Vertex root = new Vertex(mockPair, 0, 0);
    final Vertex end = new Vertex(mockPair, 0, Double.MAX_VALUE);

    private Set<Vertex> terminalPoints = new HashSet<Vertex>();

    private List<List<Vertex>> matrix = new ArrayList<List<Vertex>>();
    private int linkCount = 0;

    public void build(List<Pair> pairs, int depth) {
        end.setDepth(depth);

        final List<Vertex> colRoot = new ArrayList<Vertex>();
        colRoot.add(root);
        matrix.add(colRoot);
        for(int i=1; i<=depth; i++) {
            List<Vertex> col = new ArrayList<Vertex>();
            for(Pair p : pairs) {
                Vertex vertex = new Vertex(p, i);
                col.add(vertex);
            }
            matrix.add(col);
        }

        final List<Vertex> colEnd = new ArrayList<Vertex>();
        colEnd.add(end);
        matrix.add(colEnd);

        for(int i=0; i<=depth; i++) {
            List<Vertex> col1 = matrix.get(i);
            List<Vertex> col2 = matrix.get(i+1);
            for(int j = 0; j<col1.size(); j++) {
                for(int k = 0; k<col2.size(); k++) {
                    if (isEligible(col1.get(j), col2.get(k))) {
                        col1.get(j).getNext().add(col2.get(k));
                        linkCount++;
                    }
                }
            }
        }

    }

    public void dijkstra() {
        Set<Vertex> unvisited = new HashSet<Vertex>();
        for(int i=0; i<matrix.size(); i++) {
            for(Vertex v : matrix.get(i)) {
                unvisited.add(v);
            }
        }
        Vertex cur = matrix.get(0).get(0);
        while(!unvisited.isEmpty()) {
            for (Vertex v : cur.getNext()) {
                if (!isPathWalkable(cur, v)) {
                    continue;
                }
                double actDistance = v.getDistance();
                double newDistance = cur.getDistance() + v.getPair().getDistance();
                if (newDistance < actDistance) {
                    v.setDistance(newDistance);
                    v.setParent(cur);
                    terminalPoints.add(v);
                    if (terminalPoints.contains(cur)) {
                        terminalPoints.remove(cur);
                    }
                }
            }
            unvisited.remove(cur);
            cur = getClosestVertex(unvisited);
            if (cur == null) {
                break;
            }
        }
    }

    public Vertex getClosestVertex(Set<Vertex> vs) {
        Vertex closest = null;
        double closestDistance = Double.MAX_VALUE;
        for (Vertex v : vs) {
            if (v.getDistance() < closestDistance) {
                closest = v;
                closestDistance = v.getDistance();
            }
        }
        return closest;
    }

    /**
     * Return false if path back to root contains same Word1 as next node does.
     * (Avoid word repetitions.)
     * @param cur
     * @param next
     * @return
     */
    private boolean isPathWalkable(Vertex cur, Vertex next) {
        if (next == end) {
            return true;
        }
        Word w1 = next.getPair().getS1().getWord();
        while(cur != root) {
            Word w2 = cur.getPair().getS1().getWord();
            if (w1.equals(w2)) {
                return false;
            }
            cur = cur.getParent();
        }
        return true;
    }

    public boolean isEligible(Vertex v1, Vertex v2) {
        // root
        if (v1.getPair() == mockPair) {
            return true;
        }
        // end
        if (v2.getPair() == mockPair ) {
            return true;
        }
        // if tail of v1 equals head of v2
        if (v1.getPair().getS2().equals(v2.getPair().getS1())) {
            return true;
        }
        return false;
    }

    public List<Vertex> getBestTerminalPointPath() {
        List<Vertex> bestPath = null;
        int bestSize = 0;
        double bestDistance = Double.MAX_VALUE;
        for (Vertex v : getTerminalPoints()) {
            List<Vertex> l = new ArrayList<Vertex>();
            Vertex cur = v;
            while(cur.getParent() != null ) {
                if (cur.getPair().getS1() == null) {
                    cur = cur.getParent();
                    continue;
                }
                l.add(0, cur);
                cur = cur.getParent();
            }
            if (v.getDistance() < bestDistance && l.size() >= bestSize) {
                bestSize = l.size();
                bestPath = l;
                bestDistance = v.getDistance();
            }
        }
        return  bestPath;
    }


    public List<List<Vertex>> getMatrix() {
        return matrix;
    }

    public int getLinkCount() {
        return linkCount;
    }

    public Set<Vertex> getTerminalPoints() {
        return terminalPoints;
    }
}
