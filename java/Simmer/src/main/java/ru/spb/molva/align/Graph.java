package ru.spb.molva.align;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by lonlylocly on 4/16/15.
 */
public class Graph {

    private List<List<Vertex>> matrix = new ArrayList<List<Vertex>>();
    private int linkCount = 0;

    public void build(List<Pair> pairs, int depth) {
        final Vertex root = new Vertex(null, 0, 0);
        final Vertex end = new Vertex(null, 0, 0);

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

    public boolean isEligible(Vertex v1, Vertex v2) {
        // root
        if (v1.getPair() == null) {
            return true;
        }
        // end
        if (v2.getPair() == null ) {
            return true;
        }
        // if tail of v1 equals head of v2
        if (v1.getPair().getS2().equals(v2.getPair().getS1())) {
            return true;
        }
        return false;
    }
}
