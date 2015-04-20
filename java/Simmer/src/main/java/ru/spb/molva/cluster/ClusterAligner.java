package ru.spb.molva.cluster;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import ru.spb.molva.Aligner;
import ru.spb.molva.align.*;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

import static java.lang.String.format;

/**
 * Created by lonlylocly on 4/20/15.
 */
public class ClusterAligner {
    private static Logger log = LogManager.getLogger(ClusterAligner.class);

    private Aligner aligner;
    private Cluster cluster;
    private Cluster alignedCluster = new Cluster();

    public ClusterAligner(Cluster cluster) {
        
    }

    public ClusterAligner(Aligner aligner, Cluster cluster) {
        this.aligner = aligner;
        this.cluster = cluster;
        log.info(format("Cluster size: %d; best trend: %f", getDepth(), cluster.bestTrend()));

        this.alignedCluster.setUnaligned(cluster);
        this.alignedCluster.setMembers(new ArrayList<Member>());
    }

    public static String getStringMd5(String s) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            md.update(s.getBytes());
            byte[] digest = md.digest();
            StringBuffer sb = new StringBuffer();
            for (byte b : digest) {
                sb.append(String.format("%02x", b & 0xff));
            }

            return sb.toString();
        }catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
            return  s;
        }
    }

    public Cluster align() {
        if (getDepth() == 1) {
            alignSingleMember();
        } else {
            alignNonSingleMember();
        }

        if (alignedCluster != null) {
            List<String> memberIds = new ArrayList<>();
            List<String> memberTexts = new ArrayList<>();
            for (Member m : alignedCluster.getMembers()) {
                memberIds.add("" + m.getId());
                memberTexts.add(m.getText());
            }

            alignedCluster.setMembers_md5(getStringMd5(join(memberIds, ",")));
            alignedCluster.setGen_title(join(memberTexts, " "));
            alignedCluster.setMembers_len(alignedCluster.getMembers().size());

            log.info(format("Aligned cluster title: '%s'", alignedCluster.getGen_title()));
        }

        return alignedCluster;
    }

    private void alignSingleMember() {
        Member m = new Member();
        Word w = new Word();
        w.setWordMd5(this.cluster.getMembers().get(0).getId());
        m.setId(w.getWordMd5());
        m.setText(aligner.getSources().get(w).getText());
        m.setStem_text(aligner.getSources().get(w).getWord().getText());
        m.setTrend(aligner.getWordToMember().get(w).getTrend());

        alignedCluster.getMembers().add(m);
    }

    private void alignNonSingleMember() {
        Graph g = new Graph();
        g.build(cluster.getClusterPairs(aligner.getPairs()), getDepth());
        g.dijkstra();
        List<Vertex> best = g.getBestTerminalPointPath();

        if (best == null) {
            alignedCluster = null;
            log.warn("Failed to align cluster");
            return;
        }

        List<Source> chain = new ArrayList<>();
        for (Vertex v : best) {
            chain.add(v.getPair().getS1());
        }
        chain.add(best.get(best.size() - 1).getPair().getS2());

        for (Source source : chain) {
            Member m = new Member();
            m.setId(source.getWord().getWordMd5());
            m.setText(source.getText());
            m.setStem_text(source.getWord().getText());
            m.setTrend(aligner.getWordToMember().get(source.getWord()).getTrend());
            m.setPost_cnt(aligner.getWordToMember().get(source.getWord()).getPost_cnt());

            alignedCluster.getMembers().add(m);
        }
    }

    private int getDepth() {
        return cluster.getMembers().size();
    }

    public static String join(List<String> l, String sep){
        StringBuilder builder = new StringBuilder();
        for(int i=0; i<l.size(); i++) {
            builder.append(l.get(i));
            if (i != l.size()-1) {
                builder.append(sep);
            }
        }
        return builder.toString();
    }

}
