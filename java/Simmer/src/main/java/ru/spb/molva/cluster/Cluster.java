package ru.spb.molva.cluster;

import ru.spb.molva.align.Pair;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by lonlylocly on 4/20/15.
 */
public class Cluster {
    private List<Member> members;
    private int members_len;
    private String members_md5;
    private long centroid_md5;
    private String centroid_text;
    private Cluster unaligned;
    private String gen_title;

    public List<Pair> getClusterPairs(List<Pair> pairs) {
        List<Long> members = new ArrayList<>();
        for (Member m : getMembers()) {
            members.add(m.getId());
        }
        List<Pair> clPairs = new ArrayList<>();

        for (Pair pair : pairs) {
            long w1 = pair.getS1().getWord().getWordMd5();
            long w2 = pair.getS2().getWord().getWordMd5();
            if (w1 != w2 && members.contains(w1) &&
                    members.contains(w2)) {
                clPairs.add(pair);
            }
        }
        return clPairs;
    }

    public double bestTrend() {
        List<Double> trends = new ArrayList<>();
        for (Member m : getMembers()) {
            trends.add(m.getTrend());
        }
        Collections.sort(trends);
        return trends.get(trends.size()-1);
    }

    public List<Member> getMembers() {
        return members;
    }

    public void setMembers(List<Member> members) {
        this.members = members;
    }

    public String getMembers_md5() {
        return members_md5;
    }

    public void setMembers_md5(String members_md5) {
        this.members_md5 = members_md5;
    }

    public long getCentroid_md5() {
        return centroid_md5;
    }

    public void setCentroid_md5(long centroid_md5) {
        this.centroid_md5 = centroid_md5;
    }

    public String getCentroid_text() {
        return centroid_text;
    }

    public void setCentroid_text(String centroid_text) {
        this.centroid_text = centroid_text;
    }

    public int getMembers_len() {
        return members_len;
    }

    public void setMembers_len(int members_len) {
        this.members_len = members_len;
    }

    public Cluster getUnaligned() {
        return unaligned;
    }

    public void setUnaligned(Cluster unaligned) {
        this.unaligned = unaligned;
    }

    public String getGen_title() {
        return gen_title;
    }

    public void setGen_title(String gen_title) {
        this.gen_title = gen_title;
    }
}
