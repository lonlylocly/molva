package ru.spb.molva.cluster;

/**
 * Created by lonlylocly on 4/20/15.
 */
public class Member {
    private long id;
    private String text;
    private double trend;
    private int post_cnt;
    private String stem_text;

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    public double getTrend() {
        return trend;
    }

    public void setTrend(double trend) {
        this.trend = trend;
    }

    public int getPost_cnt() {
        return post_cnt;
    }

    public void setPost_cnt(int post_cnt) {
        this.post_cnt = post_cnt;
    }

    public String getStem_text() {
        return stem_text;
    }

    public void setStem_text(String stem_text) {
        this.stem_text = stem_text;
    }
}
