package ru.spb.molva;

/**
 * Created by lonlylocly on 08.06.14.
 */
public class SimEntry {
    private Long p1;
    private Long p2;
    private double sim;

    public SimEntry(Long p1, Long p2, double sim) {
        this.p1 = p1;
        this.p2 = p2;
        this.sim = sim;
    }

    public String toCsv() {
        return String.format("%d;%d;%f", p1, p2, sim);
    }

    public Long getP1() {
        return p1;
    }

    public void setP1(Long p1) {
        this.p1 = p1;
    }

    public Long getP2() {
        return p2;
    }

    public void setP2(Long p2) {
        this.p2 = p2;
    }

    public double getSim() {
        return sim;
    }

    public void setSim(double sim) {
        this.sim = sim;
    }
}
