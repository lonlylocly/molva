package ru.spb.molva.align;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by lonlylocly on 4/16/15.
 */
public class Pair {
    private Source s1;
    private Source s2;
    private int count;

    public double getDistance() {
        if (s1.getWord().getCount() == 0) {
            return Double.NaN;
        }
        double totCnt = s1.getWord().getCount();
        double resCnt = count / totCnt;
        double res = - Math.log(resCnt);

        return res;
    }

    public Source getS1() {
        return s1;
    }

    public void setS1(Source s1) {
        this.s1 = s1;
    }

    public Source getS2() {
        return s2;
    }

    public void setS2(Source s2) {
        this.s2 = s2;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Pair pair = (Pair) o;

        if (s1 != null ? !s1.equals(pair.s1) : pair.s1 != null) return false;
        return !(s2 != null ? !s2.equals(pair.s2) : pair.s2 != null);

    }

    @Override
    public int hashCode() {
        int result = s1 != null ? s1.hashCode() : 0;
        result = 31 * result + (s2 != null ? s2.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "Pair{" +
                "s1=" + s1 +
                ", s2=" + s2 +
                ", count=" + count +
                '}';
    }
}
