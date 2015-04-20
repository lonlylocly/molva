import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Test;
import ru.spb.molva.align.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static org.hamcrest.CoreMatchers.*;

/**
 * Created by lonlylocly on 4/17/15.
 */
public class GraphTest {

    @Test
    public void buildTest() {
        Graph g = new Graph();
        List<Pair> pairs = new ArrayList<Pair>();
        Pair p1 = getPair(10, 1, 20, 2);
        Pair p2 = getPair(20, 2, 30, 3);
        pairs.add(p1);
        pairs.add(p2);

        g.build(pairs, 2);
        Assert.assertThat(g.getMatrix().get(1).get(0).getNext().size(), is(1));
        Assert.assertThat(g.getMatrix().get(1).get(0).getPair(), is(p1));
        Assert.assertThat(g.getMatrix().get(1).get(1).getNext().size(), is(0));
        Assert.assertThat(g.getMatrix().get(1).get(1).getPair(), is(p2));
    }

    @Test
    public void distanceTest() {

        Pair p1 = getPair(10, 1, 20, 2, 10, 5);

        Assert.assertThat((int) (p1.getDistance() * 1000), is(693));

        Pair p2 = getPair(10, 1, 20, 2, 100, 5);

        Assert.assertThat((int) (p2.getDistance() * 1000), is(2995));
    }

    @Test
    public void dijkstraTest() {
        Graph g = new Graph();
        List<Pair> pairs = new ArrayList<Pair>();
        Pair p1 = getPair(10, 1, 20, 2, 10, 5);
        Pair p2 = getPair(20, 2, 30, 3, 20, 4);
        Pair p3 = getPair(11, 1, 20, 2, 10, 6);

        pairs.add(p3);
        pairs.add(p2);
        pairs.add(p1);

        g.build(pairs, 2);
        g.dijkstra();

        List<Long> expectedSources = asListLong(11, 20, 20, 30);

        List<Long> actualSources = getActualSources(g);

        Assert.assertThat(actualSources, is(expectedSources) );
    }

    public static List<Long> asListLong(Integer... xs) {
        List<Long> l = new ArrayList<Long>();
        for (Integer i : Arrays.asList(xs)) {
            l.add(new Long(i));
        }

        return l;
    }

    private List<Long> getActualSources(Graph g) {
        System.out.println("");
        List<Long> actualSources = new ArrayList<Long>();

        for (Vertex v : g.getBestTerminalPointPath()) {
            System.out.println(String.format("%d -> %d (%f)", v.getPair().getS1().getSourceMd5(),
                    v.getPair().getS2().getSourceMd5(), v.getDistance()));
            actualSources.add(v.getPair().getS1().getSourceMd5());
            actualSources.add(v.getPair().getS2().getSourceMd5());
        }
        return actualSources;
    }

    @Test
    public void dijkstraTest2() {
        Graph g = new Graph();
        List<Pair> pairs = new ArrayList<Pair>();
        Pair p1 = getPair(10, 1, 20, 2, 10, 5);
        Pair p2 = getPair(20, 2, 30, 3, 20, 4);
        Pair p3 = getPair(11, 1, 20, 2, 10, 3);

        pairs.add(p3);
        pairs.add(p2);
        pairs.add(p1);

        g.build(pairs, 2);
        g.dijkstra();

        List<Long> expectedSources = asListLong(10, 20, 20, 30);

        List<Long> actualSources = getActualSources(g);

        Assert.assertThat(actualSources, is(expectedSources) );
    }

    /**
     * Choose from two incomplete paths
     */
    @Test
    public void dijkstraTest3() {
        Graph g = new Graph();
        List<Pair> pairs = new ArrayList<Pair>();
        Pair p1 = getPair(10, 1, 20, 2, 10, 5);
        Pair p3 = getPair(11, 1, 20, 2, 10, 6);

        pairs.add(p3);
        pairs.add(p1);

        g.build(pairs, 2);
        g.dijkstra();

        List<Long> expectedSources =  asListLong(11, 20);

        List<Long> actualSources = getActualSources(g);

        Assert.assertThat(actualSources, is(expectedSources) );
    }

    /**
     * Choose from two incomplete paths, other order
     * (make sure that path size And distance are considered)
     */
    @Test
    public void dijkstraTest4() {
        Graph g = new Graph();
        List<Pair> pairs = new ArrayList<Pair>();
        Pair p1 = getPair(10, 1, 20, 2, 10, 5);
        Pair p3 = getPair(11, 1, 20, 2, 10, 6);

        pairs.add(p1);
        pairs.add(p3);

        g.build(pairs, 2);
        g.dijkstra();

        List<Long> expectedSources = asListLong(11, 20);

        List<Long> actualSources = getActualSources(g);

        Assert.assertThat(actualSources, is(expectedSources) );
    }

    /**
     * two complete paths, same size, one is discovered later
     */
    @Test
    public void dijkstraTest5() {
        Graph g = new Graph();
        List<Pair> pairs = new ArrayList<Pair>();
        Pair p1 = getPair(10, 1, 20, 2, 10, 5);
        Pair p2 = getPair(20, 2, 30, 3, 20, 4);
        Pair p3 = getPair(11, 1, 21, 2, 10, 6);
        Pair p4 = getPair(21, 2, 30, 3, 20, 4);

        pairs.add(p1);
        pairs.add(p2);
        pairs.add(p3);
        pairs.add(p4);

        g.build(pairs, 2);
        g.dijkstra();

        List<Long> expectedSources =  asListLong(11, 21, 21, 30);

        List<Long> actualSources = getActualSources(g);

        Assert.assertThat(actualSources, is(expectedSources) );
    }

    private Pair getPair(int s1, int w1, int s2, int w2) {

        return getPair(s1, w1, s2, w2, 0, 0);
    }

    private Pair getPair(int s1, int w1, int s2, int w2, int w1Cnt, int pairCnt) {
        Source _s1 = getSource(s1, w1);
        _s1.getWord().setCount(w1Cnt);
        Source _s2 = getSource(s2, w2);
        Pair p = new Pair();
        p.setS1(_s1);
        p.setS2(_s2);
        p.setCount(pairCnt);
        return p;
    }

    private Source getSource(int sourceMd5, int wordMd5) {
        Source s1 = new Source();
        Word w1 = new Word();
        w1.setWordMd5(wordMd5);
        s1.setSourceMd5(sourceMd5);
        s1.setWord(w1);
        return s1;
    }
}
