import org.junit.Test;
import ru.spb.molva.align.Graph;
import ru.spb.molva.align.Pair;
import ru.spb.molva.align.Source;
import ru.spb.molva.align.Word;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by lonlylocly on 4/17/15.
 */
public class GraphTest {

    @Test
    public void buildTest() {
        Graph g = new Graph();
        List<Pair> pairs = new ArrayList<Pair>();
        pairs.add(getPair(10, 1, 20, 2));
        pairs.add(getPair(20, 2, 30, 3));

        g.build(pairs, 2);
    }

    private Pair getPair(int s1, int w1, int s2, int w2) {
        Source _s1 = getSource(s1, w1);
        Source _s2 = getSource(s2, w2);
        Pair p = new Pair();
        p.setS1(_s1);
        p.setS2(_s2);
        return p;
    }

    private Source getSource(int sourceMd5, int wordMd5) {
        Source s1 = new Source();
        Word w1 = new Word();
        w1.setWordMd5(sourceMd5);
        s1.setSourceMd5(wordMd5);
        s1.setWord(w1);
        return s1;
    }
}
