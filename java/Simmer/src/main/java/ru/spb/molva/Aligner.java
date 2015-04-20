package ru.spb.molva;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import ru.spb.molva.align.*;
import ru.spb.molva.cluster.Cluster;
import ru.spb.molva.cluster.ClusterAligner;
import ru.spb.molva.cluster.Member;

import java.io.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

import static java.lang.String.format;

/**
 * Created by lonlylocly on 4/20/15.
 */
public class Aligner {

    public static Logger log = LogManager.getLogger(Aligner.class);

    public static final int MAX_CLUSTERS = 30;
    private Map<Word, Source> sources;
    private Map<Word,Member> wordToMember = new HashMap<>();
    private List<Pair> pairs;

    private List<Cluster> clusters;

    public Aligner(File clustersFile, File bigramFile) throws FileNotFoundException {
        Reader readerClusters = new BufferedReader(new FileReader(clustersFile));
        Reader readerBigram = new BufferedReader(new FileReader(bigramFile));

        final Gson gson = new Gson();

        log.info(format("Read bigrams from %s", bigramFile));
        pairs = gson.fromJson(readerBigram, new TypeToken<List<Pair>>() {
        }.getType());
        log.info("Done reading");

        log.info(format("Read clusters from %s", clustersFile));
        clusters = getClusters(readerClusters, gson);
        log.info("Done reading");

        sources = getWordSourceMap(pairs);

        for (Cluster cluster : clusters) {
            for (Member member : cluster.getMembers()) {
                wordToMember.put(new Word(member.getId()), member);
            }
        }
    }

    public static void main(String[] args) throws IOException {
        log.info("Start");
        File clustersFile = new File(args[0]);
        File bigramFile = new File(args[1]);
        File outputFile = new File(args[2]);

        Aligner aligner = new Aligner(clustersFile, bigramFile);

        log.info("Align clusters");
        List<Cluster> alignedClusters = new ArrayList<>();

        for (Cluster cluster : aligner.getClusters().subList(0, MAX_CLUSTERS)) {
            try {
                Cluster aligned = new ClusterAligner(aligner, cluster).align();
                if (aligned != null) {
                    alignedClusters.add(aligned);
                }
            } catch(Exception e) {
                e.printStackTrace();
            }
        }

        log.info(format("Write aligned clusters to %s", args[2]));
        PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(outputFile)));
        writer.write(new Gson().toJson(alignedClusters));

        log.info("Done");
    }

    private static Map<Word, Source> getWordSourceMap(List<Pair> pairs) {
        Map<Word, Source> sources = new HashMap<>();
        for (Pair pair : pairs) {
            for (Source s : new Source[]{pair.getS1(), pair.getS2()}) {
                if (sources.containsKey(s.getWord())) {
                    Source sOld = sources.get(s.getWord());
                    if (sOld.equals(s)) {
                        sOld.setCount(sOld.getCount() + s.getCount());
                    } else if (sOld.getCount() < s.getCount()){
                        sources.put(s.getWord(), s);
                    }
                } else {
                    sources.put(s.getWord(), s);
                }
            }
        }
        return sources;
    }

    private static List<Cluster> getClusters(Reader readerClusters, Gson gson) {
        List<Cluster> clusters = gson.fromJson(readerClusters, new TypeToken<List<Cluster>>() {
        }.getType());
        Collections.sort(clusters, new ClusterComparator());
        Collections.reverse(clusters);
        return clusters;
    }


    public static class ClusterComparator implements Comparator<Cluster> {
        @Override
        public int compare(Cluster c1, Cluster c2) {
            return getMaxTrend(c1).compareTo(getMaxTrend(c2));
        }

        public Double getMaxTrend(Cluster c) {
            List<Double> trends = new ArrayList<>();
            for (Member m : c.getMembers()) {
                trends.add(m.getTrend());
            }
            Collections.sort(trends);
            Collections.reverse(trends);
            double sumTrend = 0;
            for(int i =0; i<1 && i < trends.size(); i++) {
                sumTrend += trends.get(i);
            }

            return sumTrend;
        }
    }

    public Map<Word, Source> getSources() {
        return sources;
    }

    public Map<Word, Member> getWordToMember() {
        return wordToMember;
    }

    public List<Cluster> getClusters() {
        return clusters;
    }

    public List<Pair> getPairs() {
        return pairs;
    }
}
