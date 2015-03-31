package ru.spb.molva;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.commons.io.FileUtils;
import org.apache.commons.math3.linear.MatrixUtils;
import org.apache.commons.math3.linear.RealVector;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.Reader;

/**
 * Created by lonlylocly on 08.06.14.
 */
public class Simmer {

    public static void main(String[] args) throws IOException {
        File inputFile = new File(args[0]);
        File outputFile = new File(args[1]);

        FileUtils.writeStringToFile(outputFile, "");

        Reader reader = new BufferedReader(new FileReader(inputFile));

        final Gson gson = new Gson();
        Map<Long, Map<Long, Double>> dict = gson.fromJson(reader, new TypeToken<Map<Long, Map<Long, Double>>>() {
        }.getType());

        logVectorSizes(dict);

        System.out.println("init ready");
        System.out.println("Total keys: " + dict.size());

        List<Long> posts = new ArrayList<Long>();
        posts.addAll(dict.keySet());

        List<SimEntry> sims = new ArrayList<SimEntry>((posts.size() / 2) * posts.size());

        int cnt = 0;
        int longCnt = 0;
        DescriptiveStatistics stat = new DescriptiveStatistics();

        for(int i = 0; i < posts.size(); i++) {
            for(int j= 0; j < posts.size(); j++) {
                final Long p1 = posts.get(i);
                final Long p2 = posts.get(j);
                if (p1 <= p2) {
                    continue;
                }

                List<Long> commonKeys = getCommonKeys(dict, p1, p2);
                stat.addValue(commonKeys.size());

                final RealVector x1 = getRealVector(dict.get(p1), commonKeys);
                final RealVector x2 = getRealVector(dict.get(p2), commonKeys);

                double sim = compare(x1, x2);
                sims.add(new SimEntry(p1, p2, sim));
                cnt ++;
            }
            if (cnt > longCnt * 100000){
                System.out.println(String.format("Total %s seen", cnt));
                longCnt += 1;

                saveSims(outputFile, sims);

            }
        }

        saveSims(outputFile, sims);

        System.out.println(String.format("Total %s seen", cnt));
        System.out.println(String.format("Common keys mean length: %.2f; stddev: %.2f; skewness: %.2f",
            stat.getMean(), stat.getStandardDeviation(), stat.getSkewness())
        );

    }

    public static double compare(RealVector x1, RealVector x2) {
        final double n1 = x1.getNorm();
        final double n2 = x2.getNorm();
        if (n1 == 0 || n2 == 0) {
            return 1;
        }
        return 1 - x1.dotProduct(x2) / (n1 * n2);
    }

    private static List<Long> getCommonKeys(Map<Long, Map<Long, Double>> dict, Long p1, Long p2) {
        final Set<Long> keys1 = dict.get(p1).keySet();
        final Set<Long> keys2 = dict.get(p2).keySet();
        Set<Long> commonKeys = new LinkedHashSet<Long>();
        commonKeys.addAll(keys1);
        commonKeys.addAll(keys2);
        return new ArrayList(commonKeys);
    }

    public static RealVector getRealVector(Map<Long, Double> profile, List<Long> commonKeys) {
        double[] v1 = new double[commonKeys.size()];
        for (int i=0; i< commonKeys.size(); i++ ) {
            Long key = commonKeys.get(i);
            if (profile.containsKey(key)) {
                v1[i] = profile.get(key);
            } else {
                v1[i] = 0;
            }

        }

        return MatrixUtils.createRealVector(v1);
    }


    private static void saveSims(File outputFile, List<SimEntry> sims) throws IOException {
        StringBuilder builder = new StringBuilder();
        for (SimEntry sim : sims) {
            builder.append(sim.toCsv()).append("\n");
        }

        sims.clear();

        FileUtils.writeStringToFile(outputFile, builder.toString(), true);
    }

    public static void logVectorSizes(Map<Long, Map<Long, Double>> dict) {
        DescriptiveStatistics stat = new DescriptiveStatistics();
        for(Map<Long,Double> m : dict.values()) {
            stat.addValue(m.values().size());
        }
        System.out.println(String.format("Mean profile length: %.2f; std dev: %.2f; skewness: %.2f", 
            stat.getMean(), stat.getStandardDeviation(), stat.getSkewness())
        );
    }
}
