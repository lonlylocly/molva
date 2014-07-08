package ru.spb.molva;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.commons.io.FileUtils;
import org.apache.commons.math3.linear.MatrixUtils;
import org.apache.commons.math3.linear.RealVector;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * Created by lonlylocly on 08.06.14.
 */
public class Simmer {

    public static void main(String[] args) throws IOException {
        File inputFile = new File(args[0]);
        File outputFile = new File(args[1]);

        FileUtils.writeStringToFile(outputFile, "");

        String content = FileUtils.readFileToString(inputFile);

        Map<Long,Map<Long,Double>> dict = new HashMap<Long, Map<Long, Double>>();

        final JsonObject profiles = new JsonParser().parse(content).getAsJsonObject();

        for (Map.Entry<String, JsonElement> profileDesc : profiles.entrySet()) {
            Long postMd5 = Long.parseLong(profileDesc.getKey());

            final JsonObject postReplys = profileDesc.getValue().getAsJsonObject();
            final HashMap<Long, Double> replysDict = new HashMap<Long, Double>();
            for (Map.Entry<String, JsonElement> repl : postReplys.entrySet()) {
                Long replyMd5 = Long.parseLong(repl.getKey());
                replysDict.put(replyMd5, repl.getValue().getAsDouble());
            }
            dict.put(postMd5, replysDict);

        }
        System.out.println("init ready");

        List<Long> posts = new ArrayList<Long>();
        posts.addAll(dict.keySet());

        List<SimEntry> sims = new ArrayList<SimEntry>((posts.size() / 2) * posts.size());

        int cnt = 0;
        int longCnt = 0;

        for(int i = 0; i < posts.size(); i++) {
            for(int j= 0; j < posts.size(); j++) {
                final Long p1 = posts.get(i);
                final Long p2 = posts.get(j);
                if (p1 <= p2) {
                    continue;
                }

                List<Long> commonKeys = getCommonKeys(dict, p1, p2);

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
        List<Long> commonKeys = new ArrayList<Long>(keys1.size() + keys2.size());
        commonKeys.addAll(keys1);
        commonKeys.addAll(keys2);
        return commonKeys;
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
}
