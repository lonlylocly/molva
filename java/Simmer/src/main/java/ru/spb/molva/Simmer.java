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

        String content = FileUtils.readFileToString(inputFile);

        final JsonElement tree = new JsonParser().parse(content);

        final JsonObject profiles = tree.getAsJsonObject();
        Set<Long> replys = new HashSet<Long>();
        for (Map.Entry<String, JsonElement> profileDesc : profiles.entrySet()) {
            final JsonObject postReplys = profileDesc.getValue().getAsJsonObject();
            for (Map.Entry<String, JsonElement> repl : postReplys.entrySet()) {
                replys.add(Long.parseLong(repl.getKey()));
            }

        }
        List<Long> replysOrder = new ArrayList<Long>();
        replysOrder.addAll(replys);

        List<Long> posts = new ArrayList<Long>();
        Map<Long,RealVector> profilesMap = new HashMap<Long, RealVector>();
        for (Map.Entry<String, JsonElement> profileDesc : profiles.entrySet()) {
            Long postMd5 = Long.parseLong(profileDesc.getKey());
            posts.add(postMd5);

            double[] profileWeights = new double[replysOrder.size()];

            final JsonObject postReplys = profileDesc.getValue().getAsJsonObject();
            for (Map.Entry<String, JsonElement> repl : postReplys.entrySet()) {
                Long replyMd5 = Long.parseLong(repl.getKey());
                double weight = repl.getValue().getAsDouble();
                final int i = replysOrder.indexOf(replyMd5);
                profileWeights[i] = weight;

            }

            profilesMap.put(postMd5, MatrixUtils.createRealVector(profileWeights));
        }
        System.out.println("init ready");

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

                final RealVector x1 = profilesMap.get(p1);
                final RealVector x2 = profilesMap.get(p2);
                double sim = x1.dotProduct(x2) / (x1.getNorm() * x2.getNorm());
                sims.add(new SimEntry(p1, p2, sim));
                cnt ++;
            }
            if (cnt > longCnt * 100000){
                System.out.println(String.format("Total %s seen", cnt));
                longCnt += 1;

                StringBuilder builder = new StringBuilder();
                for (SimEntry sim : sims) {
                    builder.append(sim.toCsv()).append("\n");
                }

                sims.clear();

                FileUtils.writeStringToFile(outputFile, builder.toString(), true);

            }
        }

        System.out.println(String.format("Total %s seen", cnt));


    }
}
