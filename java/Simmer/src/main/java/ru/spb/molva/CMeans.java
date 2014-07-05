package ru.spb.molva;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.commons.io.FileUtils;
import org.apache.commons.math3.linear.RealVector;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * Created by lonlylocly on 05.07.14.
 */
public class CMeans {

    List<Long> replys;
    List<SparseVector> posts;
    Integer k;
    Integer n;
    List<RealVector> centers;
    List<List<Double>> weights;


    public void populate(File inputFile, Integer k) throws IOException {
        String content = FileUtils.readFileToString(inputFile);

        posts = new LinkedList<SparseVector>();

        Set<Long> replysSet = new HashSet<Long>();

        final JsonObject profiles = new JsonParser().parse(content).getAsJsonObject();

        for (Map.Entry<String, JsonElement> profileDesc : profiles.entrySet()) {
            final JsonObject postReplys = profileDesc.getValue().getAsJsonObject();
            for (Map.Entry<String, JsonElement> repl : postReplys.entrySet()) {
                Long replyMd5 = Long.parseLong(repl.getKey());
                replysSet.add(replyMd5);
            }
        }

        replys = new ArrayList<Long>(replysSet.size());
        replys.addAll(replysSet);

        for (Map.Entry<String, JsonElement> profileDesc : profiles.entrySet()) {
            final JsonObject postReplys = profileDesc.getValue().getAsJsonObject();
            final HashMap<Long, Double> replysDict = new HashMap<Long, Double>();
            for (Map.Entry<String, JsonElement> repl : postReplys.entrySet()) {
                Long replyMd5 = Long.parseLong(repl.getKey());
                replysDict.put(replyMd5, repl.getValue().getAsDouble());
            }
            posts.add(new SparseVector(replysDict, replys));
        }

        this.k = k;
        this.n = posts.size();
        final Random rand = new Random();

        weights = new ArrayList<List<Double>>(n);
        for(int i=0; i<n; i++) {
            List<Double> row = new ArrayList<Double>(k);
            for(int j=0; j<k; j++) {
                row.add(rand.nextDouble());
            }
            weights.add(row);
        }

        System.out.println("init ready");
    }

    public static RealVector getEmptyRealVector(List<Long> keys) {
        final Map<Long, Double> c = new HashMap<Long, Double>();

        return Simmer.getRealVector(c, keys);
    }

    public void recountCenters(){
        System.out.println("start recount centers");
        List<RealVector> centers = new ArrayList<RealVector>(k);
        for(int j=0; j<k; j++) {
            final RealVector c = getEmptyRealVector(replys);
            for(int i=0; i<n; i++) {
                c.add(posts.get(i).getRealVector().mapMultiply(weights.get(i).get(j)));
            }
            centers.add(c);
        }
        System.out.println("stop recount centers");
    }

    public double recountWeights(){
        System.out.println("start recount weights");
        List<List<Double>> weights = new ArrayList<List<Double>>();
        double error = 0;
        for(int i=0; i<n; i++) {
            weights.add(new ArrayList<Double>(k));
            for(int j=0; j<k; j++) {
                final double dk = Simmer.compare(centers.get(j), posts.get(i).getRealVector());
                double w = 0;
                for(int jj=0; jj<k; jj++) {
                    final double djj = Simmer.compare(centers.get(jj), posts.get(i).getRealVector());
                    w += dk / djj;
                }
                if (w != 0) {
                    w = 1 / w;
                }
                weights.get(i).add(w);
                error += Math.abs(w - this.weights.get(i).get(j));
            }
        }
        this.weights = weights;
        System.out.println("error " + error);
        System.out.println("done recount weights");

        return error;
    }

    public static void main(String[] args) throws IOException {
        File inputFile = new File(args[0]);
        File outputFile = new File(args[1]);
        Integer k = Integer.parseInt(args[2]);

        FileUtils.writeStringToFile(outputFile, "");

        final CMeans cMeans = new CMeans();
        cMeans.populate(inputFile, k);

        double error = 10.0;
        while(error > 0.01) {
            cMeans.recountCenters();
            error = cMeans.recountWeights();
        }
    }
}
