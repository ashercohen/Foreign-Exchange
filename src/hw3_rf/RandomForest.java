package hw3_rf;

import com.google.common.collect.ArrayTable;
import com.google.gson.Gson;
import hw2_dt.DT;

import java.io.*;
import java.util.*;

/**
 * Created by Asher
 */
public class RandomForest implements Runnable {

    private static final Gson gson = new Gson();
    protected static final String FOREST_FILE_NAME = "RF.json";

    protected int N;
    protected String trainDataSet;
    protected String testDataSet;
    protected int featureCount;
    //    private int exampleCount;
    private Random random;

    public RandomForest(int n, String trainDataSet, String testDataSet) {
        N = n;
        this.trainDataSet = trainDataSet;
        this.testDataSet = testDataSet;
        this.random = new Random(System.currentTimeMillis());
    }

    public static void main(String[] args) {

        if(args.length != 3) {
            System.err.println("Usage: RandomForest <size> <train> <test>");
            System.exit(1);
        }

        new Thread(new RandomForest(Integer.parseInt(args[0]), args[1], args[2])).start();
    }

    @Override
    public void run() {
        long start = System.currentTimeMillis();
        Forest forest = new Forest(this.N);

        try {
            DT dt = new DT(this.trainDataSet, this.testDataSet);
            Set<Integer> rowKeysSet = new HashSet<>(dt.loadTrainDataSet());
//            this.exampleCount = dt.getTrainHeight();
            this.featureCount = dt.getTrainWidth();
            ArrayTable<Integer, Integer, Float> trainDataSet = dt.getTrainDataSet();
            List<Integer> trainLabels = dt.getTrainLabels();

            //grow size trees
            for(int t = 0; t < this.N; t++) {
                //randomly split train dataset into 2/3 & 1/3 subsets
                List<Integer>[] splits = splitDataSetRandomly(new HashSet<Integer>(rowKeysSet));
                List<Integer> trainRows = splits[0];
                List<Integer> testRows = splits[1];

                //randomly select sqrt(features)
                List<Integer> columnIndexes = getKeysAsList(this.featureCount);
                List<Integer> featureList = randomFeaturesSubSet(columnIndexes);

                //learn another tree using random 2/3 of train data
                DT.Node tree = dt.learnDecisionTree(trainRows, featureList);
                forest.add(tree);

                //test it's accuracy on the remaining 1/3 of train data
                double tree_accuracy = tree.classifyTestDataSet(trainDataSet, trainLabels, testRows);
                System.out.println(String.format("tree #%d accuracy = %f", t, tree_accuracy));

                //test forest accuracy on remaining 1/3 of train data
                double forest_accuracy = testRandomForest(forest, testRows, trainDataSet, trainLabels);
                System.out.println(String.format("forest accuracy using %d trees = %f", t + 1, forest_accuracy));
            }

            forest.persistToFile(FOREST_FILE_NAME);

            //finally test forest on 20% of held out dataset
            double forestAccuracy = testRandomForest(forest,
                    dt.loadTestDataSet(true),
                    dt.getTestDataSet(),
                    dt.getTestLabels());
            System.out.println("random forest accuracy = " + forestAccuracy);

        }
        catch(IOException ioe) {
            ioe.printStackTrace();
        }

        long finish = System.currentTimeMillis();
        System.out.println("total runtime (sec): " + (finish - start) / 1000);
    }

    protected double testRandomForest(Forest forest,
                                    List<Integer> exampleKeys,
                                    ArrayTable<Integer, Integer, Float> testDataSet,
                                    List<Integer> testLabels) {

        int total = 0;
        int errors = 0;
        List<Integer> predictions = new ArrayList<>(forest.size);

        for(Integer key : exampleKeys) {

            for(DT.Node tree : forest.forest) {
                predictions.add(tree.classifyExample(testDataSet.row(key)));
            }

            int predicted = majorityVote(predictions);
            int trueLabel = testLabels.get(key);

            if(predicted != trueLabel) {
                errors++;
            }
            total++;
            predictions.clear();
        }

        System.out.println("total test examples: " + total);
        System.out.println("total test errors: " + errors);

        return (total - errors) / (1D * total);
    }

    private int majorityVote(List<Integer> predictions) {

        int positiveCount = 0;
        int negativeCount = 0;


        for(Integer prediction : predictions) {
            positiveCount += prediction;
        }

        negativeCount = predictions.size() - positiveCount;

        if(positiveCount > negativeCount) {
            return 1;
        }
        if(positiveCount < negativeCount) {
            return 0;
        }
        //coin flip
        return this.random.nextBoolean() ? 1 : 0;
    }


    /**
     * randomly split the indexes given in argument into 2/3 and 1/3
     */
    protected List<Integer>[] splitDataSetRandomly(Set<Integer> rowKeysSet) {

        int totalRows = rowKeysSet.size();
        int numRows = totalRows * 2 / 3;
        Set<Integer> subSet = new HashSet<>(numRows);

        while(subSet.size() != numRows) {
            subSet.add(this.random.nextInt(totalRows));
        }

        rowKeysSet.removeAll(subSet);
        ArrayList<Integer>[] rv = new ArrayList[2];

        rv[0] = new ArrayList<>(subSet);
        rv[1] = new ArrayList<>(rowKeysSet);

        return rv;
    }

    protected List<Integer> randomFeaturesSubSet(List<Integer> features) {

        int featuresCount = features.size();
        int size = (int)Math.sqrt(featuresCount);
        Set<Integer> subset = new HashSet<>(size);

        while(subset.size() != size) {
            subset.add(this.random.nextInt(featuresCount));
        }

        return new ArrayList<>(subset);
    }

    protected List<Integer> getKeysAsList(int size) {
        Integer[] keys = new Integer[size];
        for(int i = 0; i < size; i++) {
            keys[i] = i;
        }

        return Arrays.asList(keys);
    }

    public static class Forest {

        private int size;
        private ArrayList<DT.Node> forest;

        public Forest() {
            this.forest = new ArrayList<>();
        }

        public Forest(int size) {
            this.size = size;
            this.forest = new ArrayList<>(size);
        }

        public void add(DT.Node tree) {
            this.forest.add(tree);
        }

        public String toJson() {
            return gson.toJson(this);
        }

        public void persistToFile(String fileName) {

            String jsonString = gson.toJson(this);
            try(Writer writer = new FileWriter(fileName)) {

                writer.write(jsonString);
                writer.flush();
                System.out.println(String.format("Random forest with %d trees was successfully written to file: %s", this.size, fileName));
            }
            catch(IOException ioe) {
                ioe.printStackTrace();
            }
        }

        public static Forest loadFromFile(String fileName) {

            try(BufferedReader reader = new BufferedReader(new FileReader(fileName))) {
                String jsonString = reader.readLine();
                Forest forest = gson.fromJson(jsonString, Forest.class);
                System.out.println(String.format("Random forest with %d trees was successfully read from file: %s", forest.size, fileName));
                return forest;
            }
            catch(IOException ioe) {
                System.err.println("can't load forest from file");
            }

            return null;
        }
    }
}
