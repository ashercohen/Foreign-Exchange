package hw_4_cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.google.common.collect.ArrayTable;
import hw2_dt.DT;
import hw3_rf.RandomForest;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by Asher on 28/10/2015
 */
public class CassandraRandomForest extends RandomForest implements Runnable {

    private String hostIP;
    private String keySpace;
    private String mode;


    public CassandraRandomForest(int n, String trainDataSet, String testDataSet, String hostIP, String keySpace, String mode) {
        super(n, trainDataSet, testDataSet);
        this.hostIP = hostIP;
        this.keySpace = keySpace;
        this.mode = mode;
    }

    public static void main(String[] args) {

        if(args.length != 6) {
            System.err.println("Usage: java <create_tables|load_tables|run_rf> <hostIP> <keySpace> <trainDataSet> <testDataSet> <size>");
            System.exit(1);
        }

        new Thread(new CassandraRandomForest(Integer.parseInt(args[5]), args[3], args[4], args[1], args[2], args[0])).start();
    }

    @Override
    public void run() {

        long start = System.currentTimeMillis();

        try(Session session = getSession()) {

            CassandraDT dt = new CassandraDT(this.mode, this.hostIP, this.keySpace, this.trainDataSet, this.testDataSet);
            switch(this.mode) {
                case "create_tables":
                    dt.createKeySpaceAndTables(session);
                    break;
                case "load_tables":
                    dt.loadTrainDataToCassandra(session);
                    dt.loadTestDataToCassandra(session);
                    break;
                case "run_rf":
                    runRandomForest(session, dt);
                    break;
                default:
                    System.err.println("Unrecognized option: " + this.mode);
            }
        }
        long finish = System.currentTimeMillis();
        System.out.println("total runtime (sec): " + (finish - start) / 1000);
        System.exit(0); //must do this since driver is loaded
    }

    private void runRandomForest(Session session, CassandraDT dt) {

        Forest forest = new Forest(this.N);

        try {
            Set<Integer> rowKeysSet = new HashSet<>(dt.loadTrainDataSet());
            this.featureCount = dt.getTrainWidth();
            ArrayTable<Integer, Integer, Float> trainDataSet = dt.getTrainDataSet();
            List<Integer> trainLabels = dt.getTrainLabels();

            //grow size trees
            for(int t = 0; t < this.N; t++) {
                System.out.println("training tree #" + t);
                //randomly split train dataset into 2/3 & 1/3 subsets
                List<Integer>[] splits = splitDataSetRandomly(new HashSet<>(rowKeysSet));
                List<Integer> trainRows = splits[0];
                List<Integer> testRows = splits[1];

                //randomly select sqrt(features)
                List<Integer> columnIndexes = getKeysAsList(this.featureCount);
                List<Integer> featureList = randomFeaturesSubSet(columnIndexes);

                //learn another tree using random 2/3 of train data
                DT.Node tree = dt.learnDecisionTree(trainRows, featureList);
                forest.add(tree);
                System.out.println("tree #" + t+ " trained - classifying test set");

                //test it's accuracy on the remaining 1/3 of train data
                double tree_accuracy = tree.classifyTestDataSet(trainDataSet, trainLabels, testRows);
                System.out.println(String.format("tree #%d accuracy = %f", t, tree_accuracy));

                //test forest accuracy on remaining 1/3 of train data
                double forest_accuracy = testRandomForest(forest, testRows, trainDataSet, trainLabels);
                System.out.println(String.format("forest accuracy using %d trees = %f", t+1, forest_accuracy));
            }

            saveForestToDB(forest, dt, session);
            System.out.println("forest save to DB");

            //finally test forest on 20% of held out dataset
            double forestAccuracy = testRandomForest(forest,
                                                     dt.loadTestDataSet(true, session),
                                                     dt.getTestDataSet(),
                                                     dt.getTestLabels());
            saveMetricToDB(forestAccuracy, dt, session);
            System.out.println("random forest accuracy = " + forestAccuracy);
        }
        catch(IOException ioe) {
            ioe.printStackTrace();
        }
    }

    private void saveMetricToDB(double accuracy, CassandraDT dt, Session session) {
        dt.insertToMetricsTable(FOREST_FILE_NAME, accuracy, session);
    }

    private void saveForestToDB(Forest forest, CassandraDT dt, Session session) {
        dt.insertToTreesTable(FOREST_FILE_NAME, forest.toJson(), session);
    }

    private Session getSession() {
        Cluster cluster = Cluster.builder().addContactPoint(this.hostIP).build();
        return cluster.connect();
    }
}
