package hw_4_cassandra;

import cascading.tuple.Fields;
import com.datastax.driver.core.*;
import com.google.common.base.Joiner;
import com.google.common.collect.ArrayTable;
import hw1_data_prep.FeaturesGeneratorBuffer;
import hw1_data_prep.Main;
import hw2_dt.DT;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by Asher
 */
public class CassandraDT extends DT implements Runnable {

    private String hostIP;
    private String keySpace;
    private String trainTable;
    private String testTable;
    private String treesTable;
    private String metricsTable;
//    private String trainDataSet;
//    private String testDataSet;
    private String mode;

    public CassandraDT(String mode, String hostIP, String keySpace, String trainDataSet, String testDataSet) {

        super(trainDataSet, testDataSet);
        this.hostIP = hostIP;
        this.keySpace = keySpace;
        this.mode = mode;
        this.trainTable = "train";
        this.testTable = "test";
        this.treesTable = "trees";
        this.metricsTable = "metrics";
    }

    public static void main(String[] args) {

        if(args.length != 5) {
            System.err.println("Usage: java <create_tables|load_tables|run_dt> <hostIP> <keySpace> <trainDataSet> <testDataSet>");
            System.exit(1);
        }

        new Thread(new CassandraDT(args[0], args[1], args[2], args[3], args[4])).start();
    }


    @Override
    public void run() {

        long start = System.currentTimeMillis();

        try(Session session = getSession()) {

            switch(this.mode) {
                case "create_tables":
                    createKeySpaceAndTables(session);
                    break;
                case "load_tables":
                    loadTrainDataToCassandra(session);
                    loadTestDataToCassandra(session);
                    break;
                case "run_dt":
                    runDT(session);
                    break;
                default:
                    System.err.println("Unrecognized option: " + this.mode);
            }
        }
        long finish = System.currentTimeMillis();
        System.out.println("total runtime (sec): " + (finish - start) / 1000);
        System.exit(0); //must do this since driver is loaded
    }

    private void runDT(Session session) {

        //read data from db
        //build and run model

        try {
            //try load tree from DB
            Node tree = loadTreeFromDB(session);
            boolean ignoreWidth = true;
            //serialized tree not exists - train
            if(tree == null) {

                System.out.println("learning tree from examples...");
                List<Integer> rowKeys = loadTrainDataSet(session);
                List<Integer> columns = getKeysAsList(this.trainWidth);
                tree = learnDecisionTree(rowKeys, columns);
                saveTreeToDB(tree, session);
                ignoreWidth = false;
                System.out.println("finished training - tree depth = " + this.depth);
            }
            else {
                System.out.println("tree loaded from DB");
            }

            List<Integer> testRowKeys = loadTestDataSet(ignoreWidth, session);
            double accuracy = testDecisionTree(tree, testRowKeys);
            System.out.println("accuracy = " + accuracy);
            //save performance metrics to db
            saveMetricToDB(accuracy, session);
        }
        catch(IOException e) {
            e.printStackTrace();
        }
    }

    private void saveMetricToDB(double accuracy, Session session) {

        String treeName = this.trainDataSet.replace(".txt", "_tree");
        String insertValueCQL = "INSERT INTO %s.%s (name, accuracy) VALUES ('%s', %f)";
        session.execute(String.format(insertValueCQL, this.keySpace, this.metricsTable, treeName, accuracy));
    }

    private void saveTreeToDB(Node tree, Session session) {

        String treeName = this.trainDataSet.replace(".txt", "_tree");
        String insertTreeCQL = "INSERT INTO %s.%s (name, json) VALUES ('%s', '%s')";
        session.execute(String.format(insertTreeCQL, this.keySpace, this.treesTable, treeName, tree.toJson()));
    }

    private Node loadTreeFromDB(Session session) {

        String treeName = this.trainDataSet.replace(".txt", "_tree");
        String getTreeCQL = "SELECT json FROM %s.%s WHERE name = '%s'";
        ResultSet rows = session.execute(String.format(getTreeCQL, this.keySpace, this.treesTable, treeName));
        Row one = rows.one();
        if(one == null) {
            return null;
        }

        return Node.deserializeFromJson(one.getString(0));
    }

    public List<Integer> loadTrainDataSet(Session session) throws IOException {

        int[] widthAndHeight = getWidthAndHeight(this.trainTable, session);
        this.trainWidth = widthAndHeight[0];
        this.trainHeight = widthAndHeight[1];

        System.out.println("train width = " + this.trainWidth);
        System.out.println("train height = " + this.trainHeight);
        List<Integer> rowKeys = getKeysAsList(this.trainHeight);
        List<Integer> colKeys = getKeysAsList(this.trainWidth);

        this.X = ArrayTable.create(rowKeys, colKeys);
        this.Y = new ArrayList<>(this.trainHeight);
        loadDataSetFromDB(this.trainTable, session, this.X, this.Y);

        System.out.println("train dataset loaded from DB");
        return this.X.rowKeyList();
    }

    public List<Integer> loadTestDataSet(boolean ignoreWidth, Session session) throws IOException {

        int[] widthAndHeight = getWidthAndHeight(this.testTable, session);
        this.testWidth = widthAndHeight[0];
        this.testHeight = widthAndHeight[1];

        System.out.println("test width = " + this.testWidth);
        System.out.println("test height = " + this.testHeight);

        if(!ignoreWidth && this.testWidth != this.trainWidth) {
            throw new RuntimeException("width of train and test set do not match: " + this.trainWidth + "; " + this.testWidth);
        }

        List<Integer> rowKeys = getKeysAsList(this.testHeight);
        List<Integer> colKeys = getKeysAsList(this.testWidth);

        this.Xtest = ArrayTable.create(rowKeys, colKeys);
        this.Ytest = new ArrayList<>(this.trainHeight);
        loadDataSetFromDB(this.testTable, session, this.Xtest, this.Ytest);

        System.out.println("test dataset loaded from DB");
        return this.Xtest.rowKeyList();
    }

    private void loadDataSetFromDB(String tableName, Session session, ArrayTable<Integer, Integer, Float> table, List<Integer> labels) throws IOException {

//        int width = table.columnKeyList().size();
        String selectCQL = "SELECT %s FROM %s.%s;";
        List<String> columns = getColumns();
        columns = columns.subList(1, columns.size());
        String cols = Joiner.on(',').join(columns);

        ResultSet resultSet = session.execute(String.format(selectCQL, cols, this.keySpace, tableName));

        int rowIdx = 0;
        for(Row row : resultSet) {
            for(int colIdx = 0; colIdx < columns.size() - 1; colIdx++) {
                table.set(rowIdx, colIdx, row.getFloat(colIdx));
            }
            labels.add(rowIdx, row.getInt(columns.size() - 1));
            rowIdx++;
        }
    }

    public int[] getWidthAndHeight(String tableName, Session session) throws IOException {

        String countRowsCQL = "SELECT count(*) FROM %s.%s;";
        ResultSet resultSet = session.execute(String.format(countRowsCQL, this.keySpace, tableName));

        int height = (int)resultSet.one().getLong(0);
        int width = getColumns().size() - 2;

        return new int[]{width, height};
    }

    private void createKeySpaceAndTables(Session session) {

        /*
         * create KeySpace
         */
        String createKeySpaceCQL = "CREATE KEYSPACE %s WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '1'};";
        session.execute(String.format(createKeySpaceCQL, this.keySpace));

        /*
         * create train table
         */
        List<String> columns = getColumns();
        StringBuilder sb = new StringBuilder();

        sb.append("CREATE TABLE ")
                .append(this.keySpace)
                .append(".")
                .append(this.trainTable)
                .append(" ( ")
                .append(columns.get(0))
                .append(" int, ");
        for(int i = 1; i < columns.size() - 1; i++) {
            sb.append(columns.get(i)).append(" float, ");
        }

        sb.append(columns.get(columns.size() - 1)).append(" int, "); //directionality
        sb.append("PRIMARY KEY (id));");
        String createTrainTableCQL = sb.toString();
        session.execute(createTrainTableCQL);

        /*
         * create test table
         */
        String createTestTableCQL = createTrainTableCQL.replace(this.trainTable, this.testTable);
        session.execute(createTestTableCQL);

        /*
         * create trees table
         */
        String createTreesTableCQL = "CREATE TABLE %s.%s ( name text, json text, PRIMARY KEY(name))";
        session.execute(String.format(createTreesTableCQL, this.keySpace, this.treesTable));

        /*
         * create metrics table
         */
        String createMetricsTableCQL = "CREATE TABLE %s.%s ( name text, accuracy double, PRIMARY KEY(name))";
        session.execute(String.format(createMetricsTableCQL, this.keySpace, this.metricsTable));
    }

    private List<String> getColumns() {

        Fields fields = FeaturesGeneratorBuffer.generateFieldsByCurrencies(Arrays.asList(Main.CURRENCIES));
        List<String> columns = new ArrayList<>(fields.size());

        columns.add("id");
        //first field is not a column!
        for(int i = 1; i < fields.size(); i++) {
            columns.add(fields.get(i).toString());
        }

        columns.add("directionality");

        return columns;
    }

    private void loadTrainDataToCassandra(Session session) {
        loadDataToCassandra(session, this.trainDataSet, this.trainTable);
    }

    private void loadTestDataToCassandra(Session session) {
        loadDataToCassandra(session, this.testDataSet, this.testTable);
    }

    private void loadDataToCassandra(Session session, String fileName, String tableName) {

        List<String> columns = getColumns();
        List<String> valuesPlaceHolders = new ArrayList<>(columns.size());

        for(int i = 0; i < columns.size(); i++) {
            valuesPlaceHolders.add("?");
        }

        String insertCQL = String.format("INSERT INTO %s.%s (%s) VALUES (%s)",
                this.keySpace,
                tableName,
                Joiner.on(',').join(columns),
                Joiner.on(',').join(valuesPlaceHolders));

//        System.out.println(insertCQL);
        BatchStatement batchStatement = new BatchStatement(BatchStatement.Type.UNLOGGED);
        PreparedStatement insertStatement = session.prepare(insertCQL);

        try(BufferedReader reader = new BufferedReader(new FileReader(fileName))) {
            String line = reader.readLine(); //skip header
            int rowIdx = 1;
            while( (line = reader.readLine()) != null ) {

                BoundStatement boundStatement = insertStatement.bind();
                boundStatement.setInt(0, rowIdx);
                String[] parts = line.split(",");
                for(int i = 0; i < parts.length - 1; i++) {
                    if(parts[i].isEmpty()) {
                        continue;
                    }
                    boundStatement.setFloat(i+1, Float.parseFloat(parts[i]));
                }

                boundStatement.setInt(parts.length, Integer.parseInt(parts[parts.length - 1]));
                batchStatement.add(boundStatement);

                rowIdx++;
//                if(rowIdx == 10) break;
            }
        }
        catch(IOException ioe) {
            ioe.printStackTrace();
        }

        ResultSet resultSet = session.execute(batchStatement);
        System.out.println("data successfully loaded into table: " + tableName);
    }

    private Session getSession() {
        Cluster cluster = Cluster.builder().addContactPoint(this.hostIP).build();
        return cluster.connect();
//        return cluster.connect(this.keySpace);
    }
}
