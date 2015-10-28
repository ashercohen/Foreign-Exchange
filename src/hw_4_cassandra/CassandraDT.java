package hw_4_cassandra;

import cascading.tuple.Fields;
import com.datastax.driver.core.*;
import com.google.common.base.Joiner;
import hw1_data_prep.FeaturesGeneratorBuffer;
import hw1_data_prep.Main;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by Asher
 */
public class CassandraDT implements Runnable {

    private String hostIP;
    private String keySpace;
    private String trainTable;
    private String testTable;
    private String trainDataSet;
    private String testDataSet;
    private String mode;

    public CassandraDT(String mode, String hostIP, String keySpace, String trainDataSet, String testDataSet) {
        this.hostIP = hostIP;
        this.keySpace = keySpace;
        this.trainDataSet = trainDataSet;
        this.testDataSet = testDataSet;
        this.mode = mode;
        this.trainTable = "train";
        this.testTable = "test";
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
                    createKeySpaceAndTable(session);
                    break;
                case "load_tables":
                    loadTrainDataToCassandra(session);
                    loadTestDataToCassandra(session);
                    break;
                case "run_dt":
//                    runDT(session);
                    break;
                default:
                    System.err.println("Unrecognized option: " + this.mode);
            }
        }
        long finish = System.currentTimeMillis();
        System.out.println("total runtime (sec): " + (finish - start) / 1000);
        System.exit(0); //must do this since driver is loaded
    }

    private void createKeySpaceAndTable(Session session) {

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

        String createTestTableCQL = createTrainTableCQL.replace(this.trainTable, this.testTable);
        session.execute(createTestTableCQL);
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
