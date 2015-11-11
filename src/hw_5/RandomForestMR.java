package hw_5;

import com.google.common.base.Joiner;
import hw_4_cassandra.CassandraDT;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.hadoop.cql3.CqlConfigHelper;
import org.apache.cassandra.hadoop.cql3.CqlInputFormat;
import org.apache.cassandra.hadoop.cql3.CqlOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.List;
import java.util.Map;

/**
 * Created by Asher
 */
public class RandomForestMR extends Configured implements Tool {

    private String trainDataSetTable;
    private String forestOutputTable;
    private String host;
    private String keySpace;

    public RandomForestMR(String trainDataSetTable, String forestOutputTable, String host, String keySpace) {
        this.trainDataSetTable = trainDataSetTable;
        this.forestOutputTable = forestOutputTable;
        this.host = host;
        this.keySpace = keySpace;
    }

    public static void main(String[] args) throws Exception {

        if(args.length != 4) {
            System.err.println("Usage: hadoop|yarn <trainDataSetTable> <forestOutputTable> <host> <keySpace>");
            System.exit(1);
        }

        ToolRunner.run(new Configuration(), new RandomForestMR(args[0], args[1], args[2], args[3]), args);
        System.exit(0);
    }

    @Override
    public int run(String[] args) throws Exception {

        Job job = new Job(getConf(), "RandomForest");
        job.setJarByClass(WordCount.class);

        /**
         * mapper setup
         */
        job.setMapperClass(CassandraMapper.class);
        job.setInputFormatClass(CqlInputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        /**
         * reducer setup
         */
        job.setReducerClass(CassandraReducer.class);
        job.setOutputKeyClass(Map.class);
        job.setOutputValueClass(List.class);
        job.setOutputFormatClass(CqlOutputFormat.class);

        /**
         * job setup
         */
        ConfigHelper.setOutputColumnFamily(job.getConfiguration(), this.keySpace, this.forestOutputTable);
        job.getConfiguration().set("row_key", "name");
        String query = String.format("UPDATE %s.%s SET json = ?", this.keySpace, this.forestOutputTable);
        CqlConfigHelper.setOutputCql(job.getConfiguration(), query);
        ConfigHelper.setOutputInitialAddress(job.getConfiguration(), this.host);
        ConfigHelper.setOutputPartitioner(job.getConfiguration(), "Murmur3Partitioner"); //default partitioner in cassandra 1.2+

        /**
         * set query for retrieving data from Cassandra (for mapper)
         */
        List<String> columns = CassandraDT.getColumns();
        columns = columns.subList(1, columns.size());
        String cols = Joiner.on(',').join(columns);
        String selectCQL = String.format("SELECT %s FROM %s.%s;", cols, this.keySpace, this.trainDataSetTable);
        CqlConfigHelper.setInputCql(job.getConfiguration(), selectCQL);

        ConfigHelper.setInputRpcPort(job.getConfiguration(), "9160");
        ConfigHelper.setInputInitialAddress(job.getConfiguration(), "localhost");
        ConfigHelper.setInputColumnFamily(job.getConfiguration(), this.keySpace, this.trainDataSetTable);
        ConfigHelper.setInputPartitioner(job.getConfiguration(), "Murmur3Partitioner");

        CqlConfigHelper.setInputCQLPageRowSize(job.getConfiguration(), "3");
        job.waitForCompletion(true);

        return 0;
    }
}
