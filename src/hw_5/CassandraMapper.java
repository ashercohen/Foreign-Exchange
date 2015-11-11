package hw_5;

import com.google.common.collect.ArrayTable;
import hw2_dt.DT;
import hw_4_cassandra.CassandraDT;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Created by Asher
 */
public class CassandraMapper extends Mapper<Map<String, ByteBuffer>, Map<String, ByteBuffer>, IntWritable, Text> {

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private ByteBuffer sourceColumn;

    @Override
    public void map(Map<String, ByteBuffer> keys, Map<String, ByteBuffer> columns, Context context) throws IOException, InterruptedException {

        Object[] dataAndLabels = getData(keys, columns);
        ArrayTable<Integer, Integer, Float> trainDataSet = (ArrayTable<Integer, Integer, Float>)dataAndLabels[0];
//        ArrayList<Integer> trainLabels = (ArrayList<Integer>)dataAndLabels[1]; //no need for labels since we're not checking accuracy in this assignment

        CassandraDT dt = new CassandraDT("train", "test"); //not using this object to load data - just give names...
        DT.Node node = dt.learnDecisionTree(trainDataSet.rowKeyList(), trainDataSet.columnKeyList());

        context.write(one, new Text(node.toJson()));
    }

    public Object[] getData(Map<String, ByteBuffer> keys, Map<String, ByteBuffer> columns) {

        int height = keys.size();
        int width = columns.size();

        List<Integer> rowKeys = getKeysAsList(height);
        List<Integer> colKeys = getKeysAsList(width);

        ArrayTable<Integer, Integer, Object> dataTable = ArrayTable.create(rowKeys, colKeys);
        ArrayList<Integer> labels = new ArrayList<>(height);

        int rowIdx = 0;
        for(Map.Entry<String, ByteBuffer> entry : keys.entrySet()) {
            int colIdx = 0;
            for(Map.Entry<String, ByteBuffer> columnEntry : columns.entrySet()) {
                if(columnEntry.getKey().equals("label")) {
                    labels.add(rowIdx, ByteBufferUtil.toInt(entry.getValue()));
                }
                else {
                    dataTable.set(rowIdx, colIdx, ByteBufferUtil.toFloat(columnEntry.getValue()));
                }
                colIdx++;
            }
            rowIdx++;
        }

        return new Object[] {dataTable, labels};
    }

    private List<Integer> getKeysAsList(int size) {
        Integer[] keys = new Integer[size];
        for(int i = 0; i < size; i++) {
            keys[i] = i;
        }

        return Arrays.asList(keys);
    }
}
