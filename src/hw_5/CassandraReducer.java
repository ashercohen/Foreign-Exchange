package hw_5;

import hw2_dt.DT;
import hw3_rf.RandomForest;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Asher
 */
public class CassandraReducer extends Reducer<IntWritable, Text, Map<String, ByteBuffer>, List<ByteBuffer>> {

    private ByteBuffer key;
    private Map<String, ByteBuffer> keys;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        this.keys = new LinkedHashMap<>();
    }

    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        RandomForest.Forest forest = new RandomForest.Forest();

        //add all trees to forest
        for(Text text : values) {
            String json = text.toString(); //extract string
            DT.Node tree = DT.Node.deserializeFromJson(json); //deserialize from json
            forest.add(tree);
        }

        String jsonForest = forest.toJson(); //serialize forest to json

        this.keys.put("forest", ByteBufferUtil.bytes("forest"));
        List<ByteBuffer> valueList = new ArrayList<>();
        valueList.add(ByteBufferUtil.bytes(jsonForest));
        context.write(this.keys, valueList);
    }
}
