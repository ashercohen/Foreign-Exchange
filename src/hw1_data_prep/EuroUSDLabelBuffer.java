package hw1_data_prep;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import org.joda.time.DateTime;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by Asher Cohen asherc@andrew
 */
public class EuroUSDLabelBuffer extends BaseOperation implements Buffer {

    /**
     * Buffer (reducer) that calculates the directionality of EUR/USD
     * between consecutive time intervals
     */


    public EuroUSDLabelBuffer(Fields outputFields) {
        super(outputFields);
    }

    @Override
    public void operate(FlowProcess flowProcess, BufferCall bufferCall) {

        //input tuple: TIME_INTERVAL, ORIGINAL_DATE_TIME, AVERAGE_PRICE

        Iterator<TupleEntry> tupleEntryIterator = bufferCall.getArgumentsIterator();
        List<Tuple> tuples = new ArrayList<>();
        boolean isValueIncreased;

        while(tupleEntryIterator.hasNext()) {
            tuples.add(tupleEntryIterator.next().getTupleCopy()); //must get a copy otherwise it will be the same tuple object
        }

        switch(tuples.size()) {
            case 1:
                //TODO:
                isValueIncreased = false;
                break;
            case 2:
                Tuple t1 = tuples.get(0);
                Tuple t2 = tuples.get(1);
                double price1 = t1.getDouble(2);
                double price2 = t2.getDouble(2);
                DateTime dt1 = DateTime.parse(t1.getString(1));
                DateTime dt2 = DateTime.parse(t2.getString(1));
                isValueIncreased = dt1.compareTo(dt2) < 0 ? (price2 - price1 > 0) : (price1 - price2 > 0);
                break;

            default:
                throw new RuntimeException("unexpected number of tuples for time interval: " + tuples.size());
        }

        TupleEntry outTuple = new TupleEntry(new Fields(Main.TIME_INTERVAL,
                                                        Main.IS_VALUE_INCREASED),
                                             Tuple.size(2));

        TupleEntry group = bufferCall.getGroup();
        outTuple.setString(Main.TIME_INTERVAL, group.getString(Main.TIME_INTERVAL));
        outTuple.setInteger(Main.IS_VALUE_INCREASED, isValueIncreased ? 1 : 0);

        bufferCall.getOutputCollector().add(outTuple.getTuple());
    }
}
