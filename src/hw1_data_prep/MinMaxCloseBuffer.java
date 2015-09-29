package hw1_data_prep;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import org.joda.time.DateTime;

import java.util.Iterator;

/**
 * Created by Asher Cohen asherc@andrew
 */
public class MinMaxCloseBuffer extends BaseOperation implements Buffer {

    /**
     * Buffer (reducer) that calculates the min,max and close values, both for ask and bid,
     * at a single time interval for a fixed currencies pair
     */

    public MinMaxCloseBuffer(Fields outputFields) {
        super(outputFields);
    }

    @Override
    public void operate(FlowProcess flowProcess, BufferCall bufferCall) {

        //input tuple: TIME_INTERVAL, DATE_TIME, CURRENCY_PAIR, ASK_PRICE, BID_PRICE
        //output tuple: TIME_INTERVAL, CURRENCY_PAIR, ASK_MIN, ASK_MAX, ASK_CLOSE, BID_MIN, BID_MAX, BID_CLOSE

        double askMin = Double.POSITIVE_INFINITY;
        double askMax = Double.NEGATIVE_INFINITY;
        double askClose = 0L;
        double bidMin = Double.POSITIVE_INFINITY;
        double bidMax = Double.NEGATIVE_INFINITY;
        double bidClose = 0L;
        DateTime closeEntry = DateTime.parse("2000-01-01T01:00:00.000Z"); //dummy

        Iterator<TupleEntry> tupleEntryIterator = bufferCall.getArgumentsIterator();

        while(tupleEntryIterator.hasNext()) {
            TupleEntry tupleEntry = tupleEntryIterator.next();

            double ask = tupleEntry.getDouble(Main.ASK_PRICE);
            askMin = ask < askMin ? ask : askMin;
            askMax = ask > askMax ? ask : askMax;

            double bid = tupleEntry.getDouble(Main.BID_PRICE);
            bidMin = bid < bidMin ? bid : bidMin;
            bidMax = bid > bidMax ? bid : bidMax;

            DateTime dateTime = DateTime.parse(tupleEntry.getString(Main.DATE_TIME));
            if(dateTime.compareTo(closeEntry) > 0) {
                askClose = ask;
                bidClose = bid;
                closeEntry = dateTime;
            }
        }

        //output tuple: TIME_INTERVAL, CURRENCY_PAIR, ASK_MIN, ASK_MAX, ASK_CLOSE, BID_MIN, BID_MAX, BID_CLOSE
        TupleEntry outTuple = new TupleEntry(new Fields(Main.TIME_INTERVAL,
                                                        Main.CURRENCY_PAIR,
                                                        Main.ASK_MIN,
                                                        Main.ASK_MAX,
                                                        Main.ASK_CLOSE,
                                                        Main.BID_MIN,
                                                        Main.BID_MAX,
                                                        Main.BID_CLOSE),
                                             Tuple.size(8));

        TupleEntry group = bufferCall.getGroup();
        outTuple.setString(Main.TIME_INTERVAL, group.getString(Main.TIME_INTERVAL));
        outTuple.setString(Main.CURRENCY_PAIR, group.getString(Main.CURRENCY_PAIR));
        outTuple.setDouble(Main.ASK_MIN, askMin);
        outTuple.setDouble(Main.ASK_MAX, askMax);
        outTuple.setDouble(Main.ASK_CLOSE, askClose);
        outTuple.setDouble(Main.BID_MIN, bidMin);
        outTuple.setDouble(Main.BID_MAX, bidMax);
        outTuple.setDouble(Main.BID_CLOSE, bidClose);

        bufferCall.getOutputCollector().add(outTuple.getTuple());
    }
}
