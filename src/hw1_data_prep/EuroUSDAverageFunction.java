package hw1_data_prep;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import org.joda.time.DateTime;

/**
 * Created by Asher Cohen asherc@andrew
 */
public class EuroUSDAverageFunction extends BaseOperation implements Function {

    /**
     * function (mapper) to calculate avg of min,max,close values of EUR/USD
     * this value will help determine directionality
     */

    public EuroUSDAverageFunction(Fields outputFields) {
        super(outputFields);
    }


    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {

        //input tuple: TIME_INTERVAL, CURRENCY_PAIR, ASK_MIN, ASK_MAX, ASK_CLOSE, BID_MIN, BID_MAX, BID_CLOSE

        //2 output tuples:
        // TIME_INTERVAL, ORIGINAL_DATE_TIME, AVERAGE_PRICE
        // TIME_INTERVAL - 1 MINUTE, ORIGINAL_DATE_TIME, AVERAGE_PRICE

        TupleEntry tupleEntry = functionCall.getArguments();
        double sum = 0D;
        sum += tupleEntry.getDouble(Main.ASK_MIN);
        sum += tupleEntry.getDouble(Main.ASK_MAX);
        sum += tupleEntry.getDouble(Main.ASK_CLOSE);
        sum += tupleEntry.getDouble(Main.BID_MIN);
        sum += tupleEntry.getDouble(Main.BID_MAX);
        sum += tupleEntry.getDouble(Main.BID_CLOSE);

        double avgPrice = sum / 6;

        DateTime dateTime = DateTime.parse(tupleEntry.getString(Main.TIME_INTERVAL));
        DateTime dateTimeMinusOneMinute = dateTime.minusMinutes(1);


        TupleEntry outTuple = new TupleEntry(new Fields(Main.TIME_INTERVAL,
                                                        Main.ORIGINAL_DATE_TIME,
                                                        Main.AVERAGE_PRICE),
                                             Tuple.size(3));

        //emit <dateTime> <dateTime> <avg>
        outTuple.setString(Main.TIME_INTERVAL, dateTime.toString());
        outTuple.setString(Main.ORIGINAL_DATE_TIME, dateTime.toString());
        outTuple.setDouble(Main.AVERAGE_PRICE, avgPrice);
        functionCall.getOutputCollector().add(outTuple.getTuple());

        //emit <dateTime - 1> <dateTime> <avg>
        outTuple.setString(Main.TIME_INTERVAL, dateTimeMinusOneMinute.toString());
        outTuple.setString(Main.ORIGINAL_DATE_TIME, dateTime.toString());
        outTuple.setDouble(Main.AVERAGE_PRICE, avgPrice);
        functionCall.getOutputCollector().add(outTuple.getTuple());
    }
}
