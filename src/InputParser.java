import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 * Created by Asher Cohen asherc@andrew
 */
public class InputParser extends BaseOperation implements Function {

    /**
     * Function (mapper) that parses input line into tuples
     *
     */


    //TODO: look at cascading.operation.Debug prepare() & cleanup() for usage of Context generic: maybe allocate dateTimeFormatter + tupleEntry
//    private static DateTimeFormatter dateTimeFormatter = DateTimeFormat.forPattern("yyyyMMdd HH:mm:ss.SSS");

    public InputParser(Fields outputFields) {
        super(outputFields);
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        /**
         * outFields: TIME_INTERVAL, DATE_TIME, CURRENCY_PAIR, ASK_PRICE, BID_PRICE
         */
        TupleEntry tupleEntry = functionCall.getArguments();
        String line = tupleEntry.getString(Main.LINE);

        if(line == null || line.isEmpty()) {
            return;
        }
        DateTimeFormatter dateTimeFormatter = DateTimeFormat.forPattern("yyyyMMdd HH:mm:ss.SSS");

        // <currencies>, <datetime>, <ask_price>, <bid_price>  //ask < bid
        // AUD/USD,20100103 21:48:11.267,0.89871,0.89886
        String[] parts = line.split(",");
        if(parts.length != 4) return;

        DateTime dateTime = DateTime.parse(parts[1], dateTimeFormatter);
        DateTime roundedDateTime = dateTime.withMillisOfSecond(0).withSecondOfMinute(0);
        double askPrice = Double.parseDouble(parts[2]);
        double bidPrice = Double.parseDouble(parts[3]);

        TupleEntry outTuple = new TupleEntry(new Fields(Main.TIME_INTERVAL,
                                                        Main.DATE_TIME,
                                                        Main.CURRENCY_PAIR,
                                                        Main.ASK_PRICE,
                                                        Main.BID_PRICE),
                                             Tuple.size(5));

        outTuple.setString(Main.TIME_INTERVAL, roundedDateTime.toString());
        outTuple.setString(Main.DATE_TIME, dateTime.toString());
        outTuple.setString(Main.CURRENCY_PAIR, parts[0]);
        outTuple.setDouble(Main.ASK_PRICE, askPrice);
        outTuple.setDouble(Main.BID_PRICE, bidPrice);

        functionCall.getOutputCollector().add(outTuple.getTuple());
    }
}
