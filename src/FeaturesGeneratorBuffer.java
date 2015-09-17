import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

import java.util.*;

/**
 * Created by Asher Cohen asherc@andrew
 */
public class FeaturesGeneratorBuffer extends BaseOperation implements Buffer {

    /**
     * Buffer (reducer) that generates one row of features from multiple
     * rows that all belong to the same time interval but have different
     * currencies pairs
     */

    private static final String[] BASIC_FEATURES = new String[]{Main.ASK_MIN,
                                                                Main.ASK_MAX,
                                                                Main.ASK_CLOSE,
                                                                Main.BID_MIN,
                                                                Main.BID_MAX,
                                                                Main.BID_CLOSE};

    private List<Currencies> currencies;

    public FeaturesGeneratorBuffer(List<Currencies> currencies, Fields outputFields) {
        super(outputFields);
        this.currencies = currencies; //TODO: maybe use context
    }

    @Override
    public void operate(FlowProcess flowProcess, BufferCall bufferCall) {

        //input tuple: TIME_INTERVAL, CURRENCY_PAIR, ASK_MIN, ASK_MAX, ASK_CLOSE, BID_MIN, BID_MAX, BID_CLOSE
        // grouped by TIME_INTERVAL

        Iterator<TupleEntry> tupleEntryIterator = bufferCall.getArgumentsIterator();
        Map<Currencies, Tuple> tupleMap = new EnumMap<Currencies, Tuple>(Currencies.class);

        //collect all tuples for this time interval
        while(tupleEntryIterator.hasNext()) {
            TupleEntry tupleEntry = tupleEntryIterator.next();
            String curStr = tupleEntry.getString(Main.CURRENCY_PAIR).replace("/", "");
            tupleMap.put(Currencies.safeValueOf(curStr), tupleEntry.getTupleCopy());
        }

        //generate fields according to the list of currencies pairs used to predict eur/usd
        Fields fields = FeaturesGeneratorBuffer.generateFieldsByCurrencies(this.currencies);
        TupleEntry outTuple = new TupleEntry(fields, Tuple.size(fields.size()));

        TupleEntry group = bufferCall.getGroup();
        outTuple.setString(Main.TIME_INTERVAL, group.getString(Main.TIME_INTERVAL));

        for(Currencies currency : this.currencies) {
            Tuple tuple = tupleMap.get(currency);

            if(tuple != null) {
                //a tuple for these currencies exists
                for(int i = 0; i < BASIC_FEATURES.length; i++) {
                    outTuple.setDouble(currency + "_" + BASIC_FEATURES[i], tuple.getDouble(i + 2));
                }
            }
            else {
                //no tuple
                for(int i = 0; i < BASIC_FEATURES.length; i++) {
                    outTuple.setObject(currency + "_" + BASIC_FEATURES[i], null);
                }
            }
        }

        bufferCall.getOutputCollector().add(outTuple.getTuple());
    }

    public static Fields generateFieldsByCurrencies(List<Currencies> currenciesList) {

        Currencies[] array = currenciesList.toArray(new Currencies[currenciesList.size()]);
        Arrays.sort(array);
        List<String> fieldNames = new ArrayList<>(array.length * 6);
        fieldNames.add(Main.TIME_INTERVAL);

        for(Currencies elem : array) {
            for(String suffix : BASIC_FEATURES) {
                fieldNames.add(elem.toString() + "_" + suffix);
            }
        }

        return new Fields(fieldNames.toArray(new String[fieldNames.size()]));
    }
}
