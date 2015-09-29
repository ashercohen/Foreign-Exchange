package hw1_data_prep;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowDef;
import cascading.flow.hadoop2.Hadoop2MR1FlowConnector;
import cascading.pipe.*;
import cascading.pipe.assembly.Discard;
import cascading.pipe.joiner.InnerJoin;
import cascading.property.AppProps;
import cascading.scheme.hadoop.TextDelimited;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;

import java.util.Arrays;
import java.util.Properties;
 
/**
 * Created by Asher Cohen asherc@andrew
 */
public class Main implements Runnable {
    /**
     * hw1_data_prep.Main class of the data preparation assignment where the flow is defined
     */

    //currency pairs used for prediction (features are generated from these)
    private static final Currencies[] CURRENCIES = new Currencies[]{
            Currencies.AUDJPY,
            Currencies.AUDNZD,
            Currencies.AUDUSD,
    };
 
    private String inPath; //path in HDFS where raw data exists
    private String outPath; //output path (HDFS)


    public Main(String inPath, String outPath) {
        this.inPath = inPath;
        this.outPath = outPath;
    }
 
    public static void main(String[] args) {
 
        if(args.length != 2) {
            System.err.println("Usage: hw1_data_prep.Main <input path> <output path>");
            System.exit(1);
        }
 
        new Thread(new Main(args[0], args[1])).start();
    }
 
    @Override
    public void run() {
 
        runDataPreparationFlow();
    }
 
    private void runDataPreparationFlow() {
 
        /**
         * first assume all files are located in one dir
         * later, once the logic is done, probably need to create multi source taps
         */
 
        Properties properties = new Properties();
        AppProps.setApplicationJarClass(properties, Main.class);
        FlowConnector flowConnector = new Hadoop2MR1FlowConnector(properties);
 
        Tap inTap = new Hfs(new TextLine(new Fields(LINE)), this.inPath);
        Tap outTap = new Hfs(new TextDelimited(false, ","), this.outPath, SinkMode.REPLACE);

        /**
         * Parse input
         * from each input line extract the following fields
         */
        InputParser parser = new InputParser(new Fields(TIME_INTERVAL,
                                                        DATE_TIME,
														CURRENCY_PAIR,
                                                        ASK_PRICE,
                                                        BID_PRICE));
        Pipe parsePipe = new Each("parsePipe", parser, Fields.RESULTS); //actually apply the parser function

        /**
         * process time interval:
         * for all input lines that belong to a single currencies pair in the
         * time interval extract min/max/close values for both ask & bid
         */
        //group by TIME_INTERVAL
        GroupBy groupByPipe = new GroupBy("groupByPipe", parsePipe, new Fields(TIME_INTERVAL, CURRENCY_PAIR));
 
        //for each group (time interval) find lowValue, highValue, closePrice and emit together with rounded dateTime
        MinMaxCloseBuffer minMaxCloseBuffer = new MinMaxCloseBuffer(new Fields(TIME_INTERVAL,
                                                                               CURRENCY_PAIR,
                                                                               ASK_MIN,
                                                                               ASK_MAX,
                                                                               ASK_CLOSE,
                                                                               BID_MIN,
                                                                               BID_MAX,
                                                                               BID_CLOSE));
        //apply the buffer (reducer) on the group of lines
        Every minMaxClosePipe = new Every(groupByPipe, Fields.ALL, minMaxCloseBuffer, Fields.RESULTS);

        //split flow - handle rows belong to features and rows belong to labels differently
        Pipe euroDollarPipe = new Pipe("euroDollarPipe", minMaxClosePipe); //label
        Pipe otherCurrenciesPipe = minMaxClosePipe; //features
 
        /**
         * EUR/USD flow - generate a label for each time interval
         */
        //filter all but EUR/USD tuples
        EuroUSDFilter eurUsdFilter = new EuroUSDFilter(Fields.ALL);
        euroDollarPipe = new Each(euroDollarPipe, Fields.ALL, eurUsdFilter);
 
        //calculate average price for EUR/USD - send price for current + previous time interval
        EuroUSDAverageFunction euroUSDAverageFunction = new EuroUSDAverageFunction(
                                                                    new Fields(TIME_INTERVAL,
                                                                               ORIGINAL_DATE_TIME,
                                                                               AVERAGE_PRICE));
        euroDollarPipe = new Each(euroDollarPipe, Fields.ALL, euroUSDAverageFunction, Fields.RESULTS);
 
        //group tuples by time interval (TIME_INTERVAL) - each group should have 2 tuples
        euroDollarPipe = new GroupBy(euroDollarPipe, new Fields(TIME_INTERVAL));
 
        //calculate label for each time interval using 2 tuples (current, previous)
        EuroUSDLabelBuffer euroUSDLabelBuffer = new EuroUSDLabelBuffer(
                                                                    new Fields(TIME_INTERVAL,
                                                                               IS_VALUE_INCREASED));
        euroDollarPipe = new Every(euroDollarPipe, Fields.ALL, euroUSDLabelBuffer, Fields.RESULTS);
 
        /**
         * other currencies - generate features
         */
        //filer EUR/USD tuples
        OtherCurrenciesFilter otherCurrenciesFilter = new OtherCurrenciesFilter(Fields.ALL);
        otherCurrenciesPipe = new Each(otherCurrenciesPipe, Fields.ALL, otherCurrenciesFilter);
 
        //group by time interval in order to produce one row of features per time interval
        otherCurrenciesPipe = new GroupBy(otherCurrenciesPipe, new Fields(TIME_INTERVAL));
 
        Fields features = FeaturesGeneratorBuffer.generateFieldsByCurrencies(Arrays.asList(CURRENCIES));
        FeaturesGeneratorBuffer featuresGeneratorBuffer = new FeaturesGeneratorBuffer(Arrays.asList(CURRENCIES),
                                                                                      features);
        otherCurrenciesPipe = new Every(otherCurrenciesPipe, Fields.ALL, featuresGeneratorBuffer, Fields.RESULTS);
 
        /**
         * join pipe by time interval
         */
        Fields joinedStreamFields = features.append(new Fields(TIME_INTERVAL_JOINED_KEY, IS_VALUE_INCREASED));
        CoGroup joinedPipe = new CoGroup(otherCurrenciesPipe,       //lhs
                                         new Fields(TIME_INTERVAL), //lhs key
                                         euroDollarPipe,            //rhs
                                         new Fields(TIME_INTERVAL), //rhs key
                                         joinedStreamFields,        //output fields - since join fields can't appear using the same name twice
                                         new InnerJoin());          //don't allow missing values on either side - must have features + label on every row

        Discard finalPipe = new Discard(joinedPipe, new Fields(TIME_INTERVAL, TIME_INTERVAL_JOINED_KEY));

        FlowDef flowDef = FlowDef.flowDef()
                .setName("Forex Data")
                .addSource(parsePipe, inTap)
                .addTailSink(finalPipe, outTap);
 
        Flow flow = flowConnector.connect(flowDef);
        flow.complete();
    }

    //constants - fields names
    public static final String LINE = "line";
    public static final String TIME_INTERVAL = "timeInterval";
    public static final String ORIGINAL_DATE_TIME = "OriginalDateTime";
    public static final String IS_VALUE_INCREASED = "isValueIncreased";
    public static final String DATE_TIME = "dateTime";
    public static final String CURRENCY_PAIR = "currency_pair";
    public static final String ASK_PRICE = "lowPrice";
    public static final String BID_PRICE = "highPrice";
    public static final String AVERAGE_PRICE = "avgPrice";
    public static final String ASK_MIN = "askMin";
    public static final String ASK_MAX = "askMax";
    public static final String ASK_CLOSE = "askClose";
    public static final String BID_MIN = "bidMin";
    public static final String BID_MAX = "bidMax";
    public static final String BID_CLOSE = "bidClose";
    public static final String TIME_INTERVAL_JOINED_KEY = "timeIntervalJoinedKey";
}
