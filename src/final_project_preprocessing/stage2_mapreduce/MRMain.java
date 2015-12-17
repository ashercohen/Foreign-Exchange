package final_project_preprocessing.stage2_mapreduce;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowDef;
import cascading.flow.hadoop2.Hadoop2MR1FlowConnector;
import cascading.operation.Insert;
import cascading.operation.expression.ExpressionFunction;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.HashJoin;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.*;
import cascading.property.AppProps;
import cascading.scheme.hadoop.TextDelimited;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;

import java.util.Properties;

/**
 * Created by Asher Cohen asherc@andrew
 */
public class MRMain implements Runnable {

    public static final String LINE = "line";
    public static final String DOC_ID = "doc_id";
    public static final String TOKEN = "token";
    public static final String TF_TOKEN = "tf_token";
    public static final String TFIDF = "tfidf";
    public static final String COUNT = "count";
    public static final String TF_COUNT = "tf_count";
    public static final String DF_COUNT = "df_count";
    public static final String N_DOCS = "n_docs";
    public static final String TALLY = "tally";
    public static final String RHS_JOIN = "rhs_join";

    private String inputPath;
    private String tfidfOutPath;

    public MRMain(String inputPath, String outputPath) {
        this.inputPath = inputPath;
        this.tfidfOutPath = outputPath + "/tfidf";
    }

    public static void main(String[] args) {

        if(args.length != 2) {
            System.err.println("Usage: hadoop jar <jar name> <input path> <output path>");
            System.exit(1);
        }

        new Thread(new MRMain(args[0], args[1])).start();
    }


    @Override
    public void run() {
        runTFIDFFlow();
    }

    public void runTFIDFFlow() {

        Properties properties = new Properties();
        AppProps.setApplicationJarClass(properties, MRMain.class);
        FlowConnector flowConnector = new Hadoop2MR1FlowConnector(properties);

        // create source and sink taps
        Tap inputTap = new Hfs(new TextLine(new Fields(LINE)), this.inputPath);
        Tap tfidfOutTap = new Hfs(new TextDelimited(false, ","), tfidfOutPath, SinkMode.REPLACE);

        /**
         * parse input
         */
        InputParser parser = new InputParser(new Fields(DOC_ID, TOKEN));
        Pipe parsePipe = new Each("parsePipe", parser, Fields.RESULTS); //actually apply the parser function

        // one branch of the flow tallies the token counts for term frequency (TF)
        Pipe tfPipe = new Pipe("TF", parsePipe);
        tfPipe = new CountBy(tfPipe,
                             new Fields("doc_id", "token"),
                            new Fields(TF_COUNT));

        tfPipe = new Rename(tfPipe,
                            new Fields(TOKEN),
                            new Fields(TF_TOKEN));

        // one branch counts the number of documents (D)
        Pipe dPipe = new Unique("D", parsePipe, new Fields(DOC_ID));
        dPipe = new Each(dPipe,
                        new Insert(new Fields(TALLY), 1),
                        Fields.ALL);
        dPipe = new Each(dPipe,
                        new Insert(new Fields(RHS_JOIN), 1),
                        Fields.ALL);
        dPipe = new SumBy(dPipe,
                          new Fields(RHS_JOIN),
                          new Fields(TALLY),
                          new Fields(N_DOCS),
                          long.class);

        // one branch tallies the token counts for document frequency (DF)
        Pipe dfPipe = new Unique("DF", parsePipe, Fields.ALL);

        dfPipe = new CountBy(dfPipe,
                            new Fields(TOKEN),
                            new Fields(DF_COUNT));

        Fields df_token = new Fields("df_token");
        Fields lhs_join = new Fields("lhs_join");
        dfPipe = new Rename(dfPipe,
                            new Fields(TOKEN),
                            df_token);
        dfPipe = new Each(dfPipe, new Insert(lhs_join, 1), Fields.ALL);

        // join to bring together all the components for calculating TF-IDF
        Pipe idfPipe = new HashJoin(dfPipe, lhs_join, dPipe, new Fields(RHS_JOIN));

        // the IDF side of the join is smaller, so it goes on the RHS
        Pipe tfidfPipe = new CoGroup(tfPipe, new Fields(TF_TOKEN), idfPipe, df_token);

        // calculate the TF-IDF weights, per token, per document
        String expression = "Math.log( (double) tf_count ) * Math.log( (double) n_docs / ( 1.0 + df_count ) )";
        ExpressionFunction tfidfExpression = new ExpressionFunction(new Fields(TFIDF), expression, Double.class);
        tfidfPipe = new Each(tfidfPipe, new Fields(TF_COUNT, DF_COUNT, N_DOCS), tfidfExpression, Fields.ALL);


        tfidfPipe = new Retain(tfidfPipe, new Fields(TF_TOKEN, DOC_ID, TFIDF));

        FilterByThreshold filterByThreshold = new FilterByThreshold();
        tfidfPipe = new Each(tfidfPipe, filterByThreshold);
        tfidfPipe = new Rename(tfidfPipe, new Fields(TF_TOKEN), new Fields(TOKEN));

        // connect the taps, pipes, etc., into a flow
        FlowDef flowDef = FlowDef.flowDef()
                .setName("tfidf")
                .addSource(parsePipe, inputTap)
                .addTailSink(tfidfPipe, tfidfOutTap);


        // write a DOT file and run the flow
        Flow tfidfFlow = flowConnector.connect(flowDef);
        tfidfFlow.writeDOT("dot/tfidf.dot");
        tfidfFlow.complete();
    }
}

