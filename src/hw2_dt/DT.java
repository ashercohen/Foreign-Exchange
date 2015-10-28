package hw2_dt;

import com.google.common.collect.ArrayTable;
import com.google.gson.Gson;

import java.io.*;
import java.util.*;

/**
 * Created by Asher
 */
public class DT implements Runnable {

    protected ArrayTable<Integer, Integer, Float> X;
    protected List<Integer> Y;
    protected ArrayTable<Integer, Integer, Float> Xtest;
    protected List<Integer> Ytest;
    protected String trainDataSet;
    protected String testDataSet;
    protected int trainWidth;
    protected int trainHeight;
    protected int testWidth;
    protected int testHeight;
    private Random random;
    protected int depth;


    public DT(String trainDataSet, String testDataSet) {
        this.trainDataSet = trainDataSet;
        this.testDataSet = testDataSet;
        this.random = new Random(System.currentTimeMillis());
    }

    public static void main(String[] args) throws IOException {
        if(args.length != 2) {
            System.err.println("Usage: DT <train> <test>");
            System.exit(1);
        }

        new Thread(new DT(args[0], args[1])).start();
    }

    @Override
    public void run() {

        long start = System.currentTimeMillis();
        try {
            String jsonTreeFileName = this.trainDataSet.replace(".txt", "_tree.txt");

            //try load from file
            Node tree = Node.deserialize(jsonTreeFileName);
            boolean ignoreWidth = true;
            //serialized tree not exists - train
            if(tree == null) {

                System.out.println("learning tree from examples...");
                List<Integer> rowKeys = loadTrainDataSet();
                List<Integer> columns = getKeysAsList(this.trainWidth);
                tree = learnDecisionTree(rowKeys, columns);
//                tree.printTree();
                tree.serialize(jsonTreeFileName);
                ignoreWidth = false;
            }

            List<Integer> testRowKeys = loadTestDataSet(ignoreWidth);
            double accuracy = testDecisionTree(tree, testRowKeys);
            System.out.println("accuracy = " + accuracy);
        }
        catch(IOException e) {
            e.printStackTrace();
        }

        long finish = System.currentTimeMillis();
        System.out.println("tree depth = " + this.depth);
        System.out.println("total runtime (sec): " + (finish - start) / 1000);
    }

    public Node learnDecisionTree(List<Integer> rowKeys, List<Integer> columns) throws IOException {

//        System.out.println("learnDecisionTree(" + rowKeys.size() +")");
        if(checkIdenticalLabels(rowKeys)) {
            //return node predicting the label all rows have
            return new Node(this.Y.get(rowKeys.get(0)));
        }

        if(checkIdenticalRows(rowKeys, columns)) {
            //return node predicting majority of labels or random if tie
            return new Node(majorityLabelVote(rowKeys));
        }

        //find attribute index (j) with maximum info gain together with its split value (threshold)
        double max_IG_Y_given_X = 0D;
        float threshold = 0F;
        int featureToSplit = -1;
        for(Integer colIdx : columns) {
//        for(int j = 0; j < this.trainWidth; j++) {
            //find info gain together with its split value t for attribute j
            Object[] rvs = calculateInformationGainAndThreshold(colIdx, rowKeys);
            double IG_Y_given_X = (double)rvs[0];
            if(IG_Y_given_X >= max_IG_Y_given_X) {
                max_IG_Y_given_X = IG_Y_given_X;
                threshold = (float)rvs[1];
                featureToSplit = colIdx;
            }
        }

        if(featureToSplit == -1) {
            //no attribute can't discriminate between these rows - return majority vote (or random in case of tie)
            return new Node(majorityLabelVote(rowKeys));
        }
        //split on featureToSplit using threshold
        //calc X_LO, Y_LO, X_HI, Y_HI
        List<Integer>[] splits = splitDataSetByThreshold(rowKeys, featureToSplit, threshold);

        /**
         * in case the threshold assigns all samples to the same group just return majority vote
         * this can happen when there are multiple examples have the same value (for the attribute
         * that maximizes IG) and the threshold is one of these values
         */
        if(splits[0].size() == 0 || splits[1].size() == 0) {
            return new Node(majorityLabelVote(rowKeys));
        }

        //call recursively on LO, HI
        this.depth++;
        Node left = learnDecisionTree(splits[0], columns);
        Node right = learnDecisionTree(splits[1], columns);

        return new Node(threshold, featureToSplit, left, right);
    }

//    public double testDecisionTree(Node tree) {
//        return testDecisionTree(tree, this.X.rowKeyList());
//    }

    public double testDecisionTree(Node tree, List<Integer> testRowKeys) {

        return tree.classifyTestDataSet(this.Xtest, this.Ytest, testRowKeys);

//        int total = 0;
//        int errors = 0;
//
//        for(Integer rowIdx : testRowKeys) {
//            Map<Integer, Float> row = this.Xtest.row(rowIdx);
//            int expected = this.Ytest.get(rowIdx);
//            int actual = tree.classifyExample(row);
//
//            if(expected != actual) {
//                errors++;
//            }
//            total++;
//        }
//
//        return ((double)errors) / total;
    }

    private List<Integer>[] splitDataSetByThreshold(List<Integer> rowKeys, int featureToSplit, float threshold) {
        List<Integer> xLo = new ArrayList<>();
        List<Integer> xHi = new ArrayList<>();


        for(Integer row : rowKeys) {
            Float cell = this.X.at(row, featureToSplit);
            if(cell == null) {
                //coin flip
                if(this.random.nextBoolean()) {
                    xLo.add(row);
                }
                else {
                    xHi.add(row);
                }
            }
            else if(cell <= threshold) {
                xLo.add(row);
            }
            else
                xHi.add(row);
        }

        List<Integer>[] rv = new ArrayList[2];
        rv[0] = xLo;
        rv[1] = xHi;

        return rv;
    }

    private Object[] calculateInformationGainAndThreshold(int colIndex, List<Integer> rowKeys) {

        //sort column values + corresponding labels by values (asc)
        List<Map.Entry<Float, Integer>> pairs = new ArrayList<>(rowKeys.size());
        //iterate through cells in column and create a list of pairs (cell value, label) for indexes defined in rowKeys
        for(Integer idx : rowKeys) {
            Float val = this.X.at(idx, colIndex);
            if(val == null) {
                continue; //skip entries with missing cells - not calculating threshold and IG based on nulls
            }
            pairs.add(new AbstractMap.SimpleEntry<Float, Integer>(val, this.Y.get(idx)));
        }

        if(pairs.size() <= 1) {
            //max one non-nul cell - can't discriminate based on this column
            return new Object[] {-1D};
        }
        Collections.sort(pairs, new Comparator<Map.Entry<Float, Integer>>() {
            @Override
            public int compare(Map.Entry<Float, Integer> o1, Map.Entry<Float, Integer> o2) {
                return o1.getKey().compareTo(o2.getKey());
            }
        });

        List<Integer> labelKeys = new ArrayList<>(pairs.size());
        List<Integer> sortedLabels = new ArrayList<>(pairs.size());
        int idx = 0;
        for(Map.Entry<Float, Integer> pair : pairs) {
            if(pair.getKey() == null) {
                //can't relate to missing values when need to find threshold
                throw new RuntimeException("pairs contain null values - should not happen!");
            }
            labelKeys.add(idx++);
            sortedLabels.add(pair.getValue());
        }

        //calculate entropy of labels
        double H_Y = entropy(sortedLabels, labelKeys);

        //iterate through all possible thresholds: (total labelKeys.size() - 1 values)
        //- calc conditional entropy of label where value is below & above threshold
        //- calc probability of values to be below & above threshold
        double IG_Y_given_X = 0D;
        int bestThresholdIdx = -1;
        for(int tIdx = 1; tIdx < labelKeys.size(); tIdx++) {
            double IG_Y_given_X_with_t = informationGainConditionedOnThreshold(H_Y, sortedLabels, labelKeys, tIdx);
            if(IG_Y_given_X_with_t > IG_Y_given_X) {
                IG_Y_given_X = IG_Y_given_X_with_t;
                bestThresholdIdx = tIdx;
            }
        }

        if(IG_Y_given_X == 0D) {
            return new Object[]{-1D};
        }

        if(bestThresholdIdx < 0 && IG_Y_given_X != 0D) {
            throw new RuntimeException("bestThresholdIdx < 0 && IG_Y_given_X != 0D - should not happen!");
        }

        //we got best threshold index => need to convert it into a value: average between value at index t-1 and at t
        if(bestThresholdIdx > 0 && (pairs.get(bestThresholdIdx - 1).getKey() == null || pairs.get(bestThresholdIdx).getKey() == null)) {
            throw new RuntimeException("pairs contain null values - should not happen!");
        }
        Float threshold = bestThresholdIdx > 0
                                ? (pairs.get(bestThresholdIdx - 1).getKey() + pairs.get(bestThresholdIdx).getKey()) / 2F
                                : 0F;

        return new Object[]{IG_Y_given_X, threshold};
    }

    /**
     * calculate IG(Y|X:t) information gain for real valued X with threshold t
     *
     * IG(Y|X:t) = H(Y) - H(Y|X:t)
     *           = H(Y) - H(Y|X<t)*Pr(X<t) - H(Y|X>=t)*Pr(X>=t)
     *
     * labels are sorted by the corresponding values of X
     */
    private double informationGainConditionedOnThreshold(double H_Y, List<Integer> sortedLabels, List<Integer> labelKeys, int thresholdIndex) {

        double H_y_given_x_lt_t = entropy(sortedLabels, labelKeys, 0, thresholdIndex);
        double H_y_given_x_gte_t = entropy(sortedLabels, labelKeys, thresholdIndex, labelKeys.size());
        double totalCount = (double)labelKeys.size();
        double pr_x_lt_t = thresholdIndex / totalCount;
        double pr_x_gte_t = (totalCount - thresholdIndex) / totalCount;

        return H_Y - H_y_given_x_lt_t * pr_x_lt_t - H_y_given_x_gte_t * pr_x_gte_t;
    }

    /**
     * compute entropy of labels in indexes specified by labelKeys list
     */
    private double entropy(List<Integer> labels, List<Integer> labelKeys) {
        return entropy(labels, labelKeys, 0, labelKeys.size());
    }

    /**
     * compute entropy of labels in indexes specified by certain range in rowKeys list
     */
    private double entropy(List<Integer> labels, List<Integer> labelKeys, int fromIndex, int toIndex) {

        int oneCount = 0;
        for(int i = fromIndex; i < toIndex; i++) {
            oneCount += labels.get(labelKeys.get(i));
        }
        int totalCount = toIndex - fromIndex;
        if(oneCount == 0 || oneCount == totalCount) {
            //all 0's or all 1's
            return 0D;
        }

        double totalCountDouble = (double)totalCount;
        double prOne = oneCount / totalCountDouble;
        double prZero = (totalCount - oneCount) / totalCountDouble;

        return - prOne * log2(prOne) - prZero * log2(prZero);
    }

    private static final double LOG2 = Math.log(2);
    private static double log2(double x) {
        return Math.log(x) / LOG2;
    }

    private boolean checkIdenticalLabels(List<Integer> rowKeys) {

        int rowsCount = rowKeys.size();
        if(rowsCount == 0) {
            throw new RuntimeException("rowsCount = 0"); //shouldn't get here
        }
        if(rowsCount == 1) {
            return true;
        }

        int firstLabel = this.Y.get(rowKeys.get(0));

        for(int i = 1; i < rowsCount; i++) {
            if(firstLabel != this.Y.get(rowKeys.get(i))) {
                return false;
            }
        }
        return true;
    }

    private boolean checkIdenticalRows(List<Integer> rowKeys, List<Integer> columns) {

        int numRows = rowKeys.size();
        if(numRows == 0) {
            throw new RuntimeException("numRows = 0"); //shouldn't get here
        }
        if(numRows == 1) {
            return true;
        }
        if(numRows < 1000) {
            for(int i = 1; i < numRows; i++) {
                if(!isIdenticalRows(rowKeys.get(i-1), rowKeys.get(i), columns)) {
                    return false;
                }
            }
            return true;
        }
        //assume false if more than 100 rows
        return false;
    }

    private int majorityLabelVote(List<Integer> rowKeys) {

        int positiveCount = 0;
        int negativeCount = 0;

        for(Integer row : rowKeys) {
            if(this.Y.get(row) == 1) {
                positiveCount++;
            }
            else {
                negativeCount++;
            }
        }

        if(positiveCount > negativeCount) {
            return 1;
        }
        if(positiveCount < negativeCount) {
            return 0;
        }
        //coin flip
        return this.random.nextBoolean() ? 1 : 0;
    }

    private boolean isIdenticalRows(Integer rowIndex1, Integer rowIndex2, List<Integer> columns) {

        Map<Integer, Float> row1 = this.X.row(rowIndex1);
        Map<Integer, Float> row2 = this.X.row(rowIndex2);

        for(Integer colIdx : columns) {
//        for(int i = 0; i < this.trainWidth; i++) {
            Float v1 = row1.get(colIdx);
            Float v2 = row2.get(colIdx);

            if(v1 == null && v2 == null) {
                continue;
            }
            if(v1 == null || !v1.equals(v2)) {
                return false;
            }
        }
        return true;
    }

    public List<Integer> loadTrainDataSet() throws IOException {

        int[] widthAndHeight = getWidthAndHeight(this.trainDataSet);
        this.trainWidth = widthAndHeight[0];
        this.trainHeight = widthAndHeight[1];

        System.out.println("train width = " + this.trainWidth);
        System.out.println("train height = " + this.trainHeight);
        List<Integer> rowKeys = getKeysAsList(this.trainHeight);
        List<Integer> colKeys = getKeysAsList(this.trainWidth);

        this.X = ArrayTable.create(rowKeys, colKeys);
        this.Y = new ArrayList<>(this.trainHeight);
        DT.loadDataSet(this.trainDataSet, this.X, this.Y);

        return this.X.rowKeyList();
    }


    public List<Integer> loadTestDataSet(boolean ignoreWidth) throws IOException {

        int[] widthAndHeight = getWidthAndHeight(this.testDataSet);
        this.testWidth = widthAndHeight[0];
        this.testHeight = widthAndHeight[1];

        System.out.println("test width = " + this.testWidth);
        System.out.println("test height = " + this.testHeight);

        if(!ignoreWidth && this.testWidth != this.trainWidth) {
            throw new RuntimeException("width of train and test set do not match: " + this.trainWidth + "; " + this.testWidth);
        }

        List<Integer> rowKeys = getKeysAsList(this.testHeight);
        List<Integer> colKeys = getKeysAsList(this.testWidth);

        this.Xtest = ArrayTable.create(rowKeys, colKeys);
        this.Ytest = new ArrayList<>(this.trainHeight);
        DT.loadDataSet(this.testDataSet, this.Xtest, this.Ytest);

        return this.Xtest.rowKeyList();
    }

    private static void loadDataSet(String fileName, ArrayTable<Integer, Integer, Float> table, List<Integer> labels) throws IOException {

        int width = table.columnKeyList().size();

        try(BufferedReader reader = new BufferedReader(new FileReader(fileName))) {
            String line = reader.readLine(); //skip header
            int row = 0;
            while( (line = reader.readLine()) != null ) {
                String[] parts = line.split(",");
                for(int col = 0; col < width; col++) {
                    table.set(row, col, parts[col].length() == 0 ? null : Float.parseFloat(parts[col]));
                }
                labels.add(row, Integer.parseInt(parts[width]));
                row++;

//                if(row == 5000) break;
            }
        }
    }

    protected List<Integer> getKeysAsList(int size) {
        Integer[] keys = new Integer[size];
        for(int i = 0; i < size; i++) {
            keys[i] = i;
        }

        return Arrays.asList(keys);
    }

    public int getTrainWidth() {
        return trainWidth;
    }

    public int getTrainHeight() {
        return trainHeight;
    }

    public int getTestWidth() {
        return testWidth;
    }

    public int getTestHeight() {
        return testHeight;
    }

    public ArrayTable<Integer, Integer, Float> getTrainDataSet() {
        return this.X;
    }

    public List<Integer> getTrainLabels() {
        return this.Y;
    }

    public ArrayTable<Integer, Integer, Float> getTestDataSet() {
        return this.Xtest;
    }

    public List<Integer> getTestLabels() {
        return this.Ytest;
    }

    public static int[] getWidthAndHeight(String fileName) throws IOException {

        try(BufferedReader reader = new BufferedReader(new FileReader(fileName))) {
            //read header
            String line = reader.readLine();
            String[] parts = line.split(",");
            int width = parts.length - 1; //excluding the label (directionality)
            int height = 0;
            while(reader.readLine() != null) {
                height++;
            }

//            return new int[]{width, 5000};
            return new int[]{width, height};
        }
    }

    public static class Node {

        private static Random random = new Random(System.currentTimeMillis());
        private static final Gson gson = new Gson();

        private Double threshold;
        private Integer feature;
        private Integer label;
        private Node left;
        private Node right;
        private Integer depth;

        public Node(double threshold, int feature, Node left, Node right) {
            //threshold + feature - internal node => no label
            this.threshold = threshold;
            this.feature = feature;
            this.left = left;
            this.right = right;
            this.label = null;
        }

        public Node(int label) {
            this.label = label;
            //if label is set this is a leaf - no threshold, feature and subtrees
            this.threshold = null;
            this.feature = null;
            this.left = null;
            this.right = null;
        }

        public double classifyTestDataSet(ArrayTable<Integer, Integer, Float> dataMatrix,
                                          List<Integer> expectedLabels) {
            return classifyTestDataSet(dataMatrix, expectedLabels, dataMatrix.rowKeyList());
        }

        public double classifyTestDataSet(ArrayTable<Integer, Integer, Float> dataMatrix,
                                          List<Integer> expectedLabels,
                                          List<Integer> testRowKeys) {
            int total = 0;
            int errors = 0;

            for(Integer rowIdx : testRowKeys) {
                Map<Integer, Float> row = dataMatrix.row(rowIdx);
                Integer expected = expectedLabels.get(rowIdx);
                int actual = classifyExample(row);

                if(expected != actual) {
                    errors++;
                }
                total++;
            }

            return (total - errors) / (1D * total);
        }

        public int classifyExample(Map<Integer, Float> example) {

            Node node = this;

            while(node.feature != null) {
                Float v = example.get(node.feature);
                if(v != null) {
                    if(v <= node.threshold) {
                        node = node.left;
                    }
                    else {
                        node = node.right;
                    }
                }
                else {
                    //example doesn't include this node's feature - flip a coin
                    node = random.nextBoolean() ? node.left : node.right;
                }
            }

            return node.label;
        }

        private void serialize(String fileName) {

            String jsonString = gson.toJson(this);
            try(Writer writer = new FileWriter(fileName)) {

                writer.write(jsonString);
                writer.flush();
                System.out.println("tree was successfully written to file: " + fileName);
            }
            catch(IOException ioe) {
                ioe.printStackTrace();
            }
        }

        public String toJson() {
            return gson.toJson(this);
        }

        public static Node deserialize(String fileName) {

            try(BufferedReader reader = new BufferedReader(new FileReader(fileName))) {
                String jsonString = reader.readLine();
                Node tree = gson.fromJson(jsonString, Node.class);
                System.out.println("tree was successfully read from file: " + fileName);
                return tree;
            }
            catch(IOException ioe) {
                System.err.println("can't load tree from file");
            }

            return null;
        }

        public static Node deserializeFromJson(String json) {
            return gson.fromJson(json, Node.class);
        }


        public String printNode() {
            StringBuilder sb = new StringBuilder();
            if(feature != null) {
                sb.append("(feature: ")
                        .append(feature)
                        .append(" ; threshold: ")
                        .append(threshold)
                        .append(")");
            }
            else {
                sb.append("(label: ")
                        .append(label)
                        .append(")");

            }
            return sb.toString();
        }

        public void printTree() {
            List<Map.Entry<Node, Integer>> visited = new ArrayList<>();
            bfs(this, visited);

            int prev = 0;
            for(Map.Entry<Node, Integer> entry : visited) {
                Integer distance = entry.getValue();
                if(distance == prev) {
                    System.out.print("\t" + entry.getKey().printNode());
                }
                else if(distance > prev) {
                    System.out.println();
                    System.out.print(entry.getKey().printNode());
                }
                else {
                    System.err.println("ERROR: distance < prev");
                }
                prev = distance;
            }
            System.out.println();
        }

        private void bfs(Node root, List<Map.Entry<Node, Integer>> visited) {

            Queue<Node> queue = new ArrayDeque<>();
            root.depth = 0;
            visited.add(new AbstractMap.SimpleEntry<>(root, root.depth));
            queue.add(root);

            while(!queue.isEmpty()) {
                Node node = queue.poll();

                if(node.left != null && node.left.depth == null) {
                    node.left.depth = node.depth + 1;
                    visited.add(new AbstractMap.SimpleEntry<>(node.left, node.left.depth));
                    queue.add(node.left);
                }

                if(node.right != null && node.right.depth == null) {
                    node.right.depth = node.depth + 1;
                    visited.add(new AbstractMap.SimpleEntry<>(node.right, node.right.depth));
                    queue.add(node.right);
                }
            }
        }
    }
}
