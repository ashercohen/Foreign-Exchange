package hw2_dt;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

//@SuppressWarnings("unchecked")
public class DecisionTree implements Runnable {

    private class Node {

        public Node() {}

        private Integer attributeId;
        private String attributeName;
        private Node negative;
        private Node positive;
        private boolean isLeaf;
        private String label;
        private int positiveCount;
        private int negativeCount;

        public void setCounters(int[] counters) {
            this.positiveCount = counters[0];
            this.negativeCount = counters[1];
            this.label = this.negativeCount > this.positiveCount ? "NO" : "YES";
        }

        public String toString(boolean indent) {

            StringBuilder sb = new StringBuilder();
            sb.append("[").append(positiveCount).append("+/").append(negativeCount).append("-]\n");
            if(!isLeaf) {
                if(indent) {
                    sb.append("| ");
                }
                sb.append(attributeName).append(" = yes: ").append(positive.toString(true));
                if(indent) {
                    sb.append("| ");
                }
                sb.append(attributeName).append(" = no: ").append(negative.toString(true));
            }

            return sb.toString();
        }
    }


    private String trainingFile;
    private String testFile;
    private List<List<Double>> trainDataSet;
    private List<List<Double>> testDataSet;
    private Map<Integer, String> attributeNames;
    private int width;

    public DecisionTree(String trainingFile, String testFile) {
        this.trainingFile = trainingFile;
        this.testFile = testFile;
    }

    public static void main(String[] args) {

        if(args.length != 2) {
            System.err.println("Usage: java decisionTree <training file> <test file>");
            System.exit(1);
        }

        new Thread(new DecisionTree(args[0], args[1])).start();
    }


    @Override
    public void run() {

        try {
            //load data sets
            loadTrainData();
            loadTestData();

            List<Integer> attributes = new ArrayList<>(this.width - 1);
            for(int i = 0; i < this.width - 1; i++) {
                attributes.add(i);
            }

            //train a tree
            Node tree = ID3(this.trainDataSet, 0, attributes, 0);
            System.out.print(tree.toString(false));

            //use trained tree on testing data (and also on training data)
            classify(tree);
        }
        catch(IOException ioe) {
            ioe.printStackTrace();
        }
    }

    private void classify(Node tree) throws IOException {

        double trainError = classify(tree, this.trainDataSet);
        double testError = classify(tree, this.testDataSet);

//        System.out.println("error(train): " + trainError);
//        System.out.println("error(test): " + testError);
    }

    private double classify(Node tree, List<List<Double>> set) throws IOException {

        int total = 0;
        int errors = 0;
        for(List<Double> example : set) {
            total++;
            String rv = classifyExample(tree, example);
            int res = rv.equals("YES") ? 1 : 0;
            Double expectedResult = example.get(this.width - 1);

            if(res != expectedResult) {
                errors++;
            }
        }

        return ((double)errors) / total;
    }

    private String classifyExample(Node tree, List<Double> example) {

        if(tree.attributeId == null) { //tree is a leaf

            if(!tree.isLeaf || tree.label == null) {
                throw new IllegalArgumentException("expected leaf with label. got: " + tree.toString(false));
            }

            return tree.label;
        }

        Double v = example.get(tree.attributeId);
        if(v == null || v == 0D) {
            return "YES";
        }
        else {
            return "NO";
        }

//        switch(example.get(tree.attributeId)) {
//
//            case 0: //check tree's negative child
//                return classifyExample(tree.negative, example);
//
//            case 1:
//                return classifyExample(tree.positive, example);
//
//            default:
//                throw new IllegalArgumentException("example should contain 0|1. got: " + example.get(tree.attributeId));
//        }
    }

    private Node ID3(List<List<Double>> examples, int targetAttribute, List<Integer> attributes, int treeDepth) {

        //Create a Root node for the tree
        Node root = new Node();

        //If all Examples are positive,
        if(isAllExamplesPositive(examples)) {
            //Return the single-node tree Root, with label = +
            root.isLeaf = true;
            root.label = "YES";
            root.positiveCount = examples.size();
            root.negativeCount = 0;
            return root;
        }

        if(isAllExamplesNegative(examples)) {
            //Return the single-node tree Root, with label = -
            root.isLeaf = true;
            root.label = "NO";
            root.positiveCount = 0;
            root.negativeCount = examples.size();
            return root;
        }

        //If attributes is empty
        if(isEmpty(attributes)) {
            //Return the single-node tree Root, with label = most common value of targetAttribute in Examples
            root.isLeaf = true;
            root.setCounters(countExamples(examples));
            return root;
        }

        //select best attribute from attributes
        double[] bestAttribute = selectBestAttribute(examples, attributes);

        if(bestAttribute[1] >= 0.000001) {

            /**
             * split node
             */

            root.isLeaf = false;
            root.attributeId = (int)bestAttribute[0];
            root.attributeName = this.attributeNames.get(root.attributeId);
            root.positiveCount = (int)bestAttribute[2];
            root.negativeCount = (int)bestAttribute[3];

            /**
             * there are two possible values for attribute A: negative, positive
             * example are split into 2 groups:
             * 1) all examples where A=negative
             * 2) all example where A=positive
             *
             * if a group is empty:
             * - connect a leaf to the corresponding branch in root (root.negative or root.positive)
             * - leaf value will be the most popular attribute value of target attribute in examples
             *
             * if a group is NOT empty:
             * - call recursively to ID3 with group and attribute \ A
             * - connect return value to the corresponding branch
             */

            Object[] objects = splitExamplesByBestAttributeValue(examples, (int)bestAttribute[0]);
            List<List<Double>> examples_negative = (List<List<Double>>)objects[0];
            List<List<Double>> examples_positive = (List<List<Double>>)objects[1];

            if(examples_negative.isEmpty()) {
                Node leaf = new Node();
                leaf.isLeaf = true;
                root.setCounters(countExamples(examples));
                root.negative = leaf;
            }
            else {
                List<Integer> attributes_minus_best = new ArrayList<>(attributes);
                attributes_minus_best.set((int)bestAttribute[0], null);
                root.negative = ID3(examples_negative, targetAttribute, attributes_minus_best, treeDepth + 1);
            }

            if(examples_positive.isEmpty()) {
                Node leaf = new Node();
                leaf.isLeaf = true;
                root.setCounters(countExamples(examples));
                root.positive = leaf;
            }
            else {
                List<Integer> attributes_minus_best = new ArrayList<>(attributes);
                attributes_minus_best.set((int)bestAttribute[0], null);
                root.positive = ID3(examples_positive, targetAttribute, attributes_minus_best, treeDepth + 1);
                int x = 0;
            }
        }
        else {
            root.isLeaf = true;
            root.setCounters(countExamples(examples));
        }

        return root;
    }

    private Object[] splitExamplesByBestAttributeValue(List<List<Double>> examples, int bestAttribute) {

        List<List<Double>> negativeExamples = new ArrayList<>();
        List<List<Double>> positiveExamples = new ArrayList<>();

        for(List<Double> example : examples) {
            Double v = example.get(bestAttribute);
            if(v == null || v == 0D) {
                negativeExamples.add(example);
            }
            else {
                positiveExamples.add(example);
            }
        }

        return new Object[]{negativeExamples, positiveExamples};
    }

    private double[] selectBestAttribute(List<List<Double>> examples, List<Integer> attributes) {

        double maxValue = Double.NEGATIVE_INFINITY;
        double[] attrIdxAndStats = null;

        for(int i = 0; i < attributes.size(); i++) {

            Integer attr = attributes.get(i);
            if(attr == null) {
                continue; //attribute #i already chosen before
            }

            //calculate MI(examples;attribute(i))
            double[] miAndStats = mutualInformation(examples, attr);
            double mi = miAndStats[0];
            if(mi > maxValue) {
                maxValue = mi;
                attrIdxAndStats = new double[]{i, miAndStats[0], miAndStats[1], miAndStats[2]};
            }
        }

        if(attrIdxAndStats == null) {
            throw new RuntimeException("attrIdxAndStats == null");
        }

//        System.out.println(String.format("best attr is %d: MI = %f  [%d+/%d-]",
//                (int)attrIdxAndStats[0],
//                attrIdxAndStats[1],
//                (int)attrIdxAndStats[2],
//                (int)attrIdxAndStats[3]));

        return attrIdxAndStats;
    }

    private int[] countExamples(List<List<Double>> examples) {

        int positive = 0;
        int negative = 0;

        for(List<Double> example : examples) {
            if(example.get(this.width - 1) == 0) {
                negative++;
            }
            else {
                positive++;
            }
        }

        return new int[]{positive, negative};
    }

    //MI(examples;attribute_i)
    private double[] mutualInformation(List<List<Double>> examples, Integer attribute) {

        if(examples == null || examples.isEmpty()) {
            throw new IllegalArgumentException("cannot take null or empty example list");
        }

        double zeroNegative = 0;
        double zeroPositive = 0;
        double oneNegative = 0;
        double onePositive = 0;

        for(List<Double> example : examples) {

            Double v = example.get(attribute);
            if(v == null || v == 0D) {
                if(example.get(width - 1) == 0) {
                    zeroNegative++;
                }
                else {
                    zeroPositive++;
                }
            }
            else {
                if(example.get(width - 1) == 0D) {
                    oneNegative++;
                }
                else {
                    onePositive++;
                }
            }
        }

        double totalPositive = zeroPositive + onePositive;
        double totalNegative = zeroNegative + oneNegative;
        double zeroTotal = zeroNegative + zeroPositive;
        double oneTotal = oneNegative + onePositive;
        double total = totalNegative + totalPositive;

        double examplesEntropy = totalNegative == 0D
                                 ? 0D
                                 : entropy(Arrays.asList(totalNegative / total, totalPositive / total));
        double zeroEntropy = zeroTotal == 0D
                             ? 0D
                             : entropy(Arrays.asList(zeroNegative / zeroTotal, zeroPositive / zeroTotal));
        double oneEntropy = oneTotal == 0D
                            ? 0D
                            : entropy(Arrays.asList(oneNegative / oneTotal, onePositive / oneTotal));

        double mutualInfo = examplesEntropy - (zeroTotal / total) * zeroEntropy - (oneTotal / total) * oneEntropy;

        if(total != examples.size()) {
            throw new RuntimeException("total != examples.size(): total = " + total + ", examples.size() = " + examples.size());
        }

        return new double[]{mutualInfo, totalPositive, totalNegative};
    }

    private double entropy(List<Double> values) {

        double sum = 0D;
        double entropy = 0D;

        for(Double value : values) {
            sum += value;
            entropy -= value * log2(value);
        }

        if(sum != 1D) {
            throw new IllegalArgumentException("probabilities do not sum to 1: " + Arrays.toString(values.toArray()));
        }

        return entropy;
    }

    private boolean isEmpty(List<Integer> attributes) {

        for(Integer attribute : attributes) {
            if(attribute != null) {
                return false;
            }
        }

        return true;
    }

    private double log2(double x) {
        if(x == 0D) return 0D;
        return Math.log(x) / Math.log(2);
    }

    private boolean isAllExamplesPositive(List<List<Double>> examples) {
        return isAllExamples(examples, 1);
    }

    private boolean isAllExamplesNegative(List<List<Double>> examples) {
        return isAllExamples(examples, 0);
    }

    private boolean isAllExamples(List<List<Double>> examples, int value) {

        for(List<Double> example : examples) {
            if(example.get(this.width - 1) != value) {
                return false;
            }
        }

        return true;
    }

    private void loadTrainData() throws IOException {

        Object[] rv = loadFile(this.trainingFile);
        this.trainDataSet = (List<List<Double>>)rv[0];
        this.width = (int)rv[1];
        loadAttributeNames(rv[2].toString());
    }

    private void loadTestData() throws IOException {

        Object[] rv = loadFile(this.testFile);
        this.testDataSet = (List<List<Double>>)rv[0];
        int width = (int)rv[1];
        if(this.width != width) {
            throw new IllegalArgumentException("train and test sets have different width");
        }
    }

    private Object[] loadFile(String fileName) throws IOException {

        try(BufferedReader reader = new BufferedReader(new FileReader(fileName))) {

            String header = reader.readLine();
            Object[] dataSetAndWidth = doLoadFile(reader, header.split(",").length);
            return new Object[] {dataSetAndWidth[0], dataSetAndWidth[1], header};
        }
    }

    private void loadAttributeNames(String attributeNames) {

        String[] names = attributeNames.split(",");
        Map<Integer, String> nameMap = new HashMap<>(names.length);
        for(int i = 0; i < names.length - 1; i++) {
            nameMap.put(i, names[i]);
        }
        this.attributeNames = nameMap;
    }

    private Object[] doLoadFile(BufferedReader reader, int width) throws IOException {

        List<List<Double>> dataSet = new ArrayList<>(); //list of rows, each row is a list of integers (0|1)
        String line;

        while( (line = reader.readLine()) != null) {
            List<Double> row = new ArrayList<>(12);
            String[] parts = line.split(",");
            for(String part : parts) {
                row.add(part.length() == 0 ? null : Double.parseDouble(part));
            }

            dataSet.add(row);
        }

        return new Object[] {dataSet, width};
    }
}

