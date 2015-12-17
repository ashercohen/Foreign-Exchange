package final_project_preprocessing.stage2_mapreduce;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

import java.util.Arrays;
import java.util.HashSet;


/**
 * Created by Asher Cohen asherc@andrew
 */
public class InputParser extends BaseOperation implements Function {

    /**
     * Function (mapper) that parses input line into tuples
     *
     */
    private static HashSet<String> stopWords;
    private String docID;

    public InputParser(Fields outputFields) {
        super(outputFields);
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        /**
         * outFields: "doc_id", "token"
         */
        TupleEntry tupleEntry = functionCall.getArguments();
        String line = tupleEntry.getString(MRMain.LINE);

        if(line == null || line.isEmpty()) {
            return;
        }

        TupleEntry outTuple = new TupleEntry(new Fields(MRMain.DOC_ID, MRMain.TOKEN), Tuple.size(2));

        String[] nameAndContent = line.split("\\t");

        if(nameAndContent.length != 2) {
            return;
        }

        docID = nameAndContent[0];

        for(String word : nameAndContent[1].split("\\s")) {
            int len = word.length();
            if(len < 3 || len > 20 || stopWords.contains(word)) {
                continue;
            }

            outTuple.setString(MRMain.DOC_ID, docID);
            outTuple.setString(MRMain.TOKEN, word);
            functionCall.getOutputCollector().add(outTuple.getTuple());
        }
    }

    static {
        stopWords = new HashSet<>(Arrays.asList(new String[]
                {"-", "a", "introduction", "ltd", "about", "above", "abs", "accordingly", "across", "after",
                 "afterwards",
                 "almost", "alone", "along", "already", "also", "although", "always", "am", "among", "amongst", "an",
                 "analyze",
                 "and", "another", "any", "anyhow", "anyone", "anything", "anywhere", "applicable", "apply", "are",
                 "arise",
                 "around", "as", "assume", "at", "be", "became", "because", "become", "becomes", "becoming", "been",
                 "before",
                 "beforehand", "being", "below", "beside", "besides", "between", "beyond", "both", "but", "by", "came",
                 "can",
                 "cannot", "cc", "cm", "come", "compare", "could", "de", "dealing", "department", "depend", "did",
                 "discover",
                 "dl", "do", "does", "done", "due", "during", "each", "ec", "ed", "effected", "eg", "either", "else",
                 "elsewhere",
                 "enough", "especially", "et", "etc", "ever", "every", "everyone", "everything", "everywhere", "except",
                 "find",
                 "for", "found", "from", "further", "gave", "get", "give", "go", "gone", "got", "gov", "had", "has",
                 "have", "having",
                 "he", "hence", "her", "here", "hereafter", "hereby", "herein", "hereupon", "hers", "herself", "him",
                 "himself",
                 "his", "how", "however", "hr", "i", "ie", "if", "ii", "iii", "immediately", "importance", "important",
                 "in", "inc",
                 "incl", "indeed", "into", "investigate", "is", "it", "its", "itself", "just", "keep", "kept", "kg",
                 "km", "last",
                 "latter", "latterly", "lb", "ld", "letter", "like", "ltd", "made", "mainly", "make", "many", "may",
                 "me",
                 "meanwhile", "mg", "might", "ml", "mm", "mo", "more", "moreover", "most", "mostly", "mr", "much",
                 "mug", "must",
                 "my", "myself", "namely", "nearly", "necessarily", "neither", "never", "nevertheless", "next", "no",
                 "nobody",
                 "noone", "nor", "normally", "nos", "not", "noted", "nothing", "now", "nowhere", "obtained", "of",
                 "off", "often",
                 "on", "only", "onto", "or", "other", "others", "otherwise", "ought", "our", "ours", "ourselves", "out",
                 "over",
                 "overall", "owing", "own", "oz", "particularly", "per", "perhaps", "pm", "precede", "predominantly",
                 "present",
                 "presently", "previously", "primarily", "promptly", "pt", "quickly", "quite", "rather", "readily",
                 "really",
                 "recently", "refs", "regarding", "relate", "said", "same", "seem", "seemed", "seeming", "seems",
                 "seen",
                 "seriously", "several", "shall", "she", "should", "show", "showed", "shown", "shows", "significantly",
                 "since",
                 "slightly", "so", "some", "somehow", "someone", "something", "sometime", "sometimes", "somewhat",
                 "somewhere",
                 "soon", "specifically", "still", "strongly", "studied", "sub", "substantially", "such", "sufficiently",
                 "take",
                 "tell", "th", "than", "that", "the", "their", "theirs", "them", "themselves", "then", "thence",
                 "there", "thereafter",
                 "thereby", "therefore", "therein", "thereupon", "these", "they", "this", "thorough", "those", "though",
                 "through",
                 "throughout", "thru", "thus", "to", "together", "too", "toward", "towards", "try", "type", "ug",
                 "under", "unless",
                 "until", "up", "upon", "us", "use", "used", "usefully", "usefulness", "using", "usually", "various",
                 "very", "via",
                 "was", "we", "were", "what", "whatever", "when", "whence", "whenever", "where", "whereafter",
                 "whereas", "whereby",
                 "wherein", "whereupon", "wherever", "whether", "which", "while", "whither", "who", "whoever", "whom",
                 "whose",
                 "why", "will", "with", "within", "without", "wk", "would", "wt", "yet", "you", "your", "yours",
                 "yourself",
                 "yourselves", "yr", "a", "able", "about", "across", "after", "all", "almost", "also", "am", "among",
                 "an", "and",
                 "any", "are", "as", "at", "be", "because", "been", "but", "by", "can", "cannot", "could", "dear",
                 "did", "do", "does",
                 "either", "else", "ever", "every", "for", "from", "get", "got", "had", "has", "have", "he", "her",
                 "hers", "him",
                 "his", "how", "however", "i", "if", "in", "into", "is", "it", "its", "just", "least", "let", "like",
                 "likely", "may",
                 "me", "might", "most", "must", "my", "neither", "no", "nor", "not", "of", "off", "often", "on", "only",
                 "or", "other",
                 "our", "own", "rather", "said", "say", "says", "she", "should", "since", "so", "some", "than", "that",
                 "the", "their",
                 "them", "then", "there", "these", "they", "this", "tis", "to", "too", "twas", "us", "wants", "was",
                 "we", "were", "what",
                 "when", "where", "which", "while", "who", "whom", "why", "will", "with", "would", "yet", "you", "your",
                 "a", "able",
                 "about", " across", " after", " all", " almost", "also", "am", "among", "an", "and", "any", "are",
                 "as", "at", "be",
                 "because", "been", "but", "by", "can", "cannot", "could", "dear", "did", "do", "does", "either",
                 "else", "ever",
                 "every", "for", "from", "get", "got", "had", "has", "have", "he", "her", "hers", "him", "his", "how",
                 "however", "i",
                 "if", "in", "into", "is", "it", "its", "just", "least", "let", "like", "likely", "may", "me", "might",
                 "most", "must",
                 "my", "neither", "no", "nor", "not", "of", "off", "often", "on", "only", "or", "other", "our", "own",
                 "rather", "said",
                 "say", "says", "she", "should", "since", "so", "some", "than", "that", "the", "their", "them", "then",
                 "there", "these",
                 "they", "this", "tis", "to", "too", "twas", "us", "wants", "was", "we", "were", "what", "when",
                 "where", "which", "while",
                 "who", "whom", "why", "will", "with", "would", "yet", "you", "your", "again", "against", "all",
                 "accepted", "abc",
                 "access"}));
    }
}


