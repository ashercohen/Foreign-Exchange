package final_project_preprocessing.stage2_mapreduce;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.tuple.TupleEntry;

/**
 * Created by Asher Cohen asherc@andrew
 */
public class FilterByThreshold extends BaseOperation implements Filter {

    @Override
    public boolean isRemove(FlowProcess flowProcess, FilterCall filterCall) {

        TupleEntry entry = filterCall.getArguments();
        return (entry.getDouble(MRMain.TFIDF) <= 2.0D);
    }
}
