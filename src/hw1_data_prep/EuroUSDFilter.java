package hw1_data_prep;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;

/**
 * Created by Asher Cohen asherc@andrew
 */
public class EuroUSDFilter extends BaseOperation implements Filter {

    /**
     * Filter to retain only rows that are EUR/USD
     */

    public EuroUSDFilter(Fields fieldDeclaration) {
        super(fieldDeclaration);
    }

    @Override
    public boolean isRemove(FlowProcess flowProcess, FilterCall filterCall) {

        TupleEntry tupleEntry = filterCall.getArguments();
        String currencies = tupleEntry.getString(Main.CURRENCY_PAIR);

        return !"EUR/USD".equals(currencies);
    }
}
