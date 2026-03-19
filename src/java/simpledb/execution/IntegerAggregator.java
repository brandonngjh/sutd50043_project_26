package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.storage.TupleIterator;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private final int gbfield;
    private final Type gbfieldtype;
    private final int afield;
    private final Op what;

    private final LinkedHashMap<Field, int[]> groups;

    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field, or NO_GROUPING
     * @param gbfieldtype the type of the group by field, or null if no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        the aggregation operator
     */
    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        this.groups = new LinkedHashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        Field groupVal = (gbfield == NO_GROUPING) ? null : tup.getField(gbfield);
        int val = ((simpledb.storage.IntField) tup.getField(afield)).getValue();

        int[] state = groups.get(groupVal);
        if (state == null) {
            state = new int[]{val, 1, val, val};
        } else {
            state[0] += val;
            state[1]++;
            if (val < state[2]) state[2] = val;
            if (val > state[3]) state[3] = val;
        }
        groups.put(groupVal, state);
    }

    /**
     * Create a OpIterator over group aggregate results.
     */
    public OpIterator iterator() {
        TupleDesc td;
        if (gbfield == NO_GROUPING) {
            td = new TupleDesc(new Type[]{Type.INT_TYPE});
        } else {
            td = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE});
        }

        List<Tuple> results = new ArrayList<>();
        for (Map.Entry<Field, int[]> entry : groups.entrySet()) {
            int[] state = entry.getValue();
            int aggVal;
            switch (what) {
                case SUM: aggVal = state[0]; break;
                case COUNT: aggVal = state[1]; break;
                case MIN: aggVal = state[2]; break;
                case MAX: aggVal = state[3]; break;
                case AVG: aggVal = state[0] / state[1]; break;
                default: throw new UnsupportedOperationException("Unsupported op: " + what);
            }

            Tuple t = new Tuple(td);
            if (gbfield == NO_GROUPING) {
                t.setField(0, new IntField(aggVal));
            } else {
                t.setField(0, entry.getKey());
                t.setField(1, new IntField(aggVal));
            }
            results.add(t);
        }
        return new TupleIterator(td, results);
    }

}
