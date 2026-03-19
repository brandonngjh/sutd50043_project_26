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
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private final int gbfield;
    private final Type gbfieldtype;
    private final int afield;
    private final Op what;
    private final LinkedHashMap<Field, Integer> counts;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field, or NO_GROUPING
     * @param gbfieldtype the type of the group by field, or null if no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     */
    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        this.counts = new LinkedHashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        Field groupVal = (gbfield == NO_GROUPING) ? null : tup.getField(gbfield);
        counts.put(groupVal, counts.getOrDefault(groupVal, 0) + 1);
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
        for (Map.Entry<Field, Integer> entry : counts.entrySet()) {
            Tuple t = new Tuple(td);
            if (gbfield == NO_GROUPING) {
                t.setField(0, new IntField(entry.getValue()));
            } else {
                t.setField(0, entry.getKey());
                t.setField(1, new IntField(entry.getValue()));
            }
            results.add(t);
        }
        return new TupleIterator(td, results);
    }

}
