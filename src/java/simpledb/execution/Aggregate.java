package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.NoSuchElementException;


/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    private OpIterator child;
    private final int afield;
    private final int gfield;
    private final Aggregator.Op aop;
    private Aggregator aggregator;
    private OpIterator aggIt;

    /**
     * Constructor.
     *
     * @param child  The OpIterator that is feeding us tuples.
     * @param afield The column over which we are computing an aggregate.
     * @param gfield The column over which we are grouping the result, or -1 if no grouping
     * @param aop    The aggregation operator to use
     */
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
        this.child = child;
        this.afield = afield;
        this.gfield = gfield;
        this.aop = aop;
    }

    /**
     * @return the groupby field index in the INPUT tuples, or NO_GROUPING
     */
    public int groupField() {
        return gfield;
    }

    /**
     * @return the name of the groupby field in the OUTPUT tuples, or null
     */
    public String groupFieldName() {
        if (gfield == Aggregator.NO_GROUPING) return null;
        return child.getTupleDesc().getFieldName(gfield);
    }

    /**
     * @return the aggregate field index
     */
    public int aggregateField() {
        return afield;
    }

    /**
     * @return the name of the aggregate field in the OUTPUT tuples
     */
    public String aggregateFieldName() {
        return child.getTupleDesc().getFieldName(afield);
    }

    /**
     * @return the aggregate operator
     */
    public Aggregator.Op aggregateOp() {
        return aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
        return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
            TransactionAbortedException {
        super.open();
        child.open();

        TupleDesc childTd = child.getTupleDesc();
        Type gbType = (gfield == Aggregator.NO_GROUPING) ? null : childTd.getFieldType(gfield);
        Type aType = childTd.getFieldType(afield);

        if (aType == Type.INT_TYPE) {
            aggregator = new IntegerAggregator(gfield, gbType, afield, aop);
        } else {
            aggregator = new StringAggregator(gfield, gbType, afield, aop);
        }

        while (child.hasNext())
            aggregator.mergeTupleIntoGroup(child.next());

        aggIt = aggregator.iterator();
        aggIt.open();
    }

    /**
     * Returns the next aggregate result tuple, or null if done.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (aggIt != null && aggIt.hasNext())
            return aggIt.next();
        return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        aggIt.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate.
     */
    public TupleDesc getTupleDesc() {
        TupleDesc childTd = child.getTupleDesc();
        String aggName = aop.toString() + "(" + childTd.getFieldName(afield) + ")";
        if (gfield == Aggregator.NO_GROUPING) {
            return new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{aggName});
        } else {
            return new TupleDesc(
                new Type[]{childTd.getFieldType(gfield), Type.INT_TYPE},
                new String[]{childTd.getFieldName(gfield), aggName}
            );
        }
    }

    public void close() {
        super.close();
        if (aggIt != null) aggIt.close();
        aggIt = null;
    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[]{child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        child = children[0];
    }

}
