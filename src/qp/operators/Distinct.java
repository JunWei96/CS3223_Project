package qp.operators;

import qp.utils.Batch;
import qp.utils.Tuple;

public class Distinct extends Operator {
    Operator base;
    Batch inbatch;

    int batchsize;                  // Number of tuples per out batch
    int cursor;

    Tuple uniqueTuple;
    ExternalSort sortedOperator;
    ExternalSort.TupleSortComparator comparator;

    boolean eos;                   // Whether end of stream is reached

    public Distinct(Operator base) {
        super(OpType.DISTINCT);
        this.base = base;
        this.batchsize = Batch.getPageSize() / base.getSchema().getTupleSize();
    }

    public Distinct(Operator base, int numOfBuffer) {
        super(OpType.DISTINCT);
        this.base = base;
        sortedOperator = new ExternalSort(base, numOfBuffer);
        comparator = new ExternalSort.TupleSortComparator(sortedOperator.getAttributeList());
    }

    public Operator getBase() {
        return base;
    }

    public void setOperation(Operator base, int numBuff) {
        this.base = base;
        sortedOperator = new ExternalSort(base, numBuff);
        comparator = new ExternalSort.TupleSortComparator(sortedOperator.getAttributeList());
    }

    public void setBase(Operator base) {
        this.base = base;
    }

    @Override
    public boolean open() {
        cursor = 0;
        eos = false;
        if (sortedOperator.open()) {
            inbatch = sortedOperator.next();
            return true;
        } else {
            return false;
        }
    }

    @Override
    public Batch next() {
        if (eos) {
            return null;
        }

        Batch outbatch = new Batch(batchsize);
        while (!outbatch.isFull()) {
            if (cursor == 0) {
                inbatch = sortedOperator.next();
                if (inbatch == null) {
                    eos = true;
                    return outbatch;
                }
            }

            for (int i = cursor; i < inbatch.size(); i++) {
                cursor = i;
                if (outbatch.isFull()) {
                    return outbatch;
                }
                Tuple currentTuple = inbatch.get(i);
                // uniqueTuple occurs at the start. OR when detect another diff tuple.
                if (uniqueTuple == null || comparator.compare(uniqueTuple, currentTuple) != 0) {
                    outbatch.add(currentTuple);
                    uniqueTuple = currentTuple;
                }
            }
            cursor = 0;
        }
        return outbatch;
    }

    @Override
    public boolean close() {
        return sortedOperator.close();
    }
}
