package qp.operators;

import qp.utils.Batch;
import qp.utils.Tuple;

import java.util.ArrayList;

public class Distinct extends Operator {
    Operator base;
    Batch outbatch;
    Batch inbatch;

    int batchsize;                  // Number of tuples per out batch

    Tuple uniqueTuple;
    ExternalSort sortedOperator;
    ExternalSort.TupleSortComparator comparator;

    boolean eos;                   // Whether end of stream is reached

    public Distinct(Operator base) {
        super(OpType.DISTINCT);
        this.base = base;
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
        eos = false;
        outbatch = new Batch(batchsize);
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

        while (!outbatch.isFull()) {
            if (inbatch.isEmpty()) {
                inbatch = sortedOperator.next();
                if (inbatch == null) {
                    eos = true;
                    return outbatch;
                }
            }

            for (int i = 0; i < inbatch.size(); i++) {
                if (outbatch.isFull()) {
                    return outbatch;
                }
                Tuple currentTuple = inbatch.get(i);
                inbatch.remove(i);
                // uniqueTuple occurs at the start. OR when detect another diff tuple.
                if (uniqueTuple == null || comparator.compare(uniqueTuple, currentTuple) != 0) {
                    outbatch.add(currentTuple);
                    uniqueTuple = currentTuple;
                }
            }
        }
        return outbatch;
    }

    @Override
    public boolean close() {
        return sortedOperator.close();
    }

    public int compareTuple(Tuple firstTuple, Tuple secondTuple, ArrayList<Integer> sortIndexes) {
        for (int index : sortIndexes) {
            int compareValue = Tuple.compareTuples(firstTuple, secondTuple, index);
            if (compareValue != 0) {
                return compareValue;
            }
        }

        return 0;
    }
}
