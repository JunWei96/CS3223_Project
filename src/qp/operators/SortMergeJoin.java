package qp.operators;
import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Tuple;

import java.util.ArrayList;
import qp.utils.Condition;


public class SortMergeJoin extends Join {
    private int batchsize;
    private ExternalSort leftsort;
    private ExternalSort rightsort;
    private ArrayList<Integer> leftindex = new ArrayList<>();
    private ArrayList<Integer> rightindex = new ArrayList<>();
    private ArrayList<Tuple> backup = new ArrayList<>();

    private Batch leftbatch;
    private Batch rightbatch;
    int leftcursor = 0;
    int rightcursor = 0;
    int fallbackcursor = -1;

    public SortMergeJoin(Join jn) {
        super(jn.getLeft(), jn.getRight(), jn.getCondition(), jn.getOpType());
        schema = jn.getSchema();
        jointype = jn.getJoinType();
        numBuff = jn.getNumBuff();
    }

    @Override
    public boolean open() {
        int tupleSize = schema.getTupleSize();
        this.batchsize = Batch.getPageSize() / tupleSize;

        // find index attribute of join conditions
        for (Condition con : this.conditionList) {
            Attribute leftattr = con.getLhs();
            Attribute rightattr = (Attribute) con.getRhs();
            this.leftindex.add(left.getSchema().indexOf(leftattr));
            this.rightindex.add(right.getSchema().indexOf(rightattr));
        }
        // sort according to the attributes
        leftsort = new ExternalSort(left, numBuff, leftindex, "left", OpType.JOIN);
        rightsort = new ExternalSort(right, numBuff, rightindex, "right", OpType.JOIN);

        if (!leftsort.open() || !rightsort.open()) {
            System.out.println("(SortMerge) Failed to open left or right");
            return false;
        }
        leftbatch = leftsort.next();
        rightbatch = rightsort.next();

        return true;
    }

    @Override
    public Batch next() {
        Batch outbatch = new Batch(batchsize);

        while (leftbatch != null && rightbatch != null) {
            Tuple lefttuple = leftbatch.get(leftcursor);
            Tuple righttuple = getRightTuple();

            if (fallbackcursor == -1) {
                // advance left and right until they are equal
                Debug.PPrint(leftsort.getBase().getSchema());
//                System.out.println("Left data: " + lefttuple._data + " for " + leftindex);
//                System.out.println("Right data: " + righttuple._data + " for " + rightindex);
                while (Tuple.compareTuples(lefttuple, righttuple, leftindex, rightindex) < 0) {
                    advanceLeft();
                    if (leftbatch == null) break;
                    lefttuple = leftbatch.get(leftcursor);
                }
                while (Tuple.compareTuples(lefttuple, righttuple, leftindex, rightindex) > 0) {
                    advanceRight();
                    if (rightbatch == null) break;
                    righttuple = getRightTuple();
                }
                markFallbackCursorToRight();
            }

            if (Tuple.compareTuples(lefttuple, righttuple, leftindex, rightindex) == 0) {
                outbatch.add(lefttuple.joinWith(righttuple));
                advanceRight();
                if (rightbatch == null) break;
                if (outbatch.isFull()) {
                    return outbatch;
                }
            } else {
                rightcursor = fallbackcursor;
                advanceLeft();
                if (leftbatch == null) break;
                fallbackcursor = -1;
            }
        }

        if (outbatch.isEmpty()) {
            close();
            return null;
        } else {
            return outbatch;
        }
    }

    @Override
    public boolean close() {
        leftsort.close();
        rightsort.close();
        return true;
    }

    private Tuple getRightTuple() {
        if (backup.size() == 0) {
            return rightbatch.get(rightcursor);
        } else {
            if (rightcursor < backup.size()) {
                return backup.get(rightcursor);
            } else {
                return rightbatch.get(rightcursor - backup.size());
            }
        }
    }

    private void advanceLeft() {
        leftcursor++;
        if (leftbatch != null && leftcursor >= leftbatch.size()) {
            leftbatch = leftsort.next();
            leftcursor = 0;
        }
//        printStatus();
    }

    private void advanceRight() {
        rightcursor++;
        if (rightbatch != null && rightcursor >= rightbatch.size() + backup.size()) {
            backup.addAll(rightbatch.tuples);
            rightbatch = rightsort.next();
        }
//        printStatus();
    }

    private void markFallbackCursorToRight() {
        // mark fall back cursor with position of right cursor
        if (rightcursor >= backup.size()) {
            rightcursor -= backup.size();
        }
        fallbackcursor = rightcursor;
        backup.clear();
    }

    private void printStatus() {
        if (leftbatch == null || rightbatch == null) {
            return;
        }
        System.out.println("Left " + leftcursor + ": " + leftbatch.get(leftcursor)._data +
                " Right " + rightcursor + ": " + getRightTuple()._data);
    }
}



