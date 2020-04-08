package qp.operators;
import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Tuple;

import java.io.*;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import qp.utils.Condition;


public class SortMergeJoin extends Join {
    private int batchsize;
    private ExternalSort leftsort;
    private ExternalSort rightsort;
    private ArrayList<Integer> leftindex;
    private ArrayList<Integer> rightindex;
    private ExternalSort.TupleSortComparator comparator;

    private Batch outbatch;
    private Batch leftbatch;
    private Batch rightbatch;
    int leftcursor = 0;
    int rightcursor = 0;

    public SortMergeJoin(Join jn) {
        super(jn.getLeft(), jn.getRight(), jn.getCondition(), jn.getOpType());
        System.out.println("Sort merge join :" + jn.getJoinType());
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

        System.out.println("(SMJ) Opening left and right operator.");
        if (!leftsort.open() || !rightsort.open()) {
            return false;
        }
        leftbatch = leftsort.next();
        rightbatch = rightsort.next();

        return true;
    }

    @Override
    public Batch next() {
        outbatch = new Batch(batchsize);
        // Scan left until left's sort key >= right's current sort key
         // Then scan right until right's sort key >= current left sort key
        // When left sort key == right sort key, we at a partition where all tuples in this partition matches join condition
        // Join them and place them in outbatch
        // Start scanning from step 1 again.

        while(leftbatch != null && rightbatch != null) {
          while(leftcursor < this.batchsize && rightcursor < this.batchsize) {
              // compare the data in attributes
              Tuple lefttuple = leftbatch.get(leftcursor);
              Tuple righttuple = rightbatch.get(rightcursor);
              if (lefttuple.checkJoin(righttuple, leftindex, rightindex)) {
                  Tuple outtuple = lefttuple.joinWith(righttuple);
                  outbatch.add(outtuple);
                  rightcursor++;
                  leftcursor++;
                  if (outbatch.isFull()) {
                      return outbatch;
                  }
              }
          }
          // if exit, load another batch
          if (leftcursor >= this.batchsize) {
              leftbatch = leftsort.next();
              leftcursor = 0;
          }
          if (rightcursor >= this.batchsize) {
              rightbatch = rightsort.next();
              rightcursor = 0;
          }
        }
        return outbatch;
    }

    @Override
    public boolean close() {
        leftsort.close();
        rightsort.close();
        return true;
    }
}



