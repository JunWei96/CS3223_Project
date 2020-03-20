package qp.operators;
import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Tuple;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import qp.utils.Condition;


class SortMergeJoin extends Join {
    private int batchsize;
    private ExternalSort leftsort;
    private ExternalSort rightsort;
    private ArrayList<Integer> common_index;
    private ArrayList<Integer> leftindex;
    private ArrayList<Integer> rightindex;

    public SortMergeJoin(Join jn) {
        super(jn.getLeft(), jn.getRight(), jn.getCondition(), jn.getOpType());
        schema = jn.getSchema();
        jointype = jn.getJoinType();
        numBuff = jn.getNumBuff();
    }

    @Override
    public boolean open(){
        int tupleSize = schema.getTupleSize();
        this.batchsize = Batch.getPageSize() / tupleSize;

        // find index attribute of join conditions
        for (Condition con : this.conditionList)
        {
            Attribute leftattr = con.getLhs();
            this.common_index.add(this.left.getSchema().indexOf(leftattr));
            this.leftindex.add(left.getSchema().indexOf(leftattr));
            this.rightindex.add(right.getSchema().indexOf(leftattr));
        }
        // sort according to the attributes
        leftsort = new ExternalSort(this.left, this.numBuff, this.common_index);
        rightsort = new ExternalSort(this.right, this.numBuff, this.common_index);

        return true;
    }

    private List<ObjectInputStream> GetListOfInputStream(List<File> sortedruns_file)
    {
        List<ObjectInputStream> inputs = new ArrayList<>();
        // Generated and input stream of sorted runs.
        for (File file : sortedruns_file) {
            try {
                ObjectInputStream input = new ObjectInputStream(new FileInputStream(file));
                inputs.add(input);
            } catch (IOException e) {
                System.out.println("Error reading file into input stream.");
            }
        }
        return inputs;
    }

    protected Batch nextBatchFromStream(ObjectInputStream stream) {
        try {
            Batch batch = (Batch) stream.readObject();
            if (batch.isEmpty()) {
                return null;
            }
            return batch;
        } catch (ClassNotFoundException e) {
            System.out.println("Unable to serialize the read object.");
            return null;
        } catch (IOException e) {
            System.out.println("IO error occurred while reading input stream.");
            return null;
        }
    }

    @Override
    public Batch next()
    {
        // open file input stream to read the sorted runs.
        List<File> leftsortedfile = leftsort.GetSortedRunFile();
        List<File> rightsortedfile = rightsort.GetSortedRunFile();

        List<ObjectInputStream> inputsleft = GetListOfInputStream(leftsortedfile);
        List<ObjectInputStream> inputsright = GetListOfInputStream(rightsortedfile);

        Batch outputBuffer = new Batch(this.batchsize);
        // load one batch initially
        Batch leftBatch = nextBatchFromStream(inputsleft.get(0));
        Batch rightBatch = nextBatchFromStream(inputsright.get(0));

        int leftcursor = 0;
        int rightcursor = 0;

        while(leftBatch != null && rightBatch != null)
        {
          while(leftcursor < this.batchsize && rightcursor < this.batchsize)
          {
              // compare the data in attributes
              Tuple lefttuple = leftBatch.get(leftcursor);
              Tuple righttuple = rightBatch.get(rightcursor);
              if (lefttuple.checkJoin(righttuple, leftindex, rightindex))
              {
                  Tuple outtuple = lefttuple.joinWith(righttuple);
                  outputBuffer.add(outtuple);
                  if (outputBuffer.isFull())
                  {
                      // adjust cursor, return outputBuffer
                      if (rightcursor <= leftcursor)
                          rightcursor++;
                      else
                          leftcursor++;
                      return outputBuffer;
                  }
              }
              // adjust cursor
              if (rightcursor <= leftcursor)
                  rightcursor++;
              else
                  leftcursor++;
          }
          // if exit, load another batch
          if (leftcursor >= this.batchsize)
          {
              leftBatch = nextBatchFromStream(inputsleft.get(0));
              leftcursor = 0;
          }
          if (rightcursor >= this.batchsize)
          {
              rightBatch = nextBatchFromStream(inputsright.get(0));
              rightcursor = 0;
          }
        }
        return outputBuffer;
    }
}



