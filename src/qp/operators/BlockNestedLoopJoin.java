/**
 * Block Nested Loop Join algorithm
 **/

package qp.operators;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Condition;
import qp.utils.Tuple;

import java.io.*;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class BlockNestedLoopJoin extends Join {

    static int filenum = 0;         // To get unique filenum for this operation
    int batchsize;                  // Number of tuples per out batch
    ArrayList<Integer> leftindex;   // Indices of the join attributes in left table
    ArrayList<Integer> rightindex;  // Indices of the join attributes in right table
    String rfname;                  // The file name where the right table is materialized
    Batch outbatch;                 // Buffer page for output
    List<Batch> leftblock
            = new ArrayList<>();   // Linked list of (B-2) buffer pages for left input stream
    ArrayList<Tuple> leftTuples
            = new ArrayList<>();    // Array list of tuples from the batches in the leftblock. Only used during join
                                    // process when comparing each left batch tuple with each right batch tuple
    Batch rightbatch;               // Buffer page for right input stream
    ObjectInputStream in;           // File pointer to the right hand materialized file

    int lcurs;                      // Cursor for left side buffer
    int rcurs;                      // Cursor for right side buffer
    boolean eosl;                   // Whether end of stream (left table) is reached
    boolean eosr;                   // Whether end of stream (right table) is reached

    public BlockNestedLoopJoin(Join jn) {
        super(jn.getLeft(), jn.getRight(), jn.getConditionList(), jn.getOpType());
        schema = jn.getSchema();
        jointype = jn.getJoinType();
        numBuff = jn.getNumBuff();
    }

    /**
     * During open finds the index of the join attributes
     * * Materializes the right hand side into a file
     * * Opens the connections
     **/
    public boolean open() {
        /** select number of tuples per batch **/
        int tuplesize = schema.getTupleSize();
        /** batchsize = number of records in a page **/
        batchsize = Batch.getPageSize() / tuplesize;
        /** initialize new Linked List for left input pages with every open()**/
        leftblock = new LinkedList<Batch>();

        /** find indices attributes of join conditions **/
        leftindex = new ArrayList<>();
        rightindex = new ArrayList<>();
        for (Condition con : conditionList) {
            Attribute leftattr = con.getLhs();
            Attribute rightattr = (Attribute) con.getRhs();
            leftindex.add(left.getSchema().indexOf(leftattr));
            rightindex.add(right.getSchema().indexOf(rightattr));
        }
        Batch rightpage; // Buffer page for right input stream

        /** initialize the cursors of input buffers **/
        lcurs = 0;
        rcurs = 0;
        /** the entire left input stream will be scanned once. eosl will only == True
         ** when the left input stream has completed its scan and the join is, thus, completed
         **/
        eosl = false; // end of stream left
        /** because right stream is to be repetitively scanned
         ** if it reached end, we have to start new scan
         **/
        eosr = true; // end of stream right

        /** Right hand side table is to be materialized
         ** for the Block Nested Loop Join to perform
         **/
        if (!right.open()) {
            return false;
        } else {
            /** If the right operator is not a base table then
             ** Materialize the intermediate result from right
             ** into a file
             **/
            filenum++;
            rfname = "BNJtemp-" + String.valueOf(filenum);
            try {
                ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(rfname));
                while ((rightpage = right.next()) != null) {
                    out.writeObject(rightpage);
                }
                out.close();
            } catch (IOException io) {
                System.out.println("BlockNestedLoopJoin: Error writing to temporary file");
                return false;
            }
            if (!right.close())
                return false;
        }
        if (left.open())
            return true;
        else
            return false;
    }

    /**
     * from input buffers selects the tuples satisfying join condition
     * * And returns a page of output tuples
     **/
    public Batch next() {
        int i, j;

        /** both the left input stream has been completely scanned and the left
         **  buffer block of left input pages has been processed. End BNL Join.
         **/
        if (eosl) {
            close();
            return null;
        }
        outbatch = new Batch(batchsize);
        while (!outbatch.isFull()) {
            if (lcurs == 0 && eosr) {
                /** new left block of left input stream pages to be fetched.
                 ** clear leftblock of buffer pages of old data
                 ** clear leftTuples of old data
                 **/
                this.leftblock.clear();
                this.leftTuples.clear();

                /** load left block with batches from left input stream
                 ** load tuples from each batch inside the leftblock into leftTuples for comparing tuples during join
                 **/
                load_left_block();
                load_tuples_from_leftblock();

                /** if the whole left table has been read **/
                if (leftblock.isEmpty() || leftblock == null) {
                    eosl = true;
                    return outbatch;
                }

                /** Whenever a new leftblock of left input pages comes,
                 ** we have to start scanning the right table from the top
                 **/
                try {
                    in = new ObjectInputStream(new FileInputStream(rfname));
                    eosr = false;
                } catch (IOException io) {
                    System.err.println("BlockNestedLoopJoin: error in reading the file");
                    System.exit(1);
                }

            }
            /** repeatedly scan rightbatch
             **
             **/
            while (!eosr) {
                try {
                    if (rcurs == 0 && lcurs == 0) {
                        rightbatch = (Batch) in.readObject();
                    }
                    /** In each iteration, each tuple of the leftblock is compared with all
                     ** tuples from the rightbatch. Total no. of iterations = |left| / (numBuff - 2)
                     **/
                    for (i = lcurs; i < leftTuples.size(); i++) {
                        for (j = rcurs; j < rightbatch.size(); j++) {
                            Tuple lefttuple = leftTuples.get(i);
                            Tuple righttuple = rightbatch.get(j);
                            // compare lefttuple and righttuple to see if join conditions are satisfied
                            if (lefttuple.checkJoin(righttuple, leftindex, rightindex)) {
                                Tuple outtuple = lefttuple.joinWith(righttuple);
                                outbatch.add(outtuple);
                                if (outbatch.isFull()) {
                                    //case 1 both leftbatch and rightbatch fully scanned
                                    if (i == leftblock.size() - 1 && j == rightbatch.size() - 1) {
                                        lcurs = 0;
                                        rcurs = 0;
                                    }
                                    //case 2: rightbatch fully scanned
                                    else if (i != leftblock.size() - 1 && j == rightbatch.size() - 1) {
                                        lcurs = i + 1;
                                        rcurs = 0;
                                    }
                                    //case 3: next tuple in rightbatch
                                    else if (i == leftblock.size() - 1 && j != rightbatch.size() - 1) {
                                        lcurs = i;
                                        rcurs = j + 1;
                                    }
                                    // case 4: carry on scanning rightbatch tuples
                                    else {
                                        lcurs = i;
                                        rcurs = j + 1;
                                    }
                                    return outbatch;
                                }
                            }
                        }
                        //re-initialize to the start of the rightbatch
                        rcurs = 0;
                    }
                    //re-initialize to the first tuple of the next batch of leftblock
                    lcurs = 0;
                } catch (EOFException e) {
                    try {
                        in.close();
                    } catch (IOException io) {
                        System.out.println("BlockNestedLoopJoin: Error in reading temporary file");
                    }
                    eosr = true;
                } catch (ClassNotFoundException c) {
                    System.out.println("BlockNestedLoopJoin: Error in de-serializing temporary file ");
                    System.exit(1);
                } catch (IOException io) {
                    System.out.println("BlockNestedLoopJoin: Error in reading temporary file");
                    System.exit(1);
                }
            }
        }
        return outbatch;
    }

    /**
     * load (numBuff - 2) batches into leftblock from left table
     **/
    private void load_left_block() {
        for (int i = 0; i < (numBuff - 2); i++) {
            Batch batch = left.next(); // get next batch of data
            if (batch != null) {
                leftblock.add(batch);
            }
        }
    }

    /**
     * load leftTuples with tuples from leftblock
     **/
    private void load_tuples_from_leftblock() {
        for (int i = 0; i < leftblock.size(); i++) {
            Batch batch = leftblock.get(i);

            //for each batch contained in leftblock, read in the tuples into leftTuples
            for (int tup = 0; tup < batch.size(); tup++)
                leftTuples.add(batch.get(tup));
        }
    }

    /**
     * Close the operator
     */
    public boolean close() {
        File f = new File(rfname);
        f.delete();
        return true;
    }

}
