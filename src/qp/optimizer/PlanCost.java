/**
 * This method calculates the cost of the generated plans
 * also estimates the statistics of the result relation
 **/

package qp.optimizer;

import qp.operators.*;
import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Condition;
import qp.utils.Schema;

import javax.swing.*;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;

import static java.lang.Math.ceil;
import static java.lang.Math.log;

public class PlanCost {

    long cost;
    long numtuple;

    /**
     * If buffers are not enough for a selected join
     * * then this plan is not feasible and return
     * * a cost of infinity
     **/
    boolean isFeasible;

    /**
     * Hashtable stores mapping from Attribute name to
     * * number of distinct values of that attribute
     **/
    HashMap<Attribute, Long> ht;


    public PlanCost() {
        ht = new HashMap<>();
        cost = 0;
    }

    /**
     * Returns the cost of the plan
     **/
    public long getCost(Operator root) {
        cost = 0;
        isFeasible = true;
        System.out.println("\n(PlanCost) <getCost>");
        System.out.println("(PlanCost) <getCost> Find Plan's Optype: " + root.getOpType());

        numtuple = calculateCost(root);
        System.out.println("(PlanCost) <getCost> Find numtuples: " + numtuple);

        if (isFeasible) {
            return cost;
        } else {
            System.out.println("notFeasible");
            return Long.MAX_VALUE;
        }
    }

    /**
     * Get number of tuples in estimated results
     **/
    public long getNumTuples() {
        return numtuple;
    }


    /**
     * Returns number of tuples in the root
     **/
    protected long calculateCost(Operator node) {
        if (node.getOpType() == OpType.JOIN) {
            return getStatistics((Join) node);
        } else if (node.getOpType() == OpType.DISTINCT) {
            return getStatistics((Distinct) node);
        } else if (node.getOpType() == OpType.SELECT) {
            return getStatistics((Select) node);
        } else if (node.getOpType() == OpType.PROJECT) {
            return getStatistics((Project) node);
        } else if (node.getOpType() == OpType.SCAN) {
            return getStatistics((Scan) node);
        } else if (node.getOpType() == OpType.GROUPBY) {
            System.out.println("(PlanCost) <calculateCost> Entered OpType.GROUPBY if branch");
            return getStatistics((GroupBy) node);
        }
        System.out.println("operator is not supported");
        isFeasible = false;
        return 0;
    }

    /**
     * Projection will not change any statistics
     * * No cost involved as done on the fly
     **/
    protected long getStatistics(Project node) {
        return calculateCost(node.getBase());
    }

    /**
     * Calculates the statistics and cost of join operation
     **/
    protected long getStatistics(Join node) {
        long lefttuples = calculateCost(node.getLeft());
        long righttuples = calculateCost(node.getRight());

        if (!isFeasible) {
            return 0;
        }

        Schema leftschema = node.getLeft().getSchema();
        Schema rightschema = node.getRight().getSchema();

        /** Get size of the tuple in output & correspondigly calculate
         ** buffer capacity, i.e., number of tuples per page **/
        long tuplesize = node.getSchema().getTupleSize();
        long outcapacity = Math.max(1, Batch.getPageSize() / tuplesize);
        long leftuplesize = leftschema.getTupleSize();
        long leftcapacity = Math.max(1, Batch.getPageSize() / leftuplesize);
        long righttuplesize = rightschema.getTupleSize();
        long rightcapacity = Math.max(1, Batch.getPageSize() / righttuplesize);
        long leftpages = (long) ceil(((double) lefttuples) / (double) leftcapacity);
        long rightpages = (long) ceil(((double) righttuples) / (double) rightcapacity);

        double tuples = (double) lefttuples * righttuples;
        for (Condition con : node.getConditionList()) {
            Attribute leftjoinAttr = con.getLhs();
            Attribute rightjoinAttr = (Attribute) con.getRhs();
            int leftattrind = leftschema.indexOf(leftjoinAttr);
            int rightattrind = rightschema.indexOf(rightjoinAttr);
            leftjoinAttr = leftschema.getAttribute(leftattrind);
            rightjoinAttr = rightschema.getAttribute(rightattrind);

            /** Number of distinct values of left and right join attribute **/
            long leftattrdistn = ht.get(leftjoinAttr);
            long rightattrdistn = ht.get(rightjoinAttr);
            tuples /= (double) Math.max(leftattrdistn, rightattrdistn);
            long mindistinct = Math.min(leftattrdistn, rightattrdistn);
            ht.put(leftjoinAttr, mindistinct);
            ht.put(rightjoinAttr, mindistinct);
        }
        long outtuples = (long) ceil(tuples);

        /** Calculate the cost of the operation **/
        int joinType = node.getJoinType();
        long numbuff = BufferManager.getBuffersPerJoin();
        long joincost;

        switch (joinType) {
            case JoinType.NESTEDJOIN:
                joincost = SNLJCost(leftpages, leftuplesize, rightpages);
//                joincost = 0;
                break;
            case JoinType.BLOCKNESTED:
                joincost = BNLJCost(leftpages, rightpages, numbuff);
//                joincost = 0;
                break;
            case JoinType.SORTMERGE:
                joincost = SMJCost(leftpages, rightpages, numbuff);
//                joincost = 0;
                break;
            default:
                System.out.println("join type is not supported");
                return 0;
        }
        cost = cost + joincost;

        return outtuples;
    }

    /**
     * Find number of incoming tuples, Using the selectivity find # of output tuples
     * * And statistics about the attributes
     * * Selection is performed on the fly, so no cost involved
     **/
    protected long getStatistics(Select node) {
        long intuples = calculateCost(node.getBase());
        if (!isFeasible) {
            System.out.println("notFeasible");
            return Long.MAX_VALUE;
        }

        Condition con = node.getCondition();
        Schema schema = node.getSchema();
        Attribute attr = con.getLhs();
        int index = schema.indexOf(attr);
        Attribute fullattr = schema.getAttribute(index);
        int exprtype = con.getExprType();

        /** Get number of distinct values of selection attributes **/
        long numdistinct = intuples;
        Long temp = ht.get(fullattr);
        numdistinct = temp.longValue();

        long outtuples;
        /** Calculate the number of tuples in result **/
        if (exprtype == Condition.EQUAL) {
            outtuples = (long) ceil((double) intuples / (double) numdistinct);
        } else if (exprtype == Condition.NOTEQUAL) {
            outtuples = (long) ceil(intuples - ((double) intuples / (double) numdistinct));
        } else {
            outtuples = (long) ceil(0.5 * intuples);
        }

        /** Modify the number of distinct values of each attribute
         ** Assuming the values are distributed uniformly along entire
         ** relation
         **/
        for (int i = 0; i < schema.getNumCols(); ++i) {
            Attribute attri = schema.getAttribute(i);
            long oldvalue = ht.get(attri);
            long newvalue = (long) ceil(((double) outtuples / (double) intuples) * oldvalue);
            ht.put(attri, outtuples);
        }
        return outtuples;
    }

    protected long getStatistics(Distinct node) {
        long pages = calculateCost(node.getBase()) / Batch.getPageSize();
        int numOfBuffer = BufferManager.numBuffer;
        long distinctCost = externalSortCost(pages, numOfBuffer);
        cost = cost + distinctCost;
        return calculateCost(node.getBase());
    }

    protected long getStatistics(GroupBy node) {
        System.out.println("(Plan Cost) <getStatistics(GroupBy node)>");
        System.out.println("(Plan Cost) <getStatistics(GroupBy node)>: " + node.getBase().getOpType());
        long pages = calculateCost(node.getBase()) / Batch.getPageSize();
        System.out.println("(Plan Cost) <getStatistics(GroupBy node)> Num Pages: " + pages);
        int numOfBuffer = BufferManager.numBuffer;
        System.out.println("(Plan Cost) <getStatistics(GroupBy node)> Num Buffers: " + numOfBuffer);
        long groupbyCost = externalSortCost(pages, numOfBuffer);
        System.out.println("(Plan Cost) <getStatistics(GroupBy node)> GroupBy Cost: " + groupbyCost);
        cost = cost + groupbyCost;
        return calculateCost(node.getBase());
}

    /**
     * The statistics file <tablename>.stat to find the statistics
     * * about that table;
     * * This table contains number of tuples in the table
     * * number of distinct values of each attribute
     **/
    protected long getStatistics(Scan node) {
        String tablename = node.getTabName();
        String filename = tablename + ".stat";
        Schema schema = node.getSchema();
        int numAttr = schema.getNumCols();
        BufferedReader in = null;
        try {
            in = new BufferedReader(new FileReader(filename));
        } catch (IOException io) {
            System.out.println("Error in opening file" + filename);
            System.exit(1);
        }
        String line = null;

        // First line = number of tuples
        try {
            line = in.readLine();
        } catch (IOException io) {
            System.out.println("Error in readin first line of " + filename);
            System.exit(1);
        }
        StringTokenizer tokenizer = new StringTokenizer(line);
        if (tokenizer.countTokens() != 1) {
            System.out.println("incorrect format of statastics file " + filename);
            System.exit(1);
        }
        String temp = tokenizer.nextToken();
        long numtuples = Long.parseLong(temp);
        try {
            line = in.readLine();
        } catch (IOException io) {
            System.out.println("error in reading second line of " + filename);
            System.exit(1);
        }
        tokenizer = new StringTokenizer(line);
        if (tokenizer.countTokens() != numAttr) {
            System.out.println("incorrect format of statastics file " + filename);
            System.exit(1);
        }
        for (int i = 0; i < numAttr; ++i) {
            Attribute attr = schema.getAttribute(i);
            temp = tokenizer.nextToken();
            Long distinctValues = Long.valueOf(temp);
            ht.put(attr, distinctValues);
        }

        /** Number of tuples per page**/
        long tuplesize = schema.getTupleSize();
        long pagesize = Math.max(Batch.getPageSize() / tuplesize, 1);
        long numpages = (long) ceil((double) numtuples / (double) pagesize);

        cost = cost + numpages;

        try {
            in.close();
        } catch (IOException io) {
            System.out.println("error in closing the file " + filename);
            System.exit(1);
        }
        return numtuples;
    }

    protected long SNLJCost(long leftPages, long numOfRecordPerLeftPage, long rightPages) {
        return leftPages + (leftPages * numOfRecordPerLeftPage) * rightPages;
    }

    protected long BNLJCost(long leftPages, long rightPages, long numOfBuffers) {
        long numOfIterationsForRight = (long) ceil(rightPages / (float) numOfBuffers);
        return leftPages + numOfIterationsForRight * rightPages;
    }

    protected long SMJCost(long leftPages, long rightPages, long numOfBuffers) {
        long totalPages = leftPages + rightPages;
        return externalSortCost(leftPages, numOfBuffers) + externalSortCost(rightPages, numOfBuffers) + totalPages;
    }

    protected long externalSortCost(long pages, long numOfBuffer) {
        long numOfPasses = (1 + (long) ceil(log(ceil(pages / (double) numOfBuffer)) / log(numOfBuffer - 1)));
        return 2 * pages * numOfPasses;
    }

}











