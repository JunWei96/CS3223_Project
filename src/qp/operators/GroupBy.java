package qp.operators;

import qp.optimizer.BufferManager;
import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Schema;
import qp.utils.Tuple;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.stream.Collectors;


public class GroupBy extends Operator {

    private Operator base;                      // The table based on which the GroupBy function is acting on
    private int batchsize;                      // Number of tuples per out batch
    private int numBuff;

    private ArrayList<Attribute> projectlist;   // A list of all the attributes the query wants to project
    private ArrayList<Attribute> groupbylist;   // A list of all the attributes the query wants to group by

    private Tuple lastTuple;
    private int[] attrIndex;

    ExternalSort sortedOperator;
    ExternalSort.TupleSortComparator comparator;
    int cursor;
    boolean eos;


    public GroupBy(Operator base, ArrayList<Attribute> projectlist, ArrayList<Attribute> groupbylist, int type) {
        super(OpType.GROUPBY);
        System.out.println("\n(GroupBy) <Constructor> Base: " + base);
        this.base = base;
        this.projectlist = projectlist;
        this.groupbylist = groupbylist;
    }

    public void setBase(Operator base) {
        this.base = base;
    }

    public Operator getBase() {
        return base;
    }

    public void setOperation(Operator base, int numBuff) {
        this.base = base;
        this.numBuff = numBuff;
        sortedOperator = new ExternalSort(base, numBuff);
        comparator = new ExternalSort.TupleSortComparator(sortedOperator.getAttributeList());
    }

    /**
     ** Returns an int array of Attribute indices; the Group By will group the Operator's tuples in the order
     *  of the sorted Attribute indices in the array
     **/
    private ArrayList<Integer> getSortOrder(ArrayList<Attribute> groupbylist, Schema schema) {
        /** Initialize the array **/
        System.out.println("(GroupBy) <getSortOrder>");
        System.out.println("(GroupBy) <getSortOrder> Schema Number of Columns: " + schema.getNumCols());
        int[] result = new int[schema.getNumCols()];
        for (int i = 0; i < schema.getNumCols(); i++) {
            result[i] = -1;
        }

        /** Append all the attribute indices of the attributes listed in groupbylist to result array **/
        int index = 0;
        for (; index < groupbylist.size(); index++) {
            Attribute attr = (Attribute) groupbylist.get(index);
            result[index] = schema.indexOf(attr);
        }

        /** Append the rest of the attribute indices NOT in groupbylist arbitrarily to result array **/
        for (int i = 0; i < schema.getNumCols(); i++) {
            if (result[i] == -1) {
                result[i] = index;
                index++;
            }
        }
        System.out.println("(GroupBy) <getSortOrder> Sorted Attribute Order: " + Arrays.toString(result));
        return (ArrayList<Integer>) Arrays.stream(result) // IntStream
                                    .boxed()  		// Stream<Integer>
                                    .collect(Collectors.toList());
    }

    @Override
    public boolean open() {
        /** select number of tuples per batch **/
        int tuplesize = schema.getTupleSize();

        /** batchsize = number of records in a page **/
        this.batchsize = Batch.getPageSize() / tuplesize;

        if (!GroupByAttributeValid()) {
            System.out.println("Invalid GroupBy operation");
            return false;
        }
        lastTuple = null;

        Schema baseSchema = base.getSchema();
        attrIndex = new int[projectlist.size()];
        for (int i = 0; i < projectlist.size(); i++) {
            Attribute attr = (Attribute) projectlist.get(i);
            attrIndex[i] = baseSchema.indexOf(attr);
        }
//        return base.open();
        cursor = 0;
        eos = false;
        System.out.println("(GroupBy) <open> Before sortedOperator is opened");

        if (sortedOperator.open()) {
            System.out.println("(GroupBy) <open> Check if sortedOperator is opened");
            Batch inbatch = sortedOperator.next();
            return true;
        } else {
            return false;
        }
    }

    /**
     * Read the next Batch (i.e. page) from the Operator and read all the Tuples, group them based on specified
     * group by columns, and add them to the outbatch to be written out
     * **/
    @Override
    public Batch next() {
        Batch inbatch = base.next();

        if (inbatch == null) {
            return null;
        }

        Batch outbatch = new Batch(batchsize);

        for (int i = 0; i < inbatch.size(); i++) {
            Tuple basetuple = inbatch.get(i);
            ArrayList<Object> out = new ArrayList<>();
            for (int j = 0; j < projectlist.size(); j++) {
                out.add((Attribute) basetuple.dataAt(attrIndex[j]));
            }
            Tuple outtuple = new Tuple(out);

            if (Tuple.compareTuples(outtuple, lastTuple, getCompareOrder(groupbylist, schema),
                    getCompareOrder(groupbylist, schema)) != 0) {
                outbatch.add(outtuple);
            }
            lastTuple = outtuple;
        }
        return outbatch;
    }

    public boolean close() {
        return base.close();
    }

    private ArrayList<Integer> getCompareOrder(ArrayList<Attribute> groupbylist, Schema schema) {
        ArrayList<Integer> result = new ArrayList<Integer>(schema.getNumCols());
        for (int i = 0; i < schema.getNumCols(); i++) {
            result.set(i, -1);
        }

        int index = 0;
        for (; index < groupbylist.size(); index++) {
            Attribute attr = (Attribute) groupbylist.get(index);
            result.set(index, schema.indexOf(attr));
        }
        for (int i = 0; i < schema.getNumCols(); i++) {
            if (result.get(i) == -1) {
                result.set(i, schema.indexOf((Attribute) groupbylist.get(0)));
            }
        }
        return result;
    }

    private boolean GroupByAttributeValid() {
        /** When no specific attribute has been chosen for projection (e.g. SELECT * FROM...) **/
        if (projectlist == null || projectlist.isEmpty()) {
            /** If table only contains 1 column, no need for validity check **/
            if (schema.getNumCols() == 1) {
                return true;
            }
            /** > 1 column in specified table **/
            for (int i = 0; i < schema.getNumCols(); i++) {
                Attribute attr = schema.getAttribute(i);
                /** Attribute does not exist in groupbylist and Primary Key does not exist in groupbylist **/
                if (notInGroupbylist(attr) && primaryKeyNotInGroupby(schema, groupbylist)) {
                    return false;
                }
            }
        }
        /** Specific columns specified in SELECT operation for projection**/
        else {
            System.out.println("(Group By) <GroupByAttributeValid>");
            for (int i = 0; i < projectlist.size(); i++) {
                Attribute attr = (Attribute) projectlist.get(i);
                System.out.println("(Group By) <GroupByAttributeValid> Attr " + i + " in projectlist: " + attr);
                // no key type in projectlist attribute, need change get it from schema
                attr = schema.getAttribute(schema.indexOf(attr));
                System.out.println("(Group By) <GroupByAttributeValid> Attr in Schema" + i + " in projectlist: " + attr);
                if (notInGroupbylist(attr) && primaryKeyNotInGroupby(schema, groupbylist)) {
                    return false;
                }
            }
        }
        return true;
    }

    private boolean notInGroupbylist(Attribute attr) {
        for (int i = 0; i < groupbylist.size(); i++) {
            if (attr.equals((Attribute) groupbylist.get(i))) {
                return false;
            }
        }
        return true;
    }

    private boolean primaryKeyNotInGroupby(Schema schema, ArrayList<Attribute> groupbylist) {
        Attribute primaryKey = null;
        for (int i = 0; i < schema.getNumCols(); i++) {
            Attribute attr = schema.getAttribute(i);
            if (attr.isPrimaryKey()) {
                primaryKey = attr;
                break;
            }
        }
        if (primaryKey == null) {
            return true;
        }
        for (int i = 0; i < groupbylist.size(); i++) {
            Attribute attr = (Attribute) groupbylist.get(i);
            if (primaryKey.equals(attr)) {
                return false;
            }
        }
        return true;
    }

    public Object clone() {
        GroupBy op = new GroupBy((Operator) base.clone(), projectlist, groupbylist, OpType.GROUPBY);
        System.out.println("Inside Clone -> clone operator: " + base.clone());
        System.out.println("Inside Clone -> projectlist: " + projectlist);
        System.out.println("Inside Clone -> groupbylist: " + groupbylist);
        op.setSchema(this.getSchema());
        return op;
    }
}
