package qp.operators;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Schema;
import qp.utils.Tuple;

import java.util.ArrayList;


public class GroupBy extends Operator {

    private Operator base;                      // The table based on which the GroupBy function is acting on
    private int batchsize;                      // Number of tuples per out batch

    private ArrayList<Attribute> projectlist;   // A list of all the attributes the query wants to project
    private ArrayList<Attribute> groupbylist;   // A list of all the attributes the query wants to group by

    private Tuple lastTuple;                    // Used as a temp tuple for tuple comparisons in .next() method
    private ArrayList<Integer> attrIndex;       // Stores indices of the specified attributes in the projectlist

    ExternalSort sortedOperator;                // This sorted operator is used instead of the base operator

    public GroupBy(Operator base, ArrayList<Attribute> projectlist, ArrayList<Attribute> groupbylist, int type) {
        super(OpType.GROUPBY);
//        System.out.println("\n(GroupBy) <Constructor> Base: " + base);
        this.base = base;
        this.projectlist = projectlist;
        this.groupbylist = groupbylist;
        /** batchsize = number of records in a page **/
        this.batchsize = Batch.getPageSize() / base.getSchema().getTupleSize();
    }

    public void setBase(Operator base) {
        this.base = base;
    }

    public Operator getBase() {
        return base;
    }

    /** Preprocessing method to derive the sorted operator to carry out GroupBy function on **/
    public void setOperation(Operator base, int numBuff) {
        this.base = base;
        sortedOperator = new ExternalSort(base, numBuff, OpType.GROUPBY);
    }

    @Override
    public boolean open() {
        /** Preliminary check of whether the GroupBy function is valid **/
        if (!GroupByAttributeValid()) {
            System.out.println("Invalid GroupBy operation");
            return false;
        }
        lastTuple = null;

        /** Storing indices of the projected attributes first; will be useful when making tuple comparisons later **/
        Schema baseSchema = base.getSchema();
//        System.out.println("(GroupBy) <open> baseSchema size: " + baseSchema.getTupleSize());
//        System.out.println("(GroupBy) <open> baseSchema col size: " + baseSchema.getNumCols());
        attrIndex = new ArrayList<>(projectlist.size());
        for (int i = 0; i < projectlist.size(); i++) {
            Attribute attr = (Attribute) projectlist.get(i);
//            System.out.println("(GroupBy) <open> Project List Attribute: " + attr);
//            System.out.println("(GroupBy) <open> Value of i: " + i);
//            System.out.println("(GroupBy) <open> Index of attr: " + baseSchema.indexOf(attr));
            attrIndex.add(baseSchema.indexOf(attr));
        }
//        System.out.println("(GroupBy) <open> Before sortedOperator is opened");
        return sortedOperator.open();
    }

    /**
     * Read the next Batch (i.e. page) from the Operator and read all the Tuples, group them based on specified
     * group by columns, and add them to the outbatch to be written out
     **/
    @Override
    public Batch next() {
        Batch inbatch = sortedOperator.next();
        if (inbatch == null) {
            return null;
        }
        Batch outbatch = new Batch(batchsize);
        /** For every tuple in the inbatch, add the data record of each projected attribute to the out tuple. The out
         *  tuple will then compare itself to the lastTuple or compare itself against the ordered list of comparison
         *  attributes in the groupbylist**/
        for (int i = 0; i < inbatch.size(); i++) {
            Tuple basetuple = inbatch.get(i);
            ArrayList<Object> out = new ArrayList<>();
            for (int j = 0; j < projectlist.size(); j++) {
                out.add(basetuple.dataAt(attrIndex.get(j)));
            }
            Tuple outtuple = new Tuple(out);
            if (lastTuple == null || Tuple.compareTuples(outtuple, lastTuple,
                    getCompareOrder(groupbylist, base.getSchema()),
                    getCompareOrder(groupbylist, base.getSchema())) != 0) {
                outbatch.add(outtuple);
            }
            lastTuple = outtuple;
        }
        return outbatch;
    }

    public boolean close() {
        return base.close();
    }

    /** Returns an arraylist with sorted attributes according to its GroupBy priority stated in the sql query **/
    private ArrayList<Integer> getCompareOrder(ArrayList<Attribute> groupbylist, Schema schema) {
        ArrayList<Integer> result = new ArrayList<Integer>(schema.getNumCols());
        for (int i = 0; i < schema.getNumCols(); i++) {
            result.add(-1);
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

    /** A preliminary boolean checker ensuring that the GroupBy function is valid **/
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
//            System.out.println("(Group By) <GroupByAttributeValid>");
            for (int i = 0; i < projectlist.size(); i++) {
                Attribute attr = (Attribute) projectlist.get(i);
//                System.out.println("(Group By) <GroupByAttributeValid> Attr " + i + " in projectlist: " + attr);
                /** There is no key type in the projected list attribute, need to refer to the original schema**/
                attr = schema.getAttribute(schema.indexOf(attr));
//                System.out.println("(Group By) <GroupByAttributeValid> Attr in Schema" + i + " in projectlist: " + attr);
                /** The specified attribute and PK must exist in the groupby list for function to be valid **/
                if (notInGroupbylist(attr) && primaryKeyNotInGroupby(schema, groupbylist)) {
                    return false;
                }
            }
        }
        return true;
    }

    /** Boolean checker for ensuring if the specified attribute is inside the groupbylist **/
    private boolean notInGroupbylist(Attribute attr) {
        for (int i = 0; i < groupbylist.size(); i++) {
            if (attr.equals((Attribute) groupbylist.get(i))) {
                return false;
            }
        }
        return true;
    }

    /** Boolean checker for ensuring if the Primary Key is inside the groupbylist **/
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
        op.setSchema(this.getSchema());
        return op;
    }
}