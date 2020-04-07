/**
 * performs randomized optimization, iterative improvement algorithm
 **/

package qp.optimizer;

import qp.operators.*;
import qp.utils.Attribute;
import qp.utils.Condition;
import qp.utils.RandNumb;
import qp.utils.SQLQuery;

import java.util.ArrayList;

public class RandomOptimizer {

    /**
     * enumeration of different ways to find the neighbor plan
     **/
    public static final int METHODCHOICE = 0;  // Selecting neighbor by changing a method for an operator
    public static final int COMMUTATIVE = 1;   // By rearranging the operators by commutative rule
    public static final int ASSOCIATIVE = 2;   // Rearranging the operators by associative rule
    public static final int EXCHANGE = 3;

    /**
     * Number of altenative methods available for a node as specified above
     **/
    public static final int NUMCHOICES = 4;

    SQLQuery sqlquery;  // Vector of Vectors of Select + From + Where + GroupBy
    int numJoin;        // Number of joins in this query plan

    /**
     * constructor
     **/

    public RandomOptimizer(SQLQuery sqlquery) {
        this.sqlquery = sqlquery;
    }

    /**
     * After finding a choice of method for each operator
     * * prepare an execution plan by replacing the methods with
     * * corresponding join operator implementation
     **/
    public static Operator makeExecPlan(Operator node) {
        if (node.getOpType() == OpType.JOIN) {
            Operator left = makeExecPlan(((Join) node).getLeft());
            Operator right = makeExecPlan(((Join) node).getRight());
            int joinType = ((Join) node).getJoinType();
            int numbuff = BufferManager.getBuffersPerJoin();
            switch (joinType) {
                case JoinType.NESTEDJOIN:
                    NestedJoin nj = new NestedJoin((Join) node);
                    nj.setLeft(left);
                    nj.setRight(right);
                    nj.setNumBuff(numbuff);
                    return nj;
                case JoinType.BLOCKNESTED:
                    BlockNestedLoopJoin bj = new BlockNestedLoopJoin((Join) node);
                    bj.setLeft(left);
                    bj.setRight(right);
                    bj.setNumBuff(numbuff);
                    return bj;
                case JoinType.SORTMERGE:
                    SortMergeJoin sm = new SortMergeJoin((Join) node);
                    sm.setLeft(left);
                    sm.setRight(right);
                    sm.setNumBuff(numbuff);
                default:
                    return node;
            }
        } else if (node.getOpType() == OpType.DISTINCT) {
            Distinct distinctOp = ((Distinct) node);
            Operator base = makeExecPlan(distinctOp.getBase());
            distinctOp.setOperation(base, BufferManager.numBuffer);
            return node;
        } else if (node.getOpType() == OpType.SELECT) {
            Operator base = makeExecPlan(((Select) node).getBase());
            ((Select) node).setBase(base);
            return node;
        } else if (node.getOpType() == OpType.PROJECT) {
            Operator base = makeExecPlan(((Project) node).getBase());
            ((Project) node).setBase(base);
            return node;
        } else if (node.getOpType() == OpType.GROUPBY) {
            GroupBy GroupByOp = ((GroupBy) node);
            Operator base = makeExecPlan(GroupByOp.getBase());
            GroupByOp.setOperation(base, BufferManager.numBuffer);
            return node;
        } else {
            return node;
        }
    }

    /**
     * Randomly selects a neighbour
     **/
    protected Operator getNeighbor(Operator root) {
        // Randomly select a node to be altered to get the neighbour
        int nodeNum = RandNumb.randInt(0, numJoin - 1);
        // Randomly select type of alteration: Change Method/Associative/Commutative
        int changeType = RandNumb.randInt(0, NUMCHOICES - 1);
        Operator neighbor = null;
        switch (changeType) {
            case METHODCHOICE:   // Select a neighbour by changing the method type
                neighbor = neighborMeth(root, nodeNum);
                break;
            case COMMUTATIVE:
                neighbor = neighborCommut(root, nodeNum);
                break;
            case ASSOCIATIVE:
                neighbor = neighborAssoc(root, nodeNum);
                break;
            case EXCHANGE:
                neighbor = neighborExchange(root, nodeNum);
                break;
        }
        return neighbor;
    }

    /**
     * Implementation of Iterative Improvement Algorithm for Randomized optimization of Query Plan
     **/
    public Operator getOptimizedPlan() {
        /** get an initial plan for the given sql query **/
        System.out.println("\n(RandomOptimizer) <getOptimizedPlan>");
        RandomInitialPlan rip = new RandomInitialPlan(sqlquery);
        numJoin = rip.getNumJoins();

        PlanCost pc = new PlanCost();
        Operator initPlan = rip.prepareInitialPlan();
        System.out.println("(RandomOptimizer) <getOptimizedPlan> Prepare Initial Plan");
        modifySchema(initPlan);
        System.out.println("(RandomOptimizer) <getOptimizedPlan> Modify Schema of Initial Plan");

        long initCost = pc.getCost(initPlan);
        System.out.println("(RandomOptimizer) <getOptimizedPlan> Calculate Initial Cost: " + initCost);

        System.out.println("-----------initial Plan-------------");
        Debug.PPrint(initPlan);
        System.out.println(initCost);

        if (numJoin == 0) {
            return initPlan;
        }
        Operator initialStateOfAnnealing = iterativeImprovement(initPlan, pc);
        Operator result = simulatedAnnealing(initialStateOfAnnealing, pc);
        return result;
    }

    protected Operator iterativeImprovement(Operator initialPlan, PlanCost costPlan) {
        System.out.println("\n(RandomOptimizer) <iterativeImprovement> Going to Clone Initial Plan");
        Operator minCostPlan = (Operator) initialPlan.clone();
        System.out.println("\n(RandomOptimizer) <iterativeImprovement> Completed Cloning Initial Plan");
        int STOPPING_CONDITION = 4 * numJoin;
        int LOCAL_STOPPING_CONDITION = 16 * numJoin;
        while (STOPPING_CONDITION > 0) {
            Operator plan = (Operator) minCostPlan.clone();
            Operator randomState = getNeighbor(plan);
            while (LOCAL_STOPPING_CONDITION > 0) {
                Operator neighbourPlan = (Operator) plan.clone();
                Operator neighbour = getNeighbor(neighbourPlan);
                if (costPlan.getCost(neighbour) < costPlan.getCost(randomState)) {
                    minCostPlan = neighbour;
                }
                LOCAL_STOPPING_CONDITION--;
            }
            STOPPING_CONDITION--;
        }
        System.out.println("(RandomOptimizer) <iterativeImprovement> minCostPlan: " + minCostPlan);
        return minCostPlan;
    }

    protected Operator simulatedAnnealing(Operator initialPlan, PlanCost costPlan) {
        Operator localCostPlan = (Operator) initialPlan.clone();
        Operator minCostPlan = (Operator) localCostPlan.clone();

        double temperature = 0.1 * costPlan.getCost(minCostPlan);
        int numTimeMinCostUnchanged = 0;
        int EQUILIBRIUM = 16 * numJoin;

        while (temperature >= 1 && numTimeMinCostUnchanged < 4) {
            while (EQUILIBRIUM > 0) {
                Operator randomState = (Operator) localCostPlan.clone();
                long changeInCost = costPlan.getCost(randomState) - costPlan.getCost(localCostPlan);
                if (changeInCost <= 0) {
                    localCostPlan = randomState;
                }
                if (changeInCost > 0) {
                    double probability = Math.exp(-1 * changeInCost / temperature);
                    if (Math.random() <= probability) {
                        localCostPlan = randomState;
                    }
                }
                if (costPlan.getCost(localCostPlan) < costPlan.getCost(minCostPlan)) {
                    minCostPlan = localCostPlan;
                    numTimeMinCostUnchanged = 0;
                } else {
                    numTimeMinCostUnchanged++;
                }
                EQUILIBRIUM--;
            }
            temperature *= 0.95;
        }
        return minCostPlan;
    }

    /**
     * Selects a random method choice for join wiht number joinNum
     * *  e.g., Nested loop join, Sort-Merge Join, Hash Join etc..,
     * * returns the modified plan
     **/

    protected Operator neighborMeth(Operator root, int joinNum) {
        System.out.println("------------------neighbor by method change----------------");
        int numJMeth = JoinType.numJoinTypes();
        if (numJMeth > 1) {
            /** find the node that is to be altered **/
            Join node = (Join) findNodeAt(root, joinNum);
            int prevJoinMeth = node.getJoinType();
            int joinMeth = RandNumb.randInt(0, numJMeth - 1);
            while (joinMeth == prevJoinMeth) {
                joinMeth = RandNumb.randInt(0, numJMeth - 1);
            }
            node.setJoinType(joinMeth);
        }
        return root;
    }

    /**
     * This exchange rule will avoid the use of join commutativity which reduce the number of plateaux by adding direct
     * paths that bypass them.
     * As mentioned in the paper, exchange rules are equivalent to commutative then associative then commutative.
     */
    public Operator neighborExchange(Operator root, int joinNum) {
        System.out.println("------------------neighbor by method exchange----------------");
        root = neighborCommut(root, joinNum);
        root = neighborAssoc(root, joinNum);
        root = neighborCommut(root, joinNum);
        modifySchema(root);
        return root;
    }

    /**
     * Applies join Commutativity for the join numbered with joinNum
     * *  e.g.,  A X B  is changed as B X A
     * * returns the modifies plan
     **/
    protected Operator neighborCommut(Operator root, int joinNum) {
        System.out.println("------------------neighbor by commutative---------------");
        /** find the node to be altered**/
        Join node = (Join) findNodeAt(root, joinNum);
        System.out.println("neighborCommut node: " + node);
        System.out.println("neighborCommut left node: " + node.getLeft());
        System.out.println("neighborCommut left node: " + node.getRight());
        Operator left = node.getLeft();
        Operator right = node.getRight();
        node.setLeft(right);
        node.setRight(left);
        node.getCondition().flip();
        modifySchema(root);
        return root;
    }

    /**
     * Applies join Associativity for the join numbered with joinNum
     * *  e.g., (A X B) X C is changed to A X (B X C)
     * *  returns the modifies plan
     **/
    protected Operator neighborAssoc(Operator root, int joinNum) {
        /** find the node to be altered**/
        Join op = (Join) findNodeAt(root, joinNum);
        Operator left = op.getLeft();
        Operator right = op.getRight();

        if (left.getOpType() == OpType.JOIN && right.getOpType() != OpType.JOIN) {
            transformLefttoRight(op, (Join) left);
        } else if (left.getOpType() != OpType.JOIN && right.getOpType() == OpType.JOIN) {
            transformRighttoLeft(op, (Join) right);
        } else if (left.getOpType() == OpType.JOIN && right.getOpType() == OpType.JOIN) {
            if (RandNumb.flipCoin())
                transformLefttoRight(op, (Join) left);
            else
                transformRighttoLeft(op, (Join) right);
        } else {
            // The join is just A X B,  therefore Association rule is not applicable
        }

        /** modify the schema before returning the root **/
        modifySchema(root);
        return root;
    }

    /**
     * This is given plan (A X B) X C
     **/
    protected void transformLefttoRight(Join op, Join left) {
        System.out.println("------------------Left to Right neighbor--------------");
        Operator right = op.getRight();
        Operator leftleft = left.getLeft();
        Operator leftright = left.getRight();
        Attribute leftAttr = op.getCondition().getLhs();
        Join temp;

        if (leftright.getSchema().contains(leftAttr)) {
            System.out.println("----------------CASE 1-----------------");
            /** CASE 1 :  ( A X a1b1 B) X b4c4  C     =  A X a1b1 (B X b4c4 C)
             ** a1b1,  b4c4 are the join conditions at that join operator
             **/
            temp = new Join(leftright, right, op.getCondition(), OpType.JOIN);
            temp.setJoinType(op.getJoinType());
            temp.setNodeIndex(op.getNodeIndex());
            op.setLeft(leftleft);
            op.setJoinType(left.getJoinType());
            op.setNodeIndex(left.getNodeIndex());
            op.setRight(temp);
            op.setCondition(left.getCondition());

        } else {
            System.out.println("--------------------CASE 2---------------");
            /**CASE 2:   ( A X a1b1 B) X a4c4  C     =  B X b1a1 (A X a4c4 C)
             ** a1b1,  a4c4 are the join conditions at that join operator
             **/
            temp = new Join(leftleft, right, op.getCondition(), OpType.JOIN);
            temp.setJoinType(op.getJoinType());
            temp.setNodeIndex(op.getNodeIndex());
            op.setLeft(leftright);
            op.setRight(temp);
            op.setJoinType(left.getJoinType());
            op.setNodeIndex(left.getNodeIndex());
            Condition newcond = left.getCondition();
            newcond.flip();
            op.setCondition(newcond);
        }
    }

    protected void transformRighttoLeft(Join op, Join right) {
        System.out.println("------------------Right to Left Neighbor------------------");
        Operator left = op.getLeft();
        Operator rightleft = right.getLeft();
        Operator rightright = right.getRight();
        Attribute rightAttr = (Attribute) op.getCondition().getRhs();
        Join temp;

        if (rightleft.getSchema().contains(rightAttr)) {
            System.out.println("----------------------CASE 3-----------------------");
            /** CASE 3 :  A X a1b1 (B X b4c4  C)     =  (A X a1b1 B ) X b4c4 C
             ** a1b1,  b4c4 are the join conditions at that join operator
             **/
            temp = new Join(left, rightleft, op.getCondition(), OpType.JOIN);
            temp.setJoinType(op.getJoinType());
            temp.setNodeIndex(op.getNodeIndex());
            op.setLeft(temp);
            op.setRight(rightright);
            op.setJoinType(right.getJoinType());
            op.setNodeIndex(right.getNodeIndex());
            op.setCondition(right.getCondition());
        } else {
            System.out.println("-----------------------------CASE 4-----------------");
            /** CASE 4 :  A X a1c1 (B X b4c4  C)     =  (A X a1c1 C ) X c4b4 B
             ** a1b1,  b4c4 are the join conditions at that join operator
             **/
            temp = new Join(left, rightright, op.getCondition(), OpType.JOIN);
            temp.setJoinType(op.getJoinType());
            temp.setNodeIndex(op.getNodeIndex());
            op.setLeft(temp);
            op.setRight(rightleft);
            op.setJoinType(right.getJoinType());
            op.setNodeIndex(right.getNodeIndex());
            Condition newcond = right.getCondition();
            newcond.flip();
            op.setCondition(newcond);
        }
    }

    /**
     * This method traverses through the query plan and
     * * returns the node mentioned by joinNum
     **/
    protected Operator findNodeAt(Operator node, int joinNum) {
        if (node.getOpType() == OpType.JOIN) {
            if (((Join) node).getNodeIndex() == joinNum) {
                return node;
            } else {
                Operator temp;
                temp = findNodeAt(((Join) node).getLeft(), joinNum);
                if (temp == null)
                    temp = findNodeAt(((Join) node).getRight(), joinNum);
                return temp;
            }
        } else if (node.getOpType() == OpType.SCAN) {
            return null;
        } else if (node.getOpType() == OpType.SELECT) {
            // if sort/project/select operator
            return findNodeAt(((Select) node).getBase(), joinNum);
        } else if (node.getOpType() == OpType.PROJECT) {
            return findNodeAt(((Project) node).getBase(), joinNum);
        } else if (node.getOpType() == OpType.DISTINCT) {
            return findNodeAt(((Distinct) node).getBase(), joinNum);
        } else if (node.getOpType() == OpType.GROUPBY) {
            return findNodeAt(((GroupBy) node).getBase(), joinNum);
        } else {
            return null;
        }
    }

    /**
     * Modifies the schema of operators which are modified due to selecing an alternative neighbor plan
     **/
    private void modifySchema(Operator node) {
        if (node.getOpType() == OpType.JOIN) {
            Operator left = ((Join) node).getLeft();
            Operator right = ((Join) node).getRight();
            modifySchema(left);
            modifySchema(right);
            node.setSchema(left.getSchema().joinWith(right.getSchema()));
        } else if (node.getOpType() == OpType.SELECT) {
            Operator base = ((Select) node).getBase();
            modifySchema(base);
            node.setSchema(base.getSchema());
        } else if (node.getOpType() == OpType.PROJECT) {
            Operator base = ((Project) node).getBase();
            modifySchema(base);
            ArrayList attrlist = ((Project) node).getProjAttr();
            node.setSchema(base.getSchema().subSchema(attrlist));
        } else if (node.getOpType() == OpType.GROUPBY) {
            Operator base = ((GroupBy) node).getBase();
            modifySchema(base);
            node.setSchema(base.getSchema());
        } else if (node.getOpType() == OpType.DISTINCT) {
            Operator base = ((Distinct) node).getBase();
            modifySchema(base);
            node.setSchema(base.getSchema());
        }
    }
}
