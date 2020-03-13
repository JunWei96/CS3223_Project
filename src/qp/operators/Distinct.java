package qp.operators;

public class Distinct extends Operator {
    static int filenum = 0;         // To get unique filenum for this operation
    int batchsize;                  // Number of tuples per out batch

    public Distinct(int type) {
        super(type);
    }
}
