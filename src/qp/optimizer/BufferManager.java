/**
 * simple buffer manager that distributes the buffers equally among all the join operators
 **/

package qp.optimizer;

public class BufferManager {

    public static int numBuffer;
    public static int numJoin;

    public static int buffPerJoin;

    public BufferManager(int numBuffer, int numJoin) {
        BufferManager.numBuffer = numBuffer;
        BufferManager.numJoin = numJoin;
        if (numJoin == 0) {
            buffPerJoin = 3;
        } else {
            buffPerJoin = numBuffer / numJoin;
        }
    }

    public static int getBuffersPerJoin() {
        return buffPerJoin;
    }

}
