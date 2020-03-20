package qp.operators;

import qp.utils.Batch;
import qp.utils.Schema;
import qp.utils.Tuple;

import java.io.*;
import java.util.*;

import static java.lang.Math.min;

public class ExternalSort extends Operator{
    private Operator raw;
    private int bufferNum;
    private int filenum;
    private int roundnum;
    private int BatchSize;
    private Comparator<Tuple> comparator;
    private List<File> sortedRunsFile;
    private ArrayList<Integer> index_sort;
    private ObjectInputStream resultStream;

    public ExternalSort(Operator raw_operator, int Buffernum, ArrayList<Integer> index_sort_in) {
        super(OpType.SORT);
        this.raw = raw_operator;
        this.bufferNum = Buffernum;
        this.index_sort = index_sort_in;
    }

    public List<File> GetSortedRunFile()
    {
        return this.sortedRunsFile;
    }

    @Override
    // open() for pre-processing.
    public boolean open() {
        if (!raw.open()) {
            return false;
        }

        this.filenum = 0;
        this.roundnum = 0;
        this.sortedRunsFile = new ArrayList<>();
        this.comparator = new TupleSortComparator(this.index_sort);
        // JUST FOR TESTING
        int tupleSize;
        if (raw.getSchema() == null) {
            tupleSize = 4;
        } else {
            tupleSize = this.raw.getSchema().getTupleSize();
        }
        System.out.println("CURRENT PAGEE SIZE: " + Batch.getPageSize());
        this.BatchSize = Batch.getPageSize()/ tupleSize;

        // current batch
        Batch batchCurrent = this.raw.next();

        int initTupleNum = 0;

        // put all the batches in an array list of batch
        // generate sorted runs
        while (batchCurrent != null) {
            System.out.println("Generate Sorted run");
            ArrayList<Batch> run = new ArrayList<>();
            for (int i=0; i<this.bufferNum; i++) {
                if (batchCurrent == null) {
                    break;
                } else {
                    System.out.println(i);
                    initTupleNum += batchCurrent.size();
                    System.out.println(initTupleNum + " " + batchCurrent.size());
                    run.add(batchCurrent);
                    batchCurrent = this.raw.next();
                }
            }
            System.out.println("SIZE:" + run.size());
            List<Tuple> tuples = new ArrayList<>();
            for (Batch batch : run) {
                // for each page, append tuples to a list of tuples.
                for (int j = 0; j < batch.size(); j++) {
                    tuples.add(batch.get(j));
                }
            }
            Collections.sort(tuples, this.comparator);

            // after sorting, append back to the batches.
            List<Batch> batchesFromBuffer = new ArrayList<>();
            Batch NewCurrentBatch = new Batch(this.BatchSize);

            for (Tuple tuple : tuples) {
                NewCurrentBatch.add(tuple);
                if (NewCurrentBatch.isFull()) {
                    batchesFromBuffer.add(NewCurrentBatch);
                    NewCurrentBatch = new Batch(this.BatchSize);
                }
            }
            // last page may not always be full.
            if (!NewCurrentBatch.isEmpty()) {
                 batchesFromBuffer.add(NewCurrentBatch);
            }
            // PRINT THEM FOR TESTING. DELETE WHEN NOT NEEDED.
            System.out.println("Output batches size: " + batchesFromBuffer.size());
            System.out.println("Output batches: ");
            for (Batch batch : batchesFromBuffer) {
                for (int i=0; i < batch.size(); ++i) {
                    System.out.println(batch.get(i)._data);
                }
            }
            System.out.println("Size of batch from buffer: " + batchesFromBuffer.size());
            // write sorted runs (NewCurrentBatch) to temp file.
           if (batchesFromBuffer.size() <= 1) {
               System.out.println("NOT writing files");
           } else {
               File tempBatchFile = writeFile(batchesFromBuffer);
               this.sortedRunsFile.add(tempBatchFile);
           }
        }
        // end of phrase one
        // phrase two, merge sort implementation
        mergeRuns();

        // At the end, after the merging process, we should only have 1 run left.
        if (sortedRunsFile.size() != 1) {
            return false;
        }

        try {
            resultStream = new ObjectInputStream(new FileInputStream(sortedRunsFile.get(0)));
        } catch (IOException e) {
            System.out.println("IO Error when writing sorted file onto stream");
            return false;
        }
        return true;
    }

    @Override
    public Batch next() {
        return nextBatchFromStream(resultStream);
    }

    @Override
    public boolean close() {
        try {
            for (File file : sortedRunsFile) {
                file.delete();
            }
            resultStream.close();
        } catch (IOException e) {
            System.out.println("Error in closing result file stream.");
        }
        return true;
    }

    private File writeFile(List<Batch> batchesToWrite) {
        try {
            File tempBatchFile = new File("ExternalSort" +
                    "-" + this.roundnum +
                    "-" + this.filenum);

            ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(tempBatchFile));
            for (Batch batch : batchesToWrite) {
                out.writeObject(batch);
            }
            this.filenum++;
            // initialize files for temp batches
            out.close();
            return tempBatchFile;
        } catch(IOException e) {
            System.out.println("Error in writing external sort batches to files");
        }
        return null;
    }

    private void mergeRuns() {
        int AvailableBuffers = this.bufferNum - 1;
        int numOfMergeRuns = 0;
        while (this.sortedRunsFile.size() > 1) {
            List<File> sortedRunsThisRound = new ArrayList<>();
            for (int numOfMerges = 0; numOfMerges * AvailableBuffers < this.sortedRunsFile.size(); numOfMerges++) {
                int end = min((numOfMerges + 1) * AvailableBuffers, sortedRunsFile.size());
                List<File> extractRuns = this.sortedRunsFile.subList(numOfMerges * AvailableBuffers, end);
                File resultantRun = mergeSortedRuns(extractRuns, numOfMergeRuns, numOfMerges);
                sortedRunsThisRound.add(resultantRun);
            }
            for (File file : this.sortedRunsFile) {
                file.delete();
            }

            numOfMergeRuns++;
            this.sortedRunsFile = sortedRunsThisRound;
            System.out.println(this.sortedRunsFile.size());
        }
    }

    // input: files of sorted runs, each with a certain number of batches.
    // output: one single file of merged runs.
    private File mergeSortedRuns(List<File> sortedRuns, int numOfMergeRuns, int numOfMerges) {
        int numOfInputBuff = this.bufferNum - 1;
        if (sortedRuns.isEmpty()) {
            System.out.println("Sorted run is empty, nothing to sort here.");
            return null;
        }
        if (sortedRuns.size() > numOfInputBuff) {
            System.out.println("Number of sorted runs must be less than or equal to number of buffer - 1");
            return null;
        }

        ArrayList<Batch> inputBatches = new ArrayList<>();
        List<ObjectInputStream> inputs = new ArrayList<>();

        // Generated and input stream of sorted runs.
        for (File file : sortedRuns) {
            try {
                ObjectInputStream input = new ObjectInputStream(new FileInputStream(file));
                inputs.add(input);
            } catch (IOException e) {
                System.out.println("Error reading file into input stream.");
            }
        }

        // A single output buffer to store the sorted tuples. When it is full, we will spill it over to file.
        Batch outputBuffer = new Batch(this.BatchSize);
        // The result file to store the merged sorted runs.
        File resultFile = new File("ExternalSort_sortedRuns" + "_" + numOfMergeRuns + "_" + numOfMerges);
        ObjectOutputStream resultFileStream;
        try {
            resultFileStream = new ObjectOutputStream(new FileOutputStream(resultFile, true));
        } catch (FileNotFoundException e) {
            System.out.println("Unable to find file for output stream.");
            return null;
        } catch (IOException e) {
            System.out.println("IO error occurred while creating output stream.");
            return null;
        }

        // Feed in new batch into inputBatches.
        for (int sortedRunNum = 0; sortedRunNum < sortedRuns.size(); sortedRunNum++) {
            Batch nextBatch = nextBatchFromStream(inputs.get(sortedRunNum));
            if (nextBatch != null) {
                inputBatches.add(nextBatch);
            }
        }

        // Write all the tuples in inputBatches into the array of tuples. At the end, clear the tuples from
        // inputBatches.
        ArrayList<Tuple> inputTuples = new ArrayList<>();
        for (Batch batch : inputBatches) {
            if (batch == null) {
                continue;
            }
            while (!batch.isEmpty()) {
                Tuple tuple = batch.get(0);
                inputTuples.add(tuple);
                batch.remove(0);
            }
        }
        inputBatches.clear();

        // Sort the array of tuples in desc order.
        inputTuples.sort(this.comparator.reversed());

        // In each iteration we pop out the smallest element and add it into the output buffer
        // Once the buffer is filled write it into disk.
        while (!inputTuples.isEmpty()) {
            Tuple currentTuple = inputTuples.remove(inputTuples.size() - 1);
            outputBuffer.add(currentTuple);
            if (outputBuffer.isFull()) {
                try {
                    resultFileStream.writeObject(outputBuffer);
                    resultFileStream.reset();
                } catch (IOException e) {
                    System.out.println("Error in writing to output file during merging.");
                    return null;
                }
                outputBuffer.clear();
            }
        }
        return resultFile;
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

    class TupleSortComparator implements Comparator<Tuple>{
        private ArrayList<Integer> index_sort;
        TupleSortComparator(ArrayList<Integer> index_sort_in) {
            this.index_sort = index_sort_in;
        }

        @Override
        public int compare(Tuple firstTuple, Tuple secondTuple) {
            for (int index: this.index_sort)
            {
                int compareValue = Tuple.compareTuples(firstTuple, secondTuple, index);
                if (compareValue != 0) {
                    return compareValue;
                }
            }

            return 0;
        }
    }
}
