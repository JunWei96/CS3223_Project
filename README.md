# CS3223_Project - On Query Processing
This is a SPJ query engine which performs query optimization.

## The implementations and modifications
#### 1. Block nested join
The implementation:
* open(): Materialize the right pages and opens the connection.
* next(): For every left block, get matching tuples from the right batch and output the resultant batch.
The implementation can be found at CS3223_Project/src/qp/operators/BlockNestedLoopJoin.java

#### 2. sort Merge Join
The external sort is inplemented first, then the sort merge join is implemented. Can be found at CS3223_Project/src/qp/operators/ExternalSort.java and CS3223_Project/src/qp/operators/SortMergeJoin.java

#### 3. DISTINCT implementation
The modifications can be found at CS3223_Project/src/qp/operators/Distinct.java

#### 4. Groupby implementation
Group the records by some column attributes. The implementation is located at CS3223_Project/src/qp/operators/GroupBy.java

#### 5. Random optimizer implementation
We have implemented 2 phrase optmization. Details can be found in the report. The implementation is located at CS3223_Project/src/qp/optimizer/RandomOptimizer.java

Bugs Identified are specified in the report.

