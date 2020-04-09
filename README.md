# CS3223_Project - On Query Processing
This is a SPJ query engine which performs query optimization.

## The implementations and modifications
#### 1. Block nested join
The implementation of block nested join can be found at `src\qp\operators\BlockNestedLoopJoin.java`. And it involves the following:
* open(): Materialize the right pages and opens the connection.
* next(): For every left block, get matching tuples from the right batch and output the resultant batch.
#### 2. Sort Merge Join
For the sort merge join, we first do external sorting on the left and right relation then merge these 2 sorted relation.
The external sort implementation can be found at `src\qp\operators\ExternalSort.java`. The sort merge join implementation can be found at `src\qp\operators\SortMergeJoin.java`
#### 3. DISTINCT implementation
The implementation can be found at `src\qp\operators\Distinct.java`.
#### 4. Groupby implementation
The implementation can be found at `src\qp\operators\GroupBy.java`.
#### 5. Random optimizer implementation
The implementation can be found at `src/qp/optimizer/RandomOptimizer.java`.
