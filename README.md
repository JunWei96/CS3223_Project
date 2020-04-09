# CS3223_Project - On Query Processing
This is a SPJ query engine which performs query optimization.

## The implementations and modifications
#### 1. Block nested join
The [implementation](https://github.com/JunWei96/CS3223_Project/pull/10):
open(): Materialize the right pages and opens the connection.
next(): For every left block, get matching tuples from the right batch and output the resultant batch.
#### 2. sort Merge Join
The [external sort](https://github.com/JunWei96/CS3223_Project/pull/4) is inplemented first, then the [sort merge join](https://github.com/JunWei96/CS3223_Project/pull/5/files) is implemented.
#### 3. DISTINCT implementation
The [modifications](https://github.com/JunWei96/CS3223_Project/pull/7/commits/d443dc5ba586fab5691729a60314057779ee1b8e)
#### 4. Groupby implementation
The [implementation](https://github.com/JunWei96/CS3223_Project/pull/11/commits/ecb2d762352c65f0137d5db0196fab0294ad408e)
#### 5. Random optimizer implementation
We have implemented [2 phrase optmization](https://github.com/JunWei96/CS3223_Project/pull/1)

## Bugs identified

