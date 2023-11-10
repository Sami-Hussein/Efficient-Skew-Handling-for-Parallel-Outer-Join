# Efficient Skew Handling for Parallel Outer Join
## Introduction
This parallel outerjoin algorithm aims to reduce the increased time taken when dealing with highly skewed datasets in a distributed shared-nothing environment [1].

## Running the POC
This program is designed to perform certain operations and requires the following dependencies to be installed:
numpy
multiprocessing
time
random
string

## Results
![image](https://github.com/Sami-Hussein/Efficient-Skew-Handling-for-Parallel-Outer-Join/assets/62296680/f6627603-fc07-4b0d-82c8-27761d1c563c)

Overall, we can observe that regardless of the selectivity factor, the runtime of the outer join actually decreases as the skewness of the dataset increases.

## References
[1] L. Cheng and S. Kotoulas, "Efficient Skew Handling for Outer Joins in a Cloud Computing Environment," in IEEE Transactions 
    on Cloud Computing, vol. 6, no. 2, pp. 558-571, 1 April-June 2018, doi: 10.1109/TCC.2015.2487965.

