"""
Script Name: test.py
Author: Sami Hussein Hamid Saeed
ID: 31195261
Date : 20/5/23
Description: The following driver program performs basic correctness and runtime testing on
the query-based algorithm implemented in query_poc.py
"""
import matplotlib.pyplot as plt
import string
import random
import query_poc as q

if __name__ == "__main__":
    # testing correctness
    # Test Case 1
    r_test = [('Adele', 8), ('Bob', 22), ('Clement', 16), ('Dave', 23), ('Ed', 11),
              ('Fung', 25), ('Goel', 3), ('Harry', 17), ('Irene', 14), ('Joanna', 2),
              ('Kelly', 6), ('Lim', 20), ('Meng', 1), ('Noor', 1), ('Omar', 1)]
    s_test = [('Arts', 8), ('Business', 15), ('CompSc', 2), ('Dance', 12), ('Engineering', 7),
              ('Finance', 21), ('Geology', 10), ('Health', 11), ('IT', 18)]
    n = 5

    expected = [['Adele', 8, 'Arts'], ['Bob', 22, 'nan'], ['Clement', 16, 'nan'],
                ['Dave', 23, 'nan'], ['Ed', 11, 'Health'], ['Fung', 25, 'nan'],
                ['Goel', 3, 'nan'], ['Harry', 17, 'nan'], ['Irene', 14, 'nan'],
                ['Joanna', 2, 'CompSc'], ['Kelly', 6, 'nan'], ['Lim', 20, 'nan'],
                ['Meng', 1, 'nan'], ['Noor', 1, 'nan'], ['Omar', 1, 'nan']]

    result_query1 = q.query_join(r_test, s_test, n)

    # Assert the results
    assert expected == result_query1, "Test case 1 failed"
    print("Test case 1 passed successfully!")

    # Test Case 2
    r_test = [('John', 8), ('Alice', 12), ('Mark', 15)]
    s_test = [('Sales', 12), ('Marketing', 20), ('Finance', 8), ('IT', 15)]

    expected = [['Alice', 12, 'Sales'], ['John', 8, 'Finance'], ['Mark', 15, 'IT']]
    result_query2 = q.query_join(r_test, s_test, n)

    # Assert the results
    assert expected == result_query2, "Test case 2 failed"
    print("Test case 2 passed successfully!")

    # testing runtime
    size = 100000  # dataset size
    data_sets = []  # list of S datasets
    divisors = [1, 2, 3, 4, 6]  # divisors for skewed datasets S

    # generate datasets
    for divisor in divisors:
        data_sets.append(q.generate_dataset(size, divisor))

    # generate data set R with 0% selectivity
    R = []
    while len(R) < size:
        for i in range(size, size * 2):
            if len(R) == size:
                break
            letters = string.ascii_uppercase
            R.append((random.choice(letters), i))

    # runtime with data set R having low selectivity
    runtimes_low = []
    for S in data_sets:
        query_runtime = q.runtime_test(R, S, n)
        runtimes_low.append(query_runtime)

    # runtimes with data set R having high selectivity
    R = data_sets[0]
    runtimes_high = []
    for S in data_sets:
        query_runtime = q.runtime_test(R, S, n)
        runtimes_high.append(query_runtime)

    datasets = [1, 2, 3, 4, 5]
    plt.plot(datasets, runtimes_high, marker='o')
    plt.plot(datasets, runtimes_low, marker='o')
    plt.xticks(datasets)
    plt.xlabel('Data sets')
    plt.ylabel('Time (seconds)')
    plt.title('Query Join Runtime Performance')
    plt.legend(["Query - 100% Selectivity", "Query - 0% Selectivity"])
    plt.show()
