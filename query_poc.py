"""
Script Name: query_poc.py
Author: Sami Hussein Hamid Saeed
ID: 31195261
Date : 20/5/23
Description: The following program contains a proof-of-concept of the query-based algorithm
proposed to deal with high levels of data skew when performing outer joins in a shared nothing cloud computing
system. The program contains a implementation of the outer join algorithm and some runtime tests with different levels
of skew.

ACKNOWLEDGEMENT:
- For the purpose of the POC I have taken insipiration from the parallel outer join implementations in our week 4 lab
 therefore for this POC I have reused and improved some elements of my week 4 lab answers for the POC
"""
import numpy as np
import multiprocessing as mp
import time
import random
import string

def hash_function(record):
    """
    The hash function 'hash_function' that is used in the hashing process by
    summing the digits of the hashed attribute, which in this case is the join attribute.

    Arguments:
    record -- a record where hashing will be applied to its join attribute

    Return:
    hash_index --  the hash index of the record
    """

    # Converts the value of the join attribute into digits
    digits = [int(d) for d in str(record[1])]

    # Calulate the sum of elemenets in the digits as the hash value
    return sum(digits)


def outer_join(left_records, right_records, join="left"):
    """
        Perform an outer join operation between two sets of records by using hashing and
        dictionaries to join the left and right records based on their joining attribute.

        Arguments:
        left_records -- list of records from the left relation
        right_records -- list of records from the right relation
        join -- type of join to perform ("left", "right", or "inner")

        Returns:
        result -- the result of the outer join operation
        """
    # Inner join
    if join == "inner":
        # create dictionary
        hash_dict = {}
        # iterate through the right records and hash them into hash_dict, if the index does not exist then create one
        for record in right_records:
            hash_index = hash_function(record)
            if hash_index in hash_dict.keys():
                hash_dict[hash_index].add(record)
            else:
                hash_dict[hash_index] = {record}

        result = []
        # iterate through the left records and hash them into hash_dict, if the index does not exist then create one
        for left_record in left_records:
            left_hash = hash_function(left_record)
            if left_hash in hash_dict.keys():
                for item in hash_dict[left_hash]:
                    # if there is a match then append it to the final result of the inner join
                    if item[1] == left_record[1]:
                        result.append([left_record[0], item[1], item[0]])
        return result

    # Left or right join
    if join in ["left", "right"]:
        # create dictionary
        hash_dict = {}
        # iterate through right records
        for record in right_records:
            hash_index = hash_function(record)
            if hash_index in hash_dict.keys():
                hash_dict[hash_index].add(record)
            else:
                hash_dict[hash_index] = {record}

        result = []
        for left_record in left_records:
            is_found = False  # To check whether there is a match found.
            left_hash = hash_function(left_record)

            if left_hash in hash_dict.keys():
                for item in hash_dict[left_hash]:
                    if item[1] == left_record[1]:
                        result.append([left_record[0], item[1], item[0]])
                        is_found = True
                        break

            if not is_found:
                result.append([left_record[0], left_record[1], str(np.nan)])
        return result

    else:
        raise AttributeError('join should be in {left, right, inner}.')


def hash_distribution(relation, n):
    """
    Used to distribute data in the relation based on hash partitioning strategy.

    Arguments:
    relation -- the input relation to be partitioned
    n -- number of processors/partitions

    Returns:
    result -- a dictionary containing the n partitions of the relation
    """

    def s_hash(value, n):
        """
                A simple hash function for performing the partitioning.

                Arguments:
                value -- the value to be hashed
                n -- number of partitions

                Returns:
                h -- the hash index
        """
        hash_index = value % n
        return hash_index

    # create the dictionary
    result = {}
    # iterate through each element of the relation
    for elem in relation:
        # get the hash key of the tuple based on the key
        h_key = s_hash(elem[1], n)
        # if key exists then add the tuple
        if h_key in result.keys():
            result[h_key].add(elem)
        else:
            # if key does not exist then create an entry
            result[h_key] = {tuple(elem)}

    return result


def hash_redistribution(relation):
    """
    Redistribute data using hash partitioning. We do not need n
    when redistributing since the data to be redistributed contains
    the index of its originating partition

    Arguments:
    relation -- the input data to be redistributed

    Returns:
    result -- a dictionary containing the redistributed data
    """
    # create the dictionary
    result = {}
    # iterate through each element of relation
    for elem in relation:
        # we can retrieve the hash key from the element itself since it already contains the index of the
        # original partition
        h_key = elem[-1]
        if h_key in result.keys():
            result[h_key].add((elem[0], elem[1]))

        else:
            # if key does not exist then create an entry
            result[h_key] = {tuple(elem)}

    return result


def extract_unique_keys(distribution):
    """
        Function to extract the unique keys from a distribution.
        The function creates a set, iterates through each partition and adds the key (joining attribute) of each tuple
        into the set in order to

        Arguments:
        distribution -- a dictionary partition containing the partitioned data

        Returns:
        key_partition_pairs -- a list of key-partition_index pairs
    """
    # create a set to store the unique keys
    unique_keys = set()
    # create an array to store the unique keys and their originating partition in the form (indx, unique_key)
    key_partition_pairs = []

    # iterate through each partition
    for partition_index, partition in distribution.items():
        # iterate through each tuple within a partition
        for item in partition:
            # if the key is not in the set of unique keys then add it and store the index-unique_key pair
            if item[1] not in unique_keys:
                unique_keys.add(item[1])  # Assuming the ID is stored at index 1
                key_partition_pairs.append((partition_index, item[1]))  # Assuming the ID is stored at index 1

    return key_partition_pairs


def query_join(r_dataset, s_dataset, n):
    """
        Perform a left outer join using the query-based algorithm approach

        Arguments:
        R -- the left data set in the join operation
        S -- the right data set in the join operation, contains highly skewed data
        n -- the number of partitions or processors

        Returns:
        result -- the array result of the left outer join between the relations R and S
    """

    # distribution using hash partitioning to emulate RDDs. (phase 1)
    r_dis = hash_distribution(r_dataset, n)
    s_dis = hash_distribution(s_dataset, n)

    # extract the unique keys (phase 2)
    unique_keys = extract_unique_keys(s_dis)
    # partition the unique keys
    unique_keys_dis = hash_distribution(unique_keys, n)

    # declare arrays for collecting results
    non_matched = []
    matched = []

    # local outer join
    pool = mp.Pool(n)
    results = []
    for i in r_dis.keys():
        # Apply a local left outer join on each processor
        if i in unique_keys_dis.keys():
            result = pool.apply_async(outer_join, [r_dis[i], unique_keys_dis[i]])
            results.append(result)
        else:
            # if the an equivalent partition does not exist then that means there is no match
            for elem in r_dis[i]:
                non_matched.append([elem[0], elem[1], 'nan'])

    # iterate through all the collected results from n processors
    for result in results:
        # for each result get the array output of the local outer join
        arrays = result.get()
        for array in arrays:
            # iterate through each tuple within the arrays to know if it found a match inside the local outer join or
            # not
            if array[-1] == 'nan':
                # if nan or None then append to the non_matched list
                non_matched.append(array)
            elif array[-1] != 'nan':
                # if there is a value then append to matched list
                matched.append(array)

    # at this point of the program the non_matched results are finalized and are ready to be printed/saved/written
    # we focus on the matched results, and create a temporary distribution to inner join them with the relation S
    matched = hash_redistribution(matched)
    results = []

    # inner join between s_dis and the matched distribution
    for i in range(n):
        if i in matched.keys() and i in s_dis.keys():
            result = pool.apply_async(outer_join, [matched[i], s_dis[i], "inner"])
            results.append(result)

    # declare an array to collect the result of the inner join
    matched_array = []
    for result in results:
        for elem in result.get():
            matched_array.append(elem)

    # combine the result of matched and non_matched lists
    result = matched_array + non_matched
    # sort based on the value
    result = sorted(result, key=lambda x: x[0])
    return result


def runtime_test(r_dataset, s_dataset, n):
    """
        Measure the runtime of the query-based left outer join execution.

        Arguments:
        r_dataset -- the first input dataset
        s_dataset -- the second input dataset, contains varying levels of skew
             n    -- number of processors
        Returns:
        query_time -- the time taken to execute the left outer join using query-based algorithm
    """
    start_time = time.time()
    query_join(r_dataset, s_dataset, n)
    query_time = time.time() - start_time
    return query_time


def generate_dataset(size, divisor):
    """
        Generate a dataset of a given size and repeated values.The divisor will limit
        the range of available values when generating the dataset.

        Arguments:
        size -- the size of the dataset
        divisor -- the divisor used to determine the number of within the while loop iterations

        Returns:
        dataset -- the generated dataset
    """
    dataset = []

    while len(dataset) < size:
        for i in range(size // divisor):
            if len(dataset) == size:
                break
            letter = string.ascii_uppercase
            dataset.append((random.choice(letter), i))

    return dataset

