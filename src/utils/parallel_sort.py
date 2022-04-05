import math
import multiprocessing
from multiprocessing import Process
from functools import partial
from .combinations import *

def merge(*args):
    # Support explicit left/right args, as well as a two-item
    # tuple which works more cleanly with multiprocessing.
    left, right = args[0] if len(args) == 1 else args
    left_length, right_length = len(left), len(right)
    left_index, right_index = 0, 0
    merged = []
    while left_index < left_length and right_index < right_length:
        if left[left_index].score <= right[right_index].score:
            merged.append(left[left_index])
            left_index += 1
        else:
            merged.append(right[right_index])
            right_index += 1
    if left_index == left_length:
        merged.extend(right[right_index:])
    else:
        merged.extend(left[left_index:])
    return merged

def merge_sort(data):
    length = len(data)
    if length <= 1:
        return data
    middle = int(length / 2)
    left = merge_sort(data[:middle])
    right = merge_sort(data[middle:])
    return merge(left, right)

def parallel_sort(data):
    # Creates a pool of worker processes, one per CPU core.
    # We then split the initial data into partitions, sized
    # equally per worker, and perform a regular merge sort
    # across each partition.
    processes = multiprocessing.cpu_count()
    pool = multiprocessing.Pool(processes=processes)
    size = int(math.ceil(float(len(data)) / processes))
    data = [data[i * size:(i + 1) * size] for i in range(processes)]
    data = pool.map(merge_sort, data)
    # Each partition is now sorted - we now just merge pairs of these
    # together using the worker pool, until the partitions are reduced
    # down to a single sorted result.
    while len(data) > 1:
        # If the number of partitions remaining is odd, we pop off the
        # last one and append it back after one iteration of this loop,
        # since we're only interested in pairs of partitions to merge.
        extra = data.pop() if len(data) % 2 == 1 else None
        data = [(data[i], data[i + 1]) for i in range(0, len(data), 2)]
        data = pool.map(merge, data) + ([extra] if extra else [])
    return data[0]

def calculate(value_index, schema_index, similarity, query_match_list, sizes):
    for i in range(sizes[0], sizes[1]):
        item = query_match_list[i]
        item.calculate_total_score(value_index,schema_index, similarity)
        query_match_list[i] = item


def parallel_calc_metrics(query_matches, value_index, schema_index, similarity):
    processes = multiprocessing.cpu_count()
    #pool = multiprocessing.Pool(processes=processes)
  
        #with  multiprocessing.Pool(processes=processes) as pool:
        #    results = pool.map(partial(calculate,value_index, schema_index, similarity, query_match_list),sizes)
        
    print(query_match_list[0].total_score)



def parallel_generate_matches(keywords, keyword_matches, segments):
    processes = multiprocessing.cpu_count()
    partitions = combination_partitions(processes,keyword_matches,len(segments))

    with multiprocessing.Manager() as manager:
        shrd_keyword_match = manager.list(keyword_matches)
        shrd_partitions = manager.list(partitions)
        shrd_query_matches = manager.list([])
        shrd_keywords = manager.list(keywords)
        

        size = int(math.ceil(float(len(partitions)) / processes))
        sizes = [(i * size, (i +1)* size) for i in range(processes)]
        sizes[-1] = (sizes[-1][0], len(partitions))
        processes = [Process(target=generate_query_matches, args=(sizes[i], shrd_keywords, shrd_partitions, shrd_keyword_match, shrd_query_matches)) for i in range(processes)]
        
        for p in processes:
            p.start()

        for p in processes:
            p.join()


def partition_list_by_cpu(part_list):
    processes = multiprocessing.cpu_count()
    
    if len(part_list) < processes:
        return processes, [(0,len(part_list)), (-1, -1), (-1, -1), (-1, -1)]

    size = int(math.ceil(float(len(part_list)) / processes))
    sizes = [(i * size, (i +1)* size) for i in range(processes)]
    sizes[-1] = (sizes[-1][0], len(part_list))

    return processes, sizes
