from math import ceil,comb as choose

def combinadic(n,k,m):
    max_m = choose(n,k)-1
    indices = [0]*k
    a = n
    b = k
    x = max_m-m
    for i in range(0,k):
        
        v=a-1
        while choose(v,b)>x:
            v-=1
        
        indices[i] = v      
        x-= choose(indices[i],b)  
        a=indices[i]
        b-=1
    for i in range(0,k):
        indices[i]= (n-1)-indices[i]
    return indices
        

def combinations_in_range(iterable, r, start_position = None, end_position = None):
    pool = tuple(iterable)
    n = len(pool)
    if r > n:
        return
    
    if start_position is None:
        indices = list(range(r))
    else:
        indices = combinadic(n,r,start_position)           
        
    if end_position is None:
        available_yields = 0
    else:
        available_yields = end_position-start_position+1
    
    
    yield tuple(pool[i] for i in indices)
    while True:
        available_yields-=1
        
        if available_yields == 0:
            return
        
        for i in reversed(range(r)):
            if indices[i] != i + n - r:
                break
        else:
            return
        indices[i] += 1
        for j in range(i+1, r):
            indices[j] = indices[j-1] + 1
        yield tuple(pool[i] for i in indices)

def combination_partitions(num_partitions, pool, max_combination_size = None):
    if max_combination_size is None:
        max_combination_size=len(pool)
    
    combination_partitions = []
    for size in range(1,min(len(pool), max_combination_size)+1):
        num_comb = choose(len(pool),size)
        range_length = max(ceil(num_comb/num_partitions),100)

        start_position = 0
        end_position = min(start_position+range_length,num_comb)
        while True:
            partition = (size,start_position,end_position)         
            combination_partitions.append(partition)

            if end_position>=num_comb:
                break

            start_position = end_position+1
            end_position = min(start_position+range_length,num_comb)
    return combination_partitions