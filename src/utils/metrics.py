from math import log

def calcuate_inverse_frequency(num_total_attributes, num_attributes_with_this_word, index):
    if index in [0,2,3]:
        return log(num_total_attributes/num_attributes_with_this_word)
    if index == 1:
        return 1

def calcuate_index_weight(frequency, max_frequency, index):
    if index in [1,2]:
        return 1+log(frequency)
    if index == 0:
        return frequency
    if index == 3:
        return 0.5 + 0.5*(frequency/max_frequency)

