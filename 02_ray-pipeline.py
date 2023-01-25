import random
import ray
import time

# first remote function
## retrieve a value from somewhere
@ray.remote
def generate_number(s: int, limit: int, sl: float) -> int :
    # create a random seed from value of 's'
    random.seed(s)
    # wait for value of 'sl'
    time.sleep(sl)
    # create a random number between '0' and the value of 'limit'
    return random.randint(0, limit)

# second remote function
## take values and do some math
@ray.remote
def sum_values(v1: int, v2: int, v3: int) -> int :
    # take three integer and add them up
    return v1+v2+v3

# use first remote function return  as input for second remote function
print(ray.get(sum_values.remote(generate_number.remote(1, 10, .1), generate_number.remote(5, 20, .2), generate_number.remote(7, 15, .3))))