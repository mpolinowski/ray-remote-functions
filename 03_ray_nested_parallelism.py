import random
import ray
import time

# retrieve a value from somewhere
@ray.remote
def generate_number(s: int, limit: int) -> int :
    # create a random seed from value of 's'
    random.seed(s)
    # simulate processing time
    time.sleep(.1)
    # create a random number between '0' and the value of 'limit'
    return random.randint(0, limit)

# invoke multiple remote functions and return the 'ObjectRef's
@ray.remote
def remote_objrefs():
    results = []
    for n in range(4):
        results.append(generate_number.remote(n, 4*n))
    return results

# store returned 'ObjectRef's from remote_objrefs()
futures = ray.get(remote_objrefs.remote())

# invoke multiple remote functions and return the resulting values directly
@ray.remote
def remote_values():
    results = []
    for n in range(4):
        results.append(generate_number.remote(n, 4*n))
    return ray.get(results)

# print returned results
print(ray.get(remote_values.remote()))


# take returned 'ObjectRef's and get results
while len(futures) > 0:
    ready_futures, rest_futures = ray.wait(futures, timeout=600, num_returns=1)
    # break when the return is smaller than num_returns
    if len(ready_futures) < 1:
        ray.cancel(*rest_futures)
        break
    for id in ready_futures:
        print(f'completed result {ray.get(id)}')
        futures = rest_futures