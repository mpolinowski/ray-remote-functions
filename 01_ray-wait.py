import ray
import time
import timeit

# simulate remote functions with
# different execution times
@ray.remote
def remote_task(x):
    time.sleep(x)
    return x

# create a list of things
things = list(range(10))
# ensure that the futures won’t complete in order
things.sort(reverse=True)


# # GET

# ## get results when all results are available
# def in_order():
#     ### use remote function to retrieve futures
#     futures = list(map(lambda x: remote_task.remote(x), things))
#     ### ray.get will block your function until all futures are returned
#     values = ray.get(futures)
#     ### loop over results and print
#     for v in values:
#         print(f" Completed {v}")
#         ### simulate some business logic
#         time.sleep(1)


# ## call order and see how long it takes to complete
# print("GET took: ", timeit.timeit(lambda: in_order(), number=1))


# WAIT

# ## process as results become available
# def as_available():
#     ### use remote function to retrieve futures
#     futures = list(map(lambda x: remote_task.remote(x), things))
#     ### while still futures left
#     while len(futures) > 0:
#         ### call ray.wait to get the next future
#         ready_futures, rest_futures = ray.wait(futures)
#         ### show progress
#         print(f"Ready {len(ready_futures)} rest {len(rest_futures)}")
#         ### print results
#         for id in ready_futures:
#             print(f'completed value {id}, result {ray.get(id)}')
#             ### simulate some business logic
#             time.sleep(1)
#         ### wait on the ones that are not yet available
#         futures = rest_futures

# ## call order and see how long it takes to complete
# print("WAIT took: ", timeit.timeit(lambda: as_available(), number=1))



## same as above but with timeout and remote cancel
def as_available():
    futures = list(map(lambda x: remote_task.remote(x), things))
    ### while still futures left
    while len(futures) > 0:
        ### call ray.wait to get the next future
        ### but with a 10s timeout and always collect 5 results before returning anything
        ready_futures, rest_futures = ray.wait(futures, timeout=10, num_returns=5)
        # if we get back less than num_returns 
        if len(ready_futures) < 1:
            print(f"Timed out on {rest_futures}")
            # cancel remote function, e.g. if task is using a lot of resources
            ray.cancel(*rest_futures)
            break
        for id in ready_futures:
            print(f'completed value {id}, result {ray.get(id)}')
            futures = rest_futures

## call order and see how long it takes to complete
print("WAIT took: ", timeit.timeit(lambda: as_available(), number=1))