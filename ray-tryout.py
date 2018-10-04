import ray

ray.init()

x_id = ray.put("example")
print(ray.get(x_id))  # "example"