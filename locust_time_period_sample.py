import pprint
from locust import HttpLocust
from locust_time_taskset import TimeTaskSet
from locust_time_taskset import task_wait


class MasterTaskSet(TimeTaskSet):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @task_wait(2000)
    def task_2(self):
        self.client.post("/task_in_2_secs")

    @task_wait(3000)
    def task_3(self):
        self.client.post("/task_in_3_secs")
    
    @task_wait(5000, 5500)
    def task_5(self):
        self.client.post("/task_in_5_secs")


class WebsiteUser(HttpLocust):
    task_set = MasterTaskSet
    min_wait = 0
    max_wait = 0

