import gevent
import six
import time
import random
from locust import TaskSet
from locust.core import TaskSetMeta


random.seed(time.time())


def task_wait(ms=0, max_ms=None):
    """A time based task decorator.
    """
    def decorator_func(func):
        func.locust_task_min_wait = ms
        func.locust_task_max_wait = max_ms
        return func
    """
    Check if task was used without parentheses (not called), like this::

        @task
        def my_task()
            pass
    """
    if callable(ms):
        # No parameters
        func = ms
        ms = 0
        max_ms = 0
        return decorator_func(func)
    else:
        if max_ms is None:
            max_ms = ms
        return decorator_func


class FakeTask(object):
    def __init__(self, task):
        self._task = task
        self.locust_task_min_wait = task.locust_task_min_wait
        self.locust_task_max_wait = task.locust_task_max_wait
        self.locust_task_remain_wait = random.randint(task.locust_task_min_wait,
                                                      task.locust_task_max_wait)

    def __call__(self, *args, **kwargs):
        return self._task(*args, **kwargs)

    def __lt__(self, other):
        return ((self.locust_task_remain_wait < other.locust_task_remain_wait) or
                (self.locust_task_remain_wait == other.locust_task_remain_wait and
                 (self.locust_task_min_wait + self.locust_task_max_wait) >
                 (other.locust_task_min_wait + other.locust_task_max_wait)))


class TimeTaskSetMeta(TaskSetMeta):
    """A Time based TaskSetMeta for changing the behavior of __new__ of TaskSetMeta.
    """
    def __new__(mcs, classname, bases, classDict):
        """This code is copied from locust.core.TaskSetMeta.
        And replace the locust_task_weight with locust_task_wait 
        which is used to the new time based scheduling.
        """
        new_tasks = []
        for base in bases:
            if hasattr(base, "tasks") and base.tasks:
                new_tasks += base.tasks

        if "tasks" in classDict and classDict["tasks"] is not None:
            tasks = classDict["tasks"]
            if isinstance(tasks, dict):
                tasks = six.iteritems(tasks)

            for task in tasks:
                if isinstance(task, tuple):
                    task, ms = task
                    task.locust_task_min_wait = ms
                    task.locust_task_max_wait = ms
                    new_tasks.append(task)
                else:
                    # Default wait time is 0
                    task.locust_task_min_wait = 0
                    task.locust_task_max_wait = 0
                    new_tasks.append(task)

        for item in six.itervalues(classDict):
            if hasattr(item, "locust_task_min_wait"):
                new_tasks.append(item)

        classDict["tasks"] = new_tasks
        classDict["min_wait"] = 0
        classDict["max_wait"] = 0
        return type.__new__(mcs, classname, bases, classDict)


class TimeTaskSet(six.with_metaclass(TimeTaskSetMeta, TaskSet)):
    """A Time based TaskSet, it should be used with task_wait.
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Important: For forking instance of tasks, or TaskSet will effect each others.
        self.tasks = [FakeTask(t) for t in self.tasks]

        self.last_pick_ts = time.time() * 1000      # milliseconds
        self.last_sleep_time = None                 # milliseconds
        self.total_elapsed_time = 0                 # milliseconds
        self.total_elapsed_time_threshold = max([t.locust_task_max_wait for t in self.tasks]) * 10
        self.task_delay_ratio = 0.1
        self.task_delay_threshold_ratio = 1.1
        return

    def get_next_task(self):
        """In TaskSet, this function just choice the next task randomly.
        We record the last called time of this function, and calculate the 
        elapsed time between two calls.
        Then maintain a task remain waiting time list to decide that which task should be picked.
        """
        # Get elapsed time for caculating the new remain waiting time of each tasks.
        pick_ts = time.time() * 1000
        elapsed_time = (pick_ts - self.last_pick_ts)
        self.last_pick_ts = pick_ts

        if self.last_sleep_time is None:
            self.last_sleep_time = elapsed_time

        # Get the next task whose remain waiting time is the minimum.
        next_task = min(self.tasks)

        # Task delay for maintain fixed reqs/s
        # Important: task_delay should not be negative!!
        # Due to the Negative task_delay will cause that the sleeping time of task be increasing.
        # And then cause more serious delay which is opposite to the purpose of task_delay
        if elapsed_time > self.last_sleep_time * self.task_delay_threshold_ratio:
            task_delay = (next_task.locust_task_remain_wait - self.total_elapsed_time) * self.task_delay_ratio
            task_delay = max(task_delay, 0)
        else:
            task_delay = 0

        # Sleep time will be remain time minus total elapsed time (minus task delay)
        # New remain wait time will be wait time(random from min to max) plus total elapsed time
        # Thus we don't need to do minus elapsed time for each task.(Time Complexity O(n) => O(1))
        sleep_time = next_task.locust_task_remain_wait - self.total_elapsed_time - task_delay
        self.total_elapsed_time += elapsed_time
        next_task.locust_task_remain_wait = random.randint(next_task.locust_task_min_wait,
                                                           next_task.locust_task_max_wait)
        next_task.locust_task_remain_wait += self.total_elapsed_time

        # Due to the remain time and total elapsed time will increase constantly
        # So we need to reset total elapsed time when it reach the threshold.
        if self.total_elapsed_time >= self.total_elapsed_time_threshold:
            for t in self.tasks:
                t.locust_task_remain_wait -= self.total_elapsed_time
            self.total_elapsed_time = 0

        # Sleep until the next_task available
        if sleep_time > 0:
            gevent.sleep(sleep_time / 1000)

        self.last_sleep_time = sleep_time
        return next_task
