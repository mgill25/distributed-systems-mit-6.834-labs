# MapReduce Brainstorming

{
	"1": "assigned",
	"2": "done",
	"3": "awaiting"
}

> all tasks have been assigned (or done)
	> at this point, some workers can fail, in which case, tasks will have to be re-assigned

> some tasks are awaitig
	> assign them to workers and mark them as "assigned"

What information do I need in order to re-assign a task?
	- if it is a map task, I need the file name and task id
	- if it is a reduce task, I just need the task id (file names will be implicit)

So the workers just need to know the file name and task id at best.
	- for reduce, task id must be accurate, going up to nReduce (so that they pick the correct files)
	- for map, the task ids can be arbitrary but the file name must be exact

	- master uses 2 counters: one for map task ids, another for reduce task ids 			(internal design decision)
	- master knows at the beginning how many tasks does it need to launch for map as well as reduce
		- for map, at least as many as the number of files, can be greater
		- for reduce, user-supplied, so can be a less, equal to, or more than needed
		- *any* worker of any type can fail

we need a good way to organize the data structure in such a way that we can handle > needed workers
	- lets say we have M map workers that are needed (because we have n input files)
	- we launch M++++ workers that are present

- workers should ask for tasks from master (check)
- if all tasks have been assigned, workers should wait (till how long?)
	- if the M assigned tasks are successful, the extra workers should quietly go down
	- if any of the M assigned tasks fail to do their job, the extra workers should pick up the task
	- this should be the master's responsibility
		- basically, workers should "registry" with the master (hey I am ready to do some work!)
		- then master "assigns" work (and tracks what work it has given to the workers)
		- background threads check (using timeout) if the work was completed or not, and marks tasks as failed as needed
		- then, the master (in its internal state), re-sets the failed "task" (not task id) as "awaiting" (something that a free worker can pick up).
		- because all workers are periodically asking the master for tasks (and waiting if not given any), the next time they ask for a task, they will be assigned one of the failed tasks
		- they execute it, and hopefully they finish it this time around
		- they send a DONE call to the master. Master updates the state of the newly finished task
		- when all such tasks (both map and reduce) are done, master sends the signal for workers to close
		- workers exit
		- master marks itself as done
		- the process halts
--------------------------------------------------------------------------------------------------------------


## Master data structures
	input:
		- nReduce: Count of how many reduce workers should be launched
		- []files: length of input map file array

	these are the only things we know for sure. we can have some derived properties
		- len([]files) -> Number of files that need to be processed by workers
			- Note: this isn't a worker count! A single worker can also ideally process all the files, it will
			just be very slow. so this is just `numberOfFiles`.

## Master High Level Design
Assume locks where the master data structures need locking to support concurrent access

		- When worker asks for a task, we need a structure to track fileName with status.
			map_tasks = [
				{fileName: "input_file_name.txt", task_status: "awaiting"},
				{fileName: "input_file_name.txt", task_status: "awaiting"},
				{fileName: "some_other_file.txt", task_status: "assigned"},
				{fileName: "some_other_file.txt", task_status: "assigned"},
				...numberOfFiles times
			]

			reduce_tasks = [
				{task_status: "awaiting"},		# the index is the task id
				{task_status: "awaiting"},
				... nReduce times
			]
			map_done = false
			reduce_done = false
			map_all_assigned = false
			reduce_all_assigned = false
			map_counter = 0
			reduce_counter = 0
			nReduce
			numberOfFiles

			# called by a worker
			def GetTask()
				if not map_all_assigned:
					assign_to_worker("map") 	        # idea: the index in array becomes the task_id, which we store on master
					go launch_monitor(map_counter) 		# change status to "awaiting" if not done within the given time constraint
					map_counter++
					# Counter will only increase up until all input tasks have been assigned.
					# after that, until the tasks are done, we will wait.
					if map_counter == len(map_tasks):
						map_all_assigned = True

				else if not reduce_all_assigned:
					assign_to_worker("reduce")
					go launch_monitor(reduce_counter)
					reduce_counter++
					if reduce_counter == len(reduce_tasks):
						reduce_all_assigned = True

				else if map_done and reduce_done:
					tell_worker_to_quit()

				else:
					# in all other edge cases where map/reduce tasks aren't done and have been assigned,
					# we simply tell the worker to wait
					tell_worker_to_wait()


			def MarkDone(req)
				map_done = m.map_done
				if not map_done:
					if map_tasks[task_id].task_status == "assigned":
						map_tasks[i].task_status = "done"
					map_done = all([i.task_status == "done" for i in map_tasks])
				if not reduce_done:
					# similar for reduce_done

				if map_done and reduce_done:
					all_done = true

			def assign_to_worker(task_type, task_id):
				if task_type == "map":
					map_task[task_id].task_status = "assigned"
				elif task_type == "reduce":
					reduce_task[task_id].task_status = "assigned"

			def launch_monitor(task_id)
				time.sleep(10)
				if map_tasks[task_id].task_status != "done":
					map_task[task_id].task_status = "awaiting"
					map_all_assigned = false
				# same for reduce

## Worker High Level Design

			def Worker():
				while True:
					rv = master.GetTask()
					if rv == "close":
						# close this worker
						break
					elif rv == "map_task":
						execute_map_task()
					elif rv == "reduce_task":
						execute_reduce_task()
					else:
						# we don't know what to do. probably best to wait and retry after a second
						time.sleep(1)

			
			def execute_map_task(mapFunc, req):
				# apply mapFunc

			def execute_reduce_task(reduceFunc, req):
				# apply reduceFunc

## Design Considerations

We don't really need to use any kind of counters if we can help it. In an array of objects/structs, the index of the array itself can serve as the task_id. For an array of map files: `[file_1, file_2]`, the map tasks can simply be 0 and 1. 

Similarly, if we have been told that there are a total of nReduce tasks (say n=10), we will simply have an array of 10 task status.

## Design Revisions

**Flaws**
The above approach had a few flaws: it didn't take concurrent access into account fully. During concurrent RPC calls, it is a bad idea to maintain counter state for a few reasons:
	- It makes reasoning about the code and the calls harder
	- It lead to strange bugs where the monitor, on timeout, would decrement the count. This was particulary bad because I was using the counter itself as a task_id, which was a bad idea: because a task that needs to be reassigned would need to find the exact task id for the reassignment, and a global counter isn't really going to give us that.

Taking these things into account, I have revised my approach to now just store the entire state table of booleans. On every task assignment or re-assignment, we simply iterate over the entire state table and check which tasks are the ones which are marked as "awaiting" yet.

I have also introduced a convenience abstraction of a `Task` object, which I pass around and query (although directly mutating a task is not going to do anything because that only mutates a local copy intead of the global task inside the Master's task array).

## Current situation

The word count and the indexing examples are passing. The map parallelism test seems to only pass when it is preceeded by the wordcount test, suggesting some leftover state or process within the master that isn't properly cleaned up. 

The reduce parallelism test _always_ hangs up. We do not see any more active RPC calls in that case after the 6th worker successfully registers itself and marks its task as done.