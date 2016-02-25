'use strict'

const EventEmitter = require('events')

const EVENT_TASK_FAILED    = 'failed'
const EVENT_TASK_COMPLETED = 'completed'
const EVENT_TASK_FEEDBACK  = 'feedback'

const defaultQueueOptions = {
  concurrent: 1,
}

// Stores a reference to all created queues, mapped from the given `name` to
// the queue instance. We do this because we support `performNow` that 
// doesn't require an Redis instance to be running. In production
// we won't use `performNow` at all, but it helps a lot testing.
let queues = {}

// Keeps track of the current tasks currently being executed by each queue.
// By default, each queue can only execute one task at a time.
let tasks = {}

// Pending tasks of each queue.
let pending = {}

class TaskQueue extends EventEmitter {
  constructor(name, options) {
    super()
    this.name = name
    this.options = Object.assign({}, defaultQueueOptions, options)
    this.on(EVENT_TASK_COMPLETED, this.afterTaskCompleted.bind(this))
  }

  // This callback is called each time a task is completed.
  afterTaskCompleted(taskData, err, result) {
    let currentTasks = tasks[this.name]
    if(currentTasks.indexOf(taskData) !== -1) {
      currentTasks.splice(currentTasks.indexOf(taskData), 1)
    }
    let pendingTasks = pending[this.name]
    if(pendingTasks && pendingTasks.length > 0) {
      let pendingTask = pendingTasks.shift()
      performNowOnPromise(this.name, pendingTask.data, pendingTask.resolve, 
                          pendingTask.reject)
    }
  }

  execute(data, resolve, reject) {
    let taskCompleted = (err, result) => {
      this.emit(EVENT_TASK_COMPLETED, data, err, result)
      if(err) return reject(err);
      resolve(result);
    }
    if(this.options.process) {
      this.options.process(data, taskCompleted, this.feedback)
    } else {
      reject(new Error("[rtask] process callback not specified in queue" + this.name)) 
    }
  }
}

// Starts a new queue with the given `name` and `options`.
function queue(name, options) {
  let queue = new TaskQueue(name, options)
  queues[name] = queue
  return queue
}

function storePending(name, data, resolve, reject) {
  let pendingObject = {
    data: data,
    resolve: resolve,
    reject: reject
  }
  if(pending[name]) pending[name].push(pendingObject);
  else              pending[name] = [pendingObject];
}

function performNow(name, data, resolve, reject) {
  if(resolve && reject) {
    return performNowOnPromise(name, data, resolve, reject)
  }
  return new Promise((resolve, reject) => {
    performNowOnPromise(name, data, resolve, reject)
  })
}

function performNowOnPromise(name, data, resolve, reject) {
  let queue = queues[name]
  if(queue) {
    if(tasks[name] && tasks[name].length >= queue.options.concurrent) {
      storePending(name, data, resolve, reject)
    } else {
      tasks[name] = [data]
      queue.execute(data, resolve, reject)
    }
  } else {
    reject(new Error("[rtask] could not find queue with name: " + name))
  }
}

function stop(name) {
  return new Promise((resolve, reject) => {
    let queue = queues[name]
    if(queue) {
    } else {
      reject(new Error("[rtask] could not find queue with name: " + name))
    }
  })
}

// Public API
exports.queue = queue
exports.performNow = performNow
exports.stop = stop
