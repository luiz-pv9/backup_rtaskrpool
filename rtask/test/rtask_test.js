'use strict'

const expect = require('chai').expect
const rtask = require('../src/rtask')

describe('rtask.queue', () => {
  it('is a function', () => {
    expect(rtask.queue).to.be.a('function')
  })

  it('stores the given name and options in the queue', () => {
    let queue = rtask.queue('my-queue', { value: 'something' })
    expect(queue.name).to.eq('my-queue')
    expect(queue.options.value).to.eq('something')
  })
})

describe('rtask.performNow', () => {
  it('executes the task immediatly', () => {
    let myQueue = rtask.queue('my-queue', {
      process: (data, done) => {
        done(null, "ok!")
      }
    })
    return rtask.performNow('my-queue', {}).then((res) => {
      expect(res).to.eq('ok!')
    })
  })

  it('receives the data specified in the perform task', () => {
    let myQueue = rtask.queue('my-queue', {
      process: (data, done) => {
        done(null, data + 2)
      }
    })
    return rtask.performNow('my-queue', 4).then((res) => {
      expect(res).to.eq(6)
    })
  })

  it('executes multiple tasks', () => {
    let myQueue = rtask.queue('my-queue', {
      process: (data, done, feedback) => {
        done(null, data + 2)
      }
    })
    return rtask.performNow('my-queue', 4).then((res) => {
      expect(res).to.eq(6)
      return rtask.performNow('my-queue', 5)
    }).then((res) => {
      expect(res).to.eq(7)
    })
  })

  it('rejects the promise if no queue was found', () => {
    return rtask.performNow('non-existant', {}).catch((err) => {
      expect(err).to.match(/not find/)
    })
  })
  
  it('performs only one task at a time by default', (done) => {
    let myQueue = rtask.queue('my-queue', {
      process: (data, done, feedback) => {
        setTimeout(() => {
          done(null, data + 2)
        }, 50)
      }
    })
    let currentTime = new Date()
    let firstResult
    rtask.performNow('my-queue', 1).then((res) => {
      firstResult = res
    })
    rtask.performNow('my-queue', 5).then((res) => {
      let diff = new Date() - currentTime
      expect(firstResult).to.eq(3)
      expect(res).to.eq(7)
      expect(diff).to.be.at.least(100)
      done()
    })
  })

  it('accepts concurrent tasks', () => {
  })
})

describe('rtask.status', () => {
  it('returns registered queues', () => {
  })

  it('returns count of jobs being processed', () => {
  })

  it('returns jobs waiting to be processed', () => {
  })
})

describe('feedbacks', () => {
})

describe('rtask.stop, rtask.resume', () => {
  it('rejects the promise if no queue was found', () => {
    return rtask.stop('non-existant').catch((err) => {
      expect(err).to.match(/not find/)
    })
  })

  it('waits for the current tasks to finish before stopping', () => {
    let myQueue = rtask.queue('my-queue', {
      process: (data, done, feedback) => {
        setTimeout(() => {
          done(null, data + 1)
        }, 50)
      }
    })
    let promise = rtask.performNow('my-queue', 5)
    rtask.stop('my-queue') // This doesn't prevent the task promise to finish
    return promise.then((res) => {
      expect(res).to.eq(6)
    })
  })

  it('cache jobs that are requested while the queue is stoped', () => {
    let myQueue = rtask.queue('my-queue', {
      process: (data, done, feedback) {
        done(null, data * 2)
      }
    })
    rtask.stop('my-queue').then(() => {
      // rtask.performNow('my-queue', 
    })
  })

  it('executes pending jobs after the queue is resumed', () => {
  })
})
