'use strict'

const GenericPool = require('generic-pool').Pool
const r = require('rethinkdb')
const EventEmitter = require('events')

const defaultPoolOptions = {
  min: 8,
  max: 20,
  idleTimeoutMillis: 15000, // 15 seconds
  acquireTimeoutMillis: 2000, // 2 seconds
}

// Emitted after the user calls the `initialize` method.
const EVENT_INITIALIZED = 'initialized'

// Emitted after the pool allocates at least `min` objects.
const EVENT_MIN_ALLOCATED = 'min_allocated'

// Emitted whenever an acquire call times out. This should always be logged.
const EVENT_ACQUIRE_TIMEOUT = 'acquire_timeout'

class Pool extends EventEmitter {
  constructor(options) {
    super()
    this.options = Object.assign({}, defaultPoolOptions, options)
    this.initialized = false
  }

  // Initializes the connection pool opening `min` amounts of connections.
  initialize() {
    this.pool = new GenericPool({
      name:              'rethinkdb',
      create:            this.createConnection.bind(this),
      destroy:           this.destroyConnection.bind(this),
      min:               this.options.min,
      max:               this.options.max,
      idleTimeoutMillis: this.options.idleTimeoutMillis,
    })
    this.initialized = true
    this.created_count = 0
    this.emit(EVENT_INITIALIZED)
  }

  // Called by generic-pool
  createConnection(callback) {
    this.connect().then((conn) => {
      callback(null, conn)
      this.created_count += 1
      if(this.created_count === this.options.min) {
        this.emit(EVENT_MIN_ALLOCATED)
      }
    }).catch((err) => {
      callback(err, null)
    })
  }

  // Called by generic-pool
  destroyConnection(conn) {
    conn.close()
  }

  run(query) {
    return new Promise((resolve, reject) => {
      this.acquire().then((conn) => {
        query.run(conn, (err, result) => {
          this.release(conn)
          if(err) reject(err);
          resolve(result)
        })
      }).catch(reject)
    })
  }

  // Returns the current status of connection pool
  status() {
    if(!this.initialized) this.initialize();
    let data = {
      min: this.pool.getMinPoolSize(),
      max: this.pool.getMaxPoolSize(),
    }
    data.allocated = this.pool.getPoolSize()
    data.idle      = this.pool.availableObjectsCount()
    data.waiting   = this.pool.waitingClientsCount()
    data.scheduled = data.allocated - data.idle
    return data
  }

  // Attemps to connect to RethinkDB and returns a promise that resolve if it
  // succeeded and rejects if it failed. The error argument in the reject
  // callback will have the reason why the connection failed.
  connect() {
    return r.connect({
      host:    this.options.host,
      port:    this.options.port,
      db:      this.options.db,
      authKey: this.options.authKey,
      timeout: this.options.timeout || 20,
    })
  }

  // Closes all connections created in the pool. Subsequent calls to acquire() 
  // will throw an Error.
  close() {
    this.pool.drain(() => {
      this.pool.destroyAllNow()
    })
  }

  // Returns a promise that resolves if we're able to connect to RethinkDB
  // and rejects if we're not able. The connection is closed after the
  // promise resolves.
  canConnect() {
    return new Promise((resolve, reject) => {
      this.connect().then((conn) => {
        resolve(conn)
        conn.close()
      }).catch(reject)
    })
  }

  // Returns a promise that resolves
  acquire() {
    if(!this.initialized) this.initialize();
    return new Promise((resolve, reject) => {
      let timedOut = false
      let timeoutId = setTimeout(() => {
        timedOut = true
        reject(new Error("Acquire timeout. Call `status` for for information."))
      }, this.options.acquireTimeoutMillis)
      this.pool.acquire((err, conn) => {
        if(err) reject(err);
        if(timedOut) {
          this.pool.release(conn)
        } else {
          clearTimeout(timeoutId)
          resolve(conn)
        }
      })
    })
  }

  // Release the connection to make it available in the pool.
  release(conn) {
    this.pool.release(conn)
  }
}

// Public API
exports.Pool = Pool
