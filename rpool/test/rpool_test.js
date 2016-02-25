'use strict'

const expect = require('chai').expect
const rpool = require('../src/rpool')
const r = require('rethinkdb')


describe('rpool.Pool specs', () => {
  it('is a function', () => {
    expect(rpool.Pool).to.be.a('function')
  })

  describe('#connect', () => {
    it('resolves the promise with the connection', () => {
      let pool = new rpool.Pool({host: 'localhost', port: 28015})
      return pool.connect().then((conn) => {
        expect(conn).to.be.ok
        conn.close()
      })
    })

    it('rejects the promise if it couldnt connect', () => {
      let pool = new rpool.Pool({host: 'unknown', port: 28015})
      return pool.connect().catch((err) => {
        expect(err.msg).to.match(/Could not connect/)
      })
    })
  })

  describe('#acquire', () => {
    it('resolves the promise if it can connect', () => {
      let pool = new rpool.Pool({host: 'localhost', port: 28015})
      return pool.canConnect()
    })

    it('rejects the promise if it couldnt connect', () => {
      let pool = new rpool.Pool({host: 'unknown', port: 28015})
      return pool.canConnect().catch((err) => {
        expect(err.msg).to.match(/Could not connect/)
      })
    })
  })

  describe('#initialize', () => {
    it('allocates the pool', () => {
      let pool = new rpool.Pool({host: 'localhost', port: 28015})
      pool.initialize()
      return pool.acquire().then((conn) => {
        expect(conn).to.be.ok
        pool.release(conn)
      })
    })

    it('allocates `max` connections', () => {
      let pool = new rpool.Pool({
        host: 'localhost',
        port: 28015,
        min: 1,
        max: 2,
      })
      pool.initialize()
      let firstConn
      return pool.acquire().then((conn) => {
        expect(conn).to.be.ok
        firstConn = conn
        return pool.acquire()
      }).then((conn) => {
        expect(conn).to.be.ok
        expect(conn).not.to.eq(firstConn)
        pool.release(firstConn)
        pool.release(conn)
      })
    })

    it('rejects the promise if acquire times out', () => {
      let pool = new rpool.Pool({
        host: 'localhost',
        port: 28015,
        min: 1,
        max: 1,
        acquireTimeoutMillis: 10
      })
      return pool.acquire().then((conn) => {
        setTimeout(() => { pool.release(conn) }, 15)
        return pool.acquire()
      }).catch((err) => {
        expect(err).to.be.ok
        expect(err).to.match(/Acquire timeout/)
      })
    })

    it('emits the `initialized` event', () => {
      let pool = new rpool.Pool()
      let called = false
      pool.on('initialized', () => { called = true })
      expect(called).to.be.false
      pool.initialize()
      expect(called).to.be.true
    })

    it('emits the `min_allocated` event', (done) => {
      let pool = new rpool.Pool({
        min: 2,
        max: 3
      })
      pool.on('min_allocated', () => {
        expect(pool.status().idle).to.eq(2)
        pool.close()
        done()
      })
      pool.initialize()
    })
  })

  describe('#status', () => {
    it('returns an object with status information', () => {
      let pool = new rpool.Pool()
      let status = pool.status()
      expect(status).to.have.property('min')
      expect(status).to.have.property('max')
      expect(status).to.have.property('idle')
      expect(status).to.have.property('scheduled')
      expect(status).to.have.property('waiting')
    })

    it('returns min, max and size from the specified options', () => {
      let pool = new rpool.Pool({
        min: 3,
        max: 4,
      })
      let status = pool.status()
      expect(status.min).to.eq(3)
      expect(status.max).to.eq(4)
      pool.close()
    })

    it('updates the idle and scheduled value as connections are acquired', () => {
      let pool = new rpool.Pool({
        min: 2,
        max: 3
      })
      pool.initialize()
      let firstConn, secondConn
      return pool.acquire().then(conn => {
        firstConn = conn
        expect(pool.status().idle).to.eq(2)
        expect(pool.status().scheduled).to.eq(1)
        return pool.acquire()
      }).then(conn => {
        secondConn = conn
        expect(pool.status().idle).to.eq(1)
        expect(pool.status().scheduled).to.eq(2)
        return pool.acquire()
      }).then(conn => {
        expect(pool.status().idle).to.eq(0)
        expect(pool.status().scheduled).to.eq(3)
        pool.release(firstConn)
        pool.release(secondConn)
        pool.release(conn)
        pool.close()
      })
    })

    it('updates the waiting acquires in `status`', () => {
      let pool = new rpool.Pool({
        min: 0,
        max: 1
      })
      pool.initialize()
      return pool.acquire().then(conn => {
        expect(pool.status().idle).to.eq(0)
        expect(pool.status().waiting).to.eq(0)
        expect(pool.status().scheduled).to.eq(1)
        pool.acquire()
        expect(pool.status().idle).to.eq(0)
        expect(pool.status().waiting).to.eq(1)
        expect(pool.status().scheduled).to.eq(1)
        pool.acquire()
        expect(pool.status().idle).to.eq(0)
        expect(pool.status().waiting).to.eq(2)
        expect(pool.status().scheduled).to.eq(1)
        pool.release(conn)
        pool.close()
      })
    })
  })

  describe('#run', () => {
    it('executes the given query', () => {
      let pool = new rpool.Pool({db: 'test'})
      return pool.run(r.tableList()).then((res) => {
        expect(res).to.be.ok
        pool.close()
      })
    })

    it('uses a connection available from the pool', (done) => {
      let pool = new rpool.Pool({ db: 'test', min: 4, max: 8 })
      pool.on('min_allocated', () => {
        expect(pool.status().idle).to.eq(4)
        expect(pool.status().scheduled).to.eq(0)
        pool.acquire()
        expect(pool.status().idle).to.eq(3)
        expect(pool.status().scheduled).to.eq(1)
        pool.close()
        done()
      })
      pool.initialize()
    })

    it('releases the connection after the query finishes', () => {
      let pool = new rpool.Pool({db: 'test'})
      return pool.run(r.tableList()).then((res) => {
        expect(pool.status().scheduled).to.eq(0)
        pool.close()
      })
    })
  })
})
