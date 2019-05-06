//
//  subman.js
//
//  Redis subscription manager, handles multiple pub-sub subscriptions over a
//  single 'subscribe' connection.
//
//  Copyright © 2019-2019 Trusted Key Solutions. All rights reserved.
//
const Redis = require('ioredis')

/** Factory method. Creates a new instance and subscribes to the topic.
 * @param {String} topic Unique identifier for this cache
 * @param {Redis.RedisOptions|Redis.Redis} [redisOrOptions] Redis client instance or options
 * @returns {Promise.<SubMan>} Promise that resolves to new subman instance
 */
exports.Create = async function(topic, redisOrOptions) {
  const subman = new SubMan(topic, redisOrOptions)
  await subman.start()
  return subman
}

exports.SubMan = SubMan

/// Internal error thrown when trying to subscribe to a channel twice
function DuplicateSubscriptionError(message) {
  Error.captureStackTrace(this, DuplicateSubscriptionError)
  this.message = message
  this.name = 'DuplicateSubscriptionError'
}

DuplicateSubscriptionError.prototype = Object.create(Error.prototype)

/** Constructor
 * @constructor
 * @param {String} topic Unique identifier for this subman
 * @param {Redis.RedisOptions|Redis.Redis} [redisOrOptions] Redis client instance or options
 */
function SubMan(topic, redisOrOptions) {
  if (typeof topic !== 'string') {
    throw Error('Expected 1st argument "topic" of type string')
  }
  this.topic = topic
  // Internal map that maps a channel name to a callback function
  this._map = new Map()

  if (redisOrOptions instanceof Redis) {
    // Redis client connection: publisher can be shared, but need to duplicate for subscriber
    this._redisPub = redisOrOptions
    this._redisSub = redisOrOptions.duplicate()
  } else {
    // Redis client options: save the options for later, but connect the subscriber now
    this._redisOptions = Object({}, redisOrOptions)
    this._redisSub = new Redis(redisOrOptions)
  }

  this._redisSub.on('message', (topic, channel) => {
    const callback = this._map.get(channel)
    if (callback != null) {
      this._pop(channel, callback)
    }
  })
}

/**
 * Start the subman by subscribing to its topic
 * @returns {Promise.<void>} Promise
 */
SubMan.prototype.start = async function() {
  // FIXME: no easy way to detect whether we're already connecting or connected
  // await this._redisSub.connect()
  return this._redisSub.subscribe(this.topic)
}

/**
 * Disconnect the subscriber from Redis
 * @returns {void}
 */
SubMan.prototype.stop = function() {
  // Notify all active subscriptions and abort all pending awaits
  Array.from(this._map.entries()).forEach(([channel, callback]) => callback(channel, Error('aborted')))
  // Note: cannot always close the _redisPub connection because it might be shared
  if (this._redisOptions && this._redisPub) {
    this._redisPub.disconnect()
  }
  return this._redisSub.disconnect()
}

/**
 * Subscribe to the given channel
 * @param {string} channel Channel name
 * @param {function} callback Callback function taking channel and message
 * @returns {Promise.<void>} Promise
 */
SubMan.prototype.subscribe = async function(channel, callback) {
  this._enforceStarted()
  const prev = this._map.get(channel)

  // Prevent receiving a message but not knowing how to handle it: set map first
  this._map.set(channel, callback)

  if (prev != null) {
    // CONSIDER: should we await the callback?
    prev(channel, new DuplicateSubscriptionError('Already subscribed to ' + channel))
  }

  // Check whether we got the message already
  return this._pop(channel, callback)
}

/**
 * Unsubscribe from channel
 * @param {string} channel Channel name
 * @returns {boolean} False when no previous subscription was found
 */
SubMan.prototype.unsubscribe = function(channel) {
  // CONSIDER: what to do if there's a pending `wait` call?
  return this._map.delete(channel)
}

// Fake timer, for keeping the code clean in `wait` below
const fakeTimer = {
  unref() {}
}

/** Wait for one message from channel
 * @param {string} channel Channel name
 * @param {number} [timeout] Time out in seconds
 * @returns {Promise.<any>} Promise that resolves to received message
 */
SubMan.prototype.wait = async function(channel, timeout) {
  return new Promise((resolve, reject) => {
    const timer = timeout > 0 ? setTimeout(() => this._publish(channel, null), timeout * 1000) : fakeTimer
    this.subscribe(channel, (_, msg) => {
      timer.unref()
      // Do not unsubscribe when DuplicateSubscriptionError is thrown, because the callback has already been replaced.
      if (!(msg instanceof DuplicateSubscriptionError)) {
        this.unsubscribe(channel)
      }
      if (msg instanceof Error) {
        reject(msg)
      } else {
        resolve(msg)
      }
    }).catch(reject)
  })
}

/**
 * Cancel a pending wait operation
 * @param {string} channel Channel name
 * @returns {void}
 */
SubMan.prototype.cancel = function(channel) {
  this._publish(channel, Error('cancelled'))
}

/**
 * Publish a message to a channel
 * @param {string} channel Channel name
 * @param {*} msg The message to send to subscribers
 * @param {number} [seconds] Time-to-live for the message
 * @returns {Promise.<number>} Resolves to current message count in channel
 */
SubMan.prototype.publish = async function(channel, msg, seconds) {
  // CONSIDER: there's a small chance that the only subscriber is on this node!
  this._enforceStarted()
  const message = JSON.stringify(msg)
  const pipeline = this._redis()
    .pipeline()
    .lpush(channel, message)
  if (seconds) {
    pipeline.expire(channel, seconds)
  }
  const [[err1, count], [err2]] = await pipeline.publish(this.topic, channel).exec()
  if (err1 || err2) {
    throw err1 || err2
  }
  return count
}

/**
 * Pop a single message from the given channel
 * @private
 * @param {string} channel Channel name
 * @param {function} callback Callback function taking channel and message
 * @returns {Promise.<void>} Promise
 */
SubMan.prototype._pop = async function(channel, callback) {
  const message = await this._redis().rpop(channel)
  if (message != null) {
    // CONSIDER: should we await the callback?
    callback(channel, JSON.parse(message))
  }
}

/**
 * Publish a message to the local subscriber only
 * @private
 * @param {string} channel Channel name
 * @param {*} msg The message to send to the local subscriber
 * @returns {void}
 */
SubMan.prototype._publish = function(channel, msg) {
  const callback = this._map.get(channel)
  if (callback != null) {
    // CONSIDER: should we await the callback?
    callback(channel, msg)
  }
}

/**
 * @private
 * @returns {Redis.Redis} Shared Redis instance
 */
SubMan.prototype._redis = function() {
  if (this._redisPub == null) {
    this._redisPub = new Redis(this._redisOptions)
  }
  return this._redisPub
}

/**
 * @private
 * @throws {Error} when subscriber connection is not ready
 * @returns {void}
 */
SubMan.prototype._enforceStarted = function() {
  if (this._redisSub.status !== 'ready') {
    throw Error('not started')
  }
}

/* eslint-disable no-undef */
global.describe &&
  describe('subman', function() {
    // eslint-disable-next-line security/detect-child-process
    const ChildProcess = require('child_process')
    const Assert = require('assert')
    const testmsg = 'testmsg' + Math.random()
    const testobj = {testmsg}
    const topic = 'subman-test-topic'
    const channel = 'subman-test-channel'
    let subman, redisServer

    before(function() {
      redisServer = ChildProcess.spawn('redis-server', '--save "" --appendonly no'.split(' '))
    })

    after(function() {
      redisServer.kill()
    })

    afterEach(function() {
      subman.stop()
      subman = null
    })

    context('ctor', function() {
      it('create with options', function() {
        const options = {blah: 2}
        subman = new SubMan(topic, options)
        Assert.equal(subman._redisSub.options.blah, 2)
      })

      it('create with redis', function() {
        const redis = new Redis()
        subman = new SubMan(topic, redis)
        redis.disconnect()
        Assert.equal(subman._redisPub, redis)
      })
    })

    context('when not started', function() {
      beforeEach(function() {
        subman = new SubMan(topic)
      })

      it('#subscribe() throws', async function() {
        await Assert.rejects(() => subman.subscribe(channel, () => {}), /not started/)
      })

      it('#publish() throws', async function() {
        await Assert.rejects(() => subman.publish(channel, testmsg), /not started/)
      })

      it('#wait() throws', async function() {
        await Assert.rejects(() => subman.wait(channel, 1), /not started/)
      })
    })

    context('when started', function() {
      beforeEach(async function() {
        subman = await exports.Create(topic)
        await subman._redis().del(channel) // clean up previous failed runs
      })

      it('#start()', async function() {
        await subman.start()
      })

      it('#subscribe()', async function() {
        await subman.subscribe(channel, () => {})
        Assert.ok(subman._map.has(channel))
      })

      it('#subscribe() twice should cancel first', async function() {
        let error
        await subman.subscribe(channel, (_, msg) => (error = msg))
        await subman.subscribe(channel, () => {})
        Assert.deepEqual(error, new DuplicateSubscriptionError(`Already subscribed to ${channel}`))
        Assert.ok(subman._map.has(channel))
      })

      it('#unsubscribe()', function() {
        Assert.strictEqual(subman.unsubscribe(channel), false)
        Assert.ok(!subman._map.has(channel))
      })

      it('#unsubscribe() after #subscribe() returns true', async function() {
        await subman.subscribe(channel, () => {})
        Assert.strictEqual(subman.unsubscribe(channel), true)
        Assert.ok(!subman._map.has(channel))
      })

      it('#subscribe() returns channel name', async function() {
        const promise = new Promise(resolve => subman.subscribe(channel, channel => resolve(channel)))
        const count = await subman.publish(channel, testmsg)
        Assert.equal(count, 1)
        Assert.deepEqual(await promise, channel)
      })

      it('#subscribe() works for string', async function() {
        const promise = new Promise(resolve => subman.subscribe(channel, (_, msg) => resolve(msg)))
        const count = await subman.publish(channel, testmsg)
        Assert.equal(count, 1)
        Assert.deepEqual(await promise, testmsg)
      })

      it('#subscribe() works for object', async function() {
        const promise = new Promise(resolve => subman.subscribe(channel, (_, msg) => resolve(msg)))
        const count = await subman.publish(channel, testobj)
        Assert.equal(count, 1)
        Assert.deepEqual(await promise, testobj)
      })

      it('#unsubscribe() works', async function() {
        await subman.subscribe(channel, () => Assert.fail('unexpected'))
        Assert.strictEqual(subman.unsubscribe(channel), true)
        const count = await subman.publish(channel, testmsg)
        Assert.equal(count, 1)
      })

      it('#publish() works for string', async function() {
        const count = await subman.publish(channel, testmsg)
        Assert.equal(count, 1)
      })

      it('#publish() works for object', async function() {
        const count = await subman.publish(channel, testobj)
        Assert.equal(count, 1)
      })

      it('#publish() with timeout', async function() {
        this.slow(2000)
        const count = await subman.publish(channel, testmsg, 1)
        Assert.equal(count, 1)
        await new Promise(resolve => setTimeout(resolve, 1100))
        Assert.equal(await subman._redis().type(channel), 'none')
      })

      it('#wait() works for string', async function() {
        setTimeout(() => subman.publish(channel, testmsg), 10)
        const msg = await subman.wait(channel)
        Assert.deepEqual(msg, testmsg)
      })

      it('#wait() works for object', async function() {
        setTimeout(() => subman.publish(channel, testobj), 10)
        const msg = await subman.wait(channel)
        Assert.deepEqual(msg, testobj)
      })

      it('#wait() leaves key vacant', async function() {
        setTimeout(() => subman.publish(channel, testmsg), 10)
        const msg = await subman.wait(channel)
        Assert.deepEqual(msg, testmsg)
        Assert.equal(await subman._redis().type(channel), 'none')
      })

      it('#wait() leaves key unsubscribed', async function() {
        setTimeout(() => subman.publish(channel, testmsg), 10)
        await subman.wait(channel)
        Assert.ok(!subman._map.has(channel))
      })

      it('#wait() blocks', async function() {
        this.slow(300)
        const msg = 'timer resolved'
        const timer = new Promise(resolve => setTimeout(() => resolve(msg), 100))
        const waiter = subman.wait(channel)
        const id = await Promise.race([waiter, timer])
        Assert.equal(id, msg)
        await subman.publish(channel, null)
        Assert.equal(await waiter, null)
        Assert.ok(!subman._map.has(channel))
      })

      it('#subscribe() after #wait() should cancel', async function() {
        let error
        subman.wait(channel).catch(err => (error = err))
        await subman.subscribe(channel, () => {})
        Assert.deepEqual(error, new DuplicateSubscriptionError(`Already subscribed to ${channel}`))
        Assert.ok(subman._map.has(channel))
      })

      it('#wait() after #subscribe() should cancel', async function() {
        this.slow(2000)
        let error
        await subman.subscribe(channel, (_, msg) => (error = msg))
        await subman.wait(channel, 1)
        Assert.deepEqual(error, new DuplicateSubscriptionError(`Already subscribed to ${channel}`))
        Assert.ok(!subman._map.has(channel))
      })

      context('when stopped', function() {
        beforeEach(function(done) {
          subman.stop()
          // Hack: wait for the connection to actually close
          subman._redisSub.on('close', done)
        })

        it('#subscribe() throws', async function() {
          await Assert.rejects(() => subman.subscribe(channel, () => {}), /not started/)
        })

        it('#publish() throws', async function() {
          await Assert.rejects(() => subman.publish(channel, testmsg), /not started/)
        })

        it('#wait() throws', async function() {
          await Assert.rejects(() => subman.wait(channel, 1), /not started/)
        })
      })

      it('#cancel()', function() {
        subman.cancel(channel)
      })

      it('#cancel() unblocks #wait()', async function() {
        setTimeout(() => subman.cancel(channel), 1)
        // Assert.strictEqual(await subman.wait(channel), null) alternative
        await Assert.rejects(() => subman.wait(channel), /cancelled/)
        Assert.equal(await subman._redis().type(channel), 'none')
        Assert.ok(!subman._map.has(channel))
      })

      it('#stop()', function() {
        subman.stop()
      })

      it('#stop() unblocks #wait()', async function() {
        setTimeout(() => subman.stop(channel), 1)
        await Assert.rejects(() => subman.wait(channel), /aborted/)
        Assert.ok(!subman._map.has(channel))
      })

      it('#wait() receives older messages', async function() {
        const count = await subman.publish(channel, testmsg)
        Assert.equal(count, 1)
        const msg = await subman.wait(channel)
        Assert.deepEqual(msg, testmsg)
        Assert.ok(!subman._map.has(channel))
      })

      it('#wait() receives older messages, leaves key vacant', async function() {
        const count = await subman.publish(channel, testmsg)
        Assert.equal(count, 1)
        const msg = await subman.wait(channel)
        Assert.deepEqual(msg, testmsg)
        Assert.equal(await subman._redis().type(channel), 'none')
        Assert.ok(!subman._map.has(channel))
      })

      it('#wait() receives messages in order', async function() {
        Assert.equal(await subman.publish(channel, testmsg), 1)
        Assert.equal(await subman.publish(channel, testobj), 2)
        Assert.deepEqual(await subman.wait(channel), testmsg)
        Assert.deepEqual(await subman.wait(channel), testobj)
        Assert.ok(!subman._map.has(channel))
      })

      it('#wait() with timeout', async function() {
        this.slow(2000)
        const msg = await subman.wait(channel, 1)
        Assert.equal(msg, null)
        Assert.equal(await subman._redis().type(channel), 'none')
        Assert.ok(!subman._map.has(channel))
      })

      it('#wait() receives message before timeout', async function() {
        await subman.publish(channel, testmsg)
        const msg = await subman.wait(channel, 1)
        Assert.equal(msg, testmsg)
        Assert.equal(await subman._redis().type(channel), 'none')
        Assert.ok(!subman._map.has(channel))
      })

      it('Reentrancy: #subscribe() in callback', async function() {
        let errors = []
        function callback(_, msg) {
          errors.push(msg)
          subman.subscribe(channel, (_, msg) => {
            errors.push(msg)
          })
        }
        await subman.subscribe(channel, callback)
        // This subscription will cancel the first, triggering the callback, cancelling the second, triggering the 3rd.
        await subman.subscribe(channel, callback)
        Assert.deepEqual(errors, [
          new DuplicateSubscriptionError(`Already subscribed to ${channel}`),
          new DuplicateSubscriptionError(`Already subscribed to ${channel}`),
          new DuplicateSubscriptionError(`Already subscribed to ${channel}`)
        ])
        Assert.ok(subman._map.has(channel))
      })

      context('works across connections', function() {
        let queue2

        beforeEach(async function() {
          queue2 = await exports.Create(topic)
        })

        afterEach(function() {
          queue2.stop()
        })

        it('#subscribe() on one, #publish() on other', async function() {
          const promise = new Promise(resolve => queue2.subscribe(channel, (_, msg) => resolve(msg)))
          const count = await subman.publish(channel, testmsg)
          Assert.equal(count, 1)
          Assert.deepEqual(await promise, testmsg)
        })

        it('#subscribe() on both, only one should win the race', async function() {
          this.slow(2000)
          Assert.equal(await subman.publish(channel, testmsg), 1)
          const results = await Promise.all([subman, queue2].map(q => q.wait(channel, 1)))
          Assert.deepEqual(results.sort(), [null, testmsg])
        })
      })

      const ChannelCount = 1000
      context(`works for ${ChannelCount} channels`, function() {
        const testchannel = channel + (ChannelCount - 1)
        let waiters

        beforeEach(async function() {
          await subman._redis().del(testchannel) // clean up previous failed runs
          waiters = Array(ChannelCount)
            .fill()
            .map((_, i) => subman.wait(channel + i, 1))
        })

        afterEach(async function() {
          await Promise.all(waiters)
        })

        it('receives correct message', async function() {
          const count = await subman.publish(testchannel, testmsg)
          Assert.equal(count, 1)
          const msg = await Promise.race(waiters)
          Assert.equal(msg, testmsg)
          Assert.equal(await subman._redis().type(testchannel), 'none')
        })
      })
    })
  })
