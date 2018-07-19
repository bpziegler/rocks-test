// https://github.com/level/leveldown#leveldown_put
const rocksdb = require('rocksdb')

// argv 0 = "node", 1 = "script", 2 = first arg
const NUM_WRITE = process.argv[2] || 1000

class DbHelper {
  constructor (path) {
    this.db = rocksdb(path)
  }

  async open (options) {
    const self = this
    return new Promise(function (resolve, reject) {
      self.db.open(options, (err) => {
        if (err) {
          reject(err)
        } else {
          resolve()
        }
      })
    })
  }

  // db.put(key, value[, options], callback)
  async put (key, value, callback) {
    const self = this
    return new Promise(function (resolve, reject) {
      self.db.put(key, value, (err) => {
        if (err) {
          reject(err)
        } else {
          resolve()
        }
      })
    })
  }

  // db.get(key[, options], callback)
  async get (key, callback) {
    const self = this
    return new Promise(function (resolve, reject) {
      self.db.get(key, (err, value) => {
        if (err) {
          reject(err)
        } else {
          resolve(value)
        }
      })
    })
  }

  async compactRange (start, end) {
    const self = this
    return new Promise(function (resolve, reject) {
      self.db.compactRange(start, end, (err) => {
        if (err) {
          reject(err)
        } else {
          resolve()
        }
      })
    })
  }

  async close () {
    const self = this
    return new Promise(function (resolve, reject) {
      self.db.close((err) => {
        if (err) {
          reject(err)
        } else {
          resolve()
        }
      })
    })
  }

  iterator (options) {
    return new IteratorHelper(this.db.iterator(options))
  }

  getDb () {
    return this.db
  }
}

class IteratorHelper {
  constructor (iterator) {
    this.iterator = iterator
  }

  async next () {
    const self = this
    return new Promise(function (resolve, reject) {
      self.iterator.next((err, key, value) => {
        if (err) {
          reject(err)
        } else {
          resolve({ key, value })
        }
      })
    })
  }

  seek (key) {
    this.iterator.seek(key.toString())
  }

  async end () {
    const self = this
    return new Promise(function (resolve, reject) {
      self.iterator.end((err) => {
        if (err) {
          reject(err)
        } else {
          resolve()
        }
      })
    })
  }
}

class Runner {
  memTest () {
    const ary = []

    for (var i = 0; i < 2 * 1000; i++) {
      const h = { key: i, winners: [1, 2, 3, 4, 5], losers: [6, 7, 8, 9, 10] }
      ary.push(h)
    }

    console.log(ary.length)
    const used = process.memoryUsage().heapUsed / 1024 / 1024
    console.log(`The script uses approximately ${used} MB`)
  }

  async run () {
    const dbHelper = new DbHelper('test')
    await dbHelper.open({ createIfMissing: true })
    console.log('NUM_WRITE = ' + NUM_WRITE)
    const startTime = Date.now()
    for (var i = 0; i < NUM_WRITE; i++) {
      await dbHelper.put(i, 'abc' + i)
      if ((i + 1) % 10000 === 0) {
        var elap = Date.now() - startTime
        console.log(`wrote ${i + 1} keys   ${(elap / 1000.0).toFixed(3)} elap sec`)
      }
    }

    for (i = 0; i < 25; i++) {
      const testVal = await dbHelper.get(i)
      console.log('testVal = ' + testVal)
    }

    const iter = dbHelper.iterator({ limit: 50000 })
    iter.seek(50)
    var num = 0
    while (true) {
      const iterVal = await iter.next()
      if (iterVal.key) {
        num++
        if (num < 10) {
          console.log(`key = ${iterVal.key}   val = ${iterVal.value}`)
        }
      } else {
        break
      }
    }
    await iter.end()

    console.log('begin compact')
    await dbHelper.compactRange()
    console.log('end compact')
    await dbHelper.close()
    console.log('closed')
  }
}

const runner = new Runner()
runner.run().then(() => {
  console.log('Runner Done')
})
