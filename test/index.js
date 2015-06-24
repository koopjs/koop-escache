var should = require('should'),
  logger = require('./logger')

before(function (done) {
  key = 'test'
  repoData = require('./fixtures/data.geojson')
  snowData = require('./fixtures/snow.geojson')
  georgia = require('./fixtures/georgia.geojson')
  geohashResponse = require('./fixtures/geohashResponse')
  cache = require('../')
  cache.indexName = 'koop-tester'
  var config = {
    'db': {
      'conn': {
        host: 'localhost:9200',
      // log: 'trace'
      }
    }
  }

  cache.connect(config.db.conn, {}, function (err) {
    done()
  })

  // init the koop log based on config params  
  config.logfile = __dirname + '/test.log'
  cache.log = new logger(config)
})

before(function (done) {
  cache.insert(key, repoData[0], 0, function (e, r) {
    done()
  })
})

after(function (done) {
  cache.remove(key + '_0', function (e, r) {
    done()
  })
})

describe('ES Cache Tests', function () {
  describe('when caching a geojson data', function () {
    it('should error when missing key is sent', function (done) {
      cache.getInfo(key + '-BS', function ( err, data ) {
        should.exist(err)
        done()
      })
    })

    it('should get info', function (done) {
      cache.getInfo(key + '_0', function ( err, data ) {
        should.not.exist(err)
        data.name.should.equal('snow.geojson')
        done()
      })
    })

    it('should update info', function (done) {
      var k = key + '_0'
      cache.getInfo(k, function ( err, data ) {
        data.name = 'snowNEW.geojson'
        cache.updateInfo(k, data, function ( err, d ) {
          d.name.should.equal('snowNEW.geojson')
          should.not.exist(err)
          done()
        })
      })
    })

    it('should insert data with no features', function (done) {
      var k = key + '_zero'
      cache.insert(k, {}, 0, function ( err, success ) {
        should.not.exist(err)
        cache.remove(k + '_0', function (err, res) {
          done()
        })
      })
    })

    it('should get count', function (done) {
      cache.getCount(key + '_0', {}, function ( err, count ) {
        should.not.exist(err)
        count.should.equal(417)
        done()
      })
    })

    it('should get count inside a bbox', function (done) {
      cache.getCount(key + '_0', {geometry: '-105.55,20.0,-7.12,60.73'}, function ( err, count ) {
        should.not.exist(err)
        count.should.equal(311)
        done()
      })
    })

    it('should select data', function (done) {
      cache.select(key, { layer: 0 }, function ( error, result ) {
        should.not.exist(error)
        result[0].features.length.should.equal(417)
        done()
      })
    })

    it('should error when selecting a missing key', function (done) {
      cache.select(key + '_fake', { layer: 0 }, function ( error, result ) {
        should.exist(error)
        done()
      })
    })

    it('should select data inside a geom', function (done) {
      cache.select(key, { layer: 0, geometry: '-105.55,20.0,-7.12,60.73' }, function ( error, result ) {
        should.not.exist(error)
        result[0].features.length.should.equal(311)
        done()
      })
    })

    it('should select all data inside a geom', function (done) {
      cache.select('all', { layer: 0, geometry: '-105.55,20.0,-7.12,60.73' }, function ( error, result ) {
        should.not.exist(error)
        result[0].features.length.should.equal(311)
        done()
      })
    })

    it('should select all data', function (done) {
      cache.select('all', { }, function ( error, result ) {
        should.not.exist(error)
        result[0].features.length.should.equal(417)
        done()
      })
    })

    it('should register and get a service host', function (done) {
      cache.serviceRegister('test', {id: 'test1', host: 'http://fake.service.com'}, function ( error, result ) {
        cache.serviceGet('test', 'test1', function ( error, result ) {
          should.not.exist(error)
          done()
        })
      })
    })

    it('should register and get a service host', function (done) {
      cache.serviceGet('test', null, function ( error, result ) {
        result.length.should.equal(1)
        should.not.exist(error)
        done()
      })
    })

  })
})

describe('indexing', function (done) {
  it('should generate an array of geohash substrings', function(done) {
    var point = [-77.069306, 38.897275]
    var geohashes = cache._createGeohashes(point)
    geohashes.length.should.equal(6)
    geohashes[0].should.equal('dqc')
    geohashes[5].should.equal('dqcjq1pc')
    done()
  })

  it('should prepare the documents correctly', function (done) {
    var bulk = cache._prepareBulk('test', 0, georgia)
    var doc = bulk[1]
    doc.geohash3.should.equal('djv')
    doc.geohash8.should.equal('djv07f0s')
    done()
  })
})

describe('geohashing', function (done) {
  // set up a default query that just pulls everything from the DB
  var query
  before(function (done) {
    query = {
      query: {
        match_all: {}
      }
    }
    done()
  })

  it('should get a count of unique geohashes', function (done) {
    cache._countUniqueGeohashes(query, 3, function (err, count) {
      should.not.exist(err)
      count.should.equal(18)
      cache._countUniqueGeohashes(query, 8, function(err, count) {
        should.not.exist(err)
        count.should.equal(417)
        done()
      })
    })
  })

  it('should return a precision that contains fewer unique geohashes than the limit', function (done) {
    cache._getGeohashPrecision(query, 19, 8, function (err, precision) {
      should.not.exist(err)
      precision.should.equal(3)
      done()
    })
  })

  it('should aggregate geohashes at the requested precision', function (done) {
    cache._getGeohash(query, 3, function (err, res) {
      should.not.exist(err)
      res.aggregations.geohash.buckets.length.should.equal(18)
      cache._getGeohash(query, 8, function (err, res) {
        should.not.exist(err)
        res.aggregations.geohash.buckets.length.should.equal(417)
        done()
      })
    })
  })

  it('should munge the elasticsearch response into what koop expects', function (done) {
    var response = cache._mungeGeohashAgg(geohashResponse)
    response.length.should.equal(2)
    response[0].foobar.should.equal(10)
    response[1].foobaz.should.equal(11)
    done()
  })

  it('should aggregate geohashes from the index and return an array', function (done) {
    cache.geohashAgg('test_0', 10000, 8, {}, function (err, geohash) {
      should.not.exist(err)
      geohash.length.should.equal(415)
      done()
    })
  })


})

// TODO Support WHERE filters
/*
        it('should select data from db with filter', function(done){
          pgCache.select( key, { layer: 0, where: '\'total precip\' = \'0.31\'' }, function( error, success ){
            should.not.exist(error)
            should.exist(success[0].features)
            success[0].features.length.should.equal(5)
            done()
          })
        })

*/
