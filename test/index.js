var should = require('should'),
logger = require('./logger');

before(function (done) {
  key = 'test_repo_file';
  repoData = require('./fixtures/data.geojson');
  snowData = require('./fixtures/snow.geojson');
  cache = require('../');
  cache.indexName = 'koop-tester';
  var config = {
    "db": {  
      "conn": {
          host: 'localhost:9200',
          //log: 'trace'
      }
    }
  };

  cache.connect(config.db.conn, {}, function(err){
    done();
  });

  // init the koop log based on config params  
  config.logfile = __dirname + "/test.log";
  cache.log = new logger( config );
});

describe('ES Cache Tests', function(){
  describe('when caching a geojson data', function(){
    before(function(done){
      cache.insert( key, repoData[0], 0, function(e,r){
        // console.log('insert', e, r.items.length);
        done();
      });
    });

    after(function(done){
      //cache.remove( key+'_0', function(e,r){
        //console.log(e,r);
        //done();
      //});
      //cache.serviceRemove( 'test', 'test1', function( error, result ){
        done();
      //});
    });

    

    it('should error when missing key is sent', function(done){
      cache.getInfo(key+'-BS', function( err, data ){
        should.exist( err );
        done();
      });
    });

    it('should get info', function(done){
      cache.getInfo(key+'_0', function( err, data ){
        should.not.exist( err );
        data.name.should.equal('snow.geojson');
        done();
      });
    });

    it('should update info', function(done){
      var k = key+'_0';
      cache.getInfo(k, function( err, data ){
        data.name = 'snowNEW.geojson';
        cache.updateInfo(k, data, function( err, d ){
          d.name.should.equal('snowNEW.geojson');
          should.not.exist( err );
          done();
        });
      });
    });

    it('should insert data with no features', function(done){
      var k = key+'_0';
      cache.insert(k, {}, 0, function( err, success ){
        should.not.exist( err );
        done();
      });
    });

    it('should get count', function(done){
      cache.getCount(key+'_0', {}, function( err, count ){
        should.not.exist( err );
        count.should.equal(417);
        done();
      });
    });

    it('should get count inside a bbox', function(done){
      cache.getCount(key+'_0', {geometry: '-105.55,20.0,-7.12,60.73'}, function( err, count ){
        should.not.exist( err );
        count.should.equal(311);
        done();
      });
    });

    it('should select data', function(done){
      cache.select( key, { layer: 0 }, function( error, result ){
        should.not.exist(error);
        result[0].features.length.should.equal(417);
        done();
      });
    });

    it('should error when selecting a missing key', function(done){
      cache.select( key+'_fake', { layer: 0 }, function( error, result ){
        should.exist(error);
        done();
      });
    });

    it('should select data inside a geom', function(done){
      cache.select( key, { layer: 0, geometry: '-105.55,20.0,-7.12,60.73' }, function( error, result ){
        should.not.exist(error);
        result[0].features.length.should.equal(311);
        done();
      });
    });

    it('should select all data inside a geom', function(done){
      cache.select( 'all', { layer: 0, geometry: '-105.55,20.0,-7.12,60.73' }, function( error, result ){
        should.not.exist(error);
        result[0].features.length.should.equal(311);
        done();
      });
    });

    it('should select all data', function(done){
      cache.select( 'all', { }, function( error, result ){
        should.not.exist(error);
        result[0].features.length.should.equal(417);
        done();
      });
    });

    it('should register and get a service host', function(done){
      cache.serviceRegister( 'test', {id:'test1', host: 'http://fake.service.com'}, function( error, result ){
        cache.serviceGet( 'test', 'test1', function( error, result ){
          should.not.exist(error);
          done();
        });
      });
    });

    it('should register and get a service host', function(done){
      cache.serviceGet( 'test', null, function( error, result ){
        result.length.should.equal(1);
        should.not.exist(error);
        done();
      });
    });


  });
});

//TODO Support WHERE filters
/*
        it('should select data from db with filter', function(done){
          pgCache.select( key, { layer: 0, where: '\'total precip\' = \'0.31\'' }, function( error, success ){
            should.not.exist(error);
            should.exist(success[0].features);
            success[0].features.length.should.equal(5);
            done();
          });
        });

*/
