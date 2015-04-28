var elasticsearch = require('elasticsearch'),
  turfExtent = require('turf-extent');

module.exports = {
  type: 'elasticsearch',
  indexName: 'koop', 
  limit: 2000,

  connect: function( conn, koop, callback ){
  
    // use the koop logger 
    this.log = koop.log;
  
    this.client = new elasticsearch.Client(conn);
    // creates table only if they dont exist
    this._createIndex( this.indexName, function(err, done){
      if ( callback ){
        callback();
      }
    });
    return this; 
  },

  // returns the info doc for a key 
  getCount: function( key, options, callback ){
    var self = this;
    var params = this.buildQueryParams(key, options);
    this.client.search( params, function(err, result){
      if ( err || !result ){
        return callback(err, null);
      }
      self.log.debug('Get Count', key, result.hits.total);
      callback(null, result.hits.total);
    });
  },

  createExtent: function(geometry){
    var extent;
    var geom = this.parseGeometry( geometry );
    if ((geom.xmin || geom.xmin === 0) && (geom.ymin || geom.ymin === 0)){
      var box = geom;
      if ( box.spatialReference.wkid != 4326 ){
        var mins = merc.inverse( [box.xmin, box.ymin] ),
          maxs = merc.inverse( [box.xmax, box.ymax] );
        extent = this.convertExtent([mins[0], mins[1], maxs[0], maxs[1]]);
      } else {
        extent = this.convertExtent([box.xmin, box.ymin, box.xmax, box.ymax]);
      }
    }
    return extent;
  },

  // returns the info doc for a key 
  getInfo: function( key, callback ){
    this.client.get({
      index: this.indexName,
      type: 'info',
      id: key.replace(/:/g,'_')
    }, function(err, res){
      var info;
      if (!err && res){
        info = res._source;
      }
      callback(err, info);
    });
  },

  // updates the info doc for a key 
  updateInfo: function( key, info, callback ){
    this.log.debug('Updating info %s %s', key, info.status);
    if (!info.status) {
      info.status = '';
    }
    this.client.update({
      index: this.indexName,
      type: 'info',
      id: key.replace(/:/g,'_'),
      body: {
          doc: info
      }
    }, function(err, res){
      if ( err || !res ){
        callback(err, null);
      } else {
        callback(null, info);
      }
    });
  },

  // get data out of the db
  select: function(key, options, callback){
    var self = this;

    if ( key !== 'all'){
      key = key+'_'+(options.layer || 0 );
    }
    key = key.replace(/:/g,'_');

    this.client.get({
      index: this.indexName,
      type: 'info',
      id: key
    }, function(err, result){
      if ( (err || !result) && key !== 'all'){
        callback('Not Found', []);
      } else if (
        result && 
        result._source && 
        result._source.status == 'processing' && 
        !options.bypassProcessing 
        ) {
          callback( null, [{ status: 'processing' }]);
      } else {
          var info = result._source || {};

          var params = self.buildQueryParams(key, options);

          self.getCount(key, options, function(e, count) {
            if (!options.limit && !e && count && (count > self.limit && options.enforce_limit) ){
              callback( null, [{
                exceeds_limit: true,
                type: 'FeatureCollection',
                features: [{}],
                name: info.name,
                sha: info.sha,
                info: info.info,
                updated_at: info.updated_at,
                retrieved_at: info.retrieved_at,
                expires_at: info.expires_at, 
                count: count
              }]);

            } else {

              self.client.search(params, function (err, result) {

                if ( result && result.hits && result.hits.total ) {
                  var features = [];

                  result.hits.hits.forEach(function(doc, i){
                    features.push(JSON.parse(doc._source.feature));
                  });

                  callback( null, [{
                    type: 'FeatureCollection', 
                    features: features,
                    name: info.name, 
                    sha: info.sha, 
                    info: info.info, 
                    updated_at: info.updated_at,
                    retrieved_at: info.retrieved_at,
                    expires_at: info.expires_at,
                    count: result.hits.length 
                  }]);
                } else {
                  callback( 'Not Found', [{
                    type: 'FeatureCollection',
                    features: []
                  }]);
                }
              });
            }
          });
      }
    });
  },

  // build the params needed to make a search to the cache 
  buildQueryParams: function(key, options){

    var params = {
      index: this.indexName,
      type: 'features',
      size: options.limit || 10000
    };

    // apply the table/item level query
    params.body = { "query": { "filtered": {} } };
    if (key !== 'all') {
      params.body.query.filtered.query = { "match": {"itemid": key.replace(/:/g,'_') }};
    } else if (key === 'all' && options.type) { 
      params.body.query.filtered.query = { "match": {"type": options.type }};
    }

    // parse the where clause 
    /*if ( options.where ) { 
      if ( options.where != '1=1'){
        //var clause = self.createWhereFromSql(options.where, options.fields);
        //select += ' WHERE ' + clause;
      } else {
        //select += ' WHERE ' + options.where;
      }
      if (options.idFilter){
        //select += ' AND ' + options.idFilter;
      }
    } else if (options.idFilter) {
      //select += ' WHERE ' + options.idFilter;
    }*/

    // parse the geometry param from GeoServices REST
    if ( options.geometry && !options.geometryType ){
      var extent = this.createExtent( options.geometry );
      params.body.query.filtered.filter = {
        "geo_shape": { "geom": { "shape": extent } }
      };
    } else if (options.geometry && options.geometryType === 'polygon'){
      params.body.query.filtered.filter = {
        "geo_shape": { "geom": { "shape": { "type":"polygon", "coordinates":JSON.parse(options.geometry)}}}
      };
    }
    return params;
  },

  parseGeometry: function( geometry ){
    var geom = geometry;
    if ( typeof( geom ) == 'string' ){
      try {
        geom = JSON.parse( geom );
      } catch(e){
        try {
          if ( geom.split(',').length == 4 ){
            var extent = geom.split(',');
            geom = { spatialReference: {wkid: 4326} };
            geom.xmin = extent[0];
            geom.ymin = extent[1];
            geom.xmax = extent[2];
            geom.ymax = extent[3];
          }
        } catch(error){
          this.log.error('Error building bbox from query ' + geometry);
        }
      }
    }
    return geom;
  },

  // create a collection and insert features
  // create a 2d index 
  insert: function( key, geojson, layerId, callback ){
    var self = this; 
    var info = {},
      count = 0;
      error = null;
      
      info.name = geojson.name ;
      info.updated_at = geojson.updated_at;
      info.expires_at = geojson.expires_at;
      info.retrieved_at = geojson.retrieved_at;
      info.status = geojson.status;
      info.format = geojson.format;
      info.sha = geojson.sha;
      info.info = geojson.info;
      info.host = geojson.host;
   
      var table = key.replace(/:/g,'_')+'_'+layerId;

      if ( geojson.length ){
        geojson = geojson[0];
      }

      // TODO Why not use an update query here? 
      self.client.delete({
        index: self.indexName,
        type: 'info',
        id: table
      }, function(err, res){
        self.client.create({
          index: self.indexName,
          type: 'info',
          id: table,
          body: JSON.stringify(info)
        }, function (error, response) {
          if (geojson.features && geojson.features.length){
            self.insertPartial(key, geojson, layerId, callback );
          } else {
            callback();
          }
        });
      });
    
  },

  insertPartial: function( key, geojson, layerId, callback ){
    var self = this;
    var table = key.replace(/:/g,'_') + "_" + layerId;
    var bulkInsert = [], doc;
    geojson.features.forEach(function(feature, i){
      bulkInsert.push({ index:  { _index: self.indexName, _type: 'features'} });
      doc = {
        "itemid": table,
        "type": table.split('_')[0],
        "feature": JSON.stringify(feature),
        //"extent":  self.convertExtent( turfExtent( feature ))
        "geom":  feature.geometry
      };
      bulkInsert.push(doc);
    });

    self.client.bulk({
      body: bulkInsert,
    }, callback);
  },

  // inserts geojson features into the feature column of the given table
  insertFeature: function(table, feature, i, callback){
    try {
      this.client.create({
        index: this.indexName,
        type: 'features',
        id: table+'_'+i,
        body: {
          "itemid": table,
          "feature": JSON.stringify(feature),
          "extent":  this.convertExtent( turfExtent( feature ))
        }
      }, function (err, res) {
        callback(err, res);
      });
    } catch (e) {
      console.log('Error inserting feature', e);
      callback(e);
    }
  },

  convertExtent: function(coords) {
    var geometry = [];
    // upper left
    geometry.push([parseFloat(coords[0]), parseFloat(coords[3])]);
    // lower right
    geometry.push([parseFloat(coords[2]), parseFloat(coords[1])]);
    var envelope = {
        "type": "envelope",
        "coordinates": geometry
    };
    return envelope;
  },
 
  remove: function( key, callback){
    var self = this;
  
    // other caches use : and ES doesnt like that
    key = key.replace(/:/g,'_');

    this.client.delete({
      index: this.indexName,
      type: 'info',
      id: key
    }, function(err, res){
      self.client.deleteByQuery({
        index: self.indexName,
        type: 'features',
        q: 'itemid:'+key.replace(/\*/g,'\\*').replace(/,/g,'\,')
      }, function(err, res){
        callback(err, res);
      })
    });

  },

  dropTable: function(table, callback){
    this.remove(table, callback);
  },

  serviceRegister: function( type, info, callback){
    var self = this;
    try {
      this.client.create({
        index: this.indexName,
        type: 'services',
        id: info.id,
        body: {
          "type": type,
          "id": info.id,
          "host": info.host
        }
      }, function (err, res) {
        callback(err, res);
      });
    } catch (e) {
      console.log('Error inserting service', e);
      callback(e);
    }
  },

  serviceCount: function( type, callback){
    var self = this;
    this.client.search({
      index: this.indexName,
      type: 'services',
      q: 'type:'+type,
    }, function (err, res) {
      callback(err, res);
    });
  },

  serviceRemove: function( type, id, callback){
    this.client.deleteByQuery({
      index: this.indexName,
      type: 'services',
      q: 'id:'+id
    }, function(err, res){
      callback(err, res);
    });
  },

  serviceGet: function( type, id, callback){
    if (!id) {
      this.client.search({
        index: this.indexName,
        type: 'services',
        q: 'type:'+type,
        fields: ['_source']
      }, function(err, res){
        var services = res.hits.hits.map(function(s){ return s._source; });
        callback(err, services);
      });
    } else {
      this.client.search({
        index: this.indexName,
        type: 'services',
        id: id
      }, function(err, res){
        callback(err, res.hits.hits[0]._source);
      });
    }
  },

  timerSet: function(key, expires, callback){
    callback( null, true);
  },

  timerGet: function(key, callback){
    callback();
  },


  //--------------
  // PRIVATE METHODS
  //-------------

  _createIndex: function( name, callback ){
    var self = this;
    this.client.indices.create({index: name}, function(err, result) {
      // create the info index
      self.client.indices.putMapping({
        index: name, 
        type: 'info', 
        body: {
          'properties': {
            'item': {
              "type": "string",
              "index": "no"
            }
          }
        }
      }, function(err, res){
        // create the feature index
        self.client.indices.putMapping({
          index: name,
          type: 'features',
          body: {
            "properties": {
              "itemid": {
                "type": "string"
              },
              "type": {
                "type": "string"
              },
              "feature": {
                "type": "string",
                "index": "no"
              },
              "geom": {
                "type": "geo_shape", 
                "tree": "geohash", 
                "precision": "1000m"
              }
            }
          }
        }, function(err, res){
          self.client.indices.putMapping({
            index: name,
            type: 'services',
            body: {
              "properties": {
                "id": { "type": "string" },
                "type": { "type": "string" },
                "host": { "type": "string" }
              }
            }
          }, function(err, res){
            callback();
          });
        });
      });
    });
  },

  _query: function(type, query, callback){
    this.client.search({
      index: this.indexName,
      type: type,
      q: query || ''
    }).then(function (resp) {
        var hits = resp.hits.hits;
        if ( callback ) {
          callback(err, hits);
        }
      }, function (err) {
        console.trace(err.message);
    });

  }

};
