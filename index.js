var elasticsearch = require('elasticsearch')
var turfExtent = require('turf-extent')
var ngeohash = require('ngeohash')
var centroid = require('turf-centroid')
var async = require('async')
var merc = require('sphericalmercator')
var pkg = require('./package')

module.exports = {
  type: 'cache',
  name: 'elasticsearch',
  version: pkg.version,

  indexName: 'koop',
  limit: 2000,

  connect: function (conn, koop, callback) {
    // use the koop logger
    this.log = koop.log
    var self = this

    this.client = new elasticsearch.Client(conn)
    // creates table only if they dont exist
    this._createIndex(this.indexName, function (err, done) {
      if (err) self.log.debug(err)
      if (callback) {
        callback()
      }
    })
    return this
  },

  // returns the info doc for a key
  getCount: function (key, options, callback) {
    var self = this
    var params = this.buildQueryParams(key, options)
    this.client.search(params, function (err, result) {
      if (err || !result) {
        return callback(err, null)
      }
      self.log.debug('Get Count', key, result.hits.total)
      callback(null, result.hits.total)
    })
  },

  createExtent: function (geometry) {
    var extent
    var geom = this.parseGeometry(geometry)
    if ((geom.xmin || geom.xmin === 0) && (geom.ymin || geom.ymin === 0)) {
      var box = geom
      if (box.spatialReference.wkid !== 4326) {
        var mins = merc.inverse([box.xmin, box.ymin])
        var maxs = merc.inverse([box.xmax, box.ymax])
        extent = this.convertExtent([mins[0], mins[1], maxs[0], maxs[1]])
      } else {
        extent = this.convertExtent([box.xmin, box.ymin, box.xmax, box.ymax])
      }
    }
    return extent
  },

  // returns the info doc for a key
  getInfo: function (key, callback) {
    this.client.get({
      index: this.indexName,
      type: 'info',
      id: key.replace(/:/g, '_')
    }, function (err, res) {
      var info
      if (!err && res) {
        info = res._source
      }
      callback(err, info)
    })
  },

  // updates the info doc for a key
  updateInfo: function (key, info, callback) {
    this.log.debug('Updating info %s %s', key, info.status)
    if (!info.status) {
      info.status = ''
    }
    this.client.update({
      index: this.indexName,
      type: 'info',
      id: key.replace(/:/g, '_'),
      body: {
        doc: info
      }
    }, function (err, res) {
      if (err || !res) {
        callback(err, null)
      } else {
        callback(null, info)
      }
    })
  },

  // get data out of the db
  select: function (key, options, callback) {
    var self = this

    if (key !== 'all') {
      key = key + '_' + (options.layer || 0)
    }
    key = key.replace(/:/g, '_')

    this.client.get({
      index: this.indexName,
      type: 'info',
      id: key
    }, function (err, result) {
      if ((err || !result) && key !== 'all') {
        callback('Not Found', [])
      } else if (
        result &&
        result._source &&
        result._source.status === 'processing' &&
        !options.bypassProcessing
      ) {
        callback(null, [{ status: 'processing' }])
      } else {
        var info = result._source || {}

        var params = self.buildQueryParams(key, options)

        self.getCount(key, options, function (e, count) {
          if (!options.limit && !e && count && (count > self.limit && options.enforce_limit)) {
            callback(null, [{
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
            }])

          } else {
            self._scrollSearch(params, function (err, features) {
              if (err) return callback(err)
              if (features && features.length) {
                callback(null, [{
                  type: 'FeatureCollection',
                  features: features,
                  name: info.name,
                  sha: info.sha,
                  info: info.info,
                  updated_at: info.updated_at,
                  retrieved_at: info.retrieved_at,
                  expires_at: info.expires_at,
                  count: features.length
                }])
              } else {
                callback('Not Found', [{
                  type: 'FeatureCollection',
                  features: []
                }])
              }
            })
          }
        })
      }
    })
  },

  // build the params needed to make a search to the cache
  buildQueryParams: function (key, options) {
    var params = {
      index: this.indexName,
      type: 'features',
      size: options.limit || 10000
    }

    // apply the table/item level query
    params.body = {
      'query': {
        'filtered': {}
      },
      'fields': ['feature']
    }
    if (key !== 'all') {
      params.body.query.filtered.query = { 'match': {'itemid': key.replace(/:/g, '_') }}
    } else if (key === 'all' && options.type) {
      params.body.query.filtered.query = { 'match': {'type': options.type }}
    }

    // parse the where clause
    /* if ( options.where ) {
      if ( options.where != '1=1'){
        //var clause = self.createWhereFromSql(options.where, options.fields)
        //select += ' WHERE ' + clause
      } else {
        //select += ' WHERE ' + options.where
      }
      if (options.idFilter){
        //select += ' AND ' + options.idFilter
      }
    } else if (options.idFilter) {
      //select += ' WHERE ' + options.idFilter
    }*/

    // parse the geometry param from GeoServices REST
    if (options.geometry && !options.geometryType) {
      var extent = this.createExtent(options.geometry)
      params.body.query.filtered.filter = {
        'geo_shape': { 'geom': { 'shape': extent } }
      }
    } else if (options.geometry && options.geometryType === 'polygon') {
      params.body.query.filtered.filter = {
        'geo_shape': { 'geom': { 'shape': { 'type': 'polygon', 'coordinates': JSON.parse(options.geometry)}}}
      }
    }
    return params
  },

  parseGeometry: function (geometry) {
    var geom = geometry
    if (typeof (geom) === 'string') {
      try {
        geom = JSON.parse(geom)
      } catch(e) {
        try {
          if (geom.split(',').length === 4) {
            var extent = geom.split(',')
            geom = { spatialReference: {wkid: 4326} }
            geom.xmin = extent[0]
            geom.ymin = extent[1]
            geom.xmax = extent[2]
            geom.ymax = extent[3]
          }
        } catch(error) {
          this.log.error('Error building bbox from query ' + geometry)
        }
      }
    }
    return geom
  },

  // create a collection and insert features
  // create a 2d index
  insert: function (key, geojson, layerId, callback) {
    var self = this
    var info = {}

    info.name = geojson.name
    info.updated_at = geojson.updated_at
    info.expires_at = geojson.expires_at
    info.retrieved_at = geojson.retrieved_at
    info.status = geojson.status
    info.format = geojson.format
    info.sha = geojson.sha
    info.info = geojson.info
    info.host = geojson.host

    var table = key.replace(/:/g, '_') + '_' + layerId

    if (geojson.length) {
      geojson = geojson[0]
    }

    // TODO Why not use an update query here?
    self.client.delete({
      index: self.indexName,
      type: 'info',
      id: table
    }, function (err, res) {
      if (err) self.log.debug(err)
      self.client.create({
        index: self.indexName,
        type: 'info',
        id: table,
        body: JSON.stringify(info)
      }, function (error, response) {
        if (error) self.log.debug(error)
        if (geojson.features && geojson.features.length) {
          self.insertPartial(key, geojson, layerId, callback)
        } else {
          callback()
        }
      })
    })

  },

  insertPartial: function (key, geojson, layerId, callback) {
    var bulk = this._prepareBulk(key, layerId, geojson)
    this.client.bulk({
      body: bulk
    }, callback)
  },

  // inserts geojson features into the feature column of the given table
  insertFeature: function (table, feature, i, callback) {
    try {
      this.client.create({
        index: this.indexName,
        type: 'features',
        id: table + '_' + i,
        body: {
          'itemid': table,
          'feature': JSON.stringify(feature),
          'extent': this.convertExtent(turfExtent(feature))
        }
      }, function (err, res) {
        callback(err, res)
      })
    } catch (e) {
      console.log('Error inserting feature', e)
      callback(e)
    }
  },

  convertExtent: function (coords) {
    var geometry = []
    // upper left
    geometry.push([parseFloat(coords[0]), parseFloat(coords[3])])
    // lower right
    geometry.push([parseFloat(coords[2]), parseFloat(coords[1])])
    var envelope = {
      'type': 'envelope',
      'coordinates': geometry
    }
    return envelope
  },

  remove: function (key, callback) {
    var self = this

    // other caches use : and ES doesnt like that
    key = key.replace(/:/g, '_')

    this.client.delete({
      index: this.indexName,
      type: 'info',
      id: key
    }, function (err, res) {
      if (err) return callback(err)
      self.client.deleteByQuery({
        index: self.indexName,
        type: 'features',
        q: 'itemid:' + key.replace(/\*/g, '\\*').replace(/,/g, '\,')
      }, function (err, res) {
        callback(err, res)
      })
    })

  },

  dropTable: function (table, callback) {
    this.remove(table, callback)
  },

  serviceRegister: function (type, info, callback) {
    try {
      this.client.create({
        index: this.indexName,
        type: 'services',
        id: info.id,
        body: {
          'type': type,
          'id': info.id,
          'host': info.host
        }
      }, callback)
    } catch (e) {
      console.log('Error inserting service', e)
      callback(e)
    }
  },

  serviceCount: function (type, callback) {
    this.client.search({
      index: this.indexName,
      type: 'services',
      q: 'type:' + type
    }, function (err, res) {
      callback(err, res)
    })
  },

  serviceRemove: function (type, id, callback) {
    this.client.deleteByQuery({
      index: this.indexName,
      type: 'services',
      q: 'id:' + id
    }, function (err, res) {
      callback(err, res)
    })
  },

  serviceGet: function (type, id, callback) {
    if (!id) {
      this.client.search({
        index: this.indexName,
        type: 'services',
        q: 'type:' + type,
        fields: ['_source']
      }, function (err, res) {
        var services = res.hits.hits.map(function (s) { return s._source })
        callback(err, services)
      })
    } else {
      this.client.get({
        index: this.indexName,
        type: 'services',
        id: id
      }, function (err, res) {
        if (err) {
          return callback(err)
        }
        callback(err, res._source)
      })
    }
  },

  timerSet: function (key, expires, callback) {
    callback(null, true)
  },

  timerGet: function (key, callback) {
    callback()
  },

  geohashAgg: function (key, limit, startPrecision, options, callback) {
    var self = this
    var query = this.buildQueryParams(key, options).body
    this._getGeohashPrecision(query, limit, startPrecision, function (err, precision) {
      if (err) return callback(err)
      self._getGeohash(query, precision, function (err, json) {
        if (err) return callback(err)
        callback(null, self._mungeGeohashAgg(json))
      })
    })
  },

  // --------------
  // PRIVATE METHODS
  // -------------

  _createIndex: function (name, callback) {
    var self = this
    this.client.indices.create({index: name}, function (err, result) {
      if (err) self.log.debug(err)
      // create the info index
      self.client.indices.putMapping({
        index: name,
        type: 'info',
        body: {
          'properties': {
            'item': {
              'type': 'string',
              'index': 'no'
            }
          }
        }
      }, function (err, res) {
        if (err) self.log.debug(err)
        // create the feature index
        self.client.indices.putMapping({
          index: name,
          type: 'features',
          body: {
            'properties': {
              'itemid': {
                'type': 'string'
              },
              'type': {
                'type': 'string'
              },
              'feature': {
                'type': 'string',
                'index': 'no'
              },
              'geohash3': {
                'type': 'string'
              },
              'geohash4': {
                'type': 'string'
              },
              'geohash5': {
                'type': 'string'
              },
              'geohash6': {
                'type': 'string'
              },
              'geohash7': {
                'type': 'string'
              },
              'geohash8': {
                'type': 'string'
              },
              'geom': {
                'type': 'geo_shape',
                'tree': 'geohash',
                'precision': '1000m'
              }
            }
          }
        }, function (err, res) {
          if (err) self.log.debug(err)
          self.client.indices.putMapping({
            index: name,
            type: 'services',
            body: {
              'properties': {
                'id': { 'type': 'string' },
                'type': { 'type': 'string' },
                'host': { 'type': 'string' }
              }
            }
          }, function (err, res) {
            if (err) self.log.debug(err)
            callback()
          })
        })
      })
    })
  },

  _prepareBulk: function (key, layerId, geojson) {
    var self = this
    var table = key.replace(/:/g, '_') + '_' + layerId
    var bulk = []
    geojson.features.forEach(function (feature, i) {
      bulk.push({ index: { _index: self.indexName, _type: 'features'} })
      var doc = {
        'itemid': table,
        'type': table.split('_')[0],
        'feature': JSON.stringify(feature),
        // "extent":  self.convertExtent( turfExtent( feature ))
        'geom': feature.geometry
      }
      if (feature.geometry) {
        var point = centroid(feature).geometry.coordinates
        // add in the geohash substrings
        var geohashes = self._createGeohashes(point)
        var j = 0
        geohashes.forEach(function (geohash) {
          doc['geohash' + (j + 3).toString()] = geohash
          j++
        })
      }
      bulk.push(doc)
    })
    return bulk
  },

  _createGeohashes: function (point) {
    // points are coming in as geojson, so reverse the lat and long for ngeohash
    try {
      var geohash = ngeohash.encode(point[1], point[0], 8)
      var geohashes = []
      var i = 0
      while (i <= 5) {
        geohashes.push(geohash.slice(0, i + 3))
        i++
      }
    } catch (err) {
      console.trace(err)
    }
    return geohashes
  },

  _countUniqueGeohashes: function (query, precision, callback) {
    var agg = {
      count: {
        cardinality: {
          field: 'geohash' + precision.toString(),
          precision_threshold: this.limit
        }
      }
    }
    query.aggs = agg
    query.size = 0
    query.fields = []
    this.client.search({
      index: this.indexName,
      type: 'features',
      body: query
    }, function (err, res) {
      if (err) return callback(err)
      try {
        var count = res.aggregations.count.value
        callback(null, count)
      } catch (e) {
        callback(e, null)
      }
    })
  },

  _getGeohashPrecision: function (query, limit, start, callback) {
    var precision = start || 9
    var count = limit + 1
    var self = this
    async.whilst(
      function () {return count > limit && precision >= 3},
      function (callback) {
        precision--
        self._countUniqueGeohashes(query, precision, function (err, res) {
          if (err) return callback(err)
          count = res
          callback()
        })
      },
      function (err) {
        callback(err, precision)
      }
    )
  },

  _getGeohash: function (query, precision, callback) {
    query.size = 0
    var options = {
      index: this.indexName,
      type: 'features',
      body: query
    }
    options.body.aggregations = {
      geohash: {
        terms: {
          field: 'geohash' + precision.toString(),
          size: 0
        }
      }
    }
    this.client.search(options, function (err, res) {
      if (err) return callback(err)
      callback(null, res)
    })
  },

  _mungeGeohashAgg: function (json) {
    var geohashAgg = []
    json.aggregations.geohash.buckets.forEach(function (geohash) {
      var hash = {}
      hash[geohash.key] = geohash.doc_count
      geohashAgg.push(hash)
    })
    return geohashAgg
  },

  _scrollSearch: function (params, callback) {
    var features = []
    var parseFailures = 0
    var count = 0
    var self = this
    params.search_type = 'scan'
    params.scroll = '30s'
    // the actual number sent = shards * params.size = 10
    // https://www.elastic.co/guide/en/elasticsearch/guide/current/scan-scroll.html
    params.size = 40
    this.client.search(params, function scroll (err, res) {
      if (err) console.trace(err)
      count += res.hits.hits.length
      res.hits.hits.forEach(function (hit) {
        try {
          features.push(JSON.parse(hit.fields.feature))
        } catch (e) {
          self.log.error(e, hit)
          parseFailures++
        }
      })
      if (res.hits.total !== (features.length - parseFailures)) {
        var options = {
          scrollId: res._scroll_id,
          scroll: '30s'
        }
        self.client.scroll(options, scroll)
      } else {
        callback(null, features)
      }
    })
  },

  _query: function (type, query, callback) {
    this.client.search({
      index: this.indexName,
      type: type,
      q: query || ''
    }).then(function (resp) {
      var hits = resp.hits.hits
      if (callback) {
        callback(null, hits)
      }
    }, function (err) {
      console.trace(err.message)
    })

  }

}
