# koop-escache

> ElasticSearch cache for [Koop](https://github.com/koopjs/koop) (experimental).

[![npm version][npm-img]][npm-url]
[![build status][travis-img]][travis-url]

[npm-img]: https://img.shields.io/npm/v/koop-escache.svg?style=flat-square
[npm-url]: https://www.npmjs.com/package/koop-escache
[travis-img]: https://img.shields.io/travis/koopjs/koop-escache.svg?style=flat-square
[travis-url]: https://travis-ci.org/koopjs/koop-escache

## Install

```
npm install koop-escache --save
```

## Usage

Koop's data caching is by default a local, in-memory object. This allows you to use [ElasticSearch](https://www.elastic.co/products/elasticsearch) instead.

```js
var config = require('./config.json');
var koop = require('koop')(config);
var koopES = require('koop-escache');

koop.registerCache(koopES);
```

## Resources

* [Koop](https://github.com/koopjs/koop)
* [ArcGIS for Developers](http://developers.arcgis.com)
* [ArcGIS REST API Documentation](http://resources.arcgis.com/en/help/arcgis-rest-api/)
* [@esri](http://twitter.com/esri)

## Issues

Find a bug or want to request a new feature?  Please let us know by submitting an issue.

## Contributing

Esri welcomes contributions from anyone and everyone. Please see our [guidelines for contributing](https://github.com/esri/contributing).

## Licensing

[Apache 2.0](LICENSE)
