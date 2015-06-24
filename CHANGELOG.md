# Change Log
All notable changes to this project will be documented in this file.
This project adheres to [Semantic Versioning](http://semver.org/).

## [0.1.0] - 2015-06-24
### Added
* Geohashes are added to the index
* Cache now supports geohashAgg requests from Koop

## [0.0.7] - 2015-04-28
### Changed
* Indexing the actual geometry of the feature as opposed the extent, better searching on arbitrary polygons

## [0.0.6] - 2015-04-27
### Added
* Support for polygon based filters 

## [0.0.5] - 2015-04-26
### Changed 
* Made insert use auto-incrementing IDs to support paged inserts 
* Upped the max limit of returned data to 10k

## [0.0.4] - 2015-04-24
### Changed 
* fixed missing sphericalmercator package

## [0.0.3] - 2015-04-23
### Changed
* fixed an issue with chars in removing features from the index

### Added 
* support for searching for features by type

## [0.0.2] - 2015-04-23
### Changed
* Fixed the timerSet callback to return null

### Added 
* added a type property on the cache 

## [0.0.1] - 2015-04-23
### Added
* Support for insert, select, counting, and removing data
* 12 tests passing 

[0.0.7]: https://github.com/Esri/koop-pgcache/compare/v0.0.6...v0.0.7
[0.0.6]: https://github.com/Esri/koop-pgcache/compare/v0.0.5...v0.0.6
[0.0.5]: https://github.com/Esri/koop-pgcache/compare/v0.0.4...v0.0.5
[0.0.4]: https://github.com/Esri/koop-pgcache/compare/v0.0.3...v0.0.4
[0.0.3]: https://github.com/Esri/koop-pgcache/compare/v0.0.2...v0.0.3
[0.0.2]: https://github.com/Esri/koop-pgcache/compare/v0.0.1...v0.0.2
[0.0.1]: https://github.com/Esri/koop-pgcache/tags/v0.0.1
