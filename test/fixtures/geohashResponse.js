module.exports={
	"aggregations": {
		"geohash": {
			"buckets": [
				{
					"key": "foobar",
					"doc_count": 10
				},
				{
					"key": "foobaz",
					"doc_count": 11
				}
			]
		}
	}
}