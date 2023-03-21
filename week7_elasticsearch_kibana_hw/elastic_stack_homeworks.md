- Use https://github.com/erkansirin78/datasets/raw/master/market1mil.csv.gz

1. Index this data set into Elasticsearch
2. Mapping must be like this:
```commandline
"LOGICALREF":    { "type": "integer" },  
"ITEMCODE":  { "type": "integer"  }, 
"ITEMNAME":   { "type": "text"  },
"AMOUNT": {"type": "float"},
"PRICE": {"type": "float"},
"LINENETTOTAL": {"type": "float"},
 "BRANCH": {"type": "keyword"},
 "CITY": {"type": "keyword"},
 "BRAND": {"type": "keyword"},
"STARTDATE": {
"type":   "date"
},
"LOCATION": {"type": "geo_point"}
```

3. You must clean, filter or drop where necessary before indexing to Elasticsearch.
4. After indexing create a dashboard and add a line chart that show hourly total sales.
