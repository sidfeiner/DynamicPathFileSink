# DynamicFilePathSink
StreamingFileSink for Flink, that allows you to write data to different files/directories.

For example if you want data to be written to different directories based on dynamic values that are
only known during runtime, you can initialize the sink as following:

```
val basePath = "/home/etl/sink-files"
val pathFormat = "[COUNTRY]/[CITY]/[YEAR]/"
val sink = DynamicPathFileSink.apply(basePath, pathFormat)
```

In your MapFunctions, when creating the entry to be written to the file,
you must pass a DynamicPathRow that encapsulates the dynamic values to be filled,
and the data to be written to the file:
```
val dynamicValues = Map("COUNTRY" -> "france", "CITY" -> "Paris")
val record = "Eiffel Tower"
return DynamicPathRow(dynamicValues, record)
``` 

Our sink supports the following default dynamic values:
* YEAR
* MONTH
* DAY
* HOUR
* MINUTE
* SECOND