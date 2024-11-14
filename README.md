# WdsDataSource
WdsDataSource is a Spark data source implementation that allows reading and writing data in [WebDataset](https://github.com/webdataset/webdataset) format. It implements Spark 2.x/3.x DataSource V2 API to provide seamless integration with Spark SQL and DataFrame operations.

cn:

WdsDataSource 是一个 Spark 数据源实现,用于读写 WebDataset 格式的数据。它实现了 Spark 2.x/3.x 的 DataSource V2 API,可以无缝集成到 Spark SQL 和 DataFrame 操作中。
## Install
place jar file `WdsDataSource-1.0-SNAPSHOT.jar` into your cluster classpath or submit job with `--jars`

## Read
```
    spark.read.format("wds").load(tarFile)
```
1. Files with the same basename in a tar file (must be adjacent according to WebDataset format requirements) will be merged into a single Row
2. JSON file content is parsed and flattened into the Row
3. Files in other formats are added to the Row with their file extension as the key, in binary type

cn:
1. tar中同一个basename的多个文件(需要根据webdataset格式要求, 位置相邻的)会被合并为一个Row
2. 其中json文件内容被解析并平铺到Row中
3. 其他格式的文件以文件后缀作为key加入Row中, binary类型

## Write
```
    df.write.format("wds")
      .option("wds_keyfield", "key")
      .save(outputPath)
```
1. A key field is required in the DataFrame for writing as the basename in WebDataset tar files, specified via `wds_keyfield`
2. Binary type fields will be written directly as binary files inside the WebDataset tar
3. Fields of other types will be written to a JSON file
4. Partitioning needs to be controlled manually before writing to manage data volume per partition

cn:
1. 数据(Dataframe)中需要有一个key字段, 用于写出到webdataset的tar的文件basename, 通过`wds_keyfield`指定
2. binary类型的字段会直接写出为webdataset的tar的内部二进制文件
3. 其余类型的字段会写出到一个json文件中
4. 需要在写出前自行控制分区, 控制每个分区数据量


set spark.sql.jsonGenerator.ignoreNullFields=false to keep null fields in json

# Unsupported Features & TODO
- 支持内层json的gz压缩以及外层整体gz压缩
- 支持filter pushdown
- 支持columnPruning/schema
- 支持partition, split
- 支持corrupt record