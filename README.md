# WdsDataSource
WdsDataSource is a Spark data source implementation that allows reading and writing data in WebDataset format. It implements Spark 2.x/3.x DataSource V2 API to provide seamless integration with Spark SQL and DataFrame operations.

WdsDataSource 是一个 Spark 数据源实现,用于读写 WebDataset 格式的数据。它实现了 Spark 2.x/3.x 的 DataSource V2 API,可以无缝集成到 Spark SQL 和 DataFrame 操作中。

## Read


## Write
1. 数据(Dataframe)中需要有一个key字段, 用于写出到webdataset的tar的文件basename, 通过`wds_keyfield`指定
2. binary类型的字段会直接写出为webdataset的tar的内部二进制文件
3. 其余类型的字段会写出到一个json文件中

# 未支持的特性&TODO
- 支持内层json的gz压缩以及外层整体gz压缩