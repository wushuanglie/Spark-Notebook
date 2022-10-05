#### 开始

##### 起点：SparkSession

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('myTest')\
    .config("master","local")\
    .getOrCreate()
```

SparkSession支持Hive的HiveQL查询，访问Hive UDFs，可从Hive表读数据。

##### 创建DataFrames

使用SparkSession，可以从一个已存在的RDD，或者Hive表，或者是Spark的数据源中创建DataFram.

```python
df = spark.read.json("file:///usr/local/spark-3.3.0/examples/src/main/resources/people.json")
df.show()
```

##### 无类型DataSet操作（又叫DataFrame操作）

在Java和Scala中，DataFrame是多个Row的DataSet，这些操作叫做“无类型转换”，它不同于Scala/Java的强类型DataSet的“类型转换”

*在python中可用df.age或df['age']访问列，但是建议用后者*

```python
df.printSchema()
# root
# |-- age: long (nullable = true)
# |-- name: string (nullable = true)

df.select("name").show()
# +-------+
# |   name|
# +-------+
# |Michael|
# |   Andy|
# | Justin|
# +-------+

df.select(df['name'], df['age'] + 1).show()
# +-------+---------+
# |   name|(age + 1)|
# +-------+---------+
# |Michael|     null|
# |   Andy|       31|
# | Justin|       20|
# +-------+---------+

# Select people older than 21
df.filter(df['age'] > 21).show()
# +---+----+
# |age|name|
# +---+----+
# | 30|Andy|
# +---+----+

# Count people by age
df.groupBy("age").count().show()
# +----+-----+
# | age|count|
# +----+-----+
# |  19|    1|
# |null|    1|
# |  30|    1|
# +----+-----+
```

*对更多DataSet的操作可参考[API Documentation](https://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/Dataset.html)。还有一些函数，可简化列的引用和表达，参考[DataFram Function Reference](https://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/functions$.html)，这些函数的使用类似下面：*

```python
from pyspark.sql import functions
df.select(functions.isnull('age')).show()
```

##### 运行SQL查询以编程的方式

对DataFram进行SQL查询，需要现将其注册成view，使用spark.sql方法，返回一个DataFrame结果。

```python
df.createOrReplaceTempView("people")
sqlDF = spark.sql("SELECT * FROM people")
sqlDF.show()
#output
+----+-------+
| age|   name|
+----+-------+
|null|Michael|
|  30|   Andy|
|  19| Justin|
+----+-------+
spark.catalog.dropTempView("pople")
```

##### 全局临时视图

临时视图只能在创建它的那个session中可见，但这个session结束的时候临时视图也会结束。全局临时视图可以在所有的session共享，只有应用结束时，这个全局临时视图才会消失。

全局临时视图绑定到系统保存的数据库global_temp中，所以引用时需要加global_temp前缀。

```python
df.createGlobalTempView("people")

spark.sql("SELECT * FROM global_temp.people").show()

spark.newSession().sql("SELECT * FROM global_temp.people").show()
```

##### 创建DataSet

它类似于RDD，但是它不用java或者Kryo序列化，而是用Encoder序列化以便在网络上处理和传输。Encoder也是像java序列化一样，将对象变成Byte，Encoder是动态生成的代码，使用Spark支持的格式来执行像filtering，sorting等操作，但是不用进行反序列化。

```scala
case class Person(name: String, age: Long)

// Encoders are created for case classes
val caseClassDS = Seq(Person("Andy", 32)).toDS()
caseClassDS.show()
// +----+---+
// |name|age|
// +----+---+
// |Andy| 32|
// +----+---+

// Encoders for most common types are automatically provided by importing spark.implicits._
val primitiveDS = Seq(1, 2, 3).toDS()
primitiveDS.map(_ + 1).collect() // Returns: Array(2, 3, 4)

// DataFrames can be converted to a Dataset by providing a class. Mapping will be done by name
val path = "examples/src/main/resources/people.json"
val peopleDS = spark.read.json(path).as[Person]
peopleDS.show()
// +----+-------+
// | age|   name|
// +----+-------+
// |null|Michael|
// |  30|   Andy|
// |  19| Justin|
// +----+-------+
```

##### 将RDD转换成DataSet

###### 使用反射推断RDD schema

当已经知道schema的时候，可以用这种方法，代码简洁效率也很好

Spark SQL可以将Row对象的RDD转换成DataFrame，Row对象通过Row类创建，需要向类里传递键值对来构造，键就是列名，值得类型由Spark自动推断

```python
from pyspark.sql import Row

sc = spark.sparkContext

lines = sc.textFile("examples/src/main/resources/people.txt")
parts = lines.map(lambda l: l.split(","))
people = parts.map(lambda p: Row(name=p[0], age=int(p[1])))

# Infer the schema, and register the DataFrame as a table.
schemaPeople = spark.createDataFrame(people)
schemaPeople.createOrReplaceTempView("people")

# SQL can be run over DataFrames that have been registered as a table.
teenagers = spark.sql("SELECT name FROM people WHERE age >= 13 AND age <= 19")

#teenagers.rdd 这是将DataFram转换回RDD
teenNames = teenagers.rdd.map(lambda p: "Name: " + p.name).collect()
for name in teenNames:
    print(name)
```

###### 以编程的方式指定Schema

构造schema应用到存在的RDD上，当列和类型都不知道时推荐使用，但是这个方法比较冗长。

关键字参数字典不能提前知道时，可用三步创建DataFram

1. 从原始RDD创建一个RDD元祖或列表
2. 创建一个与第一步RDD元祖或列表结构相匹配的schema，用StructType表示
3. 应用schema到RDD上

```python
from pyspark.sql.types import StringType, StructType, StructField

sc = spark.sparkContext

lines = sc.textFile("examples/src/main/resources/people.txt")
parts = lines.map(lambda l: l.split(","))
#不转换也行
people = parts.map(lambda p: (p[0], p[1].strip()))

schemaString = "name age"

fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
schema = StructType(fields)

schemaPeople = spark.createDataFrame(people, schema)

schemaPeople.createOrReplaceTempView("people")

results = spark.sql("SELECT name FROM people")

results.show()
# +-------+
# |   name|
# +-------+
# |Michael|
# |   Andy|
# | Justin|
# +-------+
```

##### 标量函数(Scalar Functions)

对每一行返回一值。可查看 [Built-in Scalar Functions](https://spark.apache.org/docs/latest/sql-ref-functions.html#scalar-functions),另外还有[User Defined Scalar Functions](https://spark.apache.org/docs/latest/sql-ref-functions-udf-scalar.html)

##### 聚合函数(Aggregate Functions)

对多行返回一个值。可查看[Built-in Aggregation Functions](https://spark.apache.org/docs/latest/sql-ref-functions-builtin.html#aggregate-functions)，[User Defined Aggregate Functions](https://spark.apache.org/docs/latest/sql-ref-functions-udf-aggregate.html).

#### 数据源

##### 一般的加载、保存函数

默认加载的数据源是parquet，除非修改了spark.sql.sources.defalut

```python
df = spark.read.load("examples/src/main/resources/users.parquet")
df.select("name", "favorite_color").write.save("namesAndFavColors.parquet")
```

###### 手动指定选项

手动指定时候，数据源需要用它的全称（比如org.apache.spark.sql.parquet），对于内置的数据源可以用简称(`json`, `parquet`, `jdbc`, `orc`, `libsvm`, `csv`, `text`)

**加载JSON文件**

```python
df = spark.read.load("examples/src/main/resources/people.json", format="json")
#或者spark.read.json
df.select("name", "age").write.save("namesAndAges.parquet", format="parquet")
#或者df.write.parquet
```

**加载CSV文件**

```python
df = spark.read.load("examples/src/main/resources/people.csv",
                     format="csv", sep=";", inferSchema="true", header="true")
#或者spark.read.csv
```

**ORC and Parquet数据源**

更多的ORC或Parquet可选项，可参考 [ORC](https://orc.apache.org/docs/spark-config.html) / [Parquet](https://github.com/apache/parquet-mr/tree/master/parquet-hadoop)

```python
#ORC
df = spark.read.orc("examples/src/main/resources/users.orc")
(df.write.format("orc")
    .option("orc.bloom.filter.columns", "favorite_color")
    .option("orc.dictionary.key.threshold", "1.0")
    .option("orc.column.encoding.direct", "name")
    .save("users_with_options.orc"))
```

```python
#Parquet
df = spark.read.parquet("examples/src/main/resources/users.parquet")
(df.write.format("parquet")
    .option("parquet.bloom.filter.enabled#favorite_color", "true")
    .option("parquet.bloom.filter.expected.ndv#favorite_color", "1000000")
    .option("parquet.enable.dictionary", "true")
    .option("parquet.page.write-checksum.enabled", "false")
    .save("users_with_options.parquet"))
```

###### 直接在文件上运行SQL

```python
df = spark.sql(
    "SELECT * FROM parquet.`file:///usr/local/spark-3.3.0/examples/src/main/resources/users.parquet`"
)
```

###### 保存模式

| Scala/Java                         | Any Language                           | Meaning                                                      |
| :--------------------------------- | :------------------------------------- | :----------------------------------------------------------- |
| `SaveMode.ErrorIfExists` (default) | `"error" or "errorifexists"` (default) | When saving a DataFrame to a data source, if data already exists, an exception is expected to be thrown. |
| `SaveMode.Append`                  | `"append"`                             | When saving a DataFrame to a data source, if data/table already exists, contents of the DataFrame are expected to be appended to existing data. |
| `SaveMode.Overwrite`               | `"overwrite"`                          | Overwrite mode means that when saving a DataFrame to a data source, if data/table already exists, existing data is expected to be overwritten by the contents of the DataFrame. |
| `SaveMode.Ignore`                  | `"ignore"`                             | Ignore mode means that when saving a DataFrame to a data source, if data already exists, the save operation is expected not to save the contents of the DataFrame and not to change the existing data. This is similar to a `CREATE TABLE IF NOT EXISTS` in SQL. |

###### 保存到持久化表

DataFrame可以使用saveAsTable方法保存成Hive matestore中的持久化表。机器中没配置Hive也没事，Spark会给我们创建一个本地的Hive matesotre(使用Derby)。

即使Spark重启了，这个数据也会存在。

将持久化表创建成一个DataFrame，可以使用SparkSession.table方法。

可用path选项指定表的路径，例如df.write.option("path","/some/path").saveAsTable("t")。当表被删除时，表路径不会被删除，表数据仍然在那，就是Hive中的外部表。要是没指定path，表会存储在默认的仓库路径下，当表被删除时，表路径会被删除。

从Spark2.1开始，持久化数据源表按分区存储在Hive中，好处有：

- 执行查询的时候，只需要返回需要的分区就行，而不用返回所有的分区
- 对于使用Datasource API创建的表，可以使用Hive DDLs（例如ALTER TABLE PARTITION ... SET LOCATION）

当创建一个外部数据源表（用了path选项，那不就相当于Hive的外部表）时，默认情况下不会收集分区信息，要是想同步分区信息到元数据中，可以调用MSCK REPAIR TABLE

###### 分桶、排序、分区

对基于文件(csv\json etc.)的数据源，可以对其输出进行分桶、排序、分区。分桶和排序只适用于持久化表。

```python
df.write.bucketBy(42, "name").sortBy("age").saveAsTable("people_bucketed")
```

分区可以用于save和saveAsTable

```python
df.write.partitionBy("favorite_color").format("parquet").save("namesPartByColor.parquet")

df = spark.read.parquet("examples/src/main/resources/users.parquet")
(df
    .write
    .partitionBy("favorite_color")
    .bucketBy(42, "name")
    .saveAsTable("users_partitioned_bucketed"))
```

##### 一般文件源选项

一般的选项和配置只对parquet, orc, avro, json, csv, text有效

###### 忽略错误的文件

通过配置spark.sql.files.ignoreCorruptFiles，当其为True时，忽略错误文件。

```python
spark.sql("set spark.sql.files.ignoreCorruptFiles=true")

test_corrupt_df = spark.read.parquet("examples/src/main/resources/dir1/",
                                     "examples/src/main/resources/dir1/dir2/")

```

###### 忽略丢失的文件

spark.sql.files.ignoreMissingFiles为true就可以

###### 路径全局过滤器

语法遵循org.apach.hadoop.fs.GlobFilter。且不改变partition discovery行为

```python
df = spark.read.load("examples/src/main/resources/dir1",
                     format="parquet", pathGlobFilter="*.parquet")
```

###### 递归查找文件

就是当指定一个查找路径为文件夹，文件夹里还有文件夹，那么这个文件的查找就会进到这个文件夹里的文件夹继续查找

recursiveFileLookup，默认是false且禁用了分区推断。如果它为true时，还指定了数据源的partitionSepc，那么会报异常

```python
recursive_loaded_df = spark.read.format("parquet")\
    .option("recursiveFileLookup", "true")\
    .load("examples/src/main/resources/dir1")
```

###### 与修改时间有关的路径过滤器

- modifiedBefore：一个可选的时间戳，只用于修改时间发生在指定时间之前的文件
- modifiedAfter：一个可选的时间戳，只用于修改时间发生在指定时间之后的文件。

时间戳的形式必须是YYYY-MM-DDTHH:mm:ss (e.g. 2020-06-01T13:00:00)。

在Spark批查询时间里，他们俩可以一起或者单独使用。（结构流文件源不支持这两个选项）。

要是时区（timezone）选项没指定，那么时间戳会按照Spark session时区（spark.sql.session.timeZone）进行解释



```
df = spark.read.load(path+"examples/src/main/resources/dir1",
                     format="parquet",modifiedBefore=
                     "2050-07-01T08:30:00")
```

##### Parquet文件

**用到时候看文档**

##### ORC文件

**用到时候看文档**

##### JSON文件

**用到时候看文档**

##### CSV文件

Spark SQL提供了 spark.read().csv("file_name or file_directory_name")来将csv文件读成DataFrame，dataframe.write().csv("path")来将dataframe写成一个csv文件。option()函数用来指定一些行为。

```
df3 = spark.read.option("delimiter", ";").option("header", True).csv(path)

df4 = spark.read.options(delimiter=";", header=True).csv(path)

df3.write.csv("output")
#不用option函数也行，直接在csv()函数里指定也可以
df = spark.read\
    .csv(path+"examples/src/main/resources/people.csv",
         sep=";", header=True)
```

###### 数据源选项

Data source options of CSV can be set via:

- the .option/.options methods of
  - `DataFrameReader`
  - `DataFrameWriter`
  - `DataStreamReader`
  - `DataStreamWriter`
- the built-in functions below
  - `from_csv`
  - `to_csv`
  - `schema_of_csv`
- `OPTIONS` clause at [CREATE TABLE USING DATA_SOURCE](https://spark.apache.org/docs/latest/sql-ref-syntax-ddl-create-table-datasource.html)

| **Property Name**           | **Default**                                                  | **Meaning**                                                  | **Scope**  |
| :-------------------------- | :----------------------------------------------------------- | :----------------------------------------------------------- | :--------- |
| `sep`                       | ,                                                            | Sets a separator for each field and value. This separator can be one or more characters. | read/write |
| `encoding`                  | UTF-8                                                        | For reading, decodes the CSV files by the given encoding type. For writing, specifies encoding (charset) of saved CSV files. CSV built-in functions ignore this option. | read/write |
| `quote`                     | "                                                            | Sets a single character used for escaping quoted values where the separator can be part of the value. For reading, if you would like to turn off quotations, you need to set not `null` but an empty string. For writing, if an empty string is set, it uses `u0000` (null character). | read/write |
| `quoteAll`                  | false                                                        | A flag indicating whether all values should always be enclosed in quotes. Default is to only escape values containing a quote character. | write      |
| `escape`                    | \                                                            | Sets a single character used for escaping quotes inside an already quoted value. | read/write |
| `escapeQuotes`              | true                                                         | A flag indicating whether values containing quotes should always be enclosed in quotes. Default is to escape all values containing a quote character. | write      |
| `comment`                   |                                                              | Sets a single character used for skipping lines beginning with this character. By default, it is disabled. | read       |
| `header`                    | false                                                        | For reading, uses the first line as names of columns. For writing, writes the names of columns as the first line. Note that if the given path is a RDD of Strings, this header option will remove all lines same with the header if exists. CSV built-in functions ignore this option. | read/write |
| `inferSchema`               | false                                                        | Infers the input schema automatically from data. It requires one extra pass over the data. CSV built-in functions ignore this option. | read       |
| `enforceSchema`             | true                                                         | If it is set to `true`, the specified or inferred schema will be forcibly applied to datasource files, and headers in CSV files will be ignored. If the option is set to `false`, the schema will be validated against all headers in CSV files in the case when the `header` option is set to `true`. Field names in the schema and column names in CSV headers are checked by their positions taking into account `spark.sql.caseSensitive`. Though the default value is true, it is recommended to disable the `enforceSchema` option to avoid incorrect results. CSV built-in functions ignore this option. | read       |
| `ignoreLeadingWhiteSpace`   | `false` (for reading), `true` (for writing)                  | A flag indicating whether or not leading whitespaces from values being read/written should be skipped. | read/write |
| `ignoreTrailingWhiteSpace`  | `false` (for reading), `true` (for writing)                  | A flag indicating whether or not trailing whitespaces from values being read/written should be skipped. | read/write |
| `nullValue`                 |                                                              | Sets the string representation of a null value. Since 2.0.1, this `nullValue` param applies to all supported types including the string type. | read/write |
| `nanValue`                  | NaN                                                          | Sets the string representation of a non-number value.        | read       |
| `positiveInf`               | Inf                                                          | Sets the string representation of a positive infinity value. | read       |
| `negativeInf`               | -Inf                                                         | Sets the string representation of a negative infinity value. | read       |
| `dateFormat`                | yyyy-MM-dd                                                   | Sets the string that indicates a date format. Custom date formats follow the formats at [Datetime Patterns](https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html). This applies to date type. | read/write |
| `timestampFormat`           | yyyy-MM-dd'T'HH:mm:ss[.SSS][XXX]                             | Sets the string that indicates a timestamp format. Custom date formats follow the formats at [Datetime Patterns](https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html). This applies to timestamp type. | read/write |
| `timestampNTZFormat`        | yyyy-MM-dd'T'HH:mm:ss[.SSS]                                  | Sets the string that indicates a timestamp without timezone format. Custom date formats follow the formats at [Datetime Patterns](https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html). This applies to timestamp without timezone type, note that zone-offset and time-zone components are not supported when writing or reading this data type. | read/write |
| `maxColumns`                | 20480                                                        | Defines a hard limit of how many columns a record can have.  | read       |
| `maxCharsPerColumn`         | -1                                                           | Defines the maximum number of characters allowed for any given value being read. By default, it is -1 meaning unlimited length | read       |
| `mode`                      | PERMISSIVE                                                   | Allows a mode for dealing with corrupt records during parsing. It supports the following case-insensitive modes. Note that Spark tries to parse only required columns in CSV under column pruning. Therefore, corrupt records can be different based on required set of fields. This behavior can be controlled by `spark.sql.csv.parser.columnPruning.enabled` (enabled by default). `PERMISSIVE`: when it meets a corrupted record, puts the malformed string into a field configured by `columnNameOfCorruptRecord`, and sets malformed fields to `null`. To keep corrupt records, an user can set a string type field named `columnNameOfCorruptRecord` in an user-defined schema. If a schema does not have the field, it drops corrupt records during parsing. A record with less/more tokens than schema is not a corrupted record to CSV. When it meets a record having fewer tokens than the length of the schema, sets `null` to extra fields. When the record has more tokens than the length of the schema, it drops extra tokens.`DROPMALFORMED`: ignores the whole corrupted records. This mode is unsupported in the CSV built-in functions.`FAILFAST`: throws an exception when it meets corrupted records. | read       |
| `columnNameOfCorruptRecord` | (value of `spark.sql.columnNameOfCorruptRecord` configuration) | Allows renaming the new field having malformed string created by `PERMISSIVE` mode. This overrides `spark.sql.columnNameOfCorruptRecord`. | read       |
| `multiLine`                 | false                                                        | Parse one record, which may span multiple lines, per file. CSV built-in functions ignore this option. | read       |
| `charToEscapeQuoteEscaping` | `escape` or `\0`                                             | Sets a single character used for escaping the escape for the quote character. The default value is escape character when escape and quote characters are different, `\0` otherwise. | read/write |
| `samplingRatio`             | 1.0                                                          | Defines fraction of rows used for schema inferring. CSV built-in functions ignore this option. | read       |
| `emptyValue`                | ``(for reading), `""` (for writing)                          | Sets the string representation of an empty value.            | read/write |
| `locale`                    | en-US                                                        | Sets a locale as language tag in IETF BCP 47 format. For instance, this is used while parsing dates and timestamps. | read       |
| `lineSep`                   | `\r`, `\r\n` and `\n` (for reading), `\n` (for writing)      | Defines the line separator that should be used for parsing/writing. Maximum length is 1 character. CSV built-in functions ignore this option. | read/write |
| `unescapedQuoteHandling`    | STOP_AT_DELIMITER                                            | Defines how the CsvParser will handle values with unescaped quotes. `STOP_AT_CLOSING_QUOTE`: If unescaped quotes are found in the input, accumulate the quote character and proceed parsing the value as a quoted value, until a closing quote is found.`BACK_TO_DELIMITER`: If unescaped quotes are found in the input, consider the value as an unquoted value. This will make the parser accumulate all characters of the current parsed value until the delimiter is found. If no delimiter is found in the value, the parser will continue accumulating characters from the input until a delimiter or line ending is found.`STOP_AT_DELIMITER`: If unescaped quotes are found in the input, consider the value as an unquoted value. This will make the parser accumulate all characters until the delimiter or a line ending is found in the input.`SKIP_VALUE`: If unescaped quotes are found in the input, the content parsed for the given value will be skipped and the value set in nullValue will be produced instead.`RAISE_ERROR`: If unescaped quotes are found in the input, a TextParsingException will be thrown. | read       |
| `compression`               | (none)                                                       | Compression codec to use when saving to file. This can be one of the known case-insensitive shorten names (`none`, `bzip2`, `gzip`, `lz4`, `snappy` and `deflate`). CSV built-in functions ignore this option. |            |

##### Text文件

spark.read.text，dataframe.write.text，option()函数。这是一行一行读取的

Data source options of text can be set via:

- the .option/.options methods of
  - `DataFrameReader`
  - `DataFrameWriter`
  - `DataStreamReader`
  - `DataStreamWriter`
- `OPTIONS` clause at [CREATE TABLE USING DATA_SOURCE](https://spark.apache.org/docs/latest/sql-ref-syntax-ddl-create-table-datasource.html)

| **Property Name** | **Default**                                          | **Meaning**                                                  | **Scope**  |
| :---------------- | :--------------------------------------------------- | :----------------------------------------------------------- | :--------- |
| `wholetext`       | `false`                                              | If true, read each file from input path(s) as a single row.  | read       |
| `lineSep`         | `\r`, `\r\n`, `\n` (for reading), `\n` (for writing) | Defines the line separator that should be used for reading or writing. | read/write |
| `compression`     | (none)                                               | Compression codec to use when saving to file. This can be one of the known case-insensitive shorten names (none, bzip2, gzip, lz4, snappy and deflate). | write      |

##### Hive表

**用到再看**

##### JDBC到其他数据库

**先写一点点，其他的用到再看**

这个功能应该优于使用jdbcRDD。因为他返回dataframe，那么操作起来更简单。JDBC数据源在java和python使用也很方便，因为不需要用户提供ClassTag。

使用的时候，需要在Spark的classpath中包含你想使用的数据库的JDBC驱动。

```python
#比如连接 postgres数据库
./bin/spark-shell --driver-class-path postgresql-9.4.1207.jar --jars postgresql-9.4.1207.jar
```

##### Avro文件

**用到再看**

##### 所有二进制文件

**用到在看**

#### （没看）性能调优

##### 缓存数据到内存

Spark SQL使用内存中的列式存储来缓存表。使用spark.catalog.cacheTable("tableName") or dataFrame.cache()，然后Spark SQL会只扫描需要的列，还会自动调整压缩来最小化内存使用和GC压力。

使用spark.catalog.uncacheTable("name") or dataFrame.unpersist()来将表从内存中删除。

内存缓存的配置可以用SparkSession的setConf方法或者用Spark SQL的set key=value命令设置。

| Property Name                                  | Default | Meaning                                                      | Since Version |
| :--------------------------------------------- | :------ | :----------------------------------------------------------- | :------------ |
| `spark.sql.inMemoryColumnarStorage.compressed` | true    | When set to true Spark SQL will automatically select a compression codec for each column based on statistics of the data. | 1.0.1         |
| `spark.sql.inMemoryColumnarStorage.batchSize`  | 10000   | Controls the size of batches for columnar caching. Larger batch sizes can improve memory utilization and compression, but risk OOMs when caching data. | 1.1.1         |

##### 其他配置选项

下面的选项同样可以用来进行调优查询是的性能。但是以后可能会被弃用

| Property Name                                              | Default             | Meaning                                                      | Since Version |
| :--------------------------------------------------------- | :------------------ | :----------------------------------------------------------- | :------------ |
| `spark.sql.files.maxPartitionBytes`                        | 134217728 (128 MB)  | The maximum number of bytes to pack into a single partition when reading files. This configuration is effective only when using file-based sources such as Parquet, JSON and ORC. | 2.0.0         |
| `spark.sql.files.openCostInBytes`                          | 4194304 (4 MB)      | The estimated cost to open a file, measured by the number of bytes could be scanned in the same time. This is used when putting multiple files into a partition. It is better to over-estimated, then the partitions with small files will be faster than partitions with bigger files (which is scheduled first). This configuration is effective only when using file-based sources such as Parquet, JSON and ORC. | 2.0.0         |
| `spark.sql.files.minPartitionNum`                          | Default Parallelism | The suggested (not guaranteed) minimum number of split file partitions. If not set, the default value is `spark.default.parallelism`. This configuration is effective only when using file-based sources such as Parquet, JSON and ORC. | 3.1.0         |
| `spark.sql.broadcastTimeout`                               | 300                 | Timeout in seconds for the broadcast wait time in broadcast joins | 1.3.0         |
| `spark.sql.autoBroadcastJoinThreshold`                     | 10485760 (10 MB)    | Configures the maximum size in bytes for a table that will be broadcast to all worker nodes when performing a join. By setting this value to -1 broadcasting can be disabled. Note that currently statistics are only supported for Hive Metastore tables where the command `ANALYZE TABLE <tableName> COMPUTE STATISTICS noscan` has been run. | 1.1.0         |
| `spark.sql.shuffle.partitions`                             | 200                 | Configures the number of partitions to use when shuffling data for joins or aggregations. | 1.1.0         |
| `spark.sql.sources.parallelPartitionDiscovery.threshold`   | 32                  | Configures the threshold to enable parallel listing for job input paths. If the number of input paths is larger than this threshold, Spark will list the files by using Spark distributed job. Otherwise, it will fallback to sequential listing. This configuration is only effective when using file-based data sources such as Parquet, ORC and JSON. | 1.5.0         |
| `spark.sql.sources.parallelPartitionDiscovery.parallelism` | 10000               | Configures the maximum listing parallelism for job input paths. In case the number of input paths is larger than this value, it will be throttled down to use this value. Same as above, this configuration is only effective when using file-based data sources such as Parquet, ORC and JSON. | 2.1.1         |

##### 用于SQL查询的join策略提示

支持四中类型:BROADCAST, MERGE, SHUFFLE_HASH and SHUFFLE_REPLICATE_NL

#### 分布式SQL引擎

Spark SQL可以通过使用它的JDBC/ODBC或者命令行接口来作为一个分布式的查询引擎。这样，终端用户或者应用可直接运行SQL查询，而不用写代码。

##### 运行Thrift JDBC/ODBC server

Thrift JDBC\ODBC server等同于Hive内置的HiveServer2。可以使用Spark或者兼容的Hive自带的beeline脚本测试JDBC服务器。

在Spark目录中，用下面的命令启动JDBC/ODBC服务器

```
./sbin/start-thriftserver.sh
```

这个脚本接受所有spark-submit的选项，外加一个--hiveconf选项来指定Hive属性。可通过./sbin/start-thriftserver.sh --help查看所有选项。

默认情况下，服务器监听localhost:10000。可用下面方法修改

```
export HIVE_SERVER2_THRIFT_PORT=<listening-port>
export HIVE_SERVER2_THRIFT_BIND_HOST=<listening-host>
./sbin/start-thriftserver.sh \
  --master <master-uri> \
  ...
```

或者

```
./sbin/start-thriftserver.sh \
  --hiveconf hive.server2.thrift.port=<listening-port> \
  --hiveconf hive.server2.thrift.bind.host=<listening-host> \
  --master <master-uri>
  ...
```

现在可以用beeline脚本测试JDBC\ODBC服务器

```
./bin/beeline
```

在beeline内连接到JDBC\ODBC服务器

```
beeline> !connect jdbc:hive2://localhost:10000
```

在非安全模式下，属于用户名和一个空白密码后就可进入。在安全模式下，需要循序文档中的说明[beeline documentation](https://cwiki.apache.org/confluence/display/Hive/HiveServer2+Clients).

通常会将Hive的配置（hive-site.xml,core-site.xml,hdfs-site.xml）放到conf目录下。

Thrift JDBC server还支持通过HTTP传输thrift RPC信息。用下面的代码来使HTTP模式成为系统属性或者保存在conf目录下的hive-site.xml中

```
hive.server2.transport.mode - Set this to value: http
hive.server2.thrift.http.port - HTTP port number to listen on; default is 10001
hive.server2.http.endpoint - HTTP endpoint; default is cliservice
```

使用beeline连接JDBC\ODBC服务器以http模式

```
beeline> !connect jdbc:hive2://<host>:<port>/<database>?hive.server2.transport.mode=http;hive.server2.thrift.http.path=<http_endpoint>
```

If you closed a session and do CTAS, you must set `fs.%s.impl.disable.cache` to true in `hive-site.xml`. See more details in [[SPARK-21067\]](https://issues.apache.org/jira/browse/SPARK-21067).

##### 运行Spark SQL命令行

```
./bin/spark-sql
```

#### PyArrow

##### 启用与Pandas的转换

Spark DataFrame转换到Pandas DataFrame使用dataframe.toPandas()，

从Pandas DataFrame创建Spark DataFrame使用SparkSession.createDataFrame().

调用这些函数首先需要将spark.sql.execution.arrow.pyspark.enabled设置为true。

如果在Spark内的计算前出现错误，那么通过spark.sql.execution.arrow.pyspark.enabled启用的优化可以回退到非Arrow优化实现。这个功能可通过属性控制：spark.sql.execution.arrow.pyspark.fallback.enabled

```python
import numpy as np
import pandas as pd

# Enable Arrow-based columnar data transfers
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

# Generate a Pandas DataFrame
pdf = pd.DataFrame(np.random.rand(100, 3))

# Create a Spark DataFrame from a Pandas DataFrame using Arrow
df = spark.createDataFrame(pdf)

# Convert the Spark DataFrame back to a Pandas DataFrame using Arrow
result_pdf = df.select("*").toPandas()

print("Pandas DataFrame result statistics:\n%s\n" % str(result_pdf.describe()))
```

不是所有的Spark数据类型都支持转换，如果一个列类型不支持的话就会出错。如果在使用SparkSession.createDataFrame()时出错了，Spark会回退然后不用Arrow来创建DataFrame。

##### Pandas UDFs（又叫做Vectorized UDFs）

Pandas UDF被Spark执行，使用arrow来转换数据，Pandas可以处理这些数据，当然还允许矢量化操作。

UDF通过使用pandas_udf()作为装饰器或包装函数来定义。

类型提示应该使用pandas.Series，但是当输入或输出列为StructType时，类型提示应该用pandas.DataFrame

```python
import pandas as pd

from pyspark.sql.functions import pandas_udf

@pandas_udf("col1 string, col2 long")  # type: ignore[call-overload]
def func(s1: pd.Series, s2: pd.Series, s3: pd.DataFrame) -> pd.DataFrame:
    s3['col2'] = s1 + s2.str.len()
    return s3

# Create a Spark DataFrame that has three columns including a struct column.
df = spark.createDataFrame(
    [[1, "a string", ("a nested string",)]],
    "long_col long, string_col string, struct_col struct<col1:string>")

df.printSchema()
# root
# |-- long_column: long (nullable = true)
# |-- string_column: string (nullable = true)
# |-- struct_column: struct (nullable = true)
# |    |-- col1: string (nullable = true)

df.select(func("long_col", "string_col", "struct_col")).printSchema()
# |-- func(long_col, string_col, struct_col): struct (nullable = true)
# |    |-- col1: string (nullable = true)
# |    |-- col2: long (nullable = true)
```

接下来会描述一些支持的类型提示组合。

##### Series to Series

PySpark通过将列分成批，并将每个批作为数据的子集来调用Pandas UDF，然后将结果连接到一起

```python
import pandas as pd

from pyspark.sql.functions import col, pandas_udf
from pyspark.sql.types import LongType

# Declare the function and create the UDF
def multiply_func(a: pd.Series, b: pd.Series) -> pd.Series:
    return a * b

multiply = pandas_udf(multiply_func, returnType=LongType())  # type: ignore[call-overload]

# The function for a pandas_udf should be able to execute with local Pandas data
x = pd.Series([1, 2, 3])
print(multiply_func(x, x))
# 0    1
# 1    4
# 2    9
# dtype: int64

# Create a Spark DataFrame, 'spark' is an existing SparkSession
df = spark.createDataFrame(pd.DataFrame(x, columns=["x"]))

# Execute function as a Spark vectorized UDF
df.select(multiply(col("x"), col("x"))).show()
# +-------------------+
# |multiply_func(x, x)|
# +-------------------+
# |                  1|
# |                  4|
# |                  9|
# +-------------------+
```

##### Iterator of Series to Iterator of Series

```
from typing import Iterator

import pandas as pd

from pyspark.sql.functions import pandas_udf

pdf = pd.DataFrame([1, 2, 3], columns=["x"])
df = spark.createDataFrame(pdf)

# Declare the function and create the UDF
@pandas_udf("long")  # type: ignore[call-overload]
def plus_one(iterator: Iterator[pd.Series]) -> Iterator[pd.Series]:
    for x in iterator:
        yield x + 1

df.select(plus_one("x")).show()
# +-----------+
# |plus_one(x)|
# +-----------+
# |          2|
# |          3|
# |          4|
# +-----------+
```

##### Iterator of Multiple Series to Iterator of Series

```python
from typing import Iterator, Tuple

import pandas as pd

from pyspark.sql.functions import pandas_udf

pdf = pd.DataFrame([1, 2, 3], columns=["x"])
df = spark.createDataFrame(pdf)

# Declare the function and create the UDF
@pandas_udf("long")  # type: ignore[call-overload]
def multiply_two_cols(
        iterator: Iterator[Tuple[pd.Series, pd.Series]]) -> Iterator[pd.Series]:
    for a, b in iterator:
        yield a * b

df.select(multiply_two_cols("x", "x")).show()
# +-----------------------+
# |multiply_two_cols(x, x)|
# +-----------------------+
# |                      1|
# |                      4|
# |                      9|
# +-----------------------+
```

##### Series to Scalar

返回的类型应该是原始数据类型，返回的标量可以使python原始类型，例如int、float或者numpy数据类型例如numpy.int64、numpy.float64。理想情况下，Any应该是相应的特定标量类型。

UDF还可以用于GroupedData.agg()和Window。它定义了一个从一个或多个pd.Series到一个标量值的聚合。每个pd.Series表示group或者window中的一个列。

*UDF不支持部分聚合，group或者window中的所有数据都会加载进内存。无界窗口支持分组聚合Pandas UDF*

```
import pandas as pd

from pyspark.sql.functions import pandas_udf
from pyspark.sql import Window

df = spark.createDataFrame(
    [(1, 1.0), (1, 2.0), (2, 3.0), (2, 5.0), (2, 10.0)],
    ("id", "v"))

# Declare the function and create the UDF
@pandas_udf("double")  # type: ignore[call-overload]
def mean_udf(v: pd.Series) -> float:
    return v.mean()

df.select(mean_udf(df['v'])).show()
# +-----------+
# |mean_udf(v)|
# +-----------+
# |        4.2|
# +-----------+

df.groupby("id").agg(mean_udf(df['v'])).show()
# +---+-----------+
# | id|mean_udf(v)|
# +---+-----------+
# |  1|        1.5|
# |  2|        6.0|
# +---+-----------+

w = Window \
    .partitionBy('id') \
    .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
df.withColumn('mean_v', mean_udf(df['v']).over(w)).show()
# +---+----+------+
# | id|   v|mean_v|
# +---+----+------+
# |  1| 1.0|   1.5|
# |  1| 2.0|   1.5|
# |  2| 3.0|   6.0|
# |  2| 5.0|   6.0|
# |  2|10.0|   6.0|
# +---+----+------+
```

##### Pandas Function APIs

**Grouped Map**

DataFrame.groupby().applyInPandas()支持使用Pandas实例的分组映射操作，它需要一个python函数，接受一个pd.DataFrame，返回另一个pd.DataFrame，它将每个组映射到一个pd.DataFrame。

这个实现由三步组成：

1. 用DataFrame.groupBy()切分数据到每个组里
2. Apply一个函数到每个组里。函数的输入和输出是pd.DataFrame，输入数据包含分组中所有的行和列
3. 结合所有的结果作为新的PySpark DataFrame。

返回的pd.DataFrame的列标签必须匹配输出schema的字段名，如果schema是是个string，否则的话，要通过位置匹配字段数据类型

```python
df = spark.createDataFrame(
    [(1, 1.0), (1, 2.0), (2, 3.0), (2, 5.0), (2, 10.0)],
    ("id", "v"))

def subtract_mean(pdf: pd.DataFrame) -> pd.DataFrame:
    # pdf is a pandas.DataFrame
    v = pdf.v
    return pdf.assign(v=v - v.mean())

df.groupby("id").applyInPandas(subtract_mean, schema="id long, v double").show()
# +---+----+
# | id|   v|
# +---+----+
# |  1|-0.5|
# |  1| 0.5|
# |  2|-3.0|
# |  2|-1.0|
# |  2| 4.0|
# +---+----+
```

**Map**

将一个pd.DataFrame的iterator  map成另一个pd.DataFrame的iterator，然后返回结果作为Spark的DataFrame。

```
df = spark.createDataFrame([(1, 21), (2, 30)], ("id", "age"))

def filter_func(iterator: Iterable[pd.DataFrame]) -> Iterable[pd.DataFrame]:
    for pdf in iterator:
        yield pdf[pdf.id == 1]

df.mapInPandas(filter_func, schema=df.schema).show()
# +---+---+
# | id|age|
# +---+---+
# |  1| 21|
# +---+---+
```

##### Co-grouped Map

DataFrame.groupby().cogroup().applyInPandas()，它使两个Spark DataFrame通过公共键进行共同分组。这个过程有下面几步：

1. shuffle数据，使组内dataframe都共享一个key的group在一起共同分组
2. Apply一个函数到每个共同分组。函数的输入是两个pd.DataFrame（有一个可选的元祖表示key），输出是一个pd.DataFrame
3. 将这些pd.DataFframe组合，变成一个Spark DataFrame

```python
import pandas as pd

df1 = spark.createDataFrame(
    [(20000101, 1, 1.0), (20000101, 2, 2.0), (20000102, 1, 3.0), (20000102, 2, 4.0)],
    ("time", "id", "v1"))

df2 = spark.createDataFrame(
    [(20000101, 1, "x"), (20000101, 2, "y")],
    ("time", "id", "v2"))

def asof_join(left: pd.DataFrame, right: pd.DataFrame) -> pd.DataFrame:
    return pd.merge_asof(left, right, on="time", by="id")

df1.groupby("id").cogroup(df2.groupby("id")).applyInPandas(
    asof_join, schema="time int, id int, v1 double, v2 string").show()
# +--------+---+---+---+
# |    time| id| v1| v2|
# +--------+---+---+---+
# |20000101|  1|1.0|  x|
# |20000102|  1|3.0|  x|
# |20000101|  2|2.0|  y|
# |20000102|  2|4.0|  y|
# +--------+---+---+---+
```

##### 注意事项

**支持的SQL类型**

除了TimeStampType的ArrayType和嵌套的StructType，其他的Spark SQL数据类型都支持基于Arrow的转换

**设置Arrow批大小**

Spark中的数据分区转换到Arrow记录批，这会增加JVM中的内存使用，为了避免出现内存溢出的异常，可配置spark.sql.execution.arrow.maxRecordsPerBatch为一个整数，来控制每个批行的最大值，默认是10000，

**Timestamp with Time Zone Semantics**

> Spark internally stores timestamps as UTC values, and timestamp data that is brought in without a specified time zone is converted as local time to UTC with microsecond resolution. When timestamp data is exported or displayed in Spark, the session time zone is used to localize the timestamp values. The session time zone is set with the configuration `spark.sql.session.timeZone` and will default to the JVM system local time zone if not set. Pandas uses a `datetime64` type with nanosecond resolution, `datetime64[ns]`, with optional time zone on a per-column basis.
>
> When timestamp data is transferred from Spark to Pandas it will be converted to nanoseconds and each column will be converted to the Spark session time zone then localized to that time zone, which removes the time zone and displays values as local time. This will occur when calling [`DataFrame.toPandas()`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.toPandas.html#pyspark.sql.DataFrame.toPandas) or `pandas_udf` with timestamp columns.
>
> When timestamp data is transferred from Pandas to Spark, it will be converted to UTC microseconds. This occurs when calling [`SparkSession.createDataFrame()`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.SparkSession.createDataFrame.html#pyspark.sql.SparkSession.createDataFrame) with a Pandas DataFrame or when returning a timestamp from a `pandas_udf`. These conversions are done automatically to ensure Spark will have data in the expected format, so it is not necessary to do any of these conversions yourself. Any nanosecond values will be truncated.
>
> Note that a standard UDF (non-Pandas) will load timestamp data as Python datetime objects, which is different than a Pandas timestamp. It is recommended to use Pandas time series functionality when working with timestamps in `pandas_udf`s to get the best performance, see [here](https://pandas.pydata.org/pandas-docs/stable/timeseries.html) for details.

**设置Arrow的self_destruct来节约内存**

从Spark3.2开始，spark.sql.execution.arrow.pyspark.selfDestruct.enabled可用来启用PyArrow的self_destruct特性，这个特性可以节约内存，当用toPandas创建一个Pandas DataFrame时，将释放Arrow分配的内存。有些情况下会出错。

#### （没看呢）SQL Reference

[Saprk3.3.0的SQL Reference（我写文档时的最新网址）](https://spark.apache.org/docs/latest/sql-ref.html)

