#### RDD 编程指南

##### 初始化Spark

```python
#创建SparkContext，它告诉Spark怎么访问集群
from pyspark import SparkContext, SparkConf
conf = SparkConf().setAppName(appName).setMaster(master)
sc = SparkContext(conf=conf)
```

```scala
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
val conf = new SparkConf().setAppName(appName).setMaster(master)
val sc = new SparkContext(conf)
```

##### 弹性数据集（RDDs）

RDD是Spark中的主要数据类型，它是具有容错性的，可以被并行操作的数据集合。

可通过两种方法创造：

1. parallelizing 一个集合。
2. 引用外部存储系统（例如本地，HDFS，Hbase或者其他支持Hadoop InputFormat存储）中的数据集。

###### 并行集合

```python
data = [1,2,3,4,5]
distData = sc.parallelize(data)
distData.reduce(lambda x,y:x+y)
#output 15

val data = Array(1, 2, 3, 4, 5)
val distData = sc.parallelize(data)
```

在sc.parallelize方法中，第二个参数表示分区数，同时也是切分数据的数量。

Spark会为集群上的每一个分区都运行一个任务，一般来说每个Cpu应该需要2-4个分区。

Spark也会根据集群状况先进自动分区。如果想手动设置分区数可以用下面的方法

```python
distData = sc.parallelize(data,10)
```

###### 外部数据集

Spark支持text files，SquenceFiles和其他的Hadoop InputFormat。

```python
#默认情况下，会从HDFS读取数据
distFile = sc.textFile("/datasets/HousingData.csv")
distFile.map(lambda x:len(x)).reduce(lambda x,y:x+y)
#output 33994

#若想从本地读取文件,要在地址前加file://
distLocalFile = sc.textFile('file:///usr/local/spark-3.3.0/jupyter.log')
```

读取文件的一些注意事项：

1. 如果是从本地读取，那么集群中的每个节点在相同的路径上都需要有这个文件。
2. 像textFile这种基于文件输入的方法，可以从目录中读取，也可以从压缩文件中读取，路径上可以包含通配符。比如：textFile('/my/directory');textFile('/my/directory/*.gz')。
3. textFile的第二个参数（可选）用于控制文件分区。默认情况下，Spark会为文件的每一个块创建一个分区（HDFS中默认块大小128MB）。**分区数不能小于块数**

Spark支持的其他数据类型还有：

1. SparkContext.wholeTextFiles：读取目录中的多个小文件，并返回文件名-文件全部内容的数据对。而textFile是一行一行读取数据。
2. RDD.saveAsPickleFile （序列化）和 RDD.PickleFile（反序列化），将RDD保存为pickled python objects，在picled的序列化中会用到批处理，默认大小是10。（Python）
3. SequenceFile ,用SparkContext.sequenceFile[K,V]，K和V应该是Writable的子类，一些常用的Writable，可用类似下面形式sequenceFile[Int,String]
4. 其他的Hadoop InputFormat，用sc.hadoopRDD。对于新的API（org.apache.hadoop.mapreduce）中的InputFormat用sc.newAPIHadoopRDD
5. RDD.saveAsObjectFile and sc.objectFile，保存RDD为可序列化对象的SequenceFile

**Writable 支持**（Python）

PySpark SequenceFile 可以从Java加载一个由键值对组成的RDD，将Writable转换成Java基本类型，使用pickle对得到的java对象做pickle操作。当把一个RDD保存到SequenceFile中时，PySpark的操作正好相反。它先unpickle Java对象，然后转换成Writable。下面的Writable会自动转换

| Writable Type   | Python Type |
| :-------------- | :---------- |
| Text            | str         |
| IntWritable     | int         |
| FloatWritable   | float       |
| DoubleWritable  | float       |
| BooleanWritable | bool        |
| BytesWritable   | bytearray   |
| NullWritable    | None        |
| MapWritable     | dict        |

数组不能被处理。当写的时候，用户要自定义转换器来将数组转换成自定义的ArrayWritable。当读的时候，转换器将自定义的ArrayWritable转成java Obeject[ ]，然后再picled成Python tuples。 

**保存和加载 SequenceFiles**（Python）

输入输出的键值对可以被指令，类似Hadoop。标准Writable不需要。

```python
rdd = sc.parallelize(range(1, 4)).map(lambda x: (x, "a" * x))
rdd.saveAsSequenceFile("path/to/file")
sorted(sc.sequenceFile("path/to/file").collect())
#output [(1, u'a'), (2, u'aa'), (3, u'aaa')]
```

**保存和加载其他 Hadoop Input/Output Formats**（Python）

```
PySpark can also read any Hadoop InputFormat or write any Hadoop OutputFormat, for both ‘new’ and ‘old’ Hadoop MapReduce APIs. If required, a Hadoop configuration can be passed in as a Python dict. Here is an example using the Elasticsearch ESInputFormat:

$ ./bin/pyspark --jars /path/to/elasticsearch-hadoop.jar
>>> conf = {"es.resource" : "index/type"}  # assume Elasticsearch is running on localhost defaults
>>> rdd = sc.newAPIHadoopRDD("org.elasticsearch.hadoop.mr.EsInputFormat",
                             "org.apache.hadoop.io.NullWritable",
                             "org.elasticsearch.hadoop.mr.LinkedMapWritable",
                             conf=conf)
>>> rdd.first()  # the result is a MapWritable that is converted to a Python dict
(u'Elasticsearch ID',
 {u'field1': True,
  u'field2': u'Some Text',
  u'field3': 12345})
Note that, if the InputFormat simply depends on a Hadoop configuration and/or input path, and the key and value classes can easily be converted according to the above table, then this approach should work well for such cases.

If you have custom serialized binary data (such as loading data from Cassandra / HBase), then you will first need to transform that data on the Scala/Java side to something which can be handled by pickle’s pickler. A Converter trait is provided for this. Simply extend this trait and implement your transformation code in the convert method. Remember to ensure that this class, along with any dependencies required to access your InputFormat, are packaged into your Spark job jar and included on the PySpark classpath.

See the Python examples and the Converter examples for examples of using Cassandra / HBase InputFormat and OutputFormat with custom converters.
```

###### RDD的操作

RDD有transoformation和action操作。

所有的transformation都是惰性的，就是他们不会立刻计算结果，而是先记住转换过程，只有当action操作执行的时候，transformation才会被执行。

对一个已经被转换过的RDD执行action操作，那么这个RDD还会被重新计算。就是说中间结果不保存。那么可以用cache方法将RDD持久化到内存，或者用persist方法，持久化到内存、本地磁盘或备份到多个节点。**持久化操作应该在action算子之前**

当执行action算子时，Spark会将计算分成多个子任务给到集群中的节点，节点计算自己的那一部分操作后再讲结果返回到driver程序。

**传递函数到Spark**

有三种方法：

1. lambda 表达式，匿名函数

2. 定义一个函数：

   ```python
   lines = sc.textFile("/datasets/HousingData.csv")
   def myFunc(s):
       words = s.split(" ")
       return len(words)
   def sum(x,y):
       return x+y
   lines.map(myFunc).reduce(sum)
   ```

3. 使用模块中的顶级函数

*注意：*当你传递了一个类对象的函数引用，那么在给集群中节点分发任务的时候，整个类也会被发送：

```python
class MyClass(object):
    def func(self, s):
        return s
    def doStuff(self, rdd):
        return rdd.map(self.func)
```

当你访问了类的元素时，也会发送整个类：

```python
class MyClass(object):
    def __init__(self):
        self.field = "Hello"
    def doStuff(self, rdd):
        return rdd.map(lambda s: self.field + s)
```

若要是避免第二种情况，可以把类中元素的值拿出来

```python
def doStuff(self, rdd):
    field = self.field
    return rdd.map(lambda s: field + s)
```

**理解闭包**

```python
counter = 0
rdd = sc.parallelize(data)

# Wrong: Don't do this!!
def increment_counter(x):
    global counter
    counter += x
rdd.foreach(increment_counter)

print("Counter value: ", counter)
```

上面的输出结果为0，这跟我们想的不一样。

因为在执行job时候，Spark会将job分成多个子task，发送到集群中的节点executor中，在执行task之前，Spark会计算一下task的闭包。闭包就是那些变量和方法对在RDD上执行操作的executor必须是可访问的。闭包是通过序列化发送到每个executor上的。

发送到executor上的闭包中的变量是一个副本，在executor上计算的是自己节点上的变量，而输出的是driver节点上的counter变量。这个driver上counter变量放在其内存上并对其他的executor不可见。

出现这样的原因还因为他们是在不同的JVM上的做的计算。

如果实在local模式下，有时driver和executor会使用相同的JVM，那么这个时候就有可能对counter进行更新。

上述行为有时候在local模式可能会起作用，但是在分布模式不会起作用的。

*若是想让counter被修改，可以尝试使用累加器。*

**输出RDD元素**

可用rdd.foreach(print)或rdd.map(print)。

在集群模式下输出会显示在执行foreach或map的executor的stdout上。

若想在driver上输出，可先用rdd.collect()方法将RDD采集到driver节点，然后rdd.collect().foreach(println)。但是这样会把其他节点上的RDD的都拿到driver上，可能会导致driver内存不足。

若是只想输出几个元素，可以这样：rdd.take(100).foreach(print)

**对于键值对RDD的操作**

大多数Spark操作可以对包含任何数据类型的RDD操作，但是有一些操作只能用在键值对RDD上。这些键值对在python中就是元组类型（1,2）。

**Transformation函数**

使用transformation函数会生成新的RDD

| Transformation                                               | Meaning                                                      |
| :----------------------------------------------------------- | :----------------------------------------------------------- |
| **map**(*func*)                                              | Return a new distributed dataset formed by passing each element of the source through a function *func*. |
| **filter**(*func*)                                           | Return a new dataset formed by selecting those elements of the source on which *func* returns true. |
| **flatMap**(*func*)                                          | Similar to map, but each input item can be mapped to 0 or more output items (so *func* should return a Seq rather than a single item). |
| **mapPartitions**(*func*)                                    | Similar to map, but runs separately on each partition (block) of the RDD, so *func* must be of type Iterator<T> => Iterator<U> when running on an RDD of type T. |
| **mapPartitionsWithIndex**(*func*)                           | Similar to mapPartitions, but also provides *func* with an integer value representing the index of the partition, so *func* must be of type (Int, Iterator<T>) => Iterator<U> when running on an RDD of type T. |
| **sample**(*withReplacement*, *fraction*, *seed*)            | Sample a fraction *fraction* of the data, with or without replacement, using a given random number generator seed. |
| **union**(*otherDataset*)                                    | Return a new dataset that contains the union of the elements in the source dataset and the argument. |
| **intersection**(*otherDataset*)                             | Return a new RDD that contains the intersection of elements in the source dataset and the argument. |
| **distinct**([*numPartitions*]))                             | Return a new dataset that contains the distinct elements of the source dataset. |
| **groupByKey**([*numPartitions*])                            | When called on a dataset of (K, V) pairs, returns a dataset of (K, Iterable<V>) pairs. **Note:** If you are grouping in order to perform an aggregation (such as a sum or average) over each key, using `reduceByKey` or `aggregateByKey` will yield much better performance. **Note:** By default, the level of parallelism in the output depends on the number of partitions of the parent RDD. You can pass an optional `numPartitions` argument to set a different number of tasks. |
| **reduceByKey**(*func*, [*numPartitions*])                   | When called on a dataset of (K, V) pairs, returns a dataset of (K, V) pairs where the values for each key are aggregated using the given reduce function *func*, which must be of type (V,V) => V. Like in `groupByKey`, the number of reduce tasks is configurable through an optional second argument. |
| **aggregateByKey**(*zeroValue*)(*seqOp*, *combOp*, [*numPartitions*]) | When called on a dataset of (K, V) pairs, returns a dataset of (K, U) pairs where the values for each key are aggregated using the given combine functions and a neutral "zero" value. Allows an aggregated value type that is different than the input value type, while avoiding unnecessary allocations. Like in `groupByKey`, the number of reduce tasks is configurable through an optional second argument. |
| **sortByKey**([*ascending*], [*numPartitions*])              | When called on a dataset of (K, V) pairs where K implements Ordered, returns a dataset of (K, V) pairs sorted by keys in ascending or descending order, as specified in the boolean `ascending` argument. |
| **join**(*otherDataset*, [*numPartitions*])                  | When called on datasets of type (K, V) and (K, W), returns a dataset of (K, (V, W)) pairs with all pairs of elements for each key. Outer joins are supported through `leftOuterJoin`, `rightOuterJoin`, and `fullOuterJoin`. |
| **cogroup**(*otherDataset*, [*numPartitions*])               | When called on datasets of type (K, V) and (K, W), returns a dataset of (K, (Iterable<V>, Iterable<W>)) tuples. This operation is also called `groupWith`. |
| **cartesian**(*otherDataset*)                                | When called on datasets of types T and U, returns a dataset of (T, U) pairs (all pairs of elements). |
| **pipe**(*command*, *[envVars]*)                             | Pipe each partition of the RDD through a shell command, e.g. a Perl or bash script. RDD elements are written to the process's stdin and lines output to its stdout are returned as an RDD of strings. |
| **coalesce**(*numPartitions*)                                | Decrease the number of partitions in the RDD to numPartitions. Useful for running operations more efficiently after filtering down a large dataset. |
| **repartition**(*numPartitions*)                             | Reshuffle the data in the RDD randomly to create either more or fewer partitions and balance it across them. This always shuffles all data over the network. |
| **repartitionAndSortWithinPartitions**(*partitioner*)        | Repartition the RDD according to the given partitioner and, within each resulting partition, sort records by their keys. This is more efficient than calling `repartition` and then sorting within each partition because it can push the sorting down into the shuffle machinery. |

**Action操作**

| Action                                             | Meaning                                                      |
| :------------------------------------------------- | :----------------------------------------------------------- |
| **reduce**(*func*)                                 | Aggregate the elements of the dataset using a function *func* (which takes two arguments and returns one). The function should be commutative and associative so that it can be computed correctly in parallel. |
| **collect**()                                      | Return all the elements of the dataset as an array at the driver program. This is usually useful after a filter or other operation that returns a sufficiently small subset of the data. |
| **count**()                                        | Return the number of elements in the dataset.                |
| **first**()                                        | Return the first element of the dataset (similar to take(1)). |
| **take**(*n*)                                      | Return an array with the first *n* elements of the dataset.  |
| **takeSample**(*withReplacement*, *num*, [*seed*]) | Return an array with a random sample of *num* elements of the dataset, with or without replacement, optionally pre-specifying a random number generator seed. |
| **takeOrdered**(*n*, *[ordering]*)                 | Return the first *n* elements of the RDD using either their natural order or a custom comparator. |
| **saveAsTextFile**(*path*)                         | Write the elements of the dataset as a text file (or set of text files) in a given directory in the local filesystem, HDFS or any other Hadoop-supported file system. Spark will call toString on each element to convert it to a line of text in the file. |
| **saveAsSequenceFile**(*path*) (Java and Scala)    | Write the elements of the dataset as a Hadoop SequenceFile in a given path in the local filesystem, HDFS or any other Hadoop-supported file system. This is available on RDDs of key-value pairs that implement Hadoop's Writable interface. In Scala, it is also available on types that are implicitly convertible to Writable (Spark includes conversions for basic types like Int, Double, String, etc). |
| **saveAsObjectFile**(*path*) (Java and Scala)      | Write the elements of the dataset in a simple format using Java serialization, which can then be loaded using `SparkContext.objectFile()`. |
| **countByKey**()                                   | Only available on RDDs of type (K, V). Returns a hashmap of (K, Int) pairs with the count of each key. |
| **foreach**(*func*)                                | Run a function *func* on each element of the dataset. This is usually done for side effects such as updating an [Accumulator](https://spark.apache.org/docs/latest/rdd-programming-guide.html#accumulators) or interacting with external storage systems. **Note**: modifying variables other than Accumulators outside of the `foreach()` may result in undefined behavior. See [Understanding closures ](https://spark.apache.org/docs/latest/rdd-programming-guide.html#understanding-closures-a-nameclosureslinka)for more details. |

**Shuffle操作**

需要在多个节点之间复制数据，所以Shuffle操作费时间且费资源。

例如reduceByKey的操作，是把所有键相同的值放在一个分区里进行计算，但并不是所有的键相同的数据都会在一个分区上，那么这个时候，Spark就会进行Shuffle操作，即把所有分区上的所有键相同的数据放到一个相同的分区上，这个过程类似于Hadoop的Shuffle过程。

shuffle后，分区的个数是确定的，顺序也是确定的，分区中的数据集是确定的，但是元素在分区中的顺序是不确定的。若是想确定顺序，可以这样：

- mapPartitions：用它在分区内排序
- repartitionAndSortWithinPartitions：重新分区的同时对分区内排序
- sortBy：创建一个全局有序的RDD

常用的shuffle操作：

- 包含repartition的操作：repartition、coalesce...
- 包含ByKey的操作：groupByKey、reduceByKey...
- 包含join的操作：cogroup、join...

**Shuffle的性能影响**

Shuffle操作非常昂贵，因为它包含了磁盘的I\O、数据的序列化、网络的I\O。Spark会生成多个task集合，map task管理数据，reduce task聚合数据。（*这里的map和reduce是一个泛指，是一个大类，并不是具体的那个map和reduce函数*）

来自各个map task的结果会被放到内存中，直到内存装不下时，开始对这些结果按目标分区排序并写到磁盘单个文件中（这导致额外的磁盘I\O开销，并增加垃圾回收）。reduce task会读取相关的排序好的块。

一些shuffle很耗内存，因为它是用内存中的数据结构来管理这个记录。

shuffle还会在磁盘上产生一些中间文件，从Spark1.3开始，当与其相关的RDD不再使用或者被当成垃圾回收时，中间文件会被删除。中间文件存在的目的是为了lineage被重新计算时不需要重新创建shuffle文件。

如果应用一直使用这RDD或者GC机制不经常机动，那么垃圾回收可能会在很长的一段时候后才发生，这意味着长时间的运行Spark jon会消耗大量的磁盘空间。

shuffle的行为可以通过配置调整，例如临时存储目录可通过spark.local.dir指定，可以放在配置文件中，也可以在程序中指定：

```python
from pyspark import SparkConf,SparkContext
conf = SparkConf().setMaster(master)
conf.set("spark.local.dir",path)
sc = SparkContext(conf=conf)
```

*更多的可以看Spark Configuration Guide中的Shuffle Behavior部分。*

###### RDD持久化

把中间数据放到内存里，然后在该数据集或者是由这个数据集派生的数据集上复用。

可用rdd.persis()或rdd.cache()（*只能持久化在内存中*）方法持久化。Spark的所有持久化级别都有容错性，都是若RDD的一个分区丢了，那么Spark会从头开始执行transformation操作生成一个相同RDD。

*在python中，被持久化的对象都是会被使用Pickle包进行序列化，我们不需要去指定*

```python
from pyspark import StorageLevel
rdd.persist(StorageLevel.XXXXX)
```

| Storage Level                          | Meaning                                                      |
| :------------------------------------- | :----------------------------------------------------------- |
| MEMORY_ONLY                            | Store RDD as deserialized Java objects in the JVM. If the RDD does not fit in memory, some partitions will not be cached and will be recomputed on the fly each time they're needed. This is the default level. |
| MEMORY_AND_DISK                        | Store RDD as deserialized Java objects in the JVM. If the RDD does not fit in memory, store the partitions that don't fit on disk, and read them from there when they're needed. |
| MEMORY_ONLY_SER (Java and Scala)       | Store RDD as *serialized* Java objects (one byte array per partition). This is generally more space-efficient than deserialized objects, especially when using a [fast serializer](https://spark.apache.org/docs/latest/tuning.html), but more CPU-intensive to read. |
| MEMORY_AND_DISK_SER (Java and Scala)   | Similar to MEMORY_ONLY_SER, but spill partitions that don't fit in memory to disk instead of recomputing them on the fly each time they're needed. |
| DISK_ONLY                              | Store the RDD partitions only on disk.                       |
| MEMORY_ONLY_2, MEMORY_AND_DISK_2, etc. | Same as the levels above, but replicate each partition on two cluster nodes. |
| OFF_HEAP (experimental)                | Similar to MEMORY_ONLY_SER, but store the data in [off-heap memory](https://spark.apache.org/docs/latest/configuration.html#memory-management). This requires off-heap memory to be enabled. |

**Storage Level的选择**

我们要考虑内存的使用和cpu性能。

- 如果用MEMORY_ONLY，可以轻松的把RDD放在内存中，那就这样。这能让CPU有更高的性能，操作RDD更快。
- 否则使用MEMORY_ONLY_SER，在选择一个合适的序列化器，这更节省空间，访问速度也很快（Java和Scala）。
- 最好不要放到磁盘上，除非你的计算函数非常昂贵或者要过滤大量的数据。放到磁盘上，会使你重新计算分区的速度和从磁盘读取文件的速度是一样的。
- 如果你想快速恢复故障，可选用复制存储级别。因为其他的存储级别的容错性都是重新计算丢失数据，但是复制存储级别不用等待重新计算的过程，可持续运行。

**移除数据**

Spark自动监控内存使用，根据最近最少使用方式(LRU)删除数据分区。

可以用rdd.unpersist()方法手动移除数据。默认情况下这个方法不会被阻塞，若是指定参数blocking=true，则会被阻塞，直到资源被释放。

##### 共享变量

执行一个job，会分成多个task，每个task会在一个executor上，每个task执行独立的函数和独立的变量，也就是说它们是一个副本，在executor上对独立变量的更新不会传递回driver中，这是Spark的闭包机制。

Spark对两种常见的是使用模式提供了两种有限类型的共享变量：broadcast和accumulator。

*共享变量在task之间是通用的，可读写的，但很低效*

###### Broadcast变量

可以看成是所有task之间的公用的一个全局变量，不受闭包机制影响。

当跨多个stage的task需要相同的数据时，和当以反序列化形式缓存数据时，是非常有用的。

```
broadcastVar = sc.broadcast([1, 2, 3])
broadcastVar.value
```

当一个变量被广播以后，为了确保其他的节点都能得到相同的值，变量不应该被修改。

使用.unpersist()方法释放广播变量复制到executor上的资源。如果释放之后，广播变量又被使用，那么会重新广播

使用.destroy()方法永久释放所有被广播变量所使用的资源。在这之后广播变量就不能被使用了。

默认情况下这些方法不会阻塞。设置blocking=true，那么会阻塞，直到资源被释放

######  Accumulators

*Tracking accumulators in the UI can be useful for understanding the progress of running stages (NOTE: this is not yet supported in Python).*

累加器类似一个在集群中的共享变量，通过SparkContext.accumulator创建，通过add方法或者+=操作符相加，对累加器值的更新只能在action算子里。是只有driver程序可以访问它的值。用value方法。

```python
accum = sc.accumulator(0)
sc.parallelize([1,2,3,4]).foreach(lambda x:accum.add(x))
accum.value
```

为其他的数据类型自定义累加器计算方法，需要继承AccumulatorPara，默认情况下是int或float类型。

```python
from pyspark import T
from pyspark.accumulators import AccumulatorParam

class VectorAccumulatorParam(AccumulatorParam):
    def zero(self, value: T) -> T:
        return Vectors.zeros(value.size)

    def addInPlace(self, value1: T, value2: T) -> T:
        value1 += value2
        return value1
vecAccum = sc.accumulator(Vectors.dense([0]),VectorAccumulatorParam())

data = sc.parallelize([Vectors.dense([1]),Vectors.dense([1])])
data.foreach(lambda x:vecAccum.add(x))
vecAccum.value

```

```scala
class VectorAccumulatorV2 extends AccumulatorV2[MyVector, MyVector] {

  private val myVector: MyVector = MyVector.createZeroVector

  def reset(): Unit = {
    myVector.reset()
  }

  def add(v: MyVector): Unit = {
    myVector.add(v)
  }
  ...
}

// Then, create an Accumulator of this type:
val myVectorAcc = new VectorAccumulatorV2
// Then, register it into spark context:
sc.register(myVectorAcc, "MyVectorAcc1")
```

