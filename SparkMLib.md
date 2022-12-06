#### 基础的数值统计

##### 相关性

spark.ml包，提供一些方法，可以在多个数据之间计算成对相关性。

现在支持Pearson和Spearman相关性。

Correlation为输入的DataSet of Vectors计算相关性矩阵，使用指定的方法。输出是一个DataFrame，其包含向量列的相关性矩阵。

```python
from pyspark.ml.linalg import Vectors
from pyspark.ml.stat import Correlation

data = [(Vectors.sparse(4, [(0, 1.0), (3, -2.0)]),),
        (Vectors.dense([4.0, 5.0, 0.0, 3.0]),),
        (Vectors.dense([6.0, 7.0, 0.0, 8.0]),),
        (Vectors.sparse(4, [(0, 9.0), (3, 1.0)]),)]
df = spark.createDataFrame(data, ["features"])

r1 = Correlation.corr(df, "features").head()


print("Pearson correlation matrix:\n" + str(r1[0]))

r2 = Correlation.corr(df, "features", "spearman").head()


print("Spearman correlation matrix:\n" + str(r2[0]))
```

##### 假设检验

假设检验在统计学中非常有用，它来检测一个结果是否具有统计学意义，是否是偶然发生的。

spark.ml现在支持Pearson’s Chi-squared ( χ2)独立检验。

**ChiSquareTest**





##### Summarizer