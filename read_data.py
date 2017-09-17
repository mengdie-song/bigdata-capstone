# dependency: ./bin/pyspark --packages com.databricks:spark-csv_2.10:1.4.0
from pyspark.sql import SQLContext

sqlContext = SQLContext(sc)
df = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load('/home/songm/bigdata-capstone/dataset/crypto-markets.csv')
print(df.dtypes)
