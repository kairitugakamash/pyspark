# pyspark
1. https://spark.apache.org/docs/latest/api/python/getting_started/quickstart_df.html
2. https://www.datacamp.com/tutorial/pyspark-tutorial-getting-started-with-pyspark
3. https://sparkbyexamples.com/
4. https://docs.databricks.com/en/pyspark/index.html
5. https://www.kaggle.com/code/tientd95/pyspark-for-data-science/notebook
6. https://www.datacamp.com/cheat-sheet/pyspark-cheat-sheet-spark-in-python#gs.L=J1zxQ

# Inspect SparkContext
1. sc.version #Retrieve SparkContext version
2. sc.pythonVer #Retrieve Python version
3. sc.master #Master URL to connect to
4. str(sc.sparkHome) #Path where Spark is installed on worker nodes
5. str(sc.sparkUser()) #Retrieve name of the Spark User running SparkContext
6. sc.appName #Return application name
7. sc.applicationld #Retrieve application ID
8. sc.defaultParallelism #Return default level of parallelism
9. sc.defaultMinPartitions #Default minimum number of partitions for RDDs

# Retrieving RDD Information
1. rdd.getNumPartitions() #List the number of partitions
2. rdd.count() #Count RDD instances 3
3. rdd.countByKey() #Count RDD instances by keydefaultdict(<type 'int'>,{'a':2,'b':1})
4. rdd.countByValue() #Count RDD instances by valuedefaultdict(<type 'int'>,{('b',2):1,('a',2):1,('a',7):1})
5. rdd.collectAsMap() #Return (key,value) pairs as a dictionary   {'a': 2, 'b': 2}
6. rdd3.sum() #Sum of RDD elements 4950
7. sc.parallelize([]).isEmpty() #Check whether RDD is emptyTrue

# Saving
1. rdd.saveAsTextFile("rdd.txt")
2. rdd.saveAsHadoopFile("hdfs://namenodehost/parent/child", 'org.apache.hadoop.mapred.TextOutputFormat')

# Loading data
1. rdd = sc.parallelize([('a',7),('a',2),('b',2)])
2. rdd2 = sc.parallelize([('a',2),('d',1),('b',1)])
3. rdd3 = sc.parallelize(range(100))
4. rdd = sc.parallelize([("a",["x","y","z"]),("b" ["p","r,"])])
