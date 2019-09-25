from mimesis import locales, Person
from mimesis.enums import Gender
from pyspark import SparkContext
from pyspark.sql import SparkSession


spark = (
    SparkSession
    .builder
    .appName("TestExampleSparkNode1")
    .master("local")
    .getOrCreate()
)


