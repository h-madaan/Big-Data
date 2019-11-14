import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import scala.util.MurmurHash
import org.apache.spark.SparkContext._
import org.apache.spark.sql.Row
sc.setLogLevel("OFF")

// a
val df_1 = spark.read.option("header","true").csv("/home/user/Documents/Datasets/simple.csv") // creating csv textFile with headers in the first row
df_1.first() // for verifying
df_1.rdd.isEmpty // for verifying

// b
df_1.printSchema // for printing the dataframe

//c
val rows:org.apache.spark.rdd.RDD[org.apache.spark.sql.Row] = df_1.rdd // converting dataframe to RDD
df_1.collect().foreach(println) //displaying contents in RDD

//d
val textfile = sc.textFile("/home/user/spark-2.2.1-bin-hadoop2.7/README.md") // reading textfile in SparkContext

//e
val l_count = textfile.flatMap(line => line.split("")).map(char => (char,1)).reduceByKey((a,b) => a + b) // forming characters from words of a line using flatmap() functions and assigning key-value pairs and making file.
l_count.collect() // collecting characters and their counts and displaying them.

//f
 val w_count = textfile.flatMap(word => word.split(" ")).map(word => (word,1)).reduceByKey((a,b) => a + b) //founding words from line using flatmap() on each line and forming a file
 w_count.collect() // collecting words in key-value pairs and displaying
 w_count.count() // counting words

 //g
val w_count = textfile.flatMap(word => word.split(" ")).map(word => ("word",1)).reduceByKey((a,b) => a + b) // taking each word as  a single string as "word" and creating file
w_count.collect() // displaying w_count file with its count.

//h
val templist = List(1,2,3,4,5); // creating a list using List() function.
var result : Int = 1 //assigning value as 1 to variable result.
for(x <- templist){ result = result*x} // for for loop for iteration
println(result) // printing result