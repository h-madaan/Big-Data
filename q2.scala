import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import spark.implicits._
import spark.sql

sc.setLogLevel("OFF")
val sqlContext = new org.apache.spark.sql.SQLContext(sc)

val df_1 = sqlContext.read.parquet("/home/user/Documents/Datasets/flight_2008_pq.parquet")

val df_2 = df_1.select($"FlightNum", $"DepDelay".cast(IntegerType))
df_2.registerTempTable("flight_table")

val solution = sql("SELECT FlightNum, MAX(DepDelay) FROM flight_table GROUP BY FlightNum ORDER BY MAX(DepDelay) DESC LIMIT 20")
solution.show()