/*
chcp 65001 && spark-shell -i \spark\files\t2.scala --conf "spark.driver.extraJavaOptions=-Dfile.encoding=utf-8"
*/
import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions.{col, collect_list, concat_ws}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
val t1 = System.currentTimeMillis()
if(1==1){
	var df = spark.read.option("delimiter",",")
		.option("header", "true")
		.csv("C:/spark/files/fifa_s2.csv")

	val df1 = df.drop("Potential")      // Удаляем дублирующуся колонку	
		df1.write.format("jdbc").option("url", "jdbc:mysql://localhost:3306/spark?user=root&password=Val_Konovalenko728410")
			.option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "tasketl2aa")
			.mode("overwrite").save()
		df1.show()

	val columns_null = df1.select(df1.columns.map(c => count(when(col(c).isNull || col(c) === "" || col(c).isNaN, c)).alias(c)): _*)
		columns_null.show()
			
	val df2 = df1.na.drop()
		df2.write.format("jdbc").option("url", "jdbc:mysql://localhost:3306/spark?user=root&password=Val_Konovalenko728410")
			.option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "tasketl2b")
			.mode("overwrite").save()
		df2.show()

	val columns = df2.columns.map(c => sum(col(c).isNull.cast("integer")).alias(c))
	val dataf = df2.agg(columns.head, columns.tail:_*)
	val t = dataf.columns.map(c => dataf.select(lit(c).alias("col_name"), col(c).alias("null_count")))
	val dataf_agg_col = t.reduce((df2, dataf) => df2.union(dataf))
		dataf_agg_col.show()

	val df3 = df2
		.withColumn("ID",col("ID").cast("int"))
		.withColumn("Age",col("Age").cast("int"))
		.withColumn("Overall",col("Overall").cast("int"))
		.withColumn("Value",col("Value").cast("int"))
		.withColumn("Wage",col("Wage").cast("int"))
		.withColumn("International Reputation",col("International Reputation").cast("int"))
		.withColumn("Skill Moves",col("Skill Moves").cast("int"))
		.withColumn("Joined",col("Joined").cast("int"))
		.withColumn("Weight",col("Weight").cast("int"))
		.withColumn("Height",col("Height").cast("float"))
		.withColumn("Release Clause",col("Release Clause").cast("float"))	
		.withColumn("Name", lower(col("Name"))) 	    // Переводим в нижний регистр				
		.withColumn("Nationality", lower(col("Nationality"))) 		
		.withColumn("Club", lower(col("Club"))) 					
		.withColumn("Preferred Foot", lower(col("Preferred Foot"))) 
		.withColumn("Position", lower(col("Position")))
		.dropDuplicates()
		df3.write.format("jdbc").option("url", "jdbc:mysql://localhost:3306/spark?user=root&password=Val_Konovalenko728410")
			.option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "tasketl2c")
			.mode("overwrite").save()
		df3.show()
			
	val df4 = df3 
		.withColumn("Category", when(col("Age") < 20, "0-19") 
		.when(col("Age") >= 20 && col("Age") < 30, "20-29") 
		.when(col("Age") >= 30 && col("Age") < 36, "30-35") 
		.otherwise("36+"))

	val agePlayerSum = df4.groupBy("Category").count().toDF("Age Player Group", "Total Players") 
		agePlayerSum.show()

		df4.write.format("jdbc").option("url", "jdbc:mysql://localhost:3306/spark?user=root&password=Val_Konovalenko728410") 
			.option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "tasketl2d") 
			.mode("overwrite").save() 
		df4.show()
		
	println("Task 2")
}

val s0 = (System.currentTimeMillis() - t1)/1000
val s = s0 % 60
val m = (s0/60) % 60
val h = (s0/60/60) % 24
println("%02d:%02d:%02d".format(h, m, s))
System.exit(0)