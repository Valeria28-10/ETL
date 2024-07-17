/*
chcp 65001 && spark-shell -i \spark\files\t4.scala --conf "spark.driver.extraJavaOptions=-Dfile.encoding=utf-8"
*/
import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions.{col, collect_list, concat_ws}
import org.apache.spark.sql.{DataFrame, SparkSession}
val t1 = System.currentTimeMillis()
if(1==1){
var df1 = spark.read.format("com.crealytics.spark.excel")
        .option("sheetName", "Sheet1")
        .option("useHeader", "false")
        .option("treatEmptyValuesAsNulls", "false")
        .option("inferSchema", "true").option("addColorColumns", "true")
		.option("usePlainNumberFormat","true")
        .option("startColumn", 0)
        .option("endColumn", 99)
        .option("timestampFormat", "MM-dd-yyyy HH:mm:ss")
        .option("maxRowsInMemory", 20)
        .option("excerptSize", 10)
        .option("header", "true")
        .format("excel")
        .load("/spark/files/Task4.xlsx")
		df1.show()
		df1.filter(df1("Employee_ID").isNotNull).select("Employee_ID","Job_Code")
		.write.format("jdbc").option("url","jdbc:mysql://localhost:3306/spark?user=root&password=Val_Konovalenko728410")
        .option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "tasketl4a")
        .mode("overwrite").save()
		import org.apache.spark.sql.expressions.Window
		val window1 = Window.partitionBy(lit(1)).orderBy(("id")).rowsBetween(Window.unboundedPreceding, Window.currentRow)
		var df2 = df1.withColumn("id",monotonicallyIncreasingId)
		df2.withColumn("Employee_ID", when(col("Employee_ID").isNull, last("Employee_ID", ignoreNulls = true).over(window1)).otherwise(col("Employee_ID")))
		.orderBy("id").drop("id","Job_Code","Job").dropDuplicates()
		.write.format("jdbc").option("url","jdbc:mysql://localhost:3306/spark?user=root&password=Val_Konovalenko728410")
        .option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "tasketl4b")
        .mode("overwrite").save()
		df2.drop("id", "Employee_ID","Name","City_code", "Home_city").dropDuplicates().write.format("jdbc").option("url","jdbc:mysql://localhost:3306/spark?user=root&password=Val_Konovalenko728410")
        .option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "tasketl4c")
        .mode("overwrite").save()
		val window2 = Window.partitionBy(lit(1)).orderBy(("id")).rowsBetween(Window.unboundedPreceding, Window.currentRow)
		var df3 = df1.withColumn("id",monotonicallyIncreasingId)
		df3.withColumn("Employee_ID", when(col("Employee_ID").isNull, last("Employee_ID", ignoreNulls = true).over(window1)).otherwise(col("Employee_ID")))
		.orderBy("id").drop("id", "Job_Code","Job","Home_city").dropDuplicates()
		.write.format("jdbc").option("url","jdbc:mysql://localhost:3306/spark?user=root&password=Val_Konovalenko728410")
        .option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "tasketl4b")
        .mode("overwrite").save()
		df3.drop("id", "Employee_ID","Name","City_code", "Home_city").dropDuplicates().write.format("jdbc").option("url","jdbc:mysql://localhost:3306/spark?user=root&password=Val_Konovalenko728410")
        .option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "tasketl4c")
        .mode("overwrite").save()
		df3.drop("id", "Employee_ID","Name","Job_Code","Job").dropDuplicates().write.format("jdbc").option("url","jdbc:mysql://localhost:3306/spark?user=root&password=Val_Konovalenko728410")
        .option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "tasketl4d")
        .mode("overwrite").save()
	
	println("task 1")
}
val s0 = (System.currentTimeMillis() - t1)/1000
val s = s0 % 60
val m = (s0/60) % 60
val h = (s0/60/60) % 24
println("%02d:%02d:%02d".format(h, m, s))
System.exit(0)