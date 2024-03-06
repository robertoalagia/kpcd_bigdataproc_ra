// Databricks notebook source
// DBTITLE 1,Importar librería para trabajar con Spark
import org.apache.spark.sql.SparkSession

// COMMAND ----------

// DBTITLE 1,Crear una sesión de spark
val spark = SparkSession.builder()
      .appName("Happiest Country")
      .master("local[*]")
      .getOrCreate()

// COMMAND ----------

// DBTITLE 1,Almacenar ruta de archivo en una variable para simplificar código
val csvFile = "dbfs:/FileStore/practica/world_happiness_report_2021.csv"

// COMMAND ----------

// DBTITLE 1,Crear el data frame (df) a partir del csv de 2021
import org.apache.spark.sql.{SparkSession, DataFrame}
val df: DataFrame = spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(csvFile)

// COMMAND ----------

// DBTITLE 1,Ver esquema de metadatos del data frame
println("Esquema del DataFrame:")
df.printSchema()

// COMMAND ----------

// DBTITLE 1,1. ¿Cuál es el país más “feliz” del 2021 según la data?
import org.apache.spark.sql.functions.{desc,col, sum, sum_distinct, avg, count}

val dfHappiestCountry21 = df.groupBy("Country name","Ladder score")
    .count
    .orderBy(desc("Ladder score"))
    .show(1)

// COMMAND ----------

// DBTITLE 1,2. ¿Cuál es el país más “feliz” del 2021 por continente según la data? (asumiendo Regional indicator como continente)
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{row_number,desc,col, sum, sum_distinct, avg, count}

val dfWithRowNumber = df.withColumn("row_number", row_number().over(Window.partitionBy("Regional indicator").orderBy(desc("Ladder score"))))

val result = dfWithRowNumber
.select("Country name","Regional indicator","Ladder score")
.orderBy(desc("Ladder score"))
.filter(col("row_number") === 1)
.show()

// COMMAND ----------

// DBTITLE 1,Crear variable para segundo CSV
val csvFile2 = "dbfs:/FileStore/practica/world_happiness_report.csv"

// COMMAND ----------

// DBTITLE 1,Crear el data frame (df) a partir del csv histórico
import org.apache.spark.sql.{SparkSession, DataFrame}
val df: DataFrame = spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(csvFile2)

// COMMAND ----------

// DBTITLE 1,Crear resultado de ganador en cada año
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{row_number,desc,col, sum, sum_distinct, avg, count}

val dfWithRowNumber = df.withColumn("row_number", row_number().over(Window.partitionBy("year").orderBy(desc("Life Ladder"))))

val result = dfWithRowNumber
.select("Country name","year","Life Ladder")
.orderBy(desc("Life Ladder"))
.filter(col("row_number") === 1)

val dfResult: DataFrame = result.toDF()
dfResult.show()

// COMMAND ----------

// DBTITLE 1,Crear ranking de paises más felices (histórico)
val mostFrequentCountry = dfResult
                            .groupBy("Country name")
                            .agg(count("*").alias("Championships"))
                            .orderBy(col("Championships").desc)

val happiestCountryNameRank = mostFrequentCountry.select("Country name","Championships").show()
      

// COMMAND ----------

// DBTITLE 1,3. ¿Cuál es el país que más veces ocupó el primer lugar en todos los años?
val happiestCountryName = mostFrequentCountry.select("Country name","Championships").show(1)

// COMMAND ----------

// DBTITLE 1,Crear ranking de 2020
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{row_number,desc,col, sum, sum_distinct, avg, count}

val ranking2020 = df
                   .select("Country name","Life Ladder","Log GDP per capita","year")
                   .orderBy(desc("Life Ladder"))
                   .where(col("year") === "2020")

val dfRanking2020: DataFrame = ranking2020.toDF()
dfRanking2020.show()

// COMMAND ----------

// DBTITLE 1,4. ¿Qué puesto de Felicidad tiene el país con mayor GDP del 2020?
val dfWithRowNumber = dfRanking2020.withColumn("ranking_number", row_number().over(Window.orderBy(desc("Life Ladder"))))

val result = dfWithRowNumber
.select("ranking_number","Country name","Life Ladder","Log GDP per capita","year")
.orderBy(desc("Log GDP per capita"))

val dfResult: DataFrame = result.toDF()
dfResult.show(1)

// COMMAND ----------

// DBTITLE 1,5.1 GDP en el año 2020
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{row_number,desc,col, sum, sum_distinct, avg, count}

val ranking2020_2021 = df
                   .groupBy("year")
                   .agg(avg(col("Log GDP per capita")).as("GDP per year"))
                   .orderBy(desc("GDP per year"))
                   .where(col("year") === "2020")

val dfRanking2020_2021: DataFrame = ranking2020_2021.toDF()
dfRanking2020_2021.show()

// COMMAND ----------

// DBTITLE 1,Recuperación de los datos 2021
import org.apache.spark.sql.{SparkSession, DataFrame}
val df2021: DataFrame = spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(csvFile)

// COMMAND ----------

// DBTITLE 1,5.2 GDP en el año 2021
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{row_number,desc,col, sum, sum_distinct, avg, count}

val gdp2021 = df
                   //.groupBy("year")
                   .agg(avg(col("Log GDP per capita")).as("GDP per year"))
                   .orderBy(desc("GDP per year"))
                   //.where(col("year") === "2021")

val dfGdp2021: DataFrame = gdp2021.toDF()
dfRanking2020_2021.show()
dfGdp2021.show()

// COMMAND ----------

// DBTITLE 1,6.1 ¿Cuál es el país con mayor expectativa de vide (“Healthy life expectancy at birth”)?
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{row_number,desc,col, sum, sum_distinct, avg, count}

val rankingLife2020 = df
                   .select("Country name","Healthy life expectancy at birth","year")
                   .orderBy(desc("Healthy life expectancy at birth"))
                   .where(col("year") === "2020")

val dfRankingLife2020: DataFrame = rankingLife2020.toDF()
dfRankingLife2020.show(1)

// COMMAND ----------

// DBTITLE 1,6.2 Y ¿Cuánto tenia en ese indicador en el 2019?
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{row_number,desc,col, sum, sum_distinct, avg, count}

val japanLife2019 = df
                   .select("Country name","Healthy life expectancy at birth","year")
                   .orderBy(desc("Healthy life expectancy at birth"))
                   .where(col("year") === "2019" && col("Country name") === "Japan")

val dfJapanLife2019: DataFrame = japanLife2019.toDF()
dfJapanLife2019.show()
