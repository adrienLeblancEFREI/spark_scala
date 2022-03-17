package fr.data.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import scala.io.Source


object DataFrame {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master( "local[*]" )
      .appName( "Spark-SQL" )
      .getOrCreate()

    val df = spark.read
      .option("header","true")
      .option("delimiter", ";")
      .csv("src/main/resources/codesPostaux.csv")
    df.show(5)

    df.agg(countDistinct("Code_commune_INSEE").as("nb de communes")).show()

    df.filter("Ligne_5 is not null" )
      .agg(countDistinct("Code_commune_INSEE").as("nb de communes"))
      .show()

    val dfNumDep = df.withColumn("Numero_departement",
      when(col("Code_postal")
        .like("97%"), col("Code_postal").substr(0,3))
        .otherwise(col("Code_postal").substr(0,2)))
    dfNumDep.show(5)

    val dfNumOT = df.filter(col("Code_postal").like("97%"))
    dfNumOT.show(5)


    val dfSave = dfNumDep.select("Code_commune_INSEE","Nom_commune","Code_postal","Numero_departement")
      .orderBy("Code_postal")
    dfSave.show()

    dfSave.write
      .option("header","true")
      .option("delimiter",";")
      .csv("src/main/resources/extract")

    val df6 = dfNumDep.filter("Numero_departement = 02").select(countDistinct("Nom_commune"))
    df6.show(truncate = false)

    val df7 = dfNumDep.select("Numero_departement","Nom_commune").distinct().groupBy("Numero_departement").count().sort(desc("count"))
    df7.show()

    spark.close()
  }

}
