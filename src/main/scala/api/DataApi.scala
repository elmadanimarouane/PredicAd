package api

import java.io.File

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer, VectorAssembler}
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.Try

object DataApi {

  // This method allows us to check if a dataset contains a specific column
  def hasColumn(dataset: DataFrame, columnName: String): Boolean = Try(dataset(columnName)).isSuccess

  // This method allows us to clean our data
  def cleanData(dataset: DataFrame, spark: SparkSession) : DataFrame =
    {
      // We create our User-Defined Function
      val timestampUDF = spark.udf.register("timestampUDF", PredicadApi.timestampToHour)
      val interestsUDF = spark.udf.register("interestsUDF", PredicadApi.handleInterests)
      val networkUDF = spark.udf.register("networkUDF", PredicadApi.handleNetwork)
      val osUDF = spark.udf.register("osUDF", PredicadApi.correctOS)
      val sizeUDF = spark.udf.register("sizeUDF", PredicadApi.convertSize)

      // We clean our data and return it
      val dataWithoutLabel = dataset
        .withColumn("timestamp", timestampUDF(dataset("timestamp")))
        .withColumn("interests", interestsUDF(dataset("interests")))
        .withColumn("network", networkUDF(dataset("network")))
        .withColumn("os", osUDF(dataset("os")))
        .withColumn("size", sizeUDF(dataset("size")))
        .na.fill(Map("type" -> "Unknown"))

      // We check if our dataset has a label column. If it is the case, we clean it then return it. If not, we simply
      // return it
      if(hasColumn(dataWithoutLabel,"label"))
        {
          val labelUDF = spark.udf.register("labelUDF", PredicadApi.convertLabel)
          dataWithoutLabel
            .withColumn("label",labelUDF(dataWithoutLabel("label")))
        }
      else
        {
          dataWithoutLabel
        }
    }

  // This method is used to "one hot encode" our dataset before testing it
  def oneHotEncodingData(dataset: DataFrame): DataFrame =
  {
    // We get our columns that we use for our one hot encoding (except label)
    val columnNames = dataset.columns.filterNot(_.contains("label"))
    val encodedFeatures = columnNames.flatMap{ name =>
      // We create our indexers
      val indexers = new StringIndexer()
        .setInputCol(name)
        .setOutputCol(name + "_Index")
        .setHandleInvalid("keep")
      // We create our encoders
      val encoders = new OneHotEncoder()
        .setInputCol(name + "_Index")
        .setOutputCol(name + "_vec")
        .setDropLast(false)
      // We create our array
      Array(indexers, encoders)
    }

    // Now, we combine both in a pipeline
    val pipeline = new Pipeline().setStages(encodedFeatures)
    // We convert our cleanData with the encoded values
    val encodedDataModel = pipeline.fit(dataset)
    encodedDataModel.write.overwrite().save("./save/encodingPipeline")
    val encodedData = encodedDataModel.transform(dataset)
    // We create a "features" column that contains our encoded vector
    val vectorFeatures = encodedData.columns.filter(_.contains("_vec"))
    val vectorAssembler = new VectorAssembler()
      .setInputCols(vectorFeatures)
      .setOutputCol("features")
    // We get our final data and get rid of all the vec except the features one
    val pipelineVectorAssembly = new Pipeline().setStages(Array(vectorAssembler))
    val pipelineVectorAssemblyModel = pipelineVectorAssembly.fit(encodedData)
    pipelineVectorAssemblyModel.write.overwrite().save("./save/vectorPipeline")
    val datasetWithoutLabel = pipelineVectorAssemblyModel.transform(encodedData)
    if(hasColumn(datasetWithoutLabel,"label"))
      {
        datasetWithoutLabel
          .select("network", "appOrSite", "timestamp","size", "label","os", "publisher","user","interests",
          "type", "features")
      }
    else
      {
        datasetWithoutLabel
          .select("network", "appOrSite", "timestamp","size", "os", "publisher","user","interests",
            "type", "features")
      }
  }

  // This method allows us to re-balance our label. It will be useful when we will use our logistic regression algorithm
  def balanceLabel(dataset: DataFrame): DataFrame =
  {
    val numberFalseLabel = dataset.filter(dataset("label") === 0.0).count()
    val datasetSize = dataset.count()
    val balanceRatio = numberFalseLabel.toDouble/datasetSize
    // We create an UDF to replace our new values with the appropriate weight
    val gradeWeights = udf { value: Double =>
      if(value == 1.0) 1 * balanceRatio else 1 * (1 - balanceRatio)
    }
    // We return our balanced dataset
      dataset.withColumn("labelWeight", gradeWeights(dataset("label")))
  }

  def saveDfToCsv(df: DataFrame, nameFile: String,
                  sep: String = ",", header: Boolean = true): Unit = {
    // We create a temporary directory where we will store our raw CSV
    val tmpDir = "tmpDir"

    // We export our dataset in a csv format
    df.coalesce(1).write.
      format("com.databricks.spark.csv").
      option("header", header.toString).
      option("delimiter", sep).
      save(tmpDir)

    // We change its name so it matches the name given
    val dir = new File(tmpDir)
    val rawCvFilePath = dir.listFiles.
      filter { f => f.isFile && f.getName.endsWith(".csv") }.
      map(_.getAbsolutePath).head
    // We check if we don't have already a file with the same name. if it's the case, we delete it so we can create
    // a new one
    val csvFile = new File(nameFile)
    if(csvFile.isFile)
      {
        csvFile.delete()
      }
    new File(rawCvFilePath).renameTo(csvFile)
    // We delete our temporary directory
    dir.listFiles.foreach( f => f.delete )
    dir.delete
  }
}
