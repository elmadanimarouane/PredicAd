package predicad

import api.{DataApi, PredicadApi}
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.tuning.TrainValidationSplitModel
import org.apache.spark.sql.functions.{monotonically_increasing_id, udf}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Test {

  // This method allows us to train our data by getting our pre-saved and trained model
  def testData(dataset: DataFrame, spark: SparkSession) : Unit =
    {
      // We get our model
      val model = TrainValidationSplitModel.read.load("./save/trainModel")

      // We clean our data
      val cleanData = DataApi.cleanData(dataset, spark)

      // We prepare our data by encoding it by taking the pre-saved encoding pipeline
      val encodingModel = PipelineModel.read.load("./save/encodingPipeline")
      val encodedData = encodingModel.transform(cleanData)

      // We vector it with our pre-saved VectorAssembler pipeline
      val vectorModel = PipelineModel.read.load("./save/vectorPipeline")
      val preparedData = vectorModel.transform(encodedData)

      // We create our UDF to convert prediction double to boolean
      val changeLabel = udf { value: Double =>
        if(value == 1.0) true else false
      }

      // We do our predictions
      val predict_dataset = model.transform(preparedData).select("prediction")

      // We set our final prediction dataset
      val finalPredictionDataset = predict_dataset
        .withColumn("prediction",changeLabel(predict_dataset("prediction")))
        .withColumnRenamed("prediction","Label")

      val sizeUDF = spark.udf.register("sizeUDF", PredicadApi.convertSize)
      val finalData = dataset.withColumn("size",sizeUDF(dataset("size")))

      // We fuse our results with the initial dataset
      val d1 = finalPredictionDataset.withColumn("id",monotonically_increasing_id())
      val d2 = finalData.withColumn("id",monotonically_increasing_id())
      val fullData = d1.join(d2,"id").drop("id")

      // We write it in a csv
      DataApi.saveDfToCsv(fullData,"./resultPredicad.csv")
    }

}
