package predicad

import api.DataApi
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Train {

  def trainData(dataset: DataFrame, spark: SparkSession) : Unit =
    {
      // We clean our data
      val cleanData = DataApi.cleanData(dataset, spark)

      // We one hot encode our dataset
      val finalData = DataApi.oneHotEncodingData(cleanData)

      // We balance it
      val balancedData = DataApi.balanceLabel(finalData)

      // We initialize our Logistic Regression model
      val lr = new LogisticRegression().setWeightCol("labelWeight").setLabelCol("label").setFeaturesCol("features")
        .setMaxIter(10)

      // We implement hyper-parameter tuning
      val paramGrid  = new ParamGridBuilder()
        .addGrid(lr.aggregationDepth,Array(2,5,10))
        .addGrid(lr.elasticNetParam,Array(0.0,0.5,1.0))
        .addGrid(lr.fitIntercept,Array(false,true))
        .addGrid(lr.maxIter,Array(1,10,100))
        .addGrid(lr.regParam, Array(0.01,0.5,0.2))
        .build()

      // We choose trainValidationSplit since it is cheaper in time than a CrossValidator
      val trainValidationSplit = new TrainValidationSplit()
        .setEstimator(lr)
        .setEvaluator(new BinaryClassificationEvaluator)
        .setEstimatorParamMaps(paramGrid)
        // 80% of the data will be used for training and the remaining 20% for validation.
        .setTrainRatio(0.8)
        // Evaluate up to 3 parameter settings in parallel
        .setParallelism(3)

      // We train our dataset
      val model = trainValidationSplit.fit(balancedData)

      // We save our model so we can use it later
      model.write.overwrite().save("./save/trainModel")
    }
}
