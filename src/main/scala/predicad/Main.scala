package predicad

import java.io.File

import api.DataApi
import org.apache.spark.sql.{DataFrame, SparkSession}

object Main extends App {

  // We check that we have indeed a file given
  if(args.length == 0)
    {
      println("No test data given in parameter")
    }
  else
    {
      // We get the input JSON
      val inputJson = args(0)
      // We check that it is indeed a JSON file
      if(new File(inputJson).isFile && new File(inputJson).getName.endsWith(".json"))
        {
          // We initiate our SparkSession
          val spark = SparkSession
            .builder()
            .appName("PredicAd")
            .master("local")
            .getOrCreate()

          // We read our JSON
          val rawData = spark.read.json(inputJson).cache()

          // We check if we have our model saved
          val modelDir = new File("./save")
          // We start our "watch" to check the execution time
          val t0 = System.nanoTime()
          if(modelDir.isDirectory)
            {
              // We do the test
              doTest(rawData,spark)
            }
            // If we don't have our model saved, we need to recreate one. Note : This can take a long time
          else
            {
              // We check that the training data is indeed a json and does exist
              if(args.length == 2 && new File(args(1)).isFile && new File(args(1)).getName.endsWith(".json"))
                {
                  // We read our training dataset
                  val rawTrainingData = spark.read.json(args(1))
                    .select("network", "appOrSite", "timestamp", "size", "label", "os", "publisher", "user",
                      "interests", "type").cache()
                  // We do our training
                  Train.trainData(rawTrainingData,spark)
                  // We do our test
                  doTest(rawData,spark)
                }
              else
                {
                  println("The pre-saved model does not exist and no training data was given")
                }
            }
          // Elapse time
          val t1 = System.nanoTime()
          println("Elapsed time: " + ((t1 - t0)/1000000000) + " seconds")

          // We close our session
          spark.close()
        }
      else
        {
          println("The test file given does not exist or is not a JSON")
    }
  }

  // This method allows us to do a test
  def doTest(data: DataFrame, spark: SparkSession) : Unit =
    {
      if(DataApi.hasColumn(data,"label"))
        {
          Test.testData(data.drop("label"),spark)
        }
      else
        {
          Test.testData(data,spark)
        }
    }
}
