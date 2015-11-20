package hw_6

import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by Asher
 */
object RandomForestScala {

  def main(args: Array[String]) {

    val conf: SparkConf = new SparkConf(true)
                                  .setAppName("RandomForestScala")
                                  .setMaster("local")

    val sparkContext: SparkContext = new SparkContext(conf)
    val sqlContext: SQLContext = new SQLContext(sparkContext)

    //load dataset
    val rdd: RDD[LabeledPoint] = MLUtils.loadLabeledPoints(sparkContext, "D:\\Development\\BigDataAnalytics\\FXDataSample\\hw2_dataset_no_header.txt")
    val df: DataFrame = sqlContext.createDataFrame(rdd)

    //index labels and apply this Estimator on dataset
    val labelIndexer = new StringIndexer()
                                  .setInputCol("label")
                                  .setOutputCol("directionalityLabel")
                                  .fit(df)

    //parse feature vector (apply parsing as Estimator)
    val featureIndexer = new VectorIndexer()
                              .setInputCol("features")
                              .setOutputCol("indexedFeatures")
                              .fit(df)

    //split into train/test
    val Array(trainingData, testData) = df.randomSplit(Array(0.7, 0.3))

    //train a RandomForest
    val rf: RandomForestClassifier = new RandomForestClassifier()
                                              .setLabelCol("directionalityLabel")
                                              .setFeaturesCol("indexedFeatures")
                                              .setNumTrees(Integer.valueOf(args(0)))

    //retrieve original labels
    val labelConverter: IndexToString = new IndexToString()
                                                .setInputCol("prediction")
                                                .setOutputCol("predictedLabel")
                                                .setLabels(labelIndexer.labels)

    // Chain indexers and forest in a Pipeline
    val pipeline: Pipeline = new Pipeline()
                                    .setStages(Array(labelIndexer, featureIndexer, rf, labelConverter))

    // Train model.  This also runs the indexers.
    val model: PipelineModel = pipeline.fit(trainingData)

    // Make predictions.
    val predictions = model.transform(testData)

    // Select (prediction, true label) and compute test error
    val evaluator: MulticlassClassificationEvaluator = new MulticlassClassificationEvaluator()
                                                                      .setLabelCol("indexedLabel")
                                                                      .setPredictionCol("prediction")
                                                                      .setMetricName("precision")
    //print stats
    val accuracy = evaluator.evaluate(predictions)
    println("accuracy = " + accuracy)

    //print model
    val rfModel: RandomForestClassificationModel = model.stages(2).asInstanceOf[RandomForestClassificationModel]
    println("Learned classification forest model:\n" + rfModel.toDebugString)
  }
}
