/* SimpleAppLocal.scala */
import java.util.Scanner
import java.util.Properties
import scala.collection.JavaConversions._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import java.io.FileReader
import java.io.FileNotFoundException
import java.io.IOException
import Array._
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.Rating
//import org.apache.spark.sql.SparkSession
//import org.apache.spark.sql.functions._
//import org.apache.spark.sql._
//import org.apache.spark.sql.types._

    	
object SimpleAppLocal {
		       case class Trans(cardID: String, item: Long, Amount: Long, Date: String)
	def main(args: Array[String]): Unit = {
		try {
	     val sc = new SparkContext("local", "Simple Application")

		 	println("Please enter card ID:")
		    var userInput = readLine()
	       	println("Your input:" + userInput)
	    	
	    	val emit = new Emitter
	        val dataFeed = emit.generateFeed(userInput) 
		
	        val dataRdd = sc.parallelize(dataFeed)
	
		       println("List of all transactions")
		    dataRdd.map{
		        line=>{val parts = line.split(",")
		        ("Card ID:"+parts(0)+" Item ID:"+parts(1)+" Amount:"+parts(2).toFloat+" Date:"+parts(3))}
	     		}.sortBy(col => col,true).foreach(println)
		    
		    println("Sum up all the transactions")
		 	    dataRdd.map{
	      line => {val parts = line.split(",")
	        ("Card ID:"+parts(0),  parts(2).toFloat)
	        }
	    }.reduceByKey((tran1,tran2) => tran1 + tran2).sortBy(col=>col._1).foreach(println)
	   
	    // Change RDD to DataFrame format
	      println("Change RDD to DataFrame format")
	    	   	val sqlContext = new org.apache.spark.sql.SQLContext(sc)
		       import sqlContext.implicits._
		       val dataDF = dataRdd.map(_.split(",")).map(t => Trans(t(0), t(1).trim.toLong, t(2).trim.toLong, t(3))).toDF()
		    dataDF.show
		    
	    dataRdd.saveAsTextFile("./src/output")
	    
// Machine Learning section, use ALS algorithm (Alternating Least Squares)    
// Create the ALS model by train method, to predict user rating on items based on existing ratings of another user	    	    
// input file has 3 columns -- user, item, rating	    
	    val rawRatingStr = sc.textFile("./src/item_rating_input.txt")
	    val rawRating = rawRatingStr.map(_.split(",").take(3))
	    val RatingRDD = rawRating.map{case Array(user,item,rating) => Rating(user.toInt, item.toInt, rating.toDouble)}

// RatingRDD is rating object
// no. of hidden factors to predict user rating, e.g. sex, age, income
// no. of iterations of ALS to run	    
	    println("Predict user ratings on items")
	    val myModel = ALS.train(RatingRDD, 5, 5)

	    println("Find list of 5 items for user 3")
	    println(myModel.recommendProducts(3,5).mkString("\n"))
	    
	    println("Find the rating of item 1 by user 3")
	    println(myModel.predict(3,1))
	    
	    println("Find 1 users who may like item 5 with rating")
	    println(myModel.recommendUsers(5,1).mkString("\n"))
	
		}
		catch {
		  case ex:FileNotFoundException => {
		    println("Missing file exception")
		    }
		  case ex:IOException => {
		    println("IO exception")
		    }
		 } finally {
		   println("Exit finally...")
		 }
	
	}
}