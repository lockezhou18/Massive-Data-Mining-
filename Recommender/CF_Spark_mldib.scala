package hw3
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import java.io.File
import scala.collection.mutable.HashMap;
object ImpTask1 {
  
  val conf = new SparkConf().setAppName("task1").setMaster("local");
  val sc = new SparkContext(conf);
  
  def main(args:Array[String])
  {
     val t1 = System.nanoTime();
     val sets = cleanData(args(0),args(1));
     println("starting prediction");
     val trainSet = sets._3;
     //trainSet.saveAsTextFile("c://553//train")
     val testSet = sets._1;
     val testSetWithRate = sets._2;
     val rank =4;
     val numIterations =10;
     val lamda = 0.01;
     val model = this.train(trainSet, rank, numIterations, lamda);
     
     val predictions1 =this.test(testSet, model);
     //predictions1.saveAsTextFile("c://553//t2R.txt");
      //val miss = testSet.subtract(predictions1.map(line =>(line.user,line.product)))
      //.saveAsTextFile("c://553//miss2");
     
      val meanRate = trainSet.map{line =>line.rating}.mean();
      val miss = testSet.subtract(predictions1.map(line =>(line.user,line.product)))
      val missWithMean = miss.map{line => (line,meanRate)}
      
     val predictions = predictions1.map{case Rating(user,movie,rating) =>
       {
         if (rating<=5)
       ((user,movie),rating)
       else
        ((user,movie),5.0) }}.union(missWithMean)
     
     //write to file
     this.writeToFile(predictions);
     
     val preAndReal = predictions.join(testSetWithRate)
     val preAndReal2 = preAndReal.map(line => (line._1,Math.abs(line._2._1-line._2._2)))
     val MSE = preAndReal.map { case ((user, product), (r1, r2)) =>
        val err = (r1 - r2)
         err * err
      }.mean()
      val RMSE = Math.sqrt(MSE)
      
     
     val duration = (System.nanoTime() - t1)/1e9d;
  println(this.collectResult(preAndReal2.map{line => Rating(line._1._1,line._1._2,line._2)}));
  println("Rooted Mean Squar Error = " + RMSE)
  println("total running time"+duration+"s");
  }
  
  def cleanData(test:String,ratings:String):(RDD[(Int,Int)],RDD[((Int,Int),Double)],RDD[Rating]) =
  {
    val testSet = sc.textFile(new File(test).toString).filter(line => line !="userId,movieId").map{
         line =>{
           
           val fields = line.split(",")
           //userId,movieId
             (fields(0).toInt,fields(1).toInt)
         }
    }
    
    val wholeSet = sc.textFile(ratings).filter(line => line !="userId,movieId,rating,timestamp").map{
      line=>{
        val fields = line.split(",")
        //userId,movieId,rating
       ((fields(0).toInt,fields(1).toInt),fields(2).toDouble)
      }
    }
    val mapWhole= wholeSet.collectAsMap();
    println("total ratings::"+wholeSet.collect().size);
    
    val tc = testSet.collect()
    
    val testSetWithRating = testSet.map(line => (line,mapWhole(line)));
    /*
    val testSetWithRating = wholeSet.filter(
      line =>{
        tc.contains(line._1); 
      }
    )
    */
    /*
    val trainSet1 = wholeSet.filter(
        line =>{
          ! tc.contains((line._1,line._2))
        }  
    );
    * */
    val trainSet1 = wholeSet.subtract(testSetWithRating);
    println("train set size::"+trainSet1.count());
    println("test set size::"+testSetWithRating.count());
 
    
    val trainSet = trainSet1.map(
      line => Rating(line._1._1,line._1._2,line._2)    
    )
    return (testSet,testSetWithRating,trainSet)
    
    
  }
  
  def train(ratings:RDD[Rating], rank:Int, numIterations:Int, lamda:Double):MatrixFactorizationModel =
  {
    val model = ALS.train(ratings, rank, numIterations, lamda)
      return model
  }
  
  
  
  // return the test set with ratings
  def test(testSet:RDD[(Int,Int)],model:MatrixFactorizationModel):RDD[Rating]=
  {
      model.predict(testSet)
  }
  
  def setParam(){
    
  }
  
  def collectResult(predictions:RDD[Rating]):String = {
    var count0 = 0;
    var count1 = 0;
    var count2 = 0;
    var count3 = 0;
    var count4 = 0;
    var count5 = 0;
    var count6 = 0
    
    /*val results = 
      predictions.foreach{
       
      case Rating(user,movie,rate) =>
        {
           rate match{
             
             case  i if (i<0) => count0+=1;
             case  i if (i>= 0 && i <1) => count1+=1;
             case  i if (i>= 1 && i <2) => count2+=1;
             case  i if (i>= 2 && i <3) => count3+=1;
             case  i if (i>= 3 && i <4) => count4+=1;
             case  i if (i>= 4 && i <5) => count5+=1;
             case  i if (i>5) => count6+=1;
           }
          
        }
      println("<0: "+count0+"\n"+">=0 and <1: "+count1+"\n"+">=1 and <2: "+count2.toString+"\n"+">=2 and <3: "+count3+"\n"+">=3 and <4: "+count4+"\n"+">=4 and <5 "+count5+"\n"+">5 "+count5+"\n");
    }
    * */
    
   val temp = predictions.collect();
    for(rate<-temp)
    {
        val num = rate.rating.toDouble
        if(num<0)
          count0+=1;
        if(num>=0 && num<1)
          count1+=1;
        if(num>=1 && num<2)
          count2+=1;
         if(num>=2 && num<3)
          count3+=1;
          if(num>=3 && num<4)
          count4+=1;
           if(num>=4 && num<=5)
          count5+=1; 
           if(num>5)
          count6+=1;
       
        
        
    }
    
    
   //return  "<0: "+count0+"\n"+">=0 and <1: "+count1+"\n"+">=1 and <2: "+count2.toString+"\n"+">=2 and <3: "+count3+"\n"+">=3 and <4: "+count4+"\n"+">=4 and <5 "+count5+"\n"+">5 "+count6+"\n"
     return  "<0: "+count0+"\n"+">=0 and <1: "+count1+"\n"+">=1 and <2: "+count2.toString+"\n"+">=2 and <3: "+count3+"\n"+">=3 and <4: "+count4+"\n"+">=4: "+(count5+count6)+"\n"
   //return count0.toString      
  }
  
  def writeToFile(testResults:RDD[((Int,Int),Double)]) = {
    val sorted = testResults.sortByKey(true).collect();
    val resultFile = new java.io.File("task1_result.txt");
    val bufw = new java.io.BufferedWriter(new java.io.FileWriter(resultFile));
    
    bufw.write("UserId,MovieId,Pred_rating");
    bufw.newLine();
    sorted.foreach{
      case ((user,movie),rate) => {
        bufw.write(user+","+movie+","+rate);
        bufw.newLine();
      }
    }
        
    bufw.close();
    
  }
}