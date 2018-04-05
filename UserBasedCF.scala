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
import scala.collection.mutable.ArrayBuffer;
object ImpTask2 {
  
  
  val conf = new SparkConf().setAppName("task2").setMaster("local");
  val sc = new SparkContext(conf);
  
  
  def main(args:Array[String])
  {
    /*
    val t1 = System.nanoTime();
    val set = this.cleanData("c://553//testing_small.csv","c://553//2//ratings.csv");
    val trainSet = set._3
    val testSet = set._1
    val testWithRate = set._2;
    
    val corespondNum = this.hashNum(trainSet);
    val matrix = this.utiMatrix(trainSet, corespondNum._1, corespondNum._2);
    val coMatrix = this.getCorrelationMatrix(matrix);
    println("user number"+corespondNum._1.size);
     println("movie number"+corespondNum._2.size);
    
     println("coMatrix size"+coMatrix.size)
     /*
     for(i<- 0 until 671)
     {
       println("");
       for (j<- 0 until 671)
       {
         print(coMatrix(i)(j)+",");
       }
       }
       */
     val co_rated = this.findCoRated(matrix(1), matrix(2));
     
      
     
     println("avg" +avgRating(matrix(1)));
     println("matrix 1 size::"+matrix(1).filter(i=>i!=0).size)
     println("matrix 2 size::"+matrix(2).filter(i=>i!=0).size);
     println("matrix(1)::"+matrix(1).filter(i=>i!=0).toList)
     println("matrix(2)::"+matrix(2).filter(i=>(i!=0)).toList)
     
     println("correlation user 4 and user 3::"+coMatrix(4)(3))
       println("correlation user 3 and user 4::"+coMatrix(3)(4))  
       val u1 = corespondNum._1(220)
       val i1 = corespondNum._2(2482)
       
       println("predict for 220,2582 "+this.predictOne(u1, i1, matrix, coMatrix));
     	*/
        /*
        // this part is testing phase and outout nan
       val meanRate = trainSet.map{line =>line._2}.mean();
       val predictAll = testSet.map{
         case (user,movie) =>{
           //val u1 = corespondNum._1(user)
          // val i1 = corespondNum._2(movie)
           if(corespondNum._1.contains(user) && corespondNum._2.contains(movie))
           {
             val u1 = corespondNum._1(user)
             val i1 = corespondNum._2(movie)
             val predictRate = this.predictOne(u1, i1, matrix, coMatrix)
             ((user,movie),predictRate)
           }
           
           else if (corespondNum._1.contains(user) && !corespondNum._2.contains(movie))
           {
             val u1 = corespondNum._1(user)
             val meanUser = this.avgRating(matrix(u1))
             ((user,movie),meanUser)
           }
           
           else if (!corespondNum._1.contains(user) && corespondNum._2.contains(movie))
           {
             val i1 = corespondNum._2(movie)
             val meanMovie = this.avgRating(matrix.map(innerArray => innerArray(i1)))
             ((user,movie),meanMovie)
           }
           
           else // if it is missed, then return average rating of all users 
             ((user,movie),meanRate)
         }
       }
     
     
       /*
     val nan =  predictAll.filter{
       
       case((user,movie),rate) =>{
          rate.isNaN()
       }
     }
       
     nan.collect().foreach(i => println("nan value::"+i));
     */
    
     
     
      //this part is output result
     val preAndReal = predictAll.join(testWithRate)
     val preAndReal2 = preAndReal.map(line => (line._1,Math.abs(line._2._1-line._2._2)))
     val biggerThan4 = preAndReal2.filter(x => x._2>=4)
     
     biggerThan4.collect.foreach(x => println("bigger than 4 " + x));
     
     val MSE = preAndReal.map { case ((user, product), (r1, r2)) =>
        val err = (r1 - r2)
         err * err
      }.mean()
      val RMSE = Math.sqrt(MSE)
      val duration = (System.nanoTime() - t1)/1e9d;
      println(this.collectResult(preAndReal2.map{line => Rating(line._1._1,line._1._2,line._2)}));
      println("MSE::"+MSE);
      println("RMSE::"+RMSE);
      println("duration::"+duration);
      */
      //this.testOne;
      this.run(args(0),args(1));
  }
  
  def testOne = {
    
     val t1 = System.nanoTime();
    val set = this.cleanData("c://553//testing_small.csv","c://553//2//ratings.csv");
    val trainSet = set._3
    val testSet = set._1
    val testWithRate = set._2;
    
    val corespondNum = this.hashNum(trainSet);
    val matrix = this.utiMatrix(trainSet, corespondNum._1, corespondNum._2);
    val coMatrix = this.getCorrelationMatrix(matrix);
    println("user number"+corespondNum._1.size);
     println("movie number"+corespondNum._2.size);
    
     println("coMatrix size"+coMatrix.size)
     /*
     for(i<- 0 until 671)
     {
       println("");
       for (j<- 0 until 671)
       {
         print(coMatrix(i)(j)+",");
       }
       }
       */
     val co_rated = this.findCoRated(matrix(1), matrix(2));
     
      
     
     println("avg" +avgRating(matrix(1)));
     println("matrix 1 size::"+matrix(1).filter(i=>i!=0).size)
     println("matrix 2 size::"+matrix(2).filter(i=>i!=0).size);
     println("matrix(1)::"+matrix(1).filter(i=>i!=0).toList)
     println("matrix(2)::"+matrix(2).filter(i=>(i!=0)).toList)
     
     println("correlation user 4 and user 3::"+coMatrix(4)(3))
       println("correlation user 3 and user 4::"+coMatrix(3)(4))  
       val u1 = corespondNum._1(232)
       val i1 = corespondNum._2(2285)
       
       println("predict for 609,3892 "+this.predictOne(u1, i1, matrix, coMatrix));
  }
  
  def run(args0:String,args1:String) = {
    val t1 = System.nanoTime();
    val set = this.cleanData(args0,args1);
    val trainSet = set._3
    val testSet = set._1
    val testWithRate = set._2;
    
    val corespondNum = this.hashNum(trainSet);
    val matrix = this.utiMatrix(trainSet, corespondNum._1, corespondNum._2);
    val coMatrix = this.getCorrelationMatrix(matrix);
    
       // this part is testing phase and outout nan
       val meanRate = trainSet.map{line =>line._2}.mean();
       val predictAll = testSet.map{
         case (user,movie) =>{
           //val u1 = corespondNum._1(user)
          // val i1 = corespondNum._2(movie)
           if(corespondNum._1.contains(user) && corespondNum._2.contains(movie))
           {
             val u1 = corespondNum._1(user)
             val i1 = corespondNum._2(movie)
             val predictRate = this.predictOne(u1, i1, matrix, coMatrix)
             ((user,movie),predictRate)
           }
           
           else if (corespondNum._1.contains(user) && !corespondNum._2.contains(movie))
           {
             val u1 = corespondNum._1(user)
             val meanUser = this.avgRating(matrix(u1))
             ((user,movie),meanUser)
           }
           
           else if (!corespondNum._1.contains(user) && corespondNum._2.contains(movie))
           {
             val i1 = corespondNum._2(movie)
             val meanMovie = this.avgRating(matrix.map(innerArray => innerArray(i1)))
             ((user,movie),meanMovie)
           }
           
           else // if it is missed, then return average rating of all users 
             ((user,movie),meanRate)
         }
       }
     
       //write to file
       this.writeToFile(predictAll);
     
       /*
     val nan =  predictAll.filter{
       
       case((user,movie),rate) =>{
          rate.isNaN()
       }
     }
       
     nan.collect().foreach(i => println("nan value::"+i));
     */
    
     
     
      //this part is output result
     val preAndReal = predictAll.join(testWithRate)
     val preAndReal2 = preAndReal.map(line => (line._1,Math.abs(line._2._1-line._2._2)))
    // val biggerThan4 = preAndReal2.filter(x => x._2>=4)
     
    // biggerThan4.collect.foreach(x => println("bigger than 4 " + x));
     
     val MSE = preAndReal.map { case ((user, product), (r1, r2)) =>
        val err = (r1 - r2)
         err * err
      }.mean()
      val RMSE = Math.sqrt(MSE)
      val duration = (System.nanoTime() - t1)/1e9d;
      println(this.collectResult(preAndReal2.map{line => Rating(line._1._1,line._1._2,line._2)}));
      //println("MSE::"+MSE);
      println("RMSE::"+RMSE);
      println("duration::"+duration+"s");
      
  }
  
  
  def avgRating(userRating:Array[Double]):Double = 
  {
    /*
    val k = userRating.filter(x => x!=0);
    val sum = k.sum;
    println("sum:"+sum);
    val num = k.size;
    println("size:"+num);
    val ave = sum/num;//this method can be improved by fold
    //println("ave:"+ave);
    */
    val avg = userRating.filter(x => x!=0).foldLeft((0.0, 1)) ((acc, i) => ((acc._1 + (i - acc._1) / acc._2), acc._2 + 1))._1
    //userRating.map{}
    return avg;
  }
  
  def findCoRated(a:Array[Double],b:Array[Double]):(Array[Double],Array[Double])= {
    val a1 = new ArrayBuffer[Double]
    val b1 = new ArrayBuffer[Double]
    for(i<- 0 until a.length)
    {
       if((a(i)!=0) && (b(i)!=0))
       {
         a1.+=(a(i))
         b1.+=(b(i))
       }
    }
    //println("matrix 1 co rated::"+a1.toList);
    //println("matrix 2 co rated::"+b1.toList);
    return (a1.toArray,b1.toArray);
  }
  
  def getCorrelation(a_0:Array[Double],b_0:Array[Double]):Double = {
    
    val co_rated = this.findCoRated(a_0, b_0)
    val a = co_rated._1;
    val b = co_rated._2;
     if (a.size == 0)
            return 0;
     
     else
     {
            val mean_a = avgRating(a);
            val mean_b = avgRating(b);
            
            var numerator = 0.0;
            var denomiator_a = 0.0;
            var denomiator_b = 0.0;
            
            for(i<-0 until a.length)
            {
              numerator+=(a(i)-mean_a)*(b(i)-mean_b);
              denomiator_a +=(a(i)-mean_a)*(a(i)-mean_a);
              denomiator_b +=(b(i)-mean_b)*(b(i)-mean_b)
            }
            
            if(denomiator_a!=0 && denomiator_b!=0){
            val correlation = numerator/((Math.sqrt(denomiator_a))*(Math.sqrt(denomiator_b)))
            return correlation
            }
            else
              return 0;
     }
  }
  
  def getCorrelationMatrix(trainMatrix:Array[Array[Double]]):Array[Array[Double]] = {
    val size = trainMatrix.length;
    val table = Array.ofDim[Double](size,size);
    
    for(i<- 0 until size)
    {
      for(j<- 0 until size)
      {
          table(i)(j) = this.getCorrelation(trainMatrix(i), trainMatrix(j))
      }
    }
    
    return table;
    
  }
  
  def filterTop(weights:Array[(Int,Double,Double)],numNeighbor:Int):Array[(Int,Double,Double)] = {
    val temp1 = weights.sortWith((x,y)=>x._3>y._3);

    if (numNeighbor<=weights.size)      
    {
      val newWeights =  temp1.dropRight(weights.size - numNeighbor);
      return newWeights;
    }
    else 
      return weights;
  }
  // find co rated user except the user itself
  def findCoRatedUserAndWeight(user:Int,movie:Int,utiMatrix:Array[Array[Double]],coMatrix:Array[Array[Double]]):(Array[Int],HashMap[Int,Double],Array[(Int,Double,Double)]) = {
    val co_rated_user = new ArrayBuffer[Int];
    val couserWithRate = new HashMap[Int,Double]
    val weight = new ArrayBuffer[(Int,Double,Double)]
    val row = utiMatrix.map{innerArray => innerArray(movie)}
    for(i<- 0 until row.length)
    {
      if(row(i)!=0 && i!=user)
      {
        co_rated_user.+=(i)
        couserWithRate.+=((i,row(i)))
        weight.+=((i,row(i),coMatrix(user)(i)))
      }
    }
   // if(co_rated_user.contains(user))
     // co_rated_user.-(user);
    return (co_rated_user.toArray,couserWithRate,weight.toArray);
  }
  
  def predictOne(user:Int,movie:Int,utiMatrix:Array[Array[Double]],coMatrix:Array[Array[Double]]):Double = {
    var predictRate = 0.0;
    val couser_rate_weight = this.findCoRatedUserAndWeight(user, movie, utiMatrix, coMatrix)._3;
    val topN = this.filterTop(couser_rate_weight, 15)
    val user_mean = this.avgRating(utiMatrix(user));
    var nominator = 0.0;
    var denominator = 0.0;
    /*
    for(i<-0 until couser_rate_weight.size)
    {
      var mean = 0.0;
      couser_rate_weight(i) match{
        case (user,rate,weight) =>{
          mean = this.avgRating(utiMatrix(user))
          nominator += (rate-mean)*weight;
          denominator += weight;
          }
      }
    }
    * 
    */
    
    topN.foreach{case (user,rate,weight)=>
      {
        println("user::"+user+",rate::"+rate+",weight::"+weight);
        var mean = 0.0;
         mean = this.avgRating(utiMatrix(user))
        // println("user::"+user+", mean::"+mean);
         
          nominator += (rate-mean)*weight;
          //println("nominator::"+ nominator);
          denominator += Math.abs(weight);
          //println("denominator::"+nominator);
      }
    }
    val mean_user_prediction = this.avgRating(utiMatrix(user));
    //println("mean_user_prediction::"+mean_user_prediction+", nominator::"+nominator+", denominator::"+denominator);
    
     denominator = BigDecimal(denominator).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
    if(denominator == 0) // if no user is related to this user, return mean rate of this user
      predictRate = mean_user_prediction;
    else
      predictRate = mean_user_prediction + (nominator/denominator);
     
     if(predictRate >= 5)
       predictRate = 5;
    return predictRate;
    
  }
  
  
  def hashNum(trainSet:RDD[((Int,Int),Double)]):(HashMap[Int,Int],HashMap[Int,Int])=
  {
    val movieHash = new HashMap[Int,Int];
    val userHash = new HashMap[Int,Int];
    var count1 = 0;
    var count2 = 0;
    trainSet.collect.foreach{
      case ((user,movie),rating)=>
        {
          if(!userHash.contains(user))
          {
            userHash.+=((user,count1));
            count1+=1;
          }
          
          if(!movieHash.contains(movie))
          {
            movieHash.+=((movie,count2));
            count2+=1;
          }
        }
      
    }
    return (userHash,movieHash)
  }
  
  
  def utiMatrix(trainSet:RDD[((Int,Int),Double)],userHash:HashMap[Int,Int],movieHash:HashMap[Int,Int]):Array[Array[Double]]=
  {
      val numUser = userHash.size;
      val numMovie = movieHash.size;
      
      val matrix = Array.ofDim[Double](numUser,numMovie);
      
      trainSet.collect.foreach{
        
        case ((user,movie),rating)=>
          {
              var userCon = userHash(user);
              var movieCon = movieHash(movie);
              
              matrix(userCon)(movieCon) = rating;
          }  
      }
      
      return matrix;
      
  }
  
  
  
  
   def cleanData(test:String,ratings:String):(RDD[(Int,Int)],RDD[((Int,Int),Double)],RDD[((Int,Int),Double)]) =
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
    return (testSet,testSetWithRating,trainSet1)
    
    
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
    
    
  // return  "<0: "+count0+"\n"+">=0 and <1: "+count1+"\n"+">=1 and <2: "+count2.toString+"\n"+">=2 and <3: "+count3+"\n"+">=3 and <4: "+count4+"\n"+">=4 and <5 "+count5+"\n"+">5 "+count6+"\n"
    //return count0.toString      
    return  "<0: "+count0+"\n"+">=0 and <1: "+count1+"\n"+">=1 and <2: "+count2.toString+"\n"+">=2 and <3: "+count3+"\n"+">=3 and <4: "+count4+"\n"+">=4: "+(count5+count6)+"\n"
  }
   
    
   
  def writeToFile(testResults:RDD[((Int,Int),Double)]) = {
    val sorted = testResults.sortByKey(true).collect();
    val resultFile = new java.io.File("task2_result.txt");
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
