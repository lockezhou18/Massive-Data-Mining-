package hw5

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import scala.collection.mutable.ArrayBuffer;
import scala.collection.mutable.HashMap;
import org.apache.spark.graphx._
import scala.collection.mutable.HashMap;
import scala.collection.mutable.Queue;
import scala.collection.mutable.ListBuffer;
import scala.collection.mutable.HashSet;
import java.io._;

object Community {
  
  def main(args:Array[String]) = {
    //testSample();
     val inputPath = args(0);
     val outputPath1 = args(1);
     val outputPath2 = args(2);
     hw5(inputPath,outputPath1,outputPath1);
    //this.testModularityOne();
    
  }
  
  
  def hw5(inputPath:String,outputPath1:String,outputPath2:String) = {
    
    val start = System.nanoTime();
    
    // build graph
     val gb = new GraphBuilder();
     
     val file = gb.readFile(inputPath)._2;
     
      val pairs = gb.findPairs(file);
      val vertices = gb.findVerticesByEdges(pairs);
      val g = gb.buildGraph(pairs,vertices);
      val calculation = new Calculation(g);
      val vertices1 = g.vertices.collect();
      
      
       val result = calculation.getEdges();
       
        val sorted = result.sortBy(x=>(x._1)).map{
      case((v1,v2),betweeness) =>{
        println("edge::"+v1+","+v2+"--betweeness--"+betweeness);
        (v1,v2,Math.floor(betweeness*10)/10)
      }
    }
//        
//        println(result.size);
//        System.exit(0);
//        
    
   val bufw = new BufferedWriter(new FileWriter(new File(outputPath1)));
    
    
    sorted.foreach{
      x =>{
        bufw.write(x.toString);
        bufw.newLine();
      }
    };
    
    bufw.close();
    
    
      
   // val bufw2 = new BufferedWriter(new FileWriter(new File("c://553//big to small.txt")));
    val sortedAsDesc = result.sortBy(x=>(-x._2));
    
    
      
      //val bufr = new BufferedReader(new FileReader(new File(
      
//      sortedAsDesc.foreach{
//      x=>{
//         bufw2.write(x.toString);
//         bufw2.newLine();
//      }
//    }
    
    val matrixUtil = new MatrixUtils();
    
    var currentMatrix = matrixUtil.createMatrix(vertices1,result);
     val constantNodesDegree = matrixUtil.findNeighbors(currentMatrix)._2; // m and k stay constant
     
    var step = 1000;
    var startPoint = 0;
    var endPoint = step;
    
    var resultCommunity:Array[Array[Int]] = Array.ofDim[Int](2,2)
    //val bufw3 = new BufferedWriter(new FileWriter(new File("c://553//modularity.txt")));
    var flag = true;
    // remove 100 for 1 time
    println("start finding biggest modularity...");
    while(flag)
    {
      currentMatrix = remove(startPoint,endPoint,currentMatrix,sortedAsDesc);
       var modularity = matrixUtil.calculateModularity(currentMatrix);
       //println("modularity is "+modularity);
       
       //bufw3.write("modularity::"+modularity);
       
       var community = matrixUtil.findCommunities(currentMatrix);
       //println("community is "+this.printCommunity(community));
       
       if(community.size == 23)
       {
         resultCommunity = community;
         flag = false;
       }
        //bufw3.write("size::"+community.size);
       // bufw3.write("community::"+this.printCommunity(community));
        
       //println("community size is "+community.size);
       
       //bufw3.newLine();
       startPoint = endPoint + 1;
       endPoint += step;
      
    }
    println("end finding biggest..");
    /*
    //for loop
    for(i <- 0 until (sortedAsDesc.size,100))
    {
         var ((v1,v2),value) = sortedAsDesc(i);
         var modularity = matrixUtil.calculateModularity(currentMatrix);
        println("modularity is "+modularity);
        var community = matrixUtil.findCommunities(currentMatrix);
        println("community is "+this.printCommunity(community));
        println("community size is "+community.size);
        
        currentMatrix(v1.toInt)(v2.toInt) = 0;
        currentMatrix(v2.toInt)(v1.toInt) = 0;
        
    }
    * 
    */
//    sortedAsDesc.foreach{
//      
//      case ((v1,v2),value) =>{
//        
//        var modularity = matrixUtil.calculateModularity(currentMatrix);
//        println("modularity is "+modularity);
//        var community = matrixUtil.findCommunities(currentMatrix);
//        println("community is "+this.printCommunity(community));
//        println("community size is "+community.size);
//        
//        currentMatrix(v1.toInt)(v2.toInt) = 0;
//        currentMatrix(v2.toInt)(v1.toInt) = 0;
//        
//      }
//    }  
    
    var s1 = resultCommunity.map{
      x =>{
        
       var y = x.sortBy(x =>(x));
       y;
      }
    }
    
    //var s2 = Array.ofDim[Int](2,2);
    //var s2 = s1.sortBy(x =>(x._1));
    
    for(i <- 0 until s1.length)
    {
      for(j <- 0 until s1.length)
      {
        if(s1(i)(0) <= s1(j)(0))
        {
        var tmp = s1(i);
        s1(i) = s1(j);
        s1(j) = tmp;
        }         
      }
    }
    
  val bufw2 = new BufferedWriter(new FileWriter(new File(outputPath2)));
  
    s1.foreach{
      x => {
        var str = "[";
        x.foreach{
          y => {  
          str+=y;
          str+=","
          }
        }
        var str2 = str.dropRight(1);
        str2+="]";
        println(str2);
        bufw2.write(str2);
        bufw2.newLine();
      }
    }
    
    bufw2.close();
    
    
    val duration = (System.nanoTime() - start)/1e9d;
    println("total running time is " + duration);
       
  }
  
  
  def testModularityOne() = {
    
    val gb = new GraphBuilder();
    
    val edges = Array((1,2),(1,3),(2,3),(2,4),(4,7),(4,5),(4,6),(5,6),(7,6));
//    //val  = Array((1,1),(2,2),(3,3),(4,4),(5,5),(6,6),(7,7)); 
    val vertices = gb.findVerticesByEdges(edges);
    
    //val edges1 = 
    
    val g = gb.buildGraph(edges, vertices);
    
    val edges1 = g.edges.collect();
    val vertices1 = g.vertices.collect();
    
    val calculation = new Calculation(g);
    
    val result = calculation.getEdges();
    
    val sorted = result.sortBy(x=>(x._1)).map{
      case((v1,v2),betweeness) =>{
        println("edge::"+v1+","+v2+"--betweeness--"+betweeness);
        (v1,v2,betweeness)
      }
    }
      
    val sortedAsDesc = result.sortBy(x=>(-x._2));
    
    val matrixUtil = new MatrixUtils();
    
    var currentMatrix = matrixUtil.createMatrix(vertices1,result);
    
    println("------matrix is -----");
    
    //println(this.printCommunity(currentMatrix));
   print(currentMatrix.map(_.mkString).mkString("\n"))
    
     currentMatrix(2)(4) = 0;
      print(currentMatrix.map(_.mkString).mkString("\n"));
      var community = matrixUtil.findCommunities(currentMatrix);
      
      var modularity = matrixUtil.calculateModularity(currentMatrix);
   
    /*
    sortedAsDesc.foreach{
      
      case ((v1,v2),value) =>{
        
        var modularity = matrixUtil.calculateModularity(currentMatrix);
        println("modularity is "+modularity);
        var community = matrixUtil.findCommunities(currentMatrix);
        println("community is "+this.printCommunity(community));
        println("community size is "+community.size);
        
        currentMatrix(v1.toInt)(v2.toInt) = 0;
        currentMatrix(v2.toInt)(v1.toInt) = 0;
        
      }
    }  
    */
  }
 
  def testSample() = {
    
    val gb = new GraphBuilder();
    
    val edges = Array((1,2),(1,3),(2,3),(2,4),(4,7),(4,5),(4,6),(5,6),(7,6));
//    //val  = Array((1,1),(2,2),(3,3),(4,4),(5,5),(6,6),(7,7)); 
    val vertices = gb.findVerticesByEdges(edges);
    
    //val edges1 = 
    
    val g = gb.buildGraph(edges, vertices);
    
    val edges1 = g.edges.collect();
    val vertices1 = g.vertices.collect();
    
    val calculation = new Calculation(g);
    
    val result = calculation.getEdges();
    
    val sorted = result.sortBy(x=>(x._1)).map{
      case((v1,v2),betweeness) =>{
        println("edge::"+v1+","+v2+"--betweeness--"+betweeness);
        (v1,v2,betweeness)
      }
    }
      
    val sortedAsDesc = result.sortBy(x=>(-x._2));
    
    val matrixUtil = new MatrixUtils();
    
    var currentMatrix = matrixUtil.createMatrix(vertices1,result);
    
    val constantNodesDegree = matrixUtil.findNeighbors(currentMatrix)._2;
    
    sortedAsDesc.foreach{
      
      case ((v1,v2),value) =>{
        
        var modularity = matrixUtil.calculateModularity(currentMatrix,constantNodesDegree);
        println("modularity is "+modularity);
        var community = matrixUtil.findCommunities(currentMatrix);
        println("community is "+this.printCommunity(community));
        println("community size is "+community.size);
        
        currentMatrix(v1.toInt)(v2.toInt) = 0;
        currentMatrix(v2.toInt)(v1.toInt) = 0;
        
      }
    }  
    }
  
  def remove(start:Int,end:Int,adj:Array[Array[Int]],sortedEdges:Array[((VertexId, VertexId), Double)]):Array[Array[Int]] = {
    
    for(i <- start to end)
    {
      var ((v1,v2),value) = sortedEdges(i);
      adj(v1.toInt)(v2.toInt) = 0;
      adj(v2.toInt)(v1.toInt) = 0;
    }
    
    return adj;
  }
  
  def printCommunity(some:Array[Array[Int]]):String = {
    
    var result:String = "";
    
    for(x<-some)
    {
      result += x.toList.toString;
    }
    
    return result;
    
    
    
    
    
  }

 
}