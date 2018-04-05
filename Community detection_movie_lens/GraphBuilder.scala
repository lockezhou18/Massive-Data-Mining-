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

class GraphBuilder {
  
  private val conf = new SparkConf().setAppName("hhh").setMaster("local");
  private val sc = new SparkContext(conf);  
  
  
//   def main(args:Array[String]) = {
//    
//    //val edges = Array((1,9),(1,2),(1,3),(1,4),(2,5),(2,6),(3,5),(3,6),(5,6),(4,6),(5,9),(6,7),(6,8),(9,7));
//    
//    //val ratings = Array((3,1),(4,1),(5,1),(3,2),(4,2),(6,3),(7,4),(8,5),(9,6),(10,7),(11,8),(12,9),(44,10),(55,11));
//    
////    val edges = Array((1,2),(1,3),(2,3),(2,4),(4,7),(4,5),(4,6),(5,6),(7,6));
////    val ratings = Array((1,1),(2,2),(3,3),(4,4),(5,5),(6,6),(7,7));
////    val g = this.buildGraph(ratings, edges);
////    
////    g.vertices.collect().foreach(x=>(println("node:::"+x._1+"____"+"weight:::"+x._2)));
////    
////    
////    val root =5.toLong;
////    val edgesWeight = this.BFS(root, g);
////    
////    edgesWeight.foreach{
////      
////      case (k,v) =>{
////        println("edges::"+k);
////        println("values::"+v);
////      }
////    }
//    
//   // val b = g.stronglyConnectedComponents(100);
//   // b.vertices.collect().foreach(x => println(x));
//    
//     
//    val file = this.readFile();
//    val pairs = this.findPairs(file);
//    val vertices = this.findVerticesByEdges(pairs);
//    val g = this.buildGraph(file, pairs,vertices );
//    val betw = this.combineBetweeness(g);
//    
//    betw.collect().foreach(x=>(println("edge:::"+x._1+"[][][][][]"+"betw"+x._2)));
//    
//    
//  }
  
  def readFile(path:String = "c://553//ratings(1).csv"):(Array[(Int,Int)],RDD[(Int,Int)]) = 
  {
        val ratings = sc.textFile(path).filter(x=>(!x.equals("userId,movieId,rating,timestamp"))).map{ //time increase
          line =>{
            val att = line.split(",");
            //userId,movieId
            (att(0).toInt,att(1).toInt)
          }
        }
        
        return (ratings.collect(),ratings);
  }
  
  
//  def findPairs(ratings:RDD[(Int,Int)]):Array[(Int,Int)] = {
//    val movieUser = ratings.map{
//      x=>{
//        (x._2,x._1)
//      }
//    }
//    
//    val tmp = movieUser.groupByKey().collect();
//    
//    
//  }
//  
  
  
  def findPairs(ratings:RDD[(Int,Int)]):Array[(Int,Int)] = {
    
    val groups = ratings.groupByKey().collect().map(x=>(x._1,x._2.toSet));
    
    val result = new ArrayBuffer[(Int,Int)];
    
     for(x<-groups)
      {
        for(y<-groups)
        {
          if(x != y)
          {
            if(x._2.intersect(y._2).size >= 3)
            {
              if(y._1 >= x._1)
                result.+=((x._1,y._1));
              else
                result.+=((y._1,x._1))
            }
          }
        }
      }
     
     return result.distinct.toArray;
    
  }
  
  
  def findPairs(ratings:Array[(Int,Int)]):Array[(Int,Int)] = {
    
    val map = new HashMap[Int,ArrayBuffer[Int]];
    
    ratings.foreach(
        
        rating => {
           if(map.get(rating._2) == None)
           {
             map.+=((rating._2, new ArrayBuffer(rating._1)));
           }
           
           else
           {
             map(rating._2).+=(rating._1)
           }
        }
        
        )
        
        val result = new ArrayBuffer[(Int,Int)];
    
        for(a<-map)
        {
            val users = a._2.distinct;
            
//            for(i<- 0 until users.length - 1)
//            {
//              for(j<- i+1 until users.length)
//              {
//                if(users(i) >= users(j))
//                {
//                  result.+=((users(j),users(i)))
//                }
//                
//                else
//                  result.+=((users(i),users(j)));
//              }
//            }
            
           for(i<-users)
           {
             for(j<-users)
             {
               if(i!=j)
               {
                   if(i >= j)
                     result.+=((j,i))
                     
                   else
                     result.+=((i,j))
               }             
             }
           }
            
        }
        
       val tmp = sc.parallelize(result.toArray.toSeq).map(x => (x,1)).reduceByKey(_+_).filter(x => (x._2>=3))
       val finalResult = tmp.keys.collect();
        return finalResult;
  }
  
  
  def findVerticesByEdges(edges:Array[(Int,Int)]):Array[Int] = {
    
      val set = new HashSet[Int];
      edges.foreach{
        
        x => {
          set.+=(x._1);
          set.+=(x._2);
        }
      }
      
      return set.toArray;
      
  }
  
  def buildGraph(edges:Array[(Int,Int)],vertices:Array[Int]):Graph[Double,Double] = {
    
//      val vertices = new ArrayBuffer[Int];
//      
//      ratings.foreach(x =>{
//        
//        if(!vertices.contains(x._2))
//            vertices.+=(x._2);
//      });
//      
//      val verticesClass:Array[(Long,(Double))] = vertices.map(x => (x.toLong,(1.00))).toArray;
      
    val verticesClass = vertices.map(x => (x.toLong,(1.00))).toArray;
    val edgesClass = edges.map(x => new Edge(x._1.toLong,x._2.toLong,0.00));
      
      val g = Graph(sc.parallelize(verticesClass.toSeq),sc.parallelize(edgesClass.toSeq));
    
      return g;
  }
  
//  def BFS(root:VertexId, g: Graph[Double,Double],vertices:Array[(VertexId, Double)],neighbors:Array[(VertexId, Array[VertexId])]):HashMap[(VertexId,VertexId),Double] = {
//    val triplets = g.triplets;
//    
//    
//    
//    
//    val mapNeighbor:HashMap[VertexId,Array[VertexId]] = new HashMap[VertexId, Array[VertexId]];
//    neighbors.foreach(x =>(println(x._1+","+x._2.size)));
//    neighbors.foreach(x => (mapNeighbor.+=(x)));
//    val edges = g.edges;
//    
//    val visited:HashMap[VertexId,Boolean] = new HashMap[VertexId,Boolean];
//    val level:HashMap[VertexId,Int] = new HashMap[VertexId,Int];
//    val parent:HashMap[VertexId,ArrayBuffer[Long]] = new HashMap[VertexId,ArrayBuffer[Long]];
//    val nodesWeight:HashMap[VertexId,Double] = new HashMap[VertexId,Double]
//    val edgesWeight:HashMap[(VertexId,VertexId),Double] = new HashMap[(VertexId,VertexId),Double];
//     vertices.foreach{
//      
//      x=>{
//        
//         visited.+=(x._1->false);
//         level.+=(x._1->0);
//         nodesWeight.+=(x._1->x._2);
//         
//      }
//    }
//    
//    
//    edges.collect().foreach{
//      
//      case Edge(in,out,weight) =>
//        {
//            edgesWeight.+=((in,out)->weight);
//        }
//      
//    }
//    
//  
//    val queue = new Queue[VertexId];
//    val resultNodes:ArrayBuffer[VertexId] = new ArrayBuffer[VertexId];
//    visited(root) = true;
//    queue.+=(root);
//    
//    var tmp = 0L;
//    while(!queue.isEmpty)
//    {
//      tmp = queue.dequeue();
//      resultNodes.insert(0, tmp);
//      
//      var tmpNeighbor = mapNeighbor(tmp);
//      
//      if(tmpNeighbor.size != 0)
//        {
//            tmpNeighbor.foreach(x =>{   //possible to be fault
//              
//              if(!visited(x))
//              {
//                level(x) = level(tmp) + 1;
//                parent.+=(x->new ArrayBuffer().+=(tmp))//possible to be fault because tmp(long) to Int
//                queue.+=(x);
//                visited(x) = true;
//               
//              }
//              
//              else if(visited(x) && ( (level(tmp)+1) == level(x)))
//              {
//                parent(x).+=(tmp);
//              }
//           
//            })
//      }
//    }
//    println("level size"+level.size);
//    level.foreach(x=>(println("node::"+x._1+"level::"+x._2)));
//    parent.foreach(x=>(println("node:::"+x._1+"parents size are "+x._2.size)));
//    resultNodes.foreach(x =>(println(x)));
//    
//    for(node<-resultNodes)
//    {
//           if(node != root)
//           {
//               var parentNodes = parent(node);
//               var size = parentNodes.size;
//               
//               parentNodes.foreach(x =>(println("parent of node "+node+" is "+x)));
//               
//               parentNodes.foreach{
//                 
//                 parentNode =>{
//                   
//               if(edgesWeight.get((node,parentNode)) != None)
//               {
//                 var weight = edgesWeight((node,parentNode)) + (nodesWeight(node)/size);
//                 edgesWeight((node,parentNode)) = weight;
//                 
//                 nodesWeight(parentNode)+=weight;
//                 
//               }
//               
//               else if(edgesWeight.get((parentNode,node)) != None)
//               {
//                 var weight = edgesWeight((parentNode,node)) + (nodesWeight(node)/size);
//                 edgesWeight((parentNode,node)) = weight;
//                 
//                 nodesWeight(parentNode)+=weight;
//               }
//               
//               else
//               {
//                 println("wrong!!!!")
//                 System.exit(0);
//               }
//                   
//                 }
//               }   
//           }
//     }
//  
//    return edgesWeight;
//    
//  }
//  
//  
//  def combineBetweeness(g:Graph[Double,Double]):RDD[((VertexId, VertexId), Double)] = {
//    
//    val vertices = g.vertices.collect();
//    val neighbors = g.collectNeighborIds(EdgeDirection.Either).collect();
//    val totalEdges:ListBuffer[((VertexId,VertexId),Double)] = new ListBuffer[((VertexId,VertexId),Double)];
//    
//    vertices.foreach{
//      
//      case(vertex,weight) =>{
//       
//        var edgeWeight = this.BFS(vertex,g,vertices,neighbors).toList;
//        totalEdges.++=(edgeWeight);
//        
//      }
//      
//    }
//    
//    println(totalEdges.size);
//    totalEdges.foreach{
//       case (k,v) =>{
//        println("total edges::"+k);
//        println("total values::"+v);
//      }
//    
//    }
//    val result = sc.parallelize(totalEdges.toSeq).reduceByKey((x,y)=>(x+y)).map(x =>(x._1,x._2/2));
//    return result;
//  }
//  

 
  
}