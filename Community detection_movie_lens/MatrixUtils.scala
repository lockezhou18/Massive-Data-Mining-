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


class MatrixUtils {
  
  //given edges and vertices, return an adjency matrix 
  def createMatrix(vertices:Array[(VertexId,Double)],edges:Array[((VertexId,VertexId),Double)]):Array[Array[Int]] = {
    
    val adjMatrix = Array.ofDim[Int](vertices.size + 1,vertices.size + 1);
    
    edges.foreach{
      case ((v1,v2),between) =>{
        adjMatrix(v1.toInt)(v2.toInt) = 1;
        adjMatrix(v2.toInt)(v1.toInt) = 1;
      }
    }
    
    return adjMatrix;
    
  }
  
  // given an adj matrix, return neighbors and store as a HashMap,also, returns every nodes's degree
  def findNeighbors(adjMatrix:Array[Array[Int]]):(HashMap[Int,ArrayBuffer[Int]],HashMap[Int,Int])= {
    
    val neighbors = new HashMap[Int,ArrayBuffer[Int]].empty;
    val nodesDegree = new HashMap[Int,Int].empty;
    for(v1 <- 1 until adjMatrix.length)
    {
      var degree = 0;
      for (v2 <- 1 until adjMatrix(v1).length)
      {
            if(adjMatrix(v1)(v2) == 1)
            {
                if(!neighbors.contains(v1))
                {
                    neighbors.+=(v1->new ArrayBuffer().+=:(v2))
                    degree += 1;
                }
                
                else{
                  neighbors(v1).+=:(v2);  
                  degree += 1;
                }
            }
            
      }
        //
        if(!neighbors.contains(v1))
        {
          nodesDegree.+=(v1->0)
        }
        else
          nodesDegree.+=(v1->degree);
    }
    
    return (neighbors,nodesDegree);
  }
  
  //given an matrix, return the communities of this matrix,return community as Array[Array[Int]]
  def findCommunities(adj: Array[Array[Int]]):Array[Array[Int]] = {
    val (neighbors,nodesDegree) = this.findNeighbors(adj);
    
    val totalVertices = new HashSet[Int];
    val visited = new HashSet[Int];
    val communities = new ArrayBuffer[Array[Int]]; //each community is an array, 
    
    var candidates = new HashSet[Int].empty;
    
    var root = 0;
    
    //init total vertices from adj matrix
    for(i <- 1 until adj.length)
    {
      totalVertices.+=(i);
    }
    
    candidates = totalVertices &~ visited; // init candidates, add all the vertices to it
    
    val queue = new Queue[Int]; // tmp queue
    
    // when all the vertices are visited, stop loop and return community
    while(!candidates.isEmpty){ 
      
      var community = new ArrayBuffer[Int] // used to store community of each loop
      root = candidates.head // pop one element not be visited
      
      queue.enqueue(root);
      visited.+=(root);
      
      while(!queue.isEmpty) // begin bfs
      {
          var tmp = queue.dequeue();
          community.+=(tmp);
          visited.+=(tmp);
          
          if(neighbors.contains(tmp))
          {
            var currentNeighbors = neighbors(tmp)
            
            currentNeighbors.foreach{ 
                   x=>{
                     if(!visited.contains(x))
                     {
                       visited.+=(x);
                       queue.+=(x);
                     }
                   }  
                 }
          }       
      }
      
       candidates = totalVertices &~ visited;
       communities.+=:(community.toArray);      
    }
    
    return communities.toArray;
  }
  
  
  //right way to calculate modularity
//   def calculateModularity(adj:Array[Array[Int]]):Double = {
//    val (neighbors, nodesDegree) = findNeighbors(adj);
//    val communities = findCommunities(adj);
//    
//     val m = (nodesDegree.values.sum/2).toDouble; // total edges == total degree/2
//     var modularity:Double = 0.00
//     
//     var v1 = 0;
//     var v2 = 0;
//     
//     //traverse the matrix, calculate 
//     for(v1 <- 1 until adj.length)
//     {
//         for(v2<- 1 until adj(v1).length)
//         {
//            if(v1 != v2){
//                var k1 = nodesDegree(v1).toDouble;
//                var k2 = nodesDegree(v2).toDouble;
//                         
//                var A = adj(v1.toInt)(v2.toInt);
//                var delta = adj(v1)(v2);
//                
//                var in = (A - (k1*k2)/(2*m))*delta;
//                
//                modularity += in;
//            }
//         }           
//     }
//     
//     return modularity/(2*m);
//     
//   }
//  
  
  //given an adjency matrix, return the modularity of this matrix
  def calculateModularity(adj:Array[Array[Int]]):Double = {
    
    val (neighbors, nodesDegree) = findNeighbors(adj);
    val communities = findCommunities(adj);
    
    val m = (nodesDegree.values.sum/2).toDouble; // total edges == total degree/2
    var modularity:Double = 0.00
    
    //  // method 1: using combinations method to get combination
    for(community <- communities)
    {
       if(community.size == 1) // indepedent node, mod is 0 
         modularity += 0.00;
       
       else
       {
//            var pair1 = community.combinations(2).toArray;
//            var pair2 = community.reverse.combinations(2).toArray;
//        
//            pair1.foreach{         
//                  case Array(i,j) =>{
//                     var k1 = nodesDegree(i).toDouble;
//                     var k2 = nodesDegree(j).toDouble;
//                     
//                      var A = adj(i.toInt)(j.toInt);
//                      
//                      var in = (A - (k1*k2)/(2*m));
//                      
//                     // println("pair is "+(i,j)+" A is "+A);
//                      //println("m is "+m);
//                     // println("this round modularity is "+in);
//                      
//                      modularity += in;
//                  }
//               }
//                  
//           pair2.foreach{         
//                  case Array(i,j) =>{
//                     var k1 = nodesDegree(i).toDouble;
//                     var k2 = nodesDegree(j).toDouble;
//                     
//                      var A = adj(i.toInt)(j.toInt);
//                      
//                      var in = (A - (k1*k2)/(2*m));
//                      
//                      modularity += in;
//                  }
//               }
    

           for(i <- community)
           {
             for(j <- community)
             {
               if( i != j)
               {
                 var k1 = nodesDegree(i).toDouble;
                 var k2 = nodesDegree(j).toDouble;
                 
                 var A = adj(i)(j).toDouble;
                 
                 var in = (A - (k1*k2)/(2*m));
                 modularity += in;
                 //println("v1 is "+i+" v2 is "+j+" A is "+A);
                 //println("k1 is "+k1+" k2 is "+k2+" m is "+m);
               }
             }
           }
           
       }
    }
    
     return modularity/(2*m);
  }
  
   def calculateModularity(adj:Array[Array[Int]],nodesDegree:HashMap[Int,Int]):Double = {
    
    val neighbors = findNeighbors(adj)._1;
    val communities = findCommunities(adj);
    
    val m = (nodesDegree.values.sum/2).toDouble; // total edges == total degree/2
    var modularity:Double = 0.00
    
    //  // method 1: using combinations method to get combination
    for(community <- communities)
    {
       if(community.size == 1) // indepedent node, mod is 0 
         modularity += 0.00;
       
       else
       {
//            var pair1 = community.combinations(2).toArray;
//            var pair2 = community.reverse.combinations(2).toArray;
//        
//            pair1.foreach{         
//                  case Array(i,j) =>{
//                     var k1 = nodesDegree(i).toDouble;
//                     var k2 = nodesDegree(j).toDouble;
//                     
//                      var A = adj(i.toInt)(j.toInt);
//                      
//                      var in = (A - (k1*k2)/(2*m));
//                      
//                     // println("pair is "+(i,j)+" A is "+A);
//                      //println("m is "+m);
//                     // println("this round modularity is "+in);
//                      
//                      modularity += in;
//                  }
//               }
//                  
//           pair2.foreach{         
//                  case Array(i,j) =>{
//                     var k1 = nodesDegree(i).toDouble;
//                     var k2 = nodesDegree(j).toDouble;
//                     
//                      var A = adj(i.toInt)(j.toInt);
//                      
//                      var in = (A - (k1*k2)/(2*m));
//                      
//                      modularity += in;
//                  }
//               }
    

           for(i <- community)
           {
             for(j <- community)
             {
               if( i != j)
               {
                 var k1 = nodesDegree(i).toDouble;
                 var k2 = nodesDegree(j).toDouble;
                 
                 var A = adj(i)(j).toDouble;
                 
                 var in = (A - (k1*k2)/(2*m));
                 modularity += in;
                 //println("v1 is "+i+" v2 is "+j+" A is "+A);
                 //println("k1 is "+k1+" k2 is "+k2+" m is "+m);
               }
             }
           }
           
       }
    }
    
     return modularity/(2*m);
  }
}