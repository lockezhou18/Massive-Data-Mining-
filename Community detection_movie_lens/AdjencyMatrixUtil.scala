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


class AdjencyMatrixUtil {
  
  var vertices:Array[(VertexId, Double)]=_;
  var edges:HashMap[(VertexId,VertexId),Double]=_
  //var neighbors:HashMap[VertexId,Array[VertexId]]=_;
  var adjMatrix:Array[Array[Int]] =_
  var index:HashMap[VertexId,Int] =_
  var modurality:Double =_;
  var communities:Array[Array[VertexId]]=_;
  def this(vertices:Array[(VertexId, Double)],edges:HashMap[(VertexId,VertexId),Double]) = {
    this;
    this.vertices = vertices;
    this.edges = edges;
    
//    this.adjMatrix = this.createMatrix()._1;
//    this.index = this.createMatrix()._2;
//    
//    var neighbors = this.findNeighbors()._1;
//    
//    //neighbors.foreach(x=>(println("vertex::"+x._1+":::neighbors:::"+this.printArray(x._2))));
//    
//    var nodeDegrees = this.findNeighbors()._2;
//    
//    //nodeDegrees.foreach(x=>(println("vertex::"+x._1+"::degree::"+x._2)));
//    
//    var communities = this.findGroups(neighbors);
//    
//    println("communities size ::"+communities.size);
//    communities.foreach(x=>(println("communities:::"+this.printArray(x))));
//    this.modurality = this.calculateModularity(communities, nodeDegrees);
//    this.communities = communities;
  }
  
  
  def createMatrix():(Array[Array[Int]],HashMap[VertexId,Int]) = {
    
    val adjMatrix = Array.ofDim[Int](vertices.size, vertices.size);
    val index = new HashMap[VertexId,Int];
    var count = 0;
    
    this.vertices.foreach{
      case(user,weight) =>{
        index.+=(user->count);
        count+=1;
      }
    }
    
    this.edges.foreach{
      case ((v1,v2),weight) =>{
        var index1 = index(v1);
        var index2 = index(v2.toLong);
        
        adjMatrix(index1)(index2) = 1;
        adjMatrix(index2)(index1) = 1;   
      }
    }
    
    return (adjMatrix,index)
  }
  
  
  def findNeighbors():(HashMap[VertexId,Array[VertexId]],HashMap[VertexId,Int]) = {
    
    val neighbors = new HashMap[VertexId,ArrayBuffer[VertexId]];
    val nodesDegree = new HashMap[VertexId,Int]
    
    val indexReverse = index.map{x=>(x._2,x._1)}
    
    
    //add adjenct vertex to the arraybuffer
    for(i<-0 until this.adjMatrix.length)
    {
      var cc = 0;
      var vertex1 = indexReverse(i);
      
      for(j<-0 until this.adjMatrix(i).length)
      {
        if(this.adjMatrix(i)(j) == 1)
        {
            
            var vertex2 = indexReverse(j);
            
            if(!neighbors.contains(vertex1)){
              neighbors.+=(vertex1->new ArrayBuffer().+=(vertex2));
               cc+=1;
            }
            
            else{
              neighbors(vertex1).+=(vertex2);
               cc+=1;
        }
//            if(!nodesDegree.contains(vertex1))
//            {
//                nodesDegree.+=(vertex1->1);
//            }
//            
//            else
//              nodesDegree(vertex1)+=1;
         
        }
      }
      
      if(cc == 0)
      {
        var vertex1 = indexReverse(i);
        neighbors.+=(vertex1->new ArrayBuffer().+=(vertex1));
      }
    }
    
    return (neighbors.map(x=>(x._1,x._2.toArray)),nodesDegree);
    
    
  }
  
  def findGroups(neighbors:HashMap[VertexId,Array[VertexId]]):Array[Array[VertexId]] = {
    
    val visited:HashSet[VertexId] = new HashSet[VertexId];
    val totalVertices:HashSet[VertexId] = new HashSet[VertexId]
    var root:VertexId = 0L;
    
    val communities:ArrayBuffer[Array[VertexId]] = new ArrayBuffer[Array[VertexId]];
    //put all vertices to a set
    this.vertices.foreach{ 
      case(vertex,weight) =>{
        totalVertices.+=(vertex);
      }
    }
    
    var candidates = totalVertices&~visited;
    
    //println("candidate size::"+candidates.size);
    while(!candidates.isEmpty)
    {
       var queue = new Queue[VertexId];
       var community:ArrayBuffer[VertexId] = new ArrayBuffer[VertexId];
       root = candidates.head;
       
       queue.+=(root)
       visited.+=(root);
       
       var tmp = 0L;
       while(!queue.isEmpty){
         
          tmp = queue.dequeue();
          community.insert(0, tmp);
          
          var tmpNeighbor = neighbors(tmp);
          
          if(tmpNeighbor.size != 0)
          {
               tmpNeighbor.foreach{
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
       if(community.size == 1) //allow self-loop
       {
         var self = community(0);
         community = community.+=(self)
       }
       communities.+=(community.toArray);
    }
  
    return communities.toArray;
  }
  
   def findGroups(neighbors:HashMap[VertexId,Array[VertexId]],vertices:Array[(VertexId,Double)]):Array[Array[VertexId]] = {
    
    val visited:HashSet[VertexId] = new HashSet[VertexId];
    val totalVertices:HashSet[VertexId] = new HashSet[VertexId]
    var root:VertexId = 0L;
    
    val communities:ArrayBuffer[Array[VertexId]] = new ArrayBuffer[Array[VertexId]];
    //put all vertices to a set
    vertices.foreach{ 
      case(vertex,weight) =>{
        totalVertices.+=(vertex);
      }
    }
    
    var candidates = totalVertices&~visited;
    
    //println("candidate size::"+candidates.size);
    while(!candidates.isEmpty)
    {
       var queue = new Queue[VertexId];
       var community:ArrayBuffer[VertexId] = new ArrayBuffer[VertexId];
       root = candidates.head;
       
       queue.+=(root)
       visited.+=(root);
       
       var tmp = 0L;
       while(!queue.isEmpty){
         
          tmp = queue.dequeue();
          community.insert(0, tmp);
          
          
          if(neighbors.contains(tmp))
          {
            var tmpNeighbor = neighbors(tmp);
            
            if(tmpNeighbor.size != 0)
            {
                 tmpNeighbor.foreach{
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
       }
       
       candidates = totalVertices &~ visited;
//       if(community.size == 1) //allow self-loop
//       {
//         var self = community(0);
//         community = community.+=(self)
//       }
       communities.+=(community.toArray);
    }
  
    return communities.toArray;
  }
  
  def calculateModularity(communities:Array[Array[VertexId]],nodesDegree:HashMap[VertexId,Int]):Double = {
    
    val m = this.edges.size;
    
    var modularity = 0.00;
    
    communities.foreach{
      
      community =>{
        
        for(i <- community)
        {
          for(j<- community)
          {
              if(i != j)
              {
                var index1 = this.index(i);
                var index2 = this.index(j);
                
                var A = this.adjMatrix(index1)(index2).toDouble;
                
                var k1 = nodesDegree(i).toDouble;
                var k2 = nodesDegree(j).toDouble;
                
                modularity += (A - ((k1*k2)/(2*m)));
                var denominator = 2*m;
                var nominator = k1*k2;
                
               // println("A is "+A+" k1 is "+k1+" k2 is "+k2+" m is "+m);
               // println("modularity in function::"+modularity);
              }
              
              else
                modularity += 0;
              
//              if((community.size == 1) &&(i == j)) // allow self join 
//              {
//                modularity +=(1 - (1/(2*m)));
//              }
          }
        }
        
      }
    }
        println("modularity is "+(modularity/(2*m)));
        return modularity/(2*m);
  }
  
  def getMatrix():Array[Array[Int]] = {
    return this.adjMatrix;
  }
  
  def getCommunities():Array[Array[VertexId]] = {
    return this.communities;
  }
  
  def getModularity():Double = {
    return this.modurality;
  }
  
//  //Just input a matrix, then output the modularity of this matrix
//  def findCommunityByMatrix(adjMatrix:
//  
  
  def printArray[T](some:Array[T]):String = {
    
    var string = "(";
    some.foreach{
      x =>{
        string = string + x+",";
      }
    }
    
    return string+")";
  }
  
  def print2dimArray[T](some:Array[T]):String = {
    
    var string = "(";
    
    for(x<-some)
    {
      for(y<-some)
      {
        string = string + y + ",";
      }
      
      string = string +") , ";
    }
    
    return string;
    
    
  } 
  
  def findCommunityByMatrix(adjMatrix:Array[Array[Int]],vertices:Array[(VertexId,Double)],index:HashMap[VertexId,Int]):(Array[Array[VertexId]],Int,HashMap[VertexId,Int]) = {
  val neighbors = new HashMap[VertexId,ArrayBuffer[VertexId]];
    val nodesDegree = new HashMap[VertexId,Int]
    
    val indexReverse = index.map{x=>(x._2,x._1)}
    
    
    //add adjenct vertex to the arraybuffer
    
    for(i<-0 until adjMatrix.length)
    {
      var cc = 0;
      var vertex1 = indexReverse(i);
      for(j<-0 until adjMatrix(i).length)
      {
        if(adjMatrix(i)(j) == 1)
        {
           
            var vertex2 = indexReverse(j);
            
            if(!neighbors.contains(vertex1)){
              neighbors.+=(vertex1->new ArrayBuffer().+=(vertex2));
            }
            
            else
              neighbors(vertex1).+=(vertex2);
        
//            if(!nodesDegree.contains(vertex1))
//            {
//                nodesDegree.+=(vertex1->1);
//            }
//            
//            else
//              nodesDegree(vertex1)+=1;
//          cc+=1;
        }
      }
      
//      if(cc == 0)
//      {
//        var vertex1 = indexReverse(i);
//        neighbors.+=(vertex1->new ArrayBuffer().+=(vertex1));
//      }
      
      //change degree of nodes, if 
      if(!neighbors.contains(vertex1))
      {
        nodesDegree.+=(vertex1->0)
      }
      else
        nodesDegree.+=(vertex1->cc);
        
   }
//    
    var totalNumEdges = 0;
    nodesDegree.foreach{
      case(id,degree) =>{
//        if(degree != 1)
//        {
            totalNumEdges += degree;
//        }
      }
    }
    
    totalNumEdges /= 2;
    var communities = this.findGroups(neighbors.map(x=>(x._1,x._2.toArray)), vertices);
    return (communities, totalNumEdges,nodesDegree);
  }
  
   
  def calculateModularity(communities:Array[Array[VertexId]],nodesDegree:HashMap[VertexId,Int],m:Int,adjMatrix:Array[Array[Int]]):Double = {
    
    //val m = this.edges.size;
    
    var modularity = 0.00;
    
    communities.foreach{
      
      community =>{
        
        var pair1 = community.combinations(2).toArray;
        var pair2 = community.reverse.combinations(2).toArray;
        
        pair1.foreach{
          
          case Array(v1,v2) =>{
            
          }
        }
        
        
        
        
//        for(i <- community)
//        {
//          for(j<- community)
//          {
//              if(i != j)
//              {
//                var index1 = this.index(i);
//                var index2 = this.index(j);
//                
//                var A = adjMatrix(index1)(index2).toDouble;
//                
//                var k1 = nodesDegree(i).toDouble;
//                var k2 = nodesDegree(j).toDouble;
//                
//                modularity += (A - ((k1*k2)/(2*m)));
//                var denominator = 2*m;
//                var nominator = k1*k2;
//                
//               // println("A is "+A+" k1 is "+k1+" k2 is "+k2+" m is "+m);
//               // println("modularity in function::"+modularity);
//              }
//              
//              else if((community.size == 1) &&(i == j)) // just one node, plus 0
//              {
//                modularity += 0;
//              }
//          }
//        }
//        
      }
    }
        println("modularity is "+(modularity/(2*m)));
        return modularity/(2*m);
  }
  
  def getIndex():HashMap[VertexId,Int] = {
    return this.index;
  }
  
  
}