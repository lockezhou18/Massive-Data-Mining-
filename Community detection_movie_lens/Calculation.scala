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

class Calculation {
  
  var vertices:Array[(VertexId, Double)]=_;
  var edges:HashMap[(VertexId,VertexId),Double]=_
  var neighbors:HashMap[VertexId,Array[VertexId]]=_;
  
  def this(g:Graph[Double,Double])
  {
      this
      var tmpV = g.vertices.collect();
      this.vertices = tmpV;
      
      //neighbors.foreach(x => (neighbors.+=(x)));
      
      var nei = g.collectNeighborIds(EdgeDirection.Either).collect();
      var tmpN:HashMap[VertexId,Array[VertexId]] = new HashMap[VertexId, Array[VertexId]];
      nei.foreach(x=>(tmpN.+=(x)));
      this.neighbors = tmpN;
      
      var tmpE = g.edges.collect();
      var tmpEW:HashMap[(VertexId,VertexId),Double] = new HashMap[(VertexId,VertexId),Double];
      
      tmpE.foreach{
        
         case Edge(in,out,weight) =>
        {
            tmpEW.+=((in,out)->weight);
        }
        
      }
      
      this.edges = tmpEW;
      
  }
  
  def BFS(root:VertexId) = {
    
    println(this.vertices.size);
    println(this.edges.size);
    
    
    val tmpV = this.vertices.clone();
    
    val visited:HashMap[VertexId,Boolean] = new HashMap[VertexId,Boolean];
    val level:HashMap[VertexId,Int] = new HashMap[VertexId,Int];
    val parent:HashMap[VertexId,ArrayBuffer[Long]] = new HashMap[VertexId,ArrayBuffer[Long]];
    val nodesWeight:HashMap[VertexId,Double] = new HashMap[VertexId,Double];
    
    tmpV.foreach{
   
      x=>{
        
         //
        //println(x._1+"value"+x._2);
         visited.+=(x._1->false);
         level.+=(x._1->0);
         nodesWeight.+=(x._1->1);
         
      }
    } 
      
//    for(x<-tmpV)
//    {
//         visited.+=(x._1->false);
//         level.+=(x._1->0);
//         nodesWeight.+=(x._1->x._2);
//    }
      
    //println(visited.size);
    
    val queue = new Queue[VertexId];
    val resultNodes:ArrayBuffer[VertexId] = new ArrayBuffer[VertexId];
    visited(root) = true;
    queue.+=(root);
    
     var tmp = 0L;
    while(!queue.isEmpty)
    {
      tmp = queue.dequeue();
      resultNodes.insert(0, tmp);
      
      var tmpNeighbor = this.neighbors(tmp);
      
      if(tmpNeighbor.size != 0)
        {
            tmpNeighbor.foreach(x =>{   //possible to be fault
              
              if(!visited(x))
              {
                level(x) = level(tmp) + 1;
                parent.+=(x->new ArrayBuffer().+=(tmp))//possible to be fault because tmp(long) to Int
                queue.+=(x);
                visited(x) = true;
               
              }
              
              else if(visited(x) && ( (level(tmp)+1) == level(x)))
              {
                parent(x).+=(tmp);
              }
           
            })
      }
    }
    
     
    for(node<-resultNodes)
    {
           if(node != root)
           {
               var parentNodes = parent(node);
               var size = parentNodes.size;
               
               //parentNodes.foreach(x =>(println("parent of node "+node+" is "+x)));
               
               parentNodes.foreach{
                 
                 parentNode =>{
                   
               if(this.edges.get((node,parentNode)) != None)
               {
                 //println("weight before update:::"+edges((node,parentNode)));
                 var weight = edges((node,parentNode)) + (nodesWeight(node)/size);
                 
                 //println("node::"+node+"::weight::"+nodesWeight(node));
                 
                 edges((node,parentNode)) = weight;
                 
                 nodesWeight(parentNode)+=nodesWeight(node)/size;
                 
               }
               
               else if(edges.get((parentNode,node)) != None)
               {
                    //println("weight before update:::"+edges((parentNode,node)));
                 var weight = edges((parentNode,node)) + (nodesWeight(node)/size);
                
                  //println("node::"+node+"::weight::"+nodesWeight(node));
                 
                 edges((parentNode,node)) = weight;
                 
                 nodesWeight(parentNode)+=nodesWeight(node)/size;
               }
               
               else
               {
                 println("wrong!!!!")
                 System.exit(0);
               }
                   
                 }
               }   
           }
        
    }   
    
   // nodesWeight.foreach(x=>(x._1->1));
  }
  
  def updateEdges() = {
    
    var count = 0;
    
//    var nodesWeight:HashMap[VertexId,Double] = HashMap.empty[VertexId,Double];
//     vertices.foreach{
//      x=>{
//        nodesWeight.+=(x);
//      }
//    }
    
    this.vertices.foreach{
      
      case (vertex,weight) => {
        
       
      
        
        this.BFS(vertex);
        this.edges.foreach{
          x=>{
            //println("edge---"+x._1+"--value---"+x._2);
          }
        }
        count+=1;
        println("finish bfs::"+count);
      }
    }
    
  }
  
  def getEdges():Array[((VertexId,VertexId),Double)] = {
    
    this.updateEdges();
    //return this.edges.map(x =>(x._1,((x._2/2).formatted("#.#").toDouble))).toArray;
    return this.edges.map(x =>(x._1,(x._2/2))).toArray;
  }
  
}