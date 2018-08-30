package comp9313.ass4
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.ListBuffer
object SetSimJoin {
  //** first define a function to sort the result as the project required 
  //** sort by the first key and then sort the sencond key
  //sssss
  def main(args: Array[String]) {
     class SecondarySort(val first:Int, val second:Int) extends Ordered[SecondarySort] with Serializable{
          override def compare(that: SecondarySort): Int = {
          if(this.first - that.first != 0)
          {
            this.first - that.first
          }else {
           this.second - that.second
          }  

          }
          }

   val inputFile = args(0)
   val outputFolder = args(1)
   val t = args(2).toDouble
   val conf = new
   SparkConf().setAppName("simi").setMaster("local")
   val sc = new SparkContext(conf)
   val input = sc.textFile(inputFile)
   //var m =0
   var prefix = 0
   var simi =0.0
   //** First Step
   //** calculate each element frequency and sort them ,store the element and frequency into rdd{(element,frequency)} and sort them
   //** after that traverse each line and sort the line with frequency rdd generated before 
   
   val frequency = input.map(line => (line.replaceFirst(" ",",").split(",")(0).toInt,line.replaceFirst(" ",",").split(",")(1)))
   val   newlist =  frequency.flatMap(word => (word._2.split(" ")))
                             .map(word => (word,1))
                             .reduceByKey(_+_)
                             .sortBy(_._2)
   val line_fre = frequency.map{line => (line._1,line._2.split(" "))
                              }.map(x => (x._1,x._2.sortBy(newlist => newlist)))
   //** Second Step 
   //** calculate each line's prefix and store the first prefix elements as (element, id, whole list)                            
 val prefix_sort = line_fre.map{ i =>
                               prefix = i._2.length.toInt - Math.ceil(i._2.length * t).toInt  +1 
                                var tem_pre = new ArrayBuffer[(Int,(Int,ArrayBuffer[Int]))]()                                
                                var whole_l = new ArrayBuffer[Int]()
                               for (j <- 0 to i._2.length -1){
                                  whole_l += i._2(j).toInt
                                }
                                var pre_nub = 0
                                for (k <- 0 to prefix -1){                                  
                                  tem_pre += ((i._2(k).toInt,(i._1.toInt,whole_l)))                                                                   
                                }
                                (tem_pre)
 }
 //** Third Step 
 //** After groupbykey , we can get all the arrays of the same key 
 //** then we calculate the each two lists' similarity
 //** In this part I used the length filter and prefix filter to optimize
 //** the result is the type as((id,id),similarity)
 var pre_filter = ArrayBuffer[(Int,Int)]()
 val simi_list = prefix_sort.flatMap(x => x)                                        
                            .groupByKey
                            .map{x =>
                              (x._1,x._2.toList)
                            }.map{i =>
                              var result = new ArrayBuffer[((Int,Int),Double)]()                             
                              for (m <- 0 to i._2.length-2){
                                for (n <- m+1  to i._2.length -1){
                                  var length_con = 0.0
                                  var verify = "false"
                                  if (i._2(m)._2.length < i._2(n)._2.length){
                                    length_con = i._2(n)._2.length * t                                     
                                    if(i._2(m)._2.length >= length_con){
                                    verify = "true"
                             }
                           }
                                
                                  if (i._2(m)._2.length >= i._2(n)._2.length){
                                       length_con = i._2(m)._2.length * t
                                       if( i._2(n)._2.length >= length_con){
                                       verify = "true"
                             }
                           }
                                  var insert = i._2(m)._2.toSet.intersect(i._2(n)._2.toSet)
                                  var unin =i._2(m)._2.toSet.union(i._2(n)._2.toSet)          
                                  var aa = insert.size.toDouble
                                  var bb = unin.size.toDouble
                                 simi = aa.toDouble/bb.toDouble 
                                 if (simi >= t && verify == "true" && pre_filter.contains(i._2(m)._1,i._2(n)._1) == false){
                                   result += (((i._2(m)._1,i._2(n)._1),simi))
                                   pre_filter += ((i._2(m)._1,i._2(n)._1))
                                 }
                                }
                              }
                              (result)
                            }
  //Final Step
  // In this part we mainly removal the replicate and sort the result as the project required                        
val final_result = simi_list.flatMap(x => x)
                            .distinct
                            .map(x => (new SecondarySort(x._1._1.toInt,x._1._2.toInt),x))
                            .sortByKey(true)
                            .map(x => (x._2._1+ "\t"  + x._2._2))                            
 final_result.saveAsTextFile(outputFolder)
  }
}
