import java.security.MessageDigest

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.math

import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.KryoRegistrator

import org.apache.spark.storage

class Location(var lat:Double=0.0, var lon:Double=0.0) extends Serializable{
	def distance(l1:Location):Double={
		var R = 6371; // Radius of the earth in km
  		var dLat = (lat-l1.lat)*math.Pi/180  
  		var dLon = (lon-l1.lon)*math.Pi/180 
  		var a = 
    			math.sin(dLat/2) * math.sin(dLat/2) +
    			math.cos((lat)*math.Pi/180) * math.cos((l1.lat)*math.Pi/180) * 
    			math.sin(dLon/2) * math.sin(dLon/2)
    		 
  		var c = 2 * math.atan2(Math.sqrt(a), math.sqrt(1-a))
  		var d = R * c; // Distance in km
  		return d
	}

}

class MyRegistrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo) {
    kryo.register(classOf[Location])
    kryo.register(classOf[DailyCOG])
    kryo.register(classOf[DSV])
  }
}


class DailyCOG extends Serializable{

def initDistrictsProvinceMap(sc:SparkContext,fileName:String,districtIndex:Int,provinceIndex:Int, delimiter:String=",")={

        var dpFile=sc.textFile(fileName).map(line=>(new DSV(line,",")))
        var dpFiltered=dpFile.filter(d=>((d.parts(0).contains("ID")==false)&&(d.parts(provinceIndex)!="")))        
        dpFiltered.map(p=>(p.parts(districtIndex),p.parts(provinceIndex))).collectAsMap()
        
}

def parseDouble(s: String) = try { Some(s.toDouble) } catch { case _ => None }

def firstLocation(sc:SparkContext,dateLoc:Iterable[(String,String,String)])={

        var dateLocRdd=sc.parallelize(dateLoc.toList)

     dateLocRdd.map(dl=>(dl._1,(dl._2.toDouble,dl._3.toDouble))).sortByKey(true).take(1)(0)._2

}


def initDistrictsLocMap(sc:SparkContext, fileName:String, districtIndex:Int,  latIndex:Int,  lngIndex:Int,delimiter:String=",")={
        var dlFile=sc.textFile(fileName).map(line=>(new DSV(line,",")))
        //var dlFiltered=dlFile.filter(d=>(d.parts(0).contains("ID")==false))
        
        dlFile.filter(p=>((p.parts(0).contains("ID")==false)&&(parseDouble(p.parts(latIndex))!=None)&&(parseDouble(p.parts(lngIndex))!=None))).map(p=>(p.parts(districtIndex),(p.parts(latIndex).toDouble,p.parts(lngIndex).toDouble))).collectAsMap()
        
}

val findMin=(a:(String, (Double,  String)), b:(String, (Double, String)))=>{
    var res:(String, (Double,  String))=null
    if((a._2._1 <= b._2._1))
        res=a
    else
        res=b
        
    println("Compared "+a._2._2+" to "+b._2._2)
    res
}

def initTowersLocMap(sc:SparkContext, fileName:String, towerIndex:Int,  latIndex:Int,  lngIndex:Int,delimiter:String=",")={
        var dlFile=sc.textFile(fileName).map(line=>(new DSV(line,",")))
        //var dlFiltered=dlFile.filter(d=>(d.parts(0).contains("ID")==false))
        var ret = dlFile.filter(p=>((p.parts(0).contains("ID")==false)&&(parseDouble(p.parts(latIndex))!=None)&&(parseDouble(p.parts(lngIndex))!=None))).map(p=>("%3s".format(p.parts(towerIndex)).replace(' ','0') ,(p.parts(latIndex).toDouble,p.parts(lngIndex).toDouble))).collectAsMap()
        
        ret
}

def assignDistrictProvince(lat: Double, lng:Double, distProvinceMap:scala.collection.Map[String,String], distLocMap:scala.collection.Map[String, (Double, Double)])={
    
	//     var t1=distLocMap.map{case(k,v)=>("current",(math.abs(v._1-lat),math.abs(v._2-lng),k))}
	var t1=distLocMap.map{case(k,v)=>("current",((new Location(v._1,v._2)).distance(new Location(lat,lng)),k))}     



    
     var t2=t1.reduceLeft(findMin)
     
     var dist=t2._2._2
     var prov=distProvinceMap.get(t2._2._2)
     var province="";	
     prov match{
	case None => province="None"
	case Some(x) =>province=x
     }
	
      println("District is "+dist+" Province is "+province)
     (dist,province)
     
}
	
def hourly_cog(s:Iterable[(String)],towersLocMap:scala.collection.Map[String,(Double,Double)])={
    //userid|YYMMDD|hour|primary_tower|secondary_tower|tertiary_tower|COG_lat|COG_long|n_events
    var towerCounts=s.groupBy(s=>s).mapValues(_.size).toList.sortBy{_._2}
    var total=0
    var x=0.0000000000000
    var y=0.0000000000000
    var z=0.0000000000000
    for(s1<-s){
        try{
            total=total+1
            var point=(towersLocMap.get(s1))

            var lat=point.get._1.toDouble*math.Pi/180
            var lon=point.get._2.toDouble*math.Pi/180
            var x1 = math.cos(lat) * math.cos(lon);
            var y1 = math.cos(lat) * math.sin(lon);
            var z1 = math.sin(lat);

            x+=x1
            y+=y1
            z+=z1
        }
        catch{ case _=> println("Got Exception for "+s1)}
    }
    x=x/total
    y=y/total
    z=z/total
    var Lon = math.atan2(y, x)
    var Hyp = math.sqrt(x * x + y * y)
    var Lat = math.atan2(z, Hyp)

    (towerCounts(0)._1,try{towerCounts(1)._1}catch { case _ => "None" },try{towerCounts(2)._1}catch { case _ => "None" },Lat * 180 / math.Pi, Lon * 180 / math.Pi,total)
}	


}

object DailyCOGMain extends Serializable{

                val conf = new SparkConf().setMaster("yarn-client")
		//setMaster("spark://messi.ischool.uw.edu:7077")
                .setAppName("DailyCOGAgg")
                .set("spark.shuffle.consolidateFiles", "true")
		.set("spark.storage.blockManagerHeartBeatMs", "300000")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .set("spark.kryo.registrator", "MyRegistrator")
                .set("spark.akka.frameSize","120")
                .set("spark.default.parallelism","800")
                 .set("spark.executor.memory", "20g")
                .set("spark.kryoserializer.buffer.max.mb","10024")
                .set("spark.kryoserializer.buffer.mb","1024")

                val sc = new SparkContext(conf)
	
	def main(args:Array[String]){

		val inputPath=args(1)
		val outputPath = args(2)
		
		val inputFileName=inputPath+args(0)
		val outputFileName=outputPath+args(0)

	var dc=new DailyCOG()

	var distProvinceMap=dc.initDistrictsProvinceMap(sc,inputPath+"Districts.csv",1,2)

	var towersLocMap = dc.initTowersLocMap(sc,inputPath+"Towers2.csv",0,3,2)

	var distLocMap=dc.initDistrictsLocMap(sc,inputPath+"Towers2.csv",4,3,2)

		var latIndex=11
		var lngIndex=12
	



var dsvData = sc.textFile(inputFileName,10).map(line=>(new DSV(line,"\\|")))
	dsvData.persist(storage.StorageLevel.MEMORY_ONLY_SER)
	//TODO Filter out the rows with invalid entries. Have to see how the invalid entries are specified
	//.filter(d=>(d.parts(latIndex)!="NaN" 

	//Rearranging the tuples as key value pairs
	//Key: (Subscriber, Day, Hour)
	//Value:primary_tower, secondary_tower, tertiary_tower, COG_lat, COG_long, n_events

	//Required Format userid|YYMMDD|hour|primary_tower|secondary_tower|tertiary_tower|COG_lat|COG_long|n_events


                var hourlyEntries=dsvData.map(d=>((d.parts(0),d.parts(1),d.parts(2).split("\\:")(0)),(d.parts(3)))).groupByKey().map{case(k,v)=>(k,dc.hourly_cog(v,towersLocMap))}
		hourlyEntries.persist(storage.StorageLevel.MEMORY_ONLY_SER)
                //((L00000041,060612,16),(062,None,None,-1.9469704600000004,30.059389370000005,1))
                var mappedHourlyEntries=hourlyEntries.map{case(k,v)=>(k._1,(k._2,k._3,v._1,v._2,v._3,v._4,v._5,v._6))}
		
		mappedHourlyEntries.persist(storage.StorageLevel.MEMORY_ONLY_SER)
                //var labelledHourlyEntries=mappedHourlyEntries .coalesce(1).mapPartitions(it=>(Seq("(SubscriberId,(Date,Hour,TowerA,TowerB,TowerC,COG_Lat,COG_Lng,TotalRecords))")++it).iterator) 
 		var labelledHourlyEntries=mappedHourlyEntries
                
		labelledHourlyEntries.saveAsTextFile(outputFileName+"-HourlyCOG")


                var eveningEntries= mappedHourlyEntries.filter{case(k,v)=>(v._2<"08"||v._2>"17")}.map{case(k,v)=>((k,v._1,"6pm-8am"),v._3)}.groupByKey().map{case(k,v)=>((k,dc.hourly_cog(v,towersLocMap)))}
		
		eveningEntries.persist(storage.StorageLevel.MEMORY_ONLY_SER)
 
                var mappedEveningEntries=eveningEntries.map{case(k,v)=>(k._1,(k._2,k._3,v._1,v._2,v._3,v._4,v._5,v._6))}
                
		mappedEveningEntries.persist(storage.StorageLevel.MEMORY_ONLY_SER)
                //var labelledEveningEntries=mappedEveningEntries.coalesce(1).mapPartitions(it=>(Seq("(SubscriberId,(Date,Hour,TowerA,TowerB,TowerC,COG_Lat,COG_Lng,TotalRecords))")++it).iterator)
		var labelledEveningEntries=mappedEveningEntries


		/*
		//distanceresult2.saveAsTextFile(outputPath+outputFileName+"dr")
		*/

		labelledEveningEntries.saveAsTextFile(outputFileName+"-DailyEveningCOG")


    /*
      Output format is SubscriberId,(Date,Hour,TowerA,TowerB,TowerC,COG_Lat,COG_Lng,TotalRecords
     */

	} 
}
