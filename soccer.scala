Soccer
/* 
Closure:
In order to avoid the copy of outer variable per task where each task get the value from master node, 
its better if we use:
Broadcast variable 
which is shared, read-only, one copy per node, no copy per task, thus all task on a node share the read only copy from node and not go all the way to master node to read the variable efficient all node participate in dist. peer to peer is enabled, reduce overhead of shuffling(spliting data to be sent to nodes for processing) by transmitting the entire variable, are held in in memory cache(data shouldnt be huge size limited by memory of cluster machine.
Accumulator variable
read-write variable, added associatively(A+(B+C)=(A+B)+C) and commutatively(A+B=B+A), support for Long, Double, Collection, create your own subclassing AccumulatorV2 lib, use for Global counter, or sum
*/
// join using broadcast
val players = spark.read.format("csv").option("header", "true").load("C:\\A\\data\\GettingStartedSpark2\\player.csv")
players.printSchema()
players.show(5)
/*
This is how my players data frame looks: it has id specific info

 |-- id: string (nullable = true)
 |-- player_api_id: string (nullable = true)
 |-- player_name: string (nullable = true)
 |-- player_fifa_api_id: string (nullable = true)
 |-- birthday: string (nullable = true)
 |-- height: string (nullable = true)
 |-- weight: string (nullable = true)

+---+-------------+------------------+------------------+-------------------+------+------+
| id|player_api_id|       player_name|player_fifa_api_id|           birthday|height|weight|
+---+-------------+------------------+------------------+-------------------+------+------+
|  1|       505942|Aaron Appindangoye|            218353|1992-02-29 00:00:00|182.88|   187|
|  2|       155782|   Aaron Cresswell|            189615|1989-12-15 00:00:00|170.18|   146|
|  3|       162549|       Aaron Doran|            186170|1991-05-13 00:00:00|170.18|   163|
|  4|        30572|     Aaron Galindo|            140161|1982-05-08 00:00:00|182.88|   198|
|  5|        23780|      Aaron Hughes|             17725|1979-11-08 00:00:00|182.88|   154|
+---+-------------+------------------+------------------+-------------------+------+------+
*/

val player_attr = spark.read.format("csv").option("header", "true").load("C:\\A\\data\\GettingStartedSpark2\\player_attributes.csv")
player_attr.printSchema()
player_attr.show(5)

/*
this is how player attribute data looks, showing just part of total attribute, as it is a very long list, it has data for years or even quaters
 |-- id: string (nullable = true)
 |-- player_fifa_api_id: string (nullable = true)
 |-- player_api_id: string (nullable = true)
 |-- date: string (nullable = true)
 |-- overall_rating: string (nullable = true) ****************
 |-- potential: string (nullable = true)
 |-- preferred_foot: string (nullable = true)
 |-- attacking_work_rate: string (nullable = true)
 |-- defensive_work_rate: string (nullable = true)
 |-- crossing: string (nullable = true) ********************
 |-- finishing: string (nullable = true) *********************
 |-- heading_accuracy: string (nullable = true)
 |-- short_passing: string (nullable = true)
 |-- volleys: string (nullable = true)
 |-- dribbling: string (nullable = true)
 |-- curve: string (nullable = true)
 |-- free_kick_accuracy: string (nullable = true)
 |-- long_passing: string (nullable = true)
 |-- ball_control: string (nullable = true)
 |-- acceleration: string (nullable = true)
 |-- sprint_speed: string (nullable = true)
*/
// numbr of records in each dataframe
players.count() 
// 11,060
player_attr.count() 
// 183,978
// distinct player id in player_attr
player_attr.select("player_api_id").distinct().count() 
// 11060 this is = number of records of players
// droping unwanted columns, dropping missing records

val playerd = players.drop("id","player_fifa_api_id").na.drop("all")

val  player_attri  = player_attr.drop(
"id",
"player_fifa_api_id",
"preferred_foot",
"attacking_work_rate",
"defensive_work_rate",
"crossing",
"crossing",
"jumping","aggression","balance","sprint_speed","potential","short_passing").na.drop("all")
//manipulate data column to extract years
//create a udf function
val yr: String => Int = _.split("-")(0).toInt
val yrUDF = udf(yr)

val player_attriYr = player_attri.withColumn("year", yrUDF($"date")).drop($"date")
player_attriYr.select($"player_api_id", $"year").show(5)
// Get only 2016 data
val Plyr_2016 = player_attriYr.where($"year" === 2016)
Plyr_2016.select($"player_api_id", $"year").show(5)
Plyr_2016.select($"player_api_id").distinct.count()
// group by player_api_id and get avg of finishing, shot_power, acceleration
// striker charecteristics
val Plyr2016ID = Plyr_2016.groupBy("player_api_id").agg(avg($"finishing"), avg($"shot_power"), avg($"acceleration"))
Plyr2016ID.show(5)
val plyr2016ID = Plyr2016ID.withColumnRenamed("avg(finishing)","finishing").withColumnRenamed("avg(shot_power)" ,"shot_power").withColumnRenamed("avg(acceleration)" ,"acceleration")
// assign wieghts and get striker grade
val w_1 = 1
val w_2 = 2
val w_3 = 1
val w_total = w_1+w_2+w_3
val strikergrade = plyr2016ID.withColumn("striker_grade", (col("finishing")*w_1 + col("shot_power")*w_2 + col("acceleration")*w_3)/w_total)
val stri_grade = strikergrade.drop("finishing", "shot_power", "acceleration")
val top_striker = stri_grade.filter($"striker_grade" > 70).sort($"striker_grade".desc)
// join top_striker and playerd dataframe
top_striker.count() //1609
playerd.count() //11060
//player_api_id appears twice in this
val joinfirst = playerd.join(top_striker, playerd.col("player_api_id") === top_striker.col("player_api_id"))
joinfirst.show(3)
//another method of join, where player_api_id occurs only once
val joinsecond = playerd.join(top_striker, "player_api_id")
joinsecond.show(3)
val jointhird = playerd.join(top_striker, Seq("player_api_id"))
jointhird.show(3)
//simple boradcast join
val simBroadjoin = playerd.join(broadcast(top_striker), "player_api_id")
// join using broadcast and only selected columns
val broadcastjoin = playerd.select("player_api_id","player_name").join(broadcast(top_striker), "player_api_id").sort($"striker_grade")
broadcastjoin.show(3)
//working with accumulator 
// whether, height effect in better accuracy

val plyraccu = player_attri.select("player_api_id","heading_accuracy").join(playerd, "player_api_id")
// define accumulators
val short_count = sc.accumulator(0)
val medium_low_count = sc.accumulator(0)
val medium_high_count = sc.accumulator(0)
val tall_count = sc.accumulator(0)
// create a function to bucket players by height
import org.apache.spark.sql.Row
/* version 1
val bucket = (r:Row) => { val height = r.getAs(r.fieldIndex("height"))
if (height <= 175){
short_count.add(1) }
else if (height <= 183 && height > 175){
medium_low_count.add(1) }
else if (height <= 195 && height > 183){
medium_high_count.add(1) }
else if (height > 195){
tall_count.add(1) }
}
*/
def bucket(r:Row) = { 
val height = r.getFloat(r.fieldIndex("height"))
if (height <= 175){
short_count.add(1) }
else if (height <= 183 && height > 175){
medium_low_count.add(1) }
else if (height <= 195 && height > 183){
medium_high_count.add(1) }
else if (height > 195){
tall_count.add(1) }
}
// We cannot use this fnction on plyraccu, since the height colum is a string,
plyraccu.printSchema()
/*
 |-- player_api_id: string (nullable = true)
 |-- heading_accuracy: string (nullable = true)
 |-- player_name: string (nullable = true)
 |-- birthday: string (nullable = true)
 |-- height: string (nullable = true)
 |-- weight: string (nullable = true)
 */
 // so let's convert the height column to Float
import org.apache.spark.sql.types.FloatType
val plyraccuHt = plyraccu.withColumn("height", plyraccu.col("height").cast(FloatType))
plyraccuHt.printSchema()
/*
 |-- player_api_id: string (nullable = true)
 |-- heading_accuracy: string (nullable = true)
 |-- player_name: string (nullable = true)
 |-- birthday: string (nullable = true)
 |-- height: float (nullable = true)
 |-- weight: string (nullable = true)
 */ 
// apply this function on each record of plyraccu data frame using foreach
plyraccuHt.foreach(x => {bucket(x)})
// another approach for the function without converting the column into float
def bucket(r:Row) = { 
val height = r.getString(r.fieldIndex("height")).toFloat
if (height <= 175){
short_count.add(1) }
else if (height <= 183 && height > 175){
medium_low_count.add(1) }
else if (height <= 195 && height > 183){
medium_high_count.add(1) }
else if (height > 195){
tall_count.add(1) }
}
// now, the previous dataframe with Height as string will also work
plyraccu.foreach(x => {bucket(x)})
// lets put our accumulators in an array
val acuulist = List(short_count.value, medium_low_count.value, medium_high_count.value, tall_count.value)
/* List(19204, 98958, 62411, 3405)
Here the count is for number od records, and not for eah player, because playeracc
we want count for records where heading accuracy is above a certain level
let's add one more parameter to function to check heading accuracy befre bucketing the records
*/
// new accumulators
val short_head_count = sc.accumulator(0)
val medium_head_low_count = sc.accumulator(0)
val medium_head_high_count = sc.accumulator(0)
val tall_head_count = sc.accumulator(0)

// convert the column heading_accuracy to Int
	import org.apache.spark.sql.types.IntegerType
	val plyraccuHead = plyraccu.withColumn("heading_accuracy", plyraccu.col("heading_accuracy").cast(IntegerType))
/*
 |-- player_api_id: string (nullable = true)
 |-- heading_accuracy: integer (nullable = true)
 |-- player_name: string (nullable = true)
 |-- birthday: string (nullable = true)
 |-- height: string (nullable = true)
 |-- weight: string (nullable = true)
 */
// add heading accuracy to the function
/* Following fnction is not working for unknown reasons so we will put the heading accuracy conition in foreach instead of the function, although I am using same function bucket with different name bucket_name

def bucket_head (r:Row, h: Int) = {
val height = r.getString(r.fieldIndex("height")).toFloat
val heading_accuracy = r.getInt(r.fieldIndex("heading_accuracy"))
if (heading_accuracy > h){
if (height <= 175){
short_head_count.add(1) }
else if (height <= 183 && height > 175){
medium_head_low_count.add(1) }
else if (height <= 195 && height > 183){
medium_head_high_count.add(1) }
else if (height > 195){
tall_head_count.add(1) }
}
}
*/
def bucket_head (r:Row) = {
val height = r.getString(r.fieldIndex("height")).toFloat
if (height <= 175){
short_head_count.add(1) }
else if (height <= 183 && height > 175){
medium_head_low_count.add(1) }
else if (height <= 195 && height > 183){
medium_head_high_count.add(1) }
else if (height > 195){
tall_head_count.add(1) }
}
// 
// adding heading accuracy condition in foreach
plyraccuHead.where($"heading_accuracy" <= 60).foreach(x => {bucket_head(x)}) //List(15434, 56518, 21447, 1814)
plyraccuHead.where($"heading_accuracy" > 60).foreach(x => {bucket_head(x)}) // List(3694, 42023, 40634, 1578) (19128)19204
short_head_count
val acuulisthead = List(short_head_count.value, medium_head_low_count.value, medium_head_high_count.value, tall_head_count.value)
val per = 2/3*100
val analysis = List((short_head_count.value / short_count.value) * 100, medium_head_low_count.value/medium_low_count.value *100, medium_head_high_count.value/medium_high_count.value *100, tall_head_count.value/tall_count.value*100) 

// saving file as csv and json using coalsce
Plyr_2016.printSchema
Plyr_2016.select("player_api_id", "overall_rating").coalesce(1).write.option("header", "true").csv("datasave")
