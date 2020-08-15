package ca.rohith.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkCore extends App {

  val sparkConf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("Spark Project") //spark configuration
    .set("spark.hadoop.validateOutputSpecs", "false") //overwrites the existing output file
  val sc = new SparkContext(sparkConf) //initializing spark context

  val trip = sc.textFile("/user/winter2020/rohith/assignment2/trips/trips.txt")
  val tripRdd: RDD[Trip] = trip.filter(!_.contains("route_id")).map(Trip.apply) //creating RDD of Trip

  val calendarDate = sc.textFile("/user/winter2020/rohith/assignment2/calendar_dates/" +
    "calendar_dates.txt")
  val calendarDateRdd: RDD[CalendarDate] = calendarDate.filter(!_.contains("service_id"))
    .map(CalendarDate.apply)

  //reading file from HDFS using SparkContest ( Spark Core)
  val route = sc.textFile("/user/winter2020/rohith/assignment2/routes/routes.txt")
  val routeRdd: RDD[Route] = route.filter(!_.contains("route_id")).map(Route.apply)

  val tripRddKey: RDD[(String, Trip)] = tripRdd.keyBy(_.service_id) //converting RDD into keyed RDD
  val calRddKey: RDD[(String, CalendarDate)] = calendarDateRdd.keyBy(_.service_id)
  val rouRddKey: RDD[(Int, Route)] = routeRdd.keyBy(_.route_id)

  //result RDD with route_id as key,joining Trip and CalendarDate with service_id as key
  val result: RDD[(Int, (String, (Trip, CalendarDate)))] = tripRddKey.join(calRddKey)
    .keyBy(x => x._2._1.route_id)

  //joining result(ie.,trip, calendarDate) with Route and converting into csv
  val enrichedTrip: RDD[String] = result.join(rouRddKey).map {
    case (_,((_, (trip: Trip, calendarDate: CalendarDate)), route: Route)) =>
      EnrichedTrip.toCsv(trip, calendarDate, route)
  }

  enrichedTrip.take(15).foreach(println)
  //writing the csv file to HDFS
  enrichedTrip.saveAsTextFile("/user/winter2020/rohith/assignment2/enriched_trip/EnrichedTrip")

  sc.stop() //stopping the sparkContext
}
