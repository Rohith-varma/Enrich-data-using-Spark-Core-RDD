package ca.rohith.bigdata.spark

case class Trip(route_id: Int,
                service_id: String,
                trip_id: String,
                trip_headsign: String,
                wheelchair_accessible: String
               )

object Trip {
    def apply(csvLine: String): Trip = {
      val a = csvLine.split(",", -1)
      Trip(a(0).toInt, a(1), a(2), a(3), a(6))
    }
}


case class CalendarDate(service_id: String,
                        date: String,
                        exception_type: Int)

object CalendarDate {
  def apply(csvline: String): CalendarDate = {
    val a = csvline.split(",")
    CalendarDate(a(0), a(1), a(2).toInt)
  }
}


case class Route(route_id: Int,
                 route_long_name: String,
                 route_color: String)

object Route {
  def apply(csvline: String): Route = {
    val a = csvline.split(",", -1)
    Route(a(0).toInt, a(1), a(2))
  }
}

case class EnrichedTrip(route_id: Int,
                        trip_headsign: String,
                        wheelchair_accessible: Boolean,
                        date: String,
                        service_exception_type: Int,
                        route_long_name: String,
                        route_color: String)

object EnrichedTrip {
  def toCsv(trip: Trip, calendarDate: CalendarDate, route: Route): String =
    s"${trip.route_id},${trip.trip_headsign},${trip.wheelchair_accessible},${calendarDate.date}" +
      s"${calendarDate.exception_type},${route.route_long_name},${route.route_color}"
}
