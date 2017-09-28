/**
  * Created by tomoakitsuruta on 9/25/17.
  */
package object domain {
  case class Activity(timestamp_hour: Long,
                      referrer: String,
                      action: String,
                      prevPage: String,
                      page: String,
                      visitor: String,
                      product: String,
                      inputProps: Map[String, String] = Map()
                     )
  case class ActivityByProduct (product : String,
                                timestampe_hour : Long,
                                purchase_count : Long,
                                add_to_cart_count : Long,
                                page_view_count: Long
                               )

  case class VisitorsByProduct (product : String, timestamp_hour: Long, unique_visitors : Long)

}
