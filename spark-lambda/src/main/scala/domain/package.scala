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

}
