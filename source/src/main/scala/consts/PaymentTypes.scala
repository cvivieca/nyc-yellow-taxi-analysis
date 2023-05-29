package consts

object PaymentTypes {
  val Types: Map[Int, String] = Map(
    1 -> "Credit Card",
    2 -> "Cash",
    3 -> "No Charge",
    4 -> "Dispute",
    5 -> "Unknown",
    6 -> "Voided Trip"
  )

  val CREDIT_CARD = 1
  val CASH = 2
  val NO_CHARGE = 3
  val DISPUTE = 4
  val UNKNOWN = 5
  val VOIDED_TRIP = 6
}
