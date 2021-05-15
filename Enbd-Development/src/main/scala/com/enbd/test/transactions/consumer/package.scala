package com.enbd.test.Enbd_Development

import java.sql.Timestamp
import java.time.Instant

import org.apache.spark.sql.{Encoder, Encoders}

package object consumer {

  val random = new scala.util.Random

  case class AccountLoan(AccountId: Long, AccountType: Int, LoanId: Long, Amount: Double,  timestamp: Timestamp = Timestamp.from(Instant.now()))
   //case class Account( AccountType: Int,  Amount: Int, total: Int, timestamp: Timestamp = Timestamp.from(Instant.now()))

  case class AccountLoanStatistics(AccountType: Int,totalCount: Int,  amount: Double, lastMinCount: Int)
  case class AccountLoanGroupState(groupState: AccountLoanStatistics)

  implicit val AccountLoanEncoder: Encoder[AccountLoan] = Encoders.product[AccountLoan]
  implicit val AccountLoanStatisticsEncoder: Encoder[AccountLoanStatistics] = Encoders.product[AccountLoanStatistics]

  /*def generateEvent(id: Int): PageVisit = {
    PageVisit(
      id = id,
      url = s"https://www.my-service.org/${generateBetween(100, 200)}"
    )
  }
*/
  def generateBetween(start: Int = 0, end: Int = 100): Int = {
    start + random.nextInt((end - start) + 1)
  }

}