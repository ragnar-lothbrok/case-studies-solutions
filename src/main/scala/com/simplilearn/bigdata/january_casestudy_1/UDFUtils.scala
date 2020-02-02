package com.simplilearn.bigdata.january_casestudy_1

import java.sql.Timestamp

import org.apache.spark.sql.functions._

object UDFUtils {

  val formatter = java.text.NumberFormat.getInstance()

  val toMonth = udf { (timestamp: Timestamp) => {
    TimeUtils.getMonth(timestamp)
  }
  }

  val toYear = udf { (timestamp: Timestamp) => {
    TimeUtils.getYear(timestamp)
  }
  }

  val toMonthName = udf { (monthNumber: Integer) => {
    TimeUtils.numberToMonthName(monthNumber)
  }
  }

  val toTimeBucket = udf { (timestamp: Timestamp) => {
    TimeUtils.getTimeBucket(timestamp)
  }
  }

  val toDay = udf { (timestamp: Timestamp) => {
    TimeUtils.getDay(timestamp)
  }
  }

  val toHour = udf { (timestamp: Timestamp) => {
    TimeUtils.getHour(timestamp)
  }
  }

  val toFloat = udf { (value: String) => {
    TimeUtils.getFloat(value)
  }
  }
}
