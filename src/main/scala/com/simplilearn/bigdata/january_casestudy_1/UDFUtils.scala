package com.simplilearn.bigdata.january_casestudy_1

import java.sql.Timestamp

import org.apache.spark.sql.functions._

import scala.collection.mutable

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

  val toDouble = udf { (value: String) => {
    TimeUtils.getDouble(value)
  }
  }

  val valueToString =
    udf { (value: String) => {
      TimeUtils.getString(value)
    }
  }

  val toPercentage = udf { (value: mutable.WrappedArray[String], total: Long) => {
    val tuple = mutable.ListMap(value.toArray.toSeq.groupBy(identity).mapValues(_.map(_ => 1).reduce(_ + _)).toSeq.sortBy(_._2):_*).toList(0)
    tuple._1 + "-"+ (tuple._2 / (.01f * total)) +" %"
  }
  }
}
