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

  val getYearFromString =
    udf { (value: String) => {
      TimeUtils.getYearFromString(value)
    }
  }

  val getMonthFromString =
    udf { (value: String) => {
      TimeUtils.getMonthFromString(value)
    }
  }

  val getTimeFromString =
    udf { (value: String) => {
      TimeUtils.getTimeFromString(value)
    }
  }

  val longToString =
    udf { (value: Long) => {
      value.toString
    }
  }

  val toPercentage = udf { (value: mutable.WrappedArray[String], total: Long) => {
      val tuple = mutable.ListMap(value.toArray.toSeq.groupBy(identity).mapValues(_.map(_ => 1).reduce(_ + _)).toSeq.sortBy(_._2):_*).toList(0)
      tuple._1 + "-"+ (tuple._2 / (.01f * total)) +" %"
    }
  }

  val getDayFromString =
    udf { (value: String) => {
      TimeUtils.getDayFromString(value)
    }
  }

  val getWeekFromString =
    udf { (value: String) => {
      TimeUtils.getWeekFromString(value)
    }
  }


  val timeTakenToApprove = udf { (orderPlacedTime: String, timeToApprove: String) => {
      TimeUtils.getWeekFromString(orderPlacedTime, timeToApprove)
    }
  }

  val timeTakenToDeliver = udf { (orderPlacedTime: String, timeToDeliver: String) => {
      TimeUtils.getWeekFromString(orderPlacedTime, timeToDeliver)
    }
  }

  val totalInteractions = udf { (views: Long, likes: Long, dislikes: Long, comment_count:Long) => {
      views + likes + dislikes + comment_count
    }
  }

  val viewsPerDuration = udf { (views: Long, currentDate: Long, publishedDate: String) => {
      TimeUtils.viewsPerDuration(views, currentDate, publishedDate)
    }
  }

  val commentPerViews = udf { (commentCount: Long, views: Long) => {
      commentCount*1.0 / (views/1000.0) >= 5
    }
  }

  val likesPerViews = udf { (likes: Long, views: Long) => {
      likes*1.0 / (views/100.0) >= 4
    }
  }

  val likeDislikeRatio = udf { (likes: Long, dislikes: Long) => {
      if(dislikes == 0) {
        likes*1.0
      } else {
        likes*1.0 / dislikes
      }
    }
  }
}
