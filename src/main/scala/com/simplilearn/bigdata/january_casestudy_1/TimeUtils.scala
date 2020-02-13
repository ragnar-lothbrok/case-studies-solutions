package com.simplilearn.bigdata.january_casestudy_1

import java.sql.Timestamp
import java.util.Calendar

object TimeUtils {

  private val monthName = Array("January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December")

  def getMonth(timestamp: Timestamp): Integer = {
    val time = timestamp.getTime
    val cal = Calendar.getInstance
    cal.setTimeInMillis(time)
    cal.get(Calendar.MONTH)
  }

  def getYear(timestamp: Timestamp): Integer = {
    val time = timestamp.getTime
    val cal = Calendar.getInstance
    cal.setTimeInMillis(time)
    cal.get(Calendar.YEAR)
  }

  def getTimeBucket(timestamp: Timestamp): Integer = {
    val time = timestamp.getTime
    val cal = Calendar.getInstance
    cal.setTimeInMillis(time)
    (cal.get(Calendar.HOUR_OF_DAY) / 4) + 1
  }

  def getHour(timestamp: Timestamp): Integer = {
    val time = timestamp.getTime
    val cal = Calendar.getInstance
    cal.setTimeInMillis(time)
    cal.get(Calendar.HOUR_OF_DAY)
  }

  def getDay(timestamp: Timestamp): String = {
    val FORMAT2 = new java.text.SimpleDateFormat("dd-MMM-yyyy")
    val time = timestamp.getTime
    val cal = Calendar.getInstance
    cal.setTimeInMillis(time)
    FORMAT2.format(cal.getTime)
  }

  def getDouble(value: String): Double = {
    if(value == null || value.trim.length ==0)
      return "11111".toDouble
    value.toDouble
  }

  def getString(value: String): String = {
    if(value == null || value.trim.length ==0)
      return "NA"
    value
  }

  def numberToMonthName(number: Int): String = monthName(number)

  def getMonthFromString(value: String): String = {
    val FORMAT2 = new java.text.SimpleDateFormat("yy.dd.MM")
    val cal = Calendar.getInstance
    cal.setTimeInMillis(FORMAT2.parse(value).getTime)
    cal.get(Calendar.MONTH)+""
  }

  def getTimeFromString(value: String): Long = {
    val FORMAT2 = new java.text.SimpleDateFormat("yy.dd.MM")
    FORMAT2.parse(value).getTime
  }

  def getYearFromString(value: String): String = {
    val FORMAT2 = new java.text.SimpleDateFormat("yy.dd.MM")
    val cal = Calendar.getInstance
    cal.setTimeInMillis(FORMAT2.parse(value).getTime)
    cal.get(Calendar.YEAR)+""
  }

  def getDayFromString(timestamp: String): String = {
    val FORMAT2 = new java.text.SimpleDateFormat("dd-MMM-yyyy")
    val FORMAT1 = new java.text.SimpleDateFormat("dd/MM/yy HH:mm")
    try
      {
        val date = FORMAT1.parse(timestamp)
        val cal = Calendar.getInstance
        cal.setTimeInMillis(date.getTime)
        return FORMAT2.format(date)
      }
    catch
      {
        // Case statement
        case x: Exception =>
        {
          println("Exception: date formatted = "+timestamp)
        }
      }
    return "Day"
  }

  def getWeekFromString(timestamp: String): String = {
    val FORMAT1 = new java.text.SimpleDateFormat("dd/MM/yy HH:mm")
    try
      {
        val date = FORMAT1.parse(timestamp)
        val cal = Calendar.getInstance
        cal.setTimeInMillis(date.getTime)
        return cal.get(Calendar.YEAR) + "-" +cal.get(Calendar.DAY_OF_YEAR)
      }
    catch
      {
        // Case statement
        case x: Exception =>
        {
          println("Exception: date formatted= "+timestamp)
        }
      }
    return "Week"
  }

  def getTimeDiffInMS(orderPlacedTime: String, timeToApprove: String): Long = {
    val FORMAT1 = new java.text.SimpleDateFormat("dd/MM/yy HH:mm")
    try
      {

        if(timeToApprove != null && timeToApprove.trim.length > 0) {
          val orderPlacedDt = FORMAT1.parse(orderPlacedTime)
          val timeToApproveDt = FORMAT1.parse(timeToApprove)
          if(orderPlacedDt.before(timeToApproveDt)) {
            return timeToApproveDt.getTime - orderPlacedDt.getTime;
          }
        }

      }
    catch
      {
        // Case statement
        case x: Exception =>
        {
          println("Exception: date formatted= getDayFromString =>>  orderPlacedTime:: "+orderPlacedTime +" timeToApprove:: "+timeToApprove)
        }
      }
    return 0;
  }

  def getWeekFromString(orderPlacedTime: String, timeToDeliver: String): Long = {
    val FORMAT1 = new java.text.SimpleDateFormat("dd/MM/yy HH:mm")
    try
      {
        if(timeToDeliver != null && timeToDeliver.trim.length > 0) {
          val orderPlacedDt = FORMAT1.parse(orderPlacedTime)
          val timeToDeliverDt = FORMAT1.parse(timeToDeliver)
          if(orderPlacedDt.before(timeToDeliverDt)) {
            return timeToDeliverDt.getTime - orderPlacedDt.getTime;
          }
        }
      }
    catch
      {
        // Case statement
        case x: Exception =>
        {
          println("Exception: date formatted= getWeekFromString =>>  orderPlacedTime:: "+orderPlacedTime +" timeToDeliver:: "+timeToDeliver)
        }
      }
    return 0
  }

}
