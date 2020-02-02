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
    val time = timestamp.getTime
    val cal = Calendar.getInstance
    cal.setTimeInMillis(time)
    cal.get(Calendar.YEAR) + "-" +cal.get(Calendar.DAY_OF_MONTH)
  }

  def getFloat(value: String): Float = {
    if(value == null || value.trim.length ==0)
      return "11111".toFloat
    value.toFloat
  }

  def numberToMonthName(number: Int): String = monthName(number)

}
