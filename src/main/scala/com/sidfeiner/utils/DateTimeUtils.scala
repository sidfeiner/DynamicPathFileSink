package com.sidfeiner.utils

import java.util.{Calendar, TimeZone}

class DateTimeUtils(timeZone: TimeZone) {
  def getCurrentYear(): Int = {
    Calendar.getInstance(timeZone).get(Calendar.YEAR)
  }

  def getCurrentMonth(): Int = {
    Calendar.getInstance(timeZone).get(Calendar.MONTH)
  }

  def getCurrentDayOfMonth(): Int = {
    Calendar.getInstance(timeZone).get(Calendar.DAY_OF_MONTH)
  }

  def getCurrentHour(): Int = {
    Calendar.getInstance(timeZone).get(Calendar.HOUR_OF_DAY)
  }

  def getCurrentMinute(): Int = {
    Calendar.getInstance(timeZone).get(Calendar.MINUTE)
  }

  def getCurrentSecond(): Int = {
    Calendar.getInstance(timeZone).get(Calendar.SECOND)
  }
}
