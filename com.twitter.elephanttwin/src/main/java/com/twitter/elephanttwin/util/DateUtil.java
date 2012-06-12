/**
 * Copyright 2012 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.twitter.elephanttwin.util;

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

import org.apache.commons.lang.time.DateUtils;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A bunch of utility classes for dealing with date.
 * @author Dmitriy Ryaboy
 */
public final class DateUtil {
  public static final TimeZone UTC_TIMEZONE = TimeZone.getTimeZone("UTC");
  public static final String CONDENSED_TIMESTAMP_FORMAT = "yyyyMMddHHmmss";
  public static final String DATEID_TIMESTAMP_FORMAT = "yyyyMMdd";
  public static final String APACHE_TIMESTAMP_FORMAT = "dd/MMM/yyyy:HH:mm:ss Z";
  public static final String MYSQL_TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss";
  public static final String MYSQL_TIMESTAMP_FORMAT_WITH_MILLIS = "yyyy-MM-dd HH:mm:ss.S";
  public static final String MYSQL_MINUTE_FORMAT = "yyyy-MM-dd HH:mm";
  public static final String YYYY_MM_DD_FORMAT = "yyyy-MM-dd";
  public static final String MM_DD_YYYY_FORMAT = "MM/dd/yyyy";
  public static final String YYYY_MM_DD_HH_MM_FORMAT = "yyyyMMddHHmm";
  public static final String HH_MM_FORMAT = "HHmm";
  public static final String UNDERSCORED_FORMAT = "yyyy_MM_dd_HH_mm_ss";
  public static final DateTimeFormatter MYSQL_TIMESTAMP_FORMATTER
      = getFormatter(MYSQL_TIMESTAMP_FORMAT);
  public static final DateTimeFormatter MYSQL_TIMESTAMP_FORMATTER_MILLIS
      = getFormatter(MYSQL_TIMESTAMP_FORMAT_WITH_MILLIS);
  public static final DateTimeFormatter DATEID_TIMESTAMP_FORMATTER
      = getFormatter(DATEID_TIMESTAMP_FORMAT);
  public static final DateTimeFormatter YYYY_MM_DD_FORMATTER = getFormatter(YYYY_MM_DD_FORMAT);
  public static final DateTimeFormatter MM_DD_YYYY_FORMATTER = getFormatter(MM_DD_YYYY_FORMAT);
  public static final DateTimeFormatter YYYY_MM_DD_HH_MM_FORMATTER
      = getFormatter(YYYY_MM_DD_HH_MM_FORMAT);
  public static final DateTimeFormatter HH_MM_FORMATTER = getFormatter(HH_MM_FORMAT);
  public static final DateTimeFormatter UNDERSCORE_FORMATTER = getFormatter(UNDERSCORED_FORMAT);
  public static final DateTimeFormatter APACHE_TIMESTAMP_FORMATTER
      = getFormatter(APACHE_TIMESTAMP_FORMAT);

  private static final Logger LOG = LoggerFactory.getLogger(DateUtil.class);

  private DateUtil() { }

  /**
   * Returns DateTimeFormatter with UTC timezone.
   */
  public static DateTimeFormatter getFormatter(String format) {
    return DateTimeFormat.forPattern(format).withZone(DateTimeZone.UTC);
  }

  /**
   * Returns DateTimeFormatter with specified timezone.
   */
  public static DateTimeFormatter getFormatter(String format, TimeZone timeZone) {
    return DateTimeFormat.forPattern(format).withZone(DateTimeZone.forTimeZone(timeZone));
  }

  /**
   * Get a calendar from a formatted timestamp.
   * @param timestamp the timestamp
   * @param format the format
   * @return the output calendar
   */
  public static Calendar formattedTimestampToCalendar(String timestamp, String format) {
    return formattedTimestampToCalendar(timestamp, getFormatter(format));
  }

  /**
   * Uses the given formatter to create a calendar object from the timestamp
   * @param timestamp
   * @param formatter
   * @return
   */
  public static Calendar formattedTimestampToCalendar(String timestamp,
      DateTimeFormatter formatter) {
    long millis = 0;
    try {
      millis = formatter.parseMillis(timestamp);
      return fromMilliseconds(millis);
    } catch (IllegalArgumentException e) {
      // Turns out MYSQL timestamps can sometimes contain milliseconds, and sometimes not.
      // Regular Java date parsing is insensitive to that, but Joda refuses to parse with a
      // non-matching format. Hence, the ugliness below.
      // Formatters don't define a legit equals method, so we just check if they format the
      // current timestamp to the same string.
      long ts = System.currentTimeMillis();
      if (formatter.print(ts).equals(MYSQL_TIMESTAMP_FORMATTER.print(ts))) {
        return formattedTimestampToCalendar(timestamp, MYSQL_TIMESTAMP_FORMATTER_MILLIS);
      } else {
        // SUPPRESS CHECKSTYLE string multiple times
        LOG.debug("Could not parse date " + timestamp + " with dateformat " + formatter, e);
        return null;
      }
    }
  }

  /**
   * Convert sql timestamp to a calendar
   * @param timestamp the sql timestamp
   * @return output calendar object
   */
  public static Calendar sqlTimestampToCalendar(String timestamp) {
    if ("0000-00-00 00:00:00".equals(timestamp)) {
      // Special case SQL's default value, because Java will
      // somehow parse this into an actual date (Nov 30, 0002).
      return null;
    }
    Calendar cal = formattedTimestampToCalendar(timestamp, MYSQL_TIMESTAMP_FORMAT);
    return cal;
  }

  /**
   * Convert dateid to calendar
   * @param dateId the dateid
   * @return the calendar
   */
  public static Calendar dateIdToCalendar(Integer dateId) {
    return formattedTimestampToCalendar(dateId.toString(), DATEID_TIMESTAMP_FORMAT);
  }

  /**
   * Convert SQL minute to calendar
   * @param timestamp sql minute timestamp
   * @return output calendar
   */
  public static Calendar sqlMinuteToCalendar(String timestamp) {
    if ("0000-00-00 00:00".equals(timestamp)) {
      // Special case SQL's default value, because Java will
      // somehow parse this into an actual date (Nov 30, 0002).
      return null;
    }
    return formattedTimestampToCalendar(timestamp, MYSQL_MINUTE_FORMAT);
  }

  /**
   * Convert apache timestamp to calendar
   * @param timestamp apache timestamp
   * @return calendar
   */
  public static Calendar apacheTimestampToCalendar(String timestamp) {
    if ("-".equals(timestamp)) {
      // Special case Apache's default value.
      return null;
    }
    // Apache timestamps look like 09/Sep/2009:01:59:58 +0000
    return formattedTimestampToCalendar(timestamp, APACHE_TIMESTAMP_FORMAT);
  }

  /**
   * Convert mmddyyy to calendar.
   * @param timestamp the timestamp string
   * @return the calendar
   */
  public static Calendar mmDdYyyyFormatToCalendar(String timestamp) {
    return formattedTimestampToCalendar(timestamp, MM_DD_YYYY_FORMAT);
  }

  /**
   * Convert condensed timestamp to epoch ms
   * @param timestamp the condensed timestamp
   * @return the output epoch ms
   */
  public static long condensedTimestampToEpochMs(String timestamp) {
    return formattedTimestampToCalendar(timestamp, CONDENSED_TIMESTAMP_FORMAT).getTimeInMillis();
  }

  /**
   * Convert a calendar to a date id
   * @param cal the calendar
   * @return output date id
   */
  public static int calendarToDateId(Calendar cal) {
    // SUPPRESS CHECKSTYLE magic number
    return cal.get(Calendar.YEAR) * 10000 + (cal.get(Calendar.MONTH) + 1) * 100
           + cal.get(Calendar.DAY_OF_MONTH);
  }

  /**
   * Convert a calendar to an hour id
   * @param cal the calendar
   * @return the hour id
   */
  public static int calendarToHourId(Calendar cal) {
    return cal.get(Calendar.HOUR_OF_DAY);
  }

  /**
   * Convert from millis to hour id
   * @param millis input millis
   * @param tz the timezone
   * @return the hour id
   */
  public static int millisFromEpochToHourId(long millis, TimeZone tz) {
    Calendar cal = Calendar.getInstance(tz);
    cal.setTimeInMillis(millis);
    return calendarToHourId(cal);
  }

  /**
   * Convert calendar to condensed format
   * @param cal the calendar
   * @return the condensed format
   */
  public static String calendarToCondensedFormat(Calendar cal) {
    return String.format("%04d%02d%02d%02d%02d%02d",
        cal.get(Calendar.YEAR), cal.get(Calendar.MONTH) + 1,
        cal.get(Calendar.DAY_OF_MONTH), cal.get(Calendar.HOUR_OF_DAY), cal.get(Calendar.MINUTE),
        cal.get(Calendar.SECOND));
  }

  /**
   * Convert calendar to mysql timestamp.
   * @param cal the calendar
   * @return the mysql timestamp
   */
  public static String calendarToMysqlTimestampFormat(Calendar cal) {
    return MYSQL_TIMESTAMP_FORMATTER.print(cal.getTimeInMillis());
  }

  /**
   * Convert calendar to YYYYMMDD format
   * @param cal the calendar
   * @return the output string
   */
  public static String calendarToYyyyMmDdFormat(Calendar cal) {
    return YYYY_MM_DD_FORMATTER.print(cal.getTimeInMillis());
  }

  /**
   * Convert calendar to MMDDYYYY format
   * @param cal the calendar
   * @return the format
   */
  public static String calendarToMmDdYyyyFormat(Calendar cal) {
    return MM_DD_YYYY_FORMATTER.print(cal.getTimeInMillis());
  }

  /**
   * Convert calendar to date id timestamp
   * @param cal the calendar
   * @return the output date it
   */
  public static String calendarToDateidTimestampFormat(Calendar cal) {
    return DATEID_TIMESTAMP_FORMATTER.print(cal.getTimeInMillis());
  }

  /**
   * Convert calendar to MMDDHHMM format
   * @param cal the calendar
   * @return output string
   */
  public static String calendarToYyyyMmDdHhMmFormat(Calendar cal) {
    return YYYY_MM_DD_HH_MM_FORMATTER.print(cal.getTimeInMillis());
  }

  /**
   * Convert calendar to HHMM format
   * @param cal calendar
   * @return output string
   */
  public static String calendarToHhMmFormat(Calendar cal) {
    return HH_MM_FORMATTER.print(cal.getTimeInMillis());
  }

  /**
   * Convert SQL timestamp to date
   * @param timestamp sql timestamp
   * @return date
   */
  public static Date sqlTimestampToDate(String timestamp) {
    Calendar cal = sqlTimestampToCalendar(timestamp);
    if (cal != null) {
      cal.set(Calendar.HOUR_OF_DAY, 0);
      cal.set(Calendar.MINUTE, 0);
      cal.set(Calendar.SECOND, 0);
      return cal.getTime();
    }
    return null;
  }

  /**
   * Convert sql timestamp to date hour
   * @param timestamp the timestamp
   * @return the date hour
   */
  public static Date sqlTimestampToDateHour(String timestamp) {
    Calendar cal = sqlTimestampToCalendar(timestamp);
    if (cal != null) {
      cal.set(Calendar.MINUTE, 0);
      cal.set(Calendar.SECOND, 0);
      return cal.getTime();
    }
    return null;
  }

  /**
   * Convert SQL timestamp to date hour minute
   * @param timestamp the timestamp
   * @return the output date hour minute
   */
  public static Date sqlTimestampToDateHourMinute(String timestamp) {
    Calendar cal = sqlTimestampToCalendar(timestamp);
    if (cal != null) {
      cal.set(Calendar.SECOND, 0);
      return cal.getTime();
    }
    return null;
  }

  /**
   * Add days to a calendar
   * @param cal calendar
   * @param numDays num days to add
   * @return the new calendar
   */
  public static Calendar addDays(Calendar cal, int numDays) {
    Date newDate = DateUtils.addDays(cal.getTime(), numDays);
    Calendar newCal = Calendar.getInstance(UTC_TIMEZONE);
    newCal.setTime(newDate);
    return newCal;
  }

  /**
   * Subtract days from a calendar
   * @param cal calendar
   * @param numDays num days to subtract
   * @return the new calendar
   */
  public static Calendar subtractDays(Calendar cal, int numDays) {
    return addDays(cal, -numDays);
  }

  /**
   * Add seconds to a calendar
   * @param cal the calendar
   * @param numSeconds the seconds to subtract
   * @return the new calendar
   */
  public static Calendar addSeconds(Calendar cal, int numSeconds) {
    Date newDate = DateUtils.addSeconds(cal.getTime(), numSeconds);
    Calendar newCal = Calendar.getInstance(UTC_TIMEZONE);
    newCal.setTime(newDate);
    return newCal;
  }

  public static Calendar fromDate(Date date) {
    Calendar cal = Calendar.getInstance(UTC_TIMEZONE);
    cal.setTime(date);
    return cal;
  }

  public static Calendar fromMilliseconds(long millis) {
    Calendar cal = Calendar.getInstance(UTC_TIMEZONE);
    cal.setTimeInMillis(millis);
    return cal;
  }

  /**
   * Parse date with using given format.
   * Returns null in case of errors.
   */
  public static Calendar fromString(String dateStr, DateFormat df) {
    try {
      return fromDate(df.parse(dateStr));
    } catch (ParseException e) {
      // SUPPRESS CHECKSTYLE string multiple times
      LOG.warn("Could not parse date " + dateStr + " with dateformat " + df, e);
      return null;
    }
  }

  /**
   * Parse date with using given format.
   * Returns null in case of errors.
   */
  public static Calendar fromString(String dateStr, DateTimeFormatter df) {
    try {
      return fromMilliseconds(df.parseMillis(dateStr));
    } catch (IllegalArgumentException e) {
      // SUPPRESS CHECKSTYLE string multiple times
      LOG.warn("Could not parse date " + dateStr + " with dateformat " + df, e);
      return null;
    }
  }

  /**
   * Round calendar up to the next hour
   * @param cal the calendar
   * @return the new calendar
   */
  public static Calendar roundUpToNextHour(Calendar cal) {
    if (cal.get(Calendar.MINUTE) > 0
        || cal.get(Calendar.SECOND) > 0 || cal.get(Calendar.MILLISECOND) > 0) {
      // SUPPRESS CHECKSTYLE magic number
      cal = DateUtil.addSeconds(cal, 3600 - 60 * cal.get(Calendar.MINUTE)
                                     - cal.get(Calendar.SECOND));
      cal.set(Calendar.MILLISECOND, 0);
    }
    return cal;
  }

  /**
   * Round calendar up to the next day
   * @param cal the calendar
   * @return the new calendar
   */
  public static Calendar roundUpToNextDay(Calendar cal) {
    if (cal.get(Calendar.HOUR_OF_DAY) > 0 || cal.get(Calendar.MINUTE) > 0
        || cal.get(Calendar.SECOND) > 0 || cal.get(Calendar.MILLISECOND) > 0) {
      cal = DateUtil.addSeconds(cal,
          // SUPPRESS CHECKSTYLE magic number
          86400 - 3600 * cal.get(Calendar.HOUR_OF_DAY) - 60 * cal.get(Calendar.MINUTE)
          - cal.get(Calendar.SECOND));
      cal.set(Calendar.MILLISECOND, 0);
    }
    return cal;
  }

  /**
   * Truncate calendar to current day
   * @param cal the calendar
   * @return the new calendar
   */
  public static Calendar truncateToDay(Calendar cal) {
    cal.set(Calendar.HOUR_OF_DAY, 0);
    cal.set(Calendar.MINUTE, 0);
    cal.set(Calendar.SECOND, 0);
    cal.set(Calendar.MILLISECOND, 0);
    return cal;
  }

  /**
   * Truncate calendar to current hour
   * @param cal the calendar
   * @return the new calendar
   */
  public static Calendar truncateToHour(Calendar cal) {
    cal.set(Calendar.MINUTE, 0);
    cal.set(Calendar.SECOND, 0);
    cal.set(Calendar.MILLISECOND, 0);
    return cal;
  }

  public static String getMysqlTimeInterval(Calendar startTime, Calendar endTime) {
    return String.format("[%s - %s]", MYSQL_TIMESTAMP_FORMATTER.print(startTime.getTimeInMillis()),
      MYSQL_TIMESTAMP_FORMATTER.print(endTime.getTimeInMillis()));
  }

  public static String epochMsToYyyyMmDd(long epochMs) {
    return epochMsToYyyyMmDd(epochMs, null);
  }

  /**
   * Convert epochMS to YYYYMMDD
   * @param epochMs the epoch ms
   * @param tz the timezone
   * @return the output string
   */
  public static String epochMsToYyyyMmDd(long epochMs, TimeZone tz) {
    if (tz == null) {
      return DATEID_TIMESTAMP_FORMATTER.print(epochMs);
    } else {
      Calendar cal = Calendar.getInstance(UTC_TIMEZONE);
      cal.setTimeInMillis(epochMs);
      SimpleDateFormat tzDateFormat = new SimpleDateFormat(DATEID_TIMESTAMP_FORMAT);
      tzDateFormat.setTimeZone(tz);
      return tzDateFormat.format(cal.getTime());
    }
  }

  /**
   * Timezones in mysql are wacky, this takes a timestamp
   * and returns a Calendar representation of it in UTC
   */
  public static Calendar fromSqlTimestamp(Timestamp t) {
    if (t == null) {
      return null;
    }

    Calendar cal = getCalendarInstance();
    cal.setTimeInMillis(t.getTime());
    return cal;
  }

  /**
   * Timezones in mysql are wacky, this takes a timestamp
   * and returns a String representation of it in UTC
   */
  public static String stringFromSqlTimestamp(Timestamp t) {
    if (t == null) {
      return null;
    }

    Calendar cal = fromSqlTimestamp(t);
    return calendarToMysqlTimestampFormat(cal);
  }

  public static java.sql.Date getSqlDate(java.util.Date date) {
    return new java.sql.Date(date.getTime());
  }

  /**
   * Get a new calendar instance from UTC timezone
   * @return Calendar.getInstance(UTC TimeZone)
   */
  public static Calendar getCalendarInstance() {
    return Calendar.getInstance(UTC_TIMEZONE);
  }
}
