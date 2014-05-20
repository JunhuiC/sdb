/**
 * Sequoia: Database clustering technology.
 * Copyright (C) 2002-2004 French National Institute For Research In Computer
 * Science And Control (INRIA).
 * Contact: sequoia@continuent.org
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. 
 *
 * Initial developer(s): Marc Wick.
 * Contributor(s): ______________________.
 */

package org.continuent.sequoia.common.sql.filters;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.continuent.sequoia.common.xml.DatabasesXmlTags;
import org.continuent.sequoia.common.xml.XmlComponent;
import org.continuent.sequoia.controller.requests.AbstractRequest;

/**
 * This class defines a MacrosHandler
 * 
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet </a>
 * @version 1.0
 */
public class MacrosHandler implements XmlComponent
{
  /** Used when level is unknown */
  public static final int      UNKNOWN_INT_VALUE               = -1;
  /** Used when level is unknown */
  public static final String   UNKNOWN_STRING_VALUE            = "unknown";

  /** String for rand() macro */
  private static final Pattern MACRO_RAND_PATTERN              = Pattern
                                                                   .compile(
                                                                       "\\brand(om)?\\s*\\(\\s*\\)\\s*",
                                                                       Pattern.CASE_INSENSITIVE
                                                                           | Pattern.DOTALL);

  /** Value if rand() macro should not be replaced */
  public static final int      RAND_OFF                        = 0;
  /** Value if rand() macro should be replaced by an integer value */
  public static final int      RAND_INT                        = 1;
  /** Value if rand() macro should be replaced by an long value */
  public static final int      RAND_LONG                       = 2;
  /** Value if rand() macro should be replaced by an float value (default) */
  public static final int      RAND_FLOAT                      = 3;
  /** Value if rand() macro should be replaced by an double value */
  public static final int      RAND_DOUBLE                     = 4;

  private final Random         randGenerator                   = new Random();

  private int                  replaceRand                     = RAND_FLOAT;

  /** String for now() macro */
  private static final Pattern MACRO_NOW_PATTERN               = Pattern
                                                                   .compile(
                                                                       "\\bnow\\s*\\(\\s*\\)\\s*",
                                                                       Pattern.CASE_INSENSITIVE
                                                                           | Pattern.DOTALL);
  /** String for current_date macro */
  private static final Pattern MACRO_CURRENT_DATE_PATTERN      = Pattern
                                                                   .compile(
                                                                       "\\bcurrent_date((\\s*\\(\\s*\\))|\\b)",
                                                                       Pattern.CASE_INSENSITIVE
                                                                           | Pattern.DOTALL);
  /** String for current_times macro */
  private static final Pattern MACRO_CURRENT_TIME_PATTERN      = Pattern
                                                                   .compile(
                                                                       "\\bcurrent_time((\\s*\\(\\s*\\))|\\b)",
                                                                       Pattern.CASE_INSENSITIVE
                                                                           | Pattern.DOTALL);
  /** String for timeofday() macro */
  private static final Pattern MACRO_TIMEOFDAY_PATTERN         = Pattern
                                                                   .compile(
                                                                       "\\btimeofday\\s*\\(\\s*\\)\\s*",
                                                                       Pattern.CASE_INSENSITIVE
                                                                           | Pattern.DOTALL);
  /** String for current_timestamp macro */
  private static final Pattern MACRO_CURRENT_TIMESTAMP_PATTERN = Pattern
                                                                   .compile(
                                                                       "\\bcurrent_timestamp((\\s*\\(\\s*\\))|\\b)",
                                                                       Pattern.CASE_INSENSITIVE
                                                                           | Pattern.DOTALL);

  /** Value if a date macro should not be replaced */
  public static final int      DATE_OFF                        = 0;
  /** Value if date macro should be replaced by an java.sql.Date value */
  public static final int      DATE_DATE                       = 1;
  /** Value if date macro should be replaced by an java.sql.Time value */
  public static final int      DATE_TIME                       = 2;
  /** Value if date macro should be replaced by an java.sql.Timestamp value */
  public static final int      DATE_TIMESTAMP                  = 3;

  private long                 clockResolution                 = 0;
  private int                  now                             = DATE_TIMESTAMP;
  private int                  currentDate                     = DATE_DATE;
  private int                  currentTime                     = DATE_TIME;
  private int                  timeOfDay                       = DATE_TIMESTAMP;
  private int                  currentTimestamp                = DATE_TIMESTAMP;

  private boolean              needsProcessing;
  private boolean              needsDateProcessing;

  /**
   * Creates a new <code>MacrosHandler</code> object
   * 
   * @param replaceRand replacement of rand() macro
   * @param clockResolution clock resolution for date macros
   * @param now replacement of now()
   * @param currentDate replacement of current_date
   * @param currentTime replacement of current_time
   * @param timeOfDay replacement of timeofday()
   * @param currentTimestamp replacement of current_timestamp
   */
  public MacrosHandler(int replaceRand, long clockResolution, int now,
      int currentDate, int currentTime, int timeOfDay, int currentTimestamp)
  {
    if ((replaceRand < RAND_OFF) || (replaceRand > RAND_DOUBLE))
      throw new RuntimeException("Invalid value for " + MACRO_RAND_PATTERN
          + " macro replacement (" + replaceRand + ")");
    this.replaceRand = replaceRand;
    if (clockResolution < 0)
      throw new RuntimeException(
          "Invalid negative value for clock resolution in date macros");
    this.clockResolution = clockResolution;
    if ((now < DATE_OFF) || (now > DATE_TIMESTAMP))
      throw new RuntimeException("Invalid value for " + MACRO_NOW_PATTERN
          + " macro replacement (" + now + ")");
    this.now = now;
    if ((currentDate < DATE_OFF) || (currentDate > DATE_DATE))
      throw new RuntimeException("Invalid value for "
          + MACRO_CURRENT_DATE_PATTERN + " macro replacement (" + currentDate
          + ")");
    this.currentDate = currentDate;
    if ((currentTime < DATE_OFF) || (currentTime > DATE_TIMESTAMP))
      throw new RuntimeException("Invalid value for "
          + MACRO_CURRENT_TIME_PATTERN + " macro replacement (" + currentTime
          + ")");
    this.currentTime = currentTime;
    if ((timeOfDay < DATE_OFF) || (timeOfDay > DATE_TIMESTAMP))
      throw new RuntimeException("Invalid value for " + MACRO_TIMEOFDAY_PATTERN
          + " macro replacement (" + timeOfDay + ")");
    this.timeOfDay = timeOfDay;
    if ((currentTimestamp < DATE_OFF) || (currentTimestamp > DATE_TIMESTAMP))
      throw new RuntimeException("Invalid value for "
          + MACRO_CURRENT_TIMESTAMP_PATTERN + " macro replacement ("
          + currentTimestamp + ")");
    this.currentTimestamp = currentTimestamp;
    needsDateProcessing = (now + currentDate + currentTime + timeOfDay + currentTimestamp) > 0;
    needsProcessing = needsDateProcessing || (replaceRand > 0);
  }

  /**
   * Convert the date level from string (xml value) to integer
   * 
   * @param dateLevel the date level
   * @return an int corresponding to the string description
   */
  public static final int getIntDateLevel(String dateLevel)
  {
    if (dateLevel.equals(DatabasesXmlTags.VAL_off))
      return DATE_OFF;
    else if (dateLevel.equals(DatabasesXmlTags.VAL_date))
      return DATE_DATE;
    else if (dateLevel.equals(DatabasesXmlTags.VAL_time))
      return DATE_TIME;
    else if (dateLevel.equals(DatabasesXmlTags.VAL_timestamp))
      return DATE_TIMESTAMP;
    else
      return UNKNOWN_INT_VALUE;
  }

  /**
   * Convert the rand level from string (xml value) to integer
   * 
   * @param randLevel the rand level
   * @return an int corresponding to the string description
   */
  public static final int getIntRandLevel(String randLevel)
  {
    if (randLevel.equalsIgnoreCase(DatabasesXmlTags.VAL_off))
      return RAND_OFF;
    else if (randLevel.equalsIgnoreCase(DatabasesXmlTags.VAL_double))
      return RAND_DOUBLE;
    else if (randLevel.equalsIgnoreCase(DatabasesXmlTags.VAL_float))
      return RAND_FLOAT;
    else if (randLevel.equalsIgnoreCase(DatabasesXmlTags.VAL_int))
      return RAND_INT;
    else if (randLevel.equalsIgnoreCase(DatabasesXmlTags.VAL_long))
      return RAND_LONG;
    else
      return UNKNOWN_INT_VALUE;
  }

  /**
   * Convert the date level from int (java code) to string (xml value)
   * 
   * @param dateLevel the date level
   * @return a string description corresponding to that level
   */
  public static final String getStringDateLevel(int dateLevel)
  {
    switch (dateLevel)
    {
      case DATE_OFF :
        return DatabasesXmlTags.VAL_off;
      case DATE_DATE :
        return DatabasesXmlTags.VAL_date;
      case DATE_TIME :
        return DatabasesXmlTags.VAL_time;
      case DATE_TIMESTAMP :
        return DatabasesXmlTags.VAL_timestamp;
      default :
        return UNKNOWN_STRING_VALUE;
    }
  }

  /**
   * Convert the rand level from int (java code) to string (xml value)
   * 
   * @param randLevel the rand level
   * @return a string description corresponding to that level
   */
  public static final String getStringRandLevel(int randLevel)
  {
    switch (randLevel)
    {
      case RAND_OFF :
        return DatabasesXmlTags.VAL_off;
      case RAND_DOUBLE :
        return DatabasesXmlTags.VAL_double;
      case RAND_FLOAT :
        return DatabasesXmlTags.VAL_float;
      case RAND_INT :
        return DatabasesXmlTags.VAL_int;
      case RAND_LONG :
        return DatabasesXmlTags.VAL_long;
      default :
        return UNKNOWN_STRING_VALUE;
    }
  }

  /**
   * Return this <code>MacrosHandler</code> to the corresponding xml form
   * 
   * @return the XML representation of this element
   */
  public String getXml()
  {
    StringBuffer sb = new StringBuffer();
    sb.append("<" + DatabasesXmlTags.ELT_MacroHandling + " "
        + DatabasesXmlTags.ATT_rand + "=\"" + getStringRandLevel(replaceRand)
        + "\" " + DatabasesXmlTags.ATT_now + "=\"" + getStringDateLevel(now)
        + "\" " + DatabasesXmlTags.ATT_currentDate + "=\""
        + getStringDateLevel(currentDate) + "\" "
        + DatabasesXmlTags.ATT_currentTime + "=\""
        + getStringDateLevel(currentTime) + "\" "
        + DatabasesXmlTags.ATT_currentTimestamp + "=\""
        + getStringDateLevel(currentTimestamp) + "\" "
        + DatabasesXmlTags.ATT_timeOfDay + "=\""
        + getStringDateLevel(timeOfDay) + "\" "
        + DatabasesXmlTags.ATT_timeResolution + "=\"" + clockResolution + "\" "
        + "/>");
    return sb.toString();
  }

  /**
   * Processes a date related macro using the given timestamp.
   * 
   * @param quotes the query to replace with the position of its quotes
   * @param macroPattern macro text to look for
   * @param replacementPolicy DATE_DATE, DATE_TIME or DATE_TIMESTAMP
   * @param currentClock current time in ms
   * @return new SQL statement
   */
  public QuoteIndicesAndReplacedSql macroDate(
      QuoteIndicesAndReplacedSql quotes, Pattern macroPattern,
      int replacementPolicy, long currentClock)
  {
    Matcher m = macroPattern.matcher(quotes.getReplacedSql());

    if (m.find())
    { // There is a match
      StringBuffer sql = null;
      String date;
      int[] quoteIdxs = quotes.getQuoteIdxs();
      int[] doubleQuoteIdxs = quotes.getDoubleQuoteIdxs();

      do
      {
        if (shouldReplaceMacro(m.start(), quoteIdxs)
            && shouldReplaceMacro(m.start(), doubleQuoteIdxs))
        {
          if (sql == null)
            sql = new StringBuffer(quotes.getReplacedSql().length());
          switch (replacementPolicy)
          {
            case DATE_DATE :
              date = "{d '" + new Date(currentClock).toString() + "'}";
              break;
            case DATE_TIME :
              date = "{t '" + new Time(currentClock).toString() + "'}";
              break;
            case DATE_TIMESTAMP :
              date = "{ts '" + new Timestamp(currentClock).toString() + "'}";
              break;
            default :
              throw new RuntimeException(
                  "Unexpected replacement strategy for date macro ("
                      + replacementPolicy + ")");
          }
          m.appendReplacement(sql, date);
        }
      }
      while (m.find());
      if (sql != null)
      {
        m.appendTail(sql);
        quotes.setReplacedSql(sql.toString());
      }
    }
    return quotes;
  }

  /**
   * Replaces rand() with a randomized value.
   * 
   * @param quotes the query to replace with the position of its quotes
   * @return new SQL statement
   */
  public QuoteIndicesAndReplacedSql macroRand(QuoteIndicesAndReplacedSql quotes)
  {
    Matcher m = MACRO_RAND_PATTERN.matcher(quotes.getReplacedSql());

    if (m.find())
    { // There is a match
      String rand;
      StringBuffer sql = null;
      int[] quoteIdxs = quotes.getQuoteIdxs();
      int[] doubleQuoteIdxs = quotes.getDoubleQuoteIdxs();

      do
      {
        if (shouldReplaceMacro(m.start(), quoteIdxs)
            && shouldReplaceMacro(m.start(), doubleQuoteIdxs))
        {
          if (sql == null)
            sql = new StringBuffer(quotes.getReplacedSql().length());
          switch (replaceRand)
          {
            case RAND_INT :
              rand = Integer.toString(randGenerator.nextInt());
              break;
            case RAND_LONG :
              rand = Long.toString(randGenerator.nextLong());
              break;
            case RAND_FLOAT :
              rand = Float.toString(randGenerator.nextFloat());
              break;
            case RAND_DOUBLE :
              rand = Double.toString(randGenerator.nextDouble());
              break;
            default :
              throw new RuntimeException(
                  "Unexpected replacement strategy for rand() macro ("
                      + replaceRand + ")");
          }
          m.appendReplacement(sql, rand);
        }
      }
      while (m.find());
      if (sql != null)
      {
        m.appendTail(sql);
        quotes.setReplacedSql(sql.toString());
      }
    }
    return quotes;
  }

  /**
   * Processes all macros in the given request and returns a new String with the
   * processed macros. If no macro has to be processed, the original String is
   * returned.
   * 
   * @param request the request to process
   */
  public final void processMacros(AbstractRequest request)
  {
    if (!needsProcessing || request.getMacrosAreProcessed())
      return;

    // Replace the statement or the template
    request.setSqlOrTemplate(replaceMacrosInString(request.getSqlOrTemplate()));

    // Process the parameters if any
    if (request.getPreparedStatementParameters() != null)
      request.setPreparedStatementParameters(replaceMacrosInString(request
          .getPreparedStatementParameters()));
  }

  /**
   * Process to the macro replacement in the given string
   * 
   * @param sql the string to process
   * @return the string with the macros replaced
   */
  private String replaceMacrosInString(String sql)
  {
    QuoteIndicesAndReplacedSql sqlWithQuotes = new QuoteIndicesAndReplacedSql(
        sql);

    // Process RAND
    if (replaceRand > RAND_OFF)
      sqlWithQuotes = macroRand(sqlWithQuotes);

    // Process Dates
    if (!needsDateProcessing)
      return sqlWithQuotes.getReplacedSql();

    long currentClock = System.currentTimeMillis();
    if (clockResolution > 0)
      currentClock = currentClock - (currentClock % clockResolution);
    if (now > DATE_OFF)
      sqlWithQuotes = macroDate(sqlWithQuotes, MACRO_NOW_PATTERN, now,
          currentClock);
    if (currentDate > DATE_OFF)
      sqlWithQuotes = macroDate(sqlWithQuotes, MACRO_CURRENT_DATE_PATTERN,
          currentDate, currentClock);
    if (currentTimestamp > DATE_OFF)
      sqlWithQuotes = macroDate(sqlWithQuotes, MACRO_CURRENT_TIMESTAMP_PATTERN,
          currentTimestamp, currentClock);
    if (currentTime > DATE_OFF)
      sqlWithQuotes = macroDate(sqlWithQuotes, MACRO_CURRENT_TIME_PATTERN,
          currentTime, currentClock);
    if (timeOfDay > DATE_OFF)
      sqlWithQuotes = macroDate(sqlWithQuotes, MACRO_TIMEOFDAY_PATTERN,
          timeOfDay, currentClock);

    return sqlWithQuotes.getReplacedSql();
  }

  /**
   * Retrieve all the indexes of quotes in the string
   * 
   * @param sql the original query
   * @return an array of int corresponding to the quote indexes
   */
  private int[] getQuoteIndexes(String sql)
  {
    ArrayList<Integer> list = new ArrayList<Integer>();
    for (int i = 0; i < sql.length(); i++)
    {
      switch (sql.charAt(i))
      {
        case '\'' :
          list.add(new Integer(i));
          break;
        case '\\' :
          i++;
          break;
        default :
          break;
      }
    }
    int[] intList = new int[list.size()];
    for (int i = 0; i < intList.length; i++)
      intList[i] = ((Integer) list.get(i)).intValue();
    return intList;
  }

  /**
   * Retrieve all the indexes of double-quotes in the string
   * 
   * @param sql the original query
   * @return an array of int corresponding to the double quote indexes
   */
  private int[] getDoubleQuoteIndexes(String sql)
  {
    ArrayList<Integer> list = new ArrayList<Integer>();
    for (int i = 0; i < sql.length(); i++)
    {
      switch (sql.charAt(i))
      {
        case '"' :
          list.add(new Integer(i));
          break;
        case '\\' :
          i++;
          break;
        default :
          break;
      }
    }
    int[] intList = new int[list.size()];
    for (int i = 0; i < intList.length; i++)
      intList[i] = ((Integer) list.get(i)).intValue();
    return intList;
  }

  /**
   * Should we replace a macro situated at index idx, knowing that the quotes
   * are at indexes list
   * 
   * @param idx the index of the macro
   * @param list the indexes of quotes
   * @return <code>true</code> if we should change the macro,
   *         <code>false</code> if the macro is within a string
   */
  private boolean shouldReplaceMacro(int idx, int[] list)
  {
    int count = 0;
    while (count < list.length && list[count] < idx)
    {
      count++;
    }
    return count % 2 == 0;
  }

  private class QuoteIndicesAndReplacedSql
  {
    private int[]   quoteIdxs;
    private int[]   doubleQuoteIdxs;
    private String  replacedSql;
    private boolean requiresQuoteComputation;

    /**
     * Creates a new <code>QuoteIndicesAndReplacedSql</code> object
     * 
     * @param sql the sql to parse
     */
    public QuoteIndicesAndReplacedSql(String sql)
    {
      replacedSql = sql;
      requiresQuoteComputation = true;
    }

    private void computeQuotes()
    {
      quoteIdxs = getQuoteIndexes(replacedSql);
      doubleQuoteIdxs = getDoubleQuoteIndexes(replacedSql);
      requiresQuoteComputation = false;
    }

    /**
     * Returns the doubleQuoteIdxs value.
     * 
     * @return Returns the doubleQuoteIdxs.
     */
    public final int[] getDoubleQuoteIdxs()
    {
      if (requiresQuoteComputation)
        computeQuotes();
      return doubleQuoteIdxs;
    }

    /**
     * Returns the quoteIdxs value.
     * 
     * @return Returns the quoteIdxs.
     */
    public final int[] getQuoteIdxs()
    {
      if (requiresQuoteComputation)
        computeQuotes();
      return quoteIdxs;
    }

    /**
     * Returns the replacedSql value.
     * 
     * @return Returns the replacedSql.
     */
    public final String getReplacedSql()
    {
      return replacedSql;
    }

    /**
     * Sets the replacedSql value.
     * 
     * @param replacedSql The replacedSql to set.
     */
    public final void setReplacedSql(String replacedSql)
    {
      this.replacedSql = replacedSql;
      requiresQuoteComputation = true;
    }
  }
}