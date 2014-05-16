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
 * Initial developer(s): Nicolas Modrzyk.
 * Contributor(s): Mathieu Peltier.
 */

package org.continuent.sequoia.console.text;

import java.io.PrintStream;

/**
 * This class defines a ColorPrinter
 * 
 * @author <a href="mailto:Nicolas.Modrzyk@inrialpes.fr">Nicolas Modrzyk </a>
 * @author <a href="mailto:mathieu.peltier@emicnetworks.com">Mathieu Peltier
 *         </a>*
 * @version 1.0
 */
public class ColorPrinter
{
  //  private static final int ATTR_NORMAL = 0;
  private static final int    ATTR_BRIGHT  = 1;
  private static final int    ATTR_DIM     = 2;
  //  private static final int ATTR_UNDERLINE = 3;
  //  private static final int ATTR_BLINK = 5;
  // private static final int ATTR_REVERSE = 7;
  //  private static final int ATTR_HIDDEN = 8;

  private static final int    FG_BLACK     = 30;
  private static final int    FG_RED       = 31;
  private static final int    FG_GREEN     = 32;
  //  private static final int FG_YELLOW = 33;
  private static final int    FG_BLUE      = 34;
  private static final int    FG_MAGENTA   = 35;
  //  private static final int FG_CYAN = 36;
  //  private static final int FG_WHITE = 37;

  //  private static final int BG_BLACK = 40;
  //  private static final int BG_RED = 41;
  //  private static final int BG_GREEN = 42;
  //  private static final int BG_YELLOW = 44;
  //  private static final int BG_BLUE = 44;
  //  private static final int BG_MAGENTA = 45;
  //  private static final int BG_CYAN = 46;
  //  private static final int BG_WHITE = 47;

  private static final String PREFIX       = "\u001b[";
  private static final String SUFFIX       = "m";
  private static final char   SEPARATOR    = ';';
  private static final String END_COLOR    = PREFIX + SUFFIX;

  private static final String STD_COLOR    = PREFIX + ATTR_DIM + SEPARATOR
                                               + FG_BLACK + SUFFIX;
  private static final String ERR_COLOR    = PREFIX + ATTR_BRIGHT + SEPARATOR
                                               + FG_RED + SUFFIX;
  //  private static final String verboseColor = PREFIX + ATTR_BRIGHT + SEPARATOR
  //                                               + FG_BLACK + SUFFIX;
  private static final String INFO_COLOR   = PREFIX + ATTR_BRIGHT + SEPARATOR
                                               + FG_GREEN + SUFFIX;
  private static final String STATUS_COLOR = PREFIX + ATTR_DIM + SEPARATOR
                                               + FG_MAGENTA + SUFFIX;
  private static final String PROMPT_COLOR = PREFIX + ATTR_BRIGHT + SEPARATOR
                                               + FG_BLUE + SUFFIX;

  //  private static final String warnColor = PREFIX + ATTR_DIM + SEPARATOR
  //                                               + FG_MAGENTA + SUFFIX;
  //  private static final String INFO_COLOR = PREFIX + ATTR_DIM + SEPARATOR
  //                                               + FG_CYAN + SUFFIX;
  //  private static final String debugColor = PREFIX + ATTR_DIM + SEPARATOR
  //                                               + FG_BLUE + SUFFIX;

  /**
   * Standard color
   */
  public static final int     STD          = 0;
  /**
   * Error color
   */
  public static final int     ERROR        = 1;
  /**
   * Info color for commands
   */
  public static final int     INFO         = 2;
  /**
   * status color for commands
   */
  public static final int     STATUS       = 3;
  /**
   * status color for commands
   */
  public static final int     PROMPT       = 4;

  /**
   * Print a message in color
   * 
   * @param message Message to print
   * @param stream Stream where to send the message
   * @param color Color for the message
   */
  public static final void printMessage(final String message,
      final PrintStream stream, final int color)
  {
    printMessage(message, stream, color, true);
  }

  /**
   * Print a message in color
   * 
   * @param message Message to print
   * @param stream Stream where to send the message
   * @param color Color for the message
   * @param endline true if a carriage return should be appended to the message
   */
  public static final void printMessage(final String message,
      final PrintStream stream, final int color, boolean endline)
  {
    final String strmessage = ColorPrinter.getColoredMessage(message, color);
    if (endline)
      stream.println(strmessage);
    else
      stream.print(strmessage);
  }

  /**
   * Get colored message.
   * 
   * @param message message to print
   * @param color color for the message
   * @return Message with color tags
   */
  public static final String getColoredMessage(final String message,
      final int color)
  {
    final StringBuffer msg = new StringBuffer(message);
    switch (color)
    {
      case STD :
        msg.insert(0, STD_COLOR);
        msg.append(END_COLOR);
        break;
      case ERROR :
        msg.insert(0, ERR_COLOR);
        msg.append(END_COLOR);
        break;
      case INFO :
        msg.insert(0, INFO_COLOR);
        msg.append(END_COLOR);
        break;
      case STATUS :
        msg.insert(0, STATUS_COLOR);
        msg.append(END_COLOR);
        break;
      case PROMPT :
        msg.insert(0, PROMPT_COLOR);
        msg.append(END_COLOR);
        break;
      default : // Use STD as default
        msg.insert(0, STD_COLOR);
        msg.append(END_COLOR);
        break;
    }
    return msg.toString();
  }
}
