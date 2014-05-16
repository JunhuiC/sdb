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
 * Initial developer(s): Emmanuel Cecchet.
 * Contributor(s): Mathieu Peltier.
 */

package org.continuent.sequoia.controller.requests;

/**
 * Defines static types values for request. This class publicizes internal
 * implementation details (like bitmasks for instance) and importing it should
 * be avoided as far as possible. Use public methods from AbstractRequest
 * instead.
 * 
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet </a>
 * @author <a href="mailto:Mathieu.Peltier@inrialpes.fr">Mathieu Peltier </a>
 * @author <a href="mailto:Marc.Herbert@emicnetworks.com">Marc Herbert </a>
 * @version 1.0
 */
public final class RequestType
{
  // it looks like we rely on: CACHEABLE <=> no init
  /** Type value for cacheable request. */
  public static final int CACHEABLE        = 0;

  /** Type value for uncacheable request. */
  public static final int UNCACHEABLE      = 1;

  /**
   * Type value for cacheable request that are not affected by an
   * <code>INSERT</code> (select based on a primary key for example).
   */
  public static final int UNIQUE_CACHEABLE = 2;

  /** Value for an undefined request type */
  public static final int UNDEFINED        = 0;
  /** Value for a delete request type */
  public static final int DELETE           = 1;
  /** Value for an insert request type */
  public static final int INSERT           = 2;
  /** Value for an update request type */
  public static final int UPDATE           = 3;
  /** Value for a select request type */
  public static final int SELECT           = 4;
  /** Value for an unknown read request type */
  public static final int UNKNOWN_READ     = 5;
  /** Value for an unknown write request type */
  public static final int UNKNOWN_WRITE    = 6;

  // All DML statements should be defined above this line and have values lower
  // than STORED_PROCEDURE

  /** Value for a stored procedure request type */
  public static final int STORED_PROCEDURE = 10;

  // All DDL statements should be defined below this line and have values
  // greater than STORED_PROCEDURE

  /** Value for a create request type */
  public static final int CREATE           = 20;
  /** Value for an alter request type */
  public static final int ALTER            = 21;
  /** Value for a drop request type */
  public static final int DROP             = 22;

  /**
   * Returns <code>true</code> if this request is a DDL (Data Definition
   * Language) statement such as CREATE, ALTER or DROP. Not supported yet are:
   * TRUNCATE, COMMENT, GRANT and REVOKE (see
   * http://www.orafaq.com/faq/Server_Utilities/SQL/faq53.htm)
   * <p>
   * Note that stored procedures are both considered as DDL and DML as they can
   * include both.
   * 
   * @param requestType the request type
   * @return true if this request is a DDL
   */
  static boolean isDDL(int requestType)
  {
    return RequestType.STORED_PROCEDURE >= requestType;
  }

  /**
   * Returns <code>true</code> if this request is a DML (Data Manipulation
   * Language) statement such SELECT, INSERT, UPDATE or DELETE (see
   * http://www.orafaq.com/faq/Server_Utilities/SQL/faq53.htm)
   * <p>
   * Note that stored procedures are both considered as DDL and DML as they can
   * include both.
   * 
   * @param requestType the request type
   * @return true if this request is a DDL
   */
  static boolean isDML(int requestType)
  {
    return RequestType.STORED_PROCEDURE <= requestType;
  }

  /**
   * Returns <code>true</code> if the request type is a <code>DELETE</code>
   * statement.
   * 
   * @param requestType the request type
   * @return true for a <code>DELETE</code> statement
   */
  static boolean isDelete(int requestType)
  {
    return RequestType.DELETE == requestType;
  }

  /**
   * Returns <code>true</code> if the request type is an <code>INSERT</code>
   * statement.
   * 
   * @param requestType the request type
   * @return true for a <code>INSERT</code> statement
   */
  static boolean isInsert(int requestType)
  {
    return RequestType.INSERT == requestType;
  }

  /**
   * Returns <code>true</code> if the request type is an <code>UPDATE</code>
   * statement.
   * 
   * @param requestType the request type
   * @return true for a <code>UPDATE</code> statement
   */
  static boolean isUpdate(int requestType)
  {
    return RequestType.UPDATE == requestType;
  }

  /**
   * Returns <code>true</code> if the request type is a <code>DROP</code>
   * statement.
   * 
   * @param requestType the request type
   * @return true for a <code>DROP</code> statement
   */
  static boolean isDrop(int requestType)
  {
    return RequestType.DROP == requestType;
  }

  /**
   * Returns <code>true</code> if the request type is a <code>CREATE</code>
   * statement.
   * 
   * @param requestType the request type
   * @return true for a <code>CREATE</code> statement
   */
  static boolean isCreate(int requestType)
  {
    return RequestType.CREATE == requestType;
  }

  /**
   * Returns <code>true</code> if the request type is an <code>ALTER</code>
   * statement.
   * 
   * @param requestType the request type
   * @return true for a <code>ALTER</code> statement
   */
  static boolean isAlter(int requestType)
  {
    return RequestType.ALTER == requestType;
  }

  /**
   * Returns <code>true</code> if the request type is a <code>SELECT</code>
   * statement.
   * 
   * @param requestType the request type
   * @return true for a <code>SELECT</code> statement
   */
  static boolean isSelect(int requestType)
  {
    return RequestType.SELECT == requestType;
  }

  /**
   * Returns <code>true</code> if the request type is a
   * <code>STORED_PROCEDURE</code> statement.
   * 
   * @param requestType the request type
   * @return true for a <code>STORED_PROCEDURE</code> statement
   */
  static boolean isStoredProcedure(int requestType)
  {
    return RequestType.STORED_PROCEDURE == requestType;
  }

  /**
   * Returns the type of the request (internal implementation, subject to
   * change).
   * 
   * @param request the request to get the type from
   * @return the request type
   */
  public static int getRequestType(AbstractRequest request)
  {
    return request.requestType;
  }

  /**
   * Sets the requestType value. Used by constructors of AbstractRequest's
   * subclasses.
   */
  static void setRequestType(AbstractRequest request, int type)
  {
    request.requestType = type;
  }

  /**
   * Returns the request type in a <code>String</code> form.
   * 
   * @param type the request type
   * @return the <code>String</code> form of the request type
   */
  public static String getInformation(int type)
  {
    switch (type)
    {
      case RequestType.CACHEABLE :
        return "CACHEABLE";
      case RequestType.UNCACHEABLE :
        return "UNCACHEABLE";
      case RequestType.UNIQUE_CACHEABLE :
        return "UNIQUE_CACHEABLE";
      default :
        return "Illegal request type";
    }
  }

  /**
   * Return a String representing the type of query. <strong>This method is
   * intended to be used for debug and log only. Do not use in other cases.</strong>
   * The returned String can be:
   * <ul>
   * <li>stored procedure</li>
   * <li>read request</li>
   * <li>write request</li>
   * <li>unknown type request</li>
   * <ul>
   * 
   * @param request the request
   * @return a String representing the type of request
   */
  static String getTypeAsString(AbstractRequest request)
  {
    if (request instanceof SelectRequest)
    {
      return "read request";
    }
    if (request instanceof AbstractWriteRequest)
    {
      return "write request";
    }
    if (request instanceof StoredProcedure)
    {
      return "stored procedure";
    }
    return "request of undefined type";
  }

  /**
   * Returns a String representing the given type. The returned String can be:
   * <ul>
   * <li>DELETE</li>
   * <li>INSERT</li>
   * <li>UPDATE</li>
   * <li>SELECT</li>
   * <li>UNKNOWN_READ</li>
   * <li>UNKNOWN_WRITE</li>
   * <li>STORED_PROCEDURE</li>
   * <li>CREATE</li>
   * <li>ALTER</li>
   * <li>DROP</li>
   * <li>UNDEFINED</li>
   * <ul>
   * 
   * @param type the request type
   * @return a String representing the type of request
   */
  static String getTypeAsString(int type)
  {
    switch (type)
    {
      case DELETE :
        return "DELETE";
      case INSERT :
        return "INSERT";
      case UPDATE :
        return "UPDATE";
      case SELECT :
        return "SELECT";
      case UNKNOWN_READ :
        return "UNKNOWN_READ";
      case UNKNOWN_WRITE :
        return "UNKNOWN_WRITE";
      case STORED_PROCEDURE :
        return "STORED_PROCEDURE";
      case CREATE :
        return "CREATE";
      case ALTER :
        return "ALTER";
      case DROP :
        return "DROP";
      default : // case UNDEFINED :
        return "UNDEFINED";
    }
  }
}
