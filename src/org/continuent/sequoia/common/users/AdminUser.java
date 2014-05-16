/**
 * Sequoia: Database clustering technology.
 * Copyright (C) 2002-2004 French National Institute For Research In Computer
 * Science And Control (INRIA).
 * Copyright (C) 2005 AmicoSoft, Inc. dba Emic Networks
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
 * Initial developer(s): Nicolas Modrzyk
 * Contributor(s): ______________________.
 */

package org.continuent.sequoia.common.users;

import org.continuent.sequoia.common.xml.DatabasesXmlTags;

/**
 * This class defines a AdminUser
 * 
 * @author <a href="mailto:Nicolas.Modrzyk@inria.fr">Nicolas Modrzyk </a>
 * @version 1.0
 */
public class AdminUser extends AbstractDatabaseUser
{
  private static final long serialVersionUID = 5080497103560621226L;

  /**
   * Creates a new <code>AdminUser</code> instance. The caller must ensure
   * that the parameters are not <code>null</code>.
   * 
   * @param login the user name.
   * @param password the password.
   */
  public AdminUser(String login, String password)
  {
    super(login, password);
  }

  /**
   * @see org.continuent.sequoia.common.users.AbstractDatabaseUser#getXml()
   */
  public String getXml()
  {
    return "<" + DatabasesXmlTags.ELT_User + " "
        + DatabasesXmlTags.ATT_username + "=\"" + this.getName() + "\" "
        + DatabasesXmlTags.ATT_password + "=\"" + this.getPassword() + "\" />";
  }
}
