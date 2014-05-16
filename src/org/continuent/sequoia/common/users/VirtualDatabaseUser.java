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
 * Initial developer(s): Emmanuel Cecchet.
 * Contributor(s): Mathieu Peltier.
 */

package org.continuent.sequoia.common.users;

import org.continuent.sequoia.common.xml.DatabasesXmlTags;

/**
 * A <code>VirtualDatabaseUser</code> is a login/password combination to
 * represent a virtual database user.
 * 
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet</a>
 * @author <a href="mailto:Mathieu.Peltier@inrialpes.fr">Mathieu Peltier</a>
 * @version 1.0
 */
public class VirtualDatabaseUser extends AbstractDatabaseUser
{
  private static final long serialVersionUID = 535330556687836840L;

  /**
   * Creates a new <code>VirtualDatabaseUser</code> instance. The caller must
   * ensure that the parameters are not <code>null</code>.
   * 
   * @param login the user name.
   * @param password the password.
   */
  public VirtualDatabaseUser(String login, String password)
  {
    super(login, password);
  }

  /**
   * @see org.continuent.sequoia.common.xml.XmlComponent#getXml()
   */
  public String getXml()
  {
    return "<" + DatabasesXmlTags.ELT_VirtualLogin + " "
        + DatabasesXmlTags.ATT_vLogin + "=\"" + getLogin() + "\" "
        + DatabasesXmlTags.ATT_vPassword + "=\"" + getPassword() + "\"/>";
  }
}
