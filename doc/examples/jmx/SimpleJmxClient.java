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
 * Initial developer(s): Marc Wick.
 * Contributor(s): Emmanuel Cecchet.
 */

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.management.MBeanServerConnection;
import javax.management.MBeanServerInvocationHandler;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import javax.naming.Context;
import javax.security.auth.Subject;

import org.continuent.sequoia.common.authentication.PasswordAuthenticator;
import org.continuent.sequoia.common.jmx.JmxConstants;
import org.continuent.sequoia.common.jmx.mbeans.VirtualDatabaseMBean;
import org.continuent.sequoia.common.users.VirtualDatabaseUser;

/**
 * This class defines a SimpleJmxClient
 * 
 * @author <a href="mailto:marc.wick@monte-bre.ch">Marc Wick </a>
 * @version 1.0
 */
public class SimpleJmxClient
{

  /**
   * a simple jmx client exmaple to show how to access the c-jdbc controller via
   * jmx
   * 
   * @param args - no args
   * @throws Exception - an exception
   */
  public static void main(String[] args) throws Exception
  {
    String port = "8091";
    String host = "localhost";
    String vdbName = "rubis";
    JMXServiceURL address = new JMXServiceURL("rmi", host, 0, "/jndi/jrmp");

    Map environment = new HashMap();
    environment.put(Context.INITIAL_CONTEXT_FACTORY,
        "com.sun.jndi.rmi.registry.RegistryContextFactory");
    environment.put(Context.PROVIDER_URL, "rmi://" + host + ":" + port);

    // use username and password for authentication of connections
    // with the controller, the values are compared to the ones
    // specified in the controller.xml config file.
    // this line is not required if no username/password has been configered
    environment.put(JMXConnector.CREDENTIALS, PasswordAuthenticator
        .createCredentials("jmxuser", "jmxpassword"));

    JMXConnector connector = JMXConnectorFactory.connect(address, environment);

    ObjectName db = JmxConstants.getVirtualDataBaseObjectName(vdbName);

    // we build a subject for authentication
    VirtualDatabaseUser dbUser = new VirtualDatabaseUser("admin", "c-jdbc");
    Set principals = new HashSet();
    principals.add(dbUser);
    Subject subj = new Subject(true, principals, new HashSet(), new HashSet());

    // we open a connection for this subject, all susequent calls with this
    // connection will be executed on the behalf of our subject.
    MBeanServerConnection delegateConnection = connector
        .getMBeanServerConnection(subj);

    // we create a proxy to the virtual database
    VirtualDatabaseMBean proxy = (VirtualDatabaseMBean) MBeanServerInvocationHandler
        .newProxyInstance(delegateConnection, db, VirtualDatabaseMBean.class,
            false);

    // we call a method on the virtual database
    System.out.println(proxy.getAllBackendNames());

  }
}
