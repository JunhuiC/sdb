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
 * Contributor(s): troby@cavion.com, Emmanuel Cecchet.
 */

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
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
 * This class defines a DBBackup
 * 
 * @author <a href="mailto:marc.wick@monte-bre.ch">Marc Wick </a>
 * @version 1.0
 */
public class DBBackup
{

  /**
   * we call the native backup tool provided by the database vendor.
   * <p>
   * In this example we are calling the postgres pg_dump tool on a linux box.
   * <p>
   * Other daily maintenance tasks such as analyze could also be done here while
   * the backend is offline.
   * 
   * @param backendpass password to access the db
   * @param backenduser login to access the db
   * @param databaseName database name
   * @return exit value of backup process
   * @throws IOException an IOError during execution of backup
   * @throws InterruptedException thread was interrupted while waiting for
   *           external backup to finish
   */
  public static int runDatabaseBackupTool(String backendpass,
      String backenduser, String databaseName) throws IOException,
      InterruptedException
  {

    // mysql
    String backupCommand = "/usr/bin/mysqlhotcopy --allowold --password="
        + backendpass + " --user=" + backenduser + " " + databaseName
        + " /mybackupdirectory";
    // or for postgres
    // backupCommand = "/usr/bin/pg_dump mydb | /usr/bin/gzip > mydb.dmp.gz"

    String[] args = {"/bin/bash", "-c", backupCommand};
    Runtime rt = Runtime.getRuntime();
    Process proc = rt.exec(args);
    proc.waitFor();
    BufferedReader in = new BufferedReader(new InputStreamReader(proc
        .getInputStream()));
    int exitStatus = proc.exitValue();
    String line;
    while ((line = in.readLine()) != null)
    {
      System.out.println("backup output: " + line);
    }
    return exitStatus;

  }

  /**
   * disable a backend, take a backup with the native database tool and take the
   * backend online again.
   * <p>
   * 
   * @param args Expected parameters are:
   * 
   * <pre>
   *  databaseName - name of the database
   *  port - jmx port ot the cjdbc controller
   *  vdbuser - cjdbc virtual database user
   *  vdbpass - cjdbc virutal database password
   *  backenduser - backenduser
   *  backendpass -password of backenduser</pre>
   * 
   * @throws Exception - problems with backup
   */
  public static void main(String[] args) throws Exception
  {
    String databaseName = args[0];
    String port = args[1];
    String vdbuser = args[2];
    String vdbpass = args[3];
    String backenduser = args[4];
    String backendpass = args[5];
    String host = "localhost";
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

    ObjectName db = JmxConstants.getVirtualDataBaseObjectName(databaseName);

    // we build a subject for authentication
    VirtualDatabaseUser dbUser = new VirtualDatabaseUser(vdbuser, vdbpass);
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

    // we disable the backend and set a checkpoint
    proxy.disableBackendWithCheckpoint(databaseName);

    // we call the database specific backup tool for the backup
    runDatabaseBackupTool(backendpass, backenduser, databaseName);

    // we enable the backend again
    proxy.enableBackendFromCheckpoint(databaseName);
  }

}
