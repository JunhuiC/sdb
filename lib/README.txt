This directory contains all external tools used by Sequoia. Note also
that Ant 1.5 or greater (http://jakarta.apache.og/ant/) is required to
build Sequoia.
Licenses of 3rd party software (hsqldb, iSQL, ...) can be found in the
doc/licenses/ directory.

Sequoia core:

  - File(s): activation.jar
    Project name: JavaBeans Activation Framework (JAF) 
    Version: 1.0.2
    CVS tag:
    License: Sun Microsystems, Inc. Binary Code License Agreement
    Web site: http://java.sun.com/products/javabeans/glasgow/jaf.html
    Description: 
      This framework is needed in order to make JavaMail work properly. 
    Notes: 
      This is only needed if you use the SMTP appender in log4j.
      
  - File(s): commons-cli.jar
    Project name: Apache Commons CLI
    Version: 1.0
    CVS tag:
    Web site: http://jakarta.apache.org/commons/cli/
    License: APL 2.0 (http://www.apache.org/licenses/LICENSE-2.0.txt)
    Description:  
      The CLI library provides a simple and easy to use API for working with 
      the command line arguments and options.
    Notes:
      Used by the controller and the text console.
 
  - File(s): commons-logging.jar
    Project name: commons logging
    Version: 1.0.4
    CVS tag:
    Web site: http://jakarta.apache.org/commons/logging/
    License: APL 2.0 (http://www.apache.org/licenses/LICENSE-2.0.txt)
    Description:  
      Commons-Logging is a wrapper around a variety of logging API 
      implementations.
    Notes:
      This is used by JGroups.

  - File(s): concurrent.jar
    Project name: EDU.oswego.cs.dl.util.concurrent
    Version: 1.3.2
    CVS tag: 
    License: Public domain (portions of the CopyOnWriteArrayList and ConcurrentReaderHashMap
      classes are adapted from Sun JDK source code. These are copyright of Sun Microsystems,
      Inc, and are used with their kind permission, as described in this license:
      http://gee.cs.oswego.edu/dl/classes/EDU/oswego/cs/dl/util/sun-u.c.license.pdf)
    Web site: http://gee.cs.oswego.edu/dl/classes/EDU/oswego/cs/dl/util/concurrent/intro.html
    Description: 
      This package provides standardized, efficient versions of utility classes commonly
      encountered in concurrent Java programming.
    Notes: 
      This file is used by JGroups.
      
  - File(s): crimson.jar
    Project name: Apache Crimson
    CVS tag:
    Version: 1.1.3
    License: APL 2.0 (http://www.apache.org/licenses/LICENSE-2.0.txt)
    Web site: http://xml.apache.org/crimson/
    Description: 
      Crimson is a Java XML parser which supports XML 1.0. 
    Notes: 
      Used to parse controller and virtual database xml configuration files.
 
  - File(s): hedera-appia.jar hedera-commons.jar hedera-jgroups.jar hedera-spread.jar
    Project name: Hedera
    Version: 1.6.3
    CVS tag: hedera-1_6_3
    License: APL 2.0 (http://www.apache.org/licenses/LICENSE-2.0.txt)
    Web site: http://www.continuent.org/
    Description: 
      Hedera is a group communication library.
    Notes: 

  - File(s): appia.jar
    Project name: Appia
    Version: 3.3
    CVS tag: release_3_3
    License: APL 2.0 (http://www.apache.org/licenses/LICENSE-2.0.txt)
    Web site: http://appia.di.fc.ul.pt/
    Description: 
      Appia is a protocol composition framework and group communication toolkit.
    Notes: 
    
  - File(s): jgroups-core.jar
    Project name: JGroups
    Version: 2.4.2 (built directly from Branch_JGroups_2_4)
    CVS tag: 
    License: LGPL (http://www.jgroups.org/javagroupsnew/docs/license.html)
    Web site: http://www.jboss.org/ 
    Description:
      JGroups is a toolkit for reliable multicast communication.

  - File(s): spread.jar
    Project name: Spread
    Version: 4.0.0
    CVS tag:
        License: The Spread Open-Source License (http://www.spread.org/license/license.html)
    Web site: http://www.spread.org/
    Description: 
      Spread is a toolkit for reliable multicast communication.
    Notes: 
      This product uses software developed by Spread Concepts LLC for use in the Spread toolkit. 
      For more information about Spread see http://www.spread.org.
      
  - File(s): log4j.jar
    Project name: Log4j 
    Version: 1.2.12
    CVS tag:
    License: APL 2.0 (http://www.apache.org/licenses/LICENSE-2.0.txt)
    Web site: http://logging.apache.org/log4j
    Description: 
      Low overhead extensible logging framework with dynamic reconfiguration
      capabilities.
    Notes: 
      This is only needed if you use the SMTP appender in log4j.
      
  - File(s): mail.jar
    Project name: JavaMail
    Version: 1.3.3_01
    CVS tag:
    License: Sun Microsystems, Inc. SOFTWARE LICENSE AGREEMENT and ENTITLEMENT for SOFTWARE
    Web site: http://java.sun.com/products/javamail/
    Description: 
      The JavaMail API implementation.
    Notes: 
      
  - File(s): jmx/mx4j.jar, jmx/mx4j-remote.jar, jmx/mx4j-tools.jar
    Project name: MX4J
    Version: 3.0.1
    CVS tag:
    License: MX4J license (http://mx4j.sourceforge.net/docs/ch01s06.html)
    Web site: http://mx4j.sourceforge.net/
    Description:
      MX4J is a project to build an Open Source implementation of the Java(TM)
      Management Extensions (JMX) and of the JMX Remote API (JSR 160)
      specifications, and to build tools relating to JMX.
    Notes: 
      MX4J license is an Apache-like license.

  - File(s): xml-apis.jar
    Project name: Java API for XML Processing (JAXP)
    Version: ?
    CVS tag:
    License: ?
    Web site: ?
    Description:
    Notes: 

  - File(s): dom4j-1.6.1.jar
    Project name: dom4j
    Version: 1.6.1
    CVS tag:
    Web site: http://www.dom4j.org/
    License: BSD style license (http://www.dom4j.org/license.html)
    Description:
       dom4j is an open source library for working with XML, XPath and
       XSLT on the Java platform using the Java Collections Framework
       and with full support for DOM, SAX and JAXP.
    Notes:
       Needed by JMX notifications.

  - File(s): jaxen-1.1-beta-8.jar
    Project name: jaxen
    Version: 1.1-beta-8
    CVS tag:
    Web site: http://www.jaxen.org/
    License: Apache-style (http://cvs.jaxen.codehaus.org/viewrep/~raw,r=1.4/jaxen/jaxen/LICENSE)
    Description:
       The jaxen project is a Java XPath Engine. jaxen is a universal
       object model walker, capable of evaluating XPath expressions
       across multiple models. Currently supported are dom4j, JDOM,
       and DOM.
    Notes: 
       Needed by JMX notifications. License is one of the least
       restrictive licenses around, you can use jaxen to create new
       products without them having to be open source.


Sequoia console:
   
   - File(s): jline.jar
    Project name: JLine
    Version: 0.9.1
    CVS tag:
    License: BSD (http://www.opensource.org/licenses/bsd-license.php)
    Web site: http://jline.sourceforge.net/
    Description: 
       JLine is a Java library for handling console input. It is similar in 
       functionality to BSD editline and GNU readline.It also supports 
       navigation keys.
    Notes: 
             
          
Octopus:

  - File(s): octopus/Octopus.jar, octopus/OctopusGenerator.jar, 
       octopus/csvjdbc.jar, octopus/xercesImpl.jar, octopus/xmlutil.jar,
       octopus/xml/
    Project name: Octopus
    Version: 3.4.1
    CVS tag:
    License: LGPL (http://octopus.objectweb.org/license.html)
    Web site: http://octopus.objectweb.org/
    Description: 
       Octopus is a Java-based Extraction, Transformation, and Loading (ETL) 
       tool. It may connect to any JDBC data sources and perform 
       transformations defined in an XML file.
    Notes: 
      
      
Others:

  - File(s): other/ant.jar
    Project name: Jakarta Ant
    Version: 1.6.5
    CVS tag:
    License: APL 2.0 (http://www.apache.org/licenses/LICENSE-2.0.txt)
    Web site: http://ant.apache.org/
    Description: 
       Java-based build tool.
    Notes:
       Used by the target which generates the shell and bat scripts.

  - File(s): other/standalone-compiler.jar
    Project name: IzPack
    Version: 3.7.2
    CVS tag:
    License: GPL (http://www.gnu.org/copyleft/gpl.html).
    Web site: http://www.izforge.com/izpack/
    Description: 
       Installers generator for the Java platform.
    Notes: 
       The GPL only applies to IzPack source code not on the binary code 
       generated (i.e. the generated installer). Next IzPack version
       should be APL.
