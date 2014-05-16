/**
 * Sequoia: Database clustering technology.
 * Copyright (C) 2002-2004 French National Institute For Research In Computer
 * Science And Control (INRIA).
 * Copyright (C) 2005 AmicoSoft, Inc. dba Emic Networks
 * Copyright (C) 2005-2006 Continuent, Inc.
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
 * Contributor(s): Mathieu Peltier, Sara Bouchenakm Nicolas Modrzyk
 */

package org.continuent.sequoia.controller.xml;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.StringTokenizer;

import javax.management.ObjectName;

import org.continuent.sequoia.common.authentication.AuthenticationManager;
import org.continuent.sequoia.common.authentication.AuthenticationManagerException;
import org.continuent.sequoia.common.exceptions.BackupException;
import org.continuent.sequoia.common.exceptions.VirtualDatabaseException;
import org.continuent.sequoia.common.i18n.Translate;
import org.continuent.sequoia.common.jmx.JmxConstants;
import org.continuent.sequoia.common.log.Trace;
import org.continuent.sequoia.common.sql.filters.MacrosHandler;
import org.continuent.sequoia.common.sql.schema.DatabaseColumn;
import org.continuent.sequoia.common.sql.schema.DatabaseProcedure;
import org.continuent.sequoia.common.sql.schema.DatabaseProcedureParameter;
import org.continuent.sequoia.common.sql.schema.DatabaseProcedureSemantic;
import org.continuent.sequoia.common.sql.schema.DatabaseSchema;
import org.continuent.sequoia.common.sql.schema.DatabaseTable;
import org.continuent.sequoia.common.users.AdminUser;
import org.continuent.sequoia.common.users.DatabaseBackendUser;
import org.continuent.sequoia.common.users.VirtualDatabaseUser;
import org.continuent.sequoia.common.xml.DatabasesXmlTags;
import org.continuent.sequoia.common.xml.XmlValidator;
import org.continuent.sequoia.controller.backend.DatabaseBackend;
import org.continuent.sequoia.controller.backend.DatabaseBackendSchemaConstants;
import org.continuent.sequoia.controller.backend.rewriting.AbstractRewritingRule;
import org.continuent.sequoia.controller.backend.rewriting.PatternRewritingRule;
import org.continuent.sequoia.controller.backend.rewriting.ReplaceAllRewritingRule;
import org.continuent.sequoia.controller.backend.rewriting.SimpleRewritingRule;
import org.continuent.sequoia.controller.backup.BackupManager;
import org.continuent.sequoia.controller.backup.Backuper;
import org.continuent.sequoia.controller.cache.metadata.MetadataCache;
import org.continuent.sequoia.controller.cache.parsing.ParsingCache;
import org.continuent.sequoia.controller.cache.parsing.ParsingCacheControl;
import org.continuent.sequoia.controller.cache.result.AbstractResultCache;
import org.continuent.sequoia.controller.cache.result.CachingGranularities;
import org.continuent.sequoia.controller.cache.result.ResultCacheFactory;
import org.continuent.sequoia.controller.cache.result.ResultCacheRule;
import org.continuent.sequoia.controller.cache.result.rules.EagerCaching;
import org.continuent.sequoia.controller.connection.FailFastPoolConnectionManager;
import org.continuent.sequoia.controller.connection.RandomWaitPoolConnectionManager;
import org.continuent.sequoia.controller.connection.SimpleConnectionManager;
import org.continuent.sequoia.controller.connection.VariablePoolConnectionManager;
import org.continuent.sequoia.controller.core.Controller;
import org.continuent.sequoia.controller.core.ControllerConstants;
import org.continuent.sequoia.controller.jmx.MBeanServerManager;
import org.continuent.sequoia.controller.loadbalancer.AbstractLoadBalancer;
import org.continuent.sequoia.controller.loadbalancer.paralleldb.ParallelDB;
import org.continuent.sequoia.controller.loadbalancer.paralleldb.ParallelDB_LPRF;
import org.continuent.sequoia.controller.loadbalancer.paralleldb.ParallelDB_RR;
import org.continuent.sequoia.controller.loadbalancer.policies.WaitForCompletionPolicy;
import org.continuent.sequoia.controller.loadbalancer.policies.createtable.CreateTableAll;
import org.continuent.sequoia.controller.loadbalancer.policies.createtable.CreateTablePolicy;
import org.continuent.sequoia.controller.loadbalancer.policies.createtable.CreateTableRandom;
import org.continuent.sequoia.controller.loadbalancer.policies.createtable.CreateTableRoundRobin;
import org.continuent.sequoia.controller.loadbalancer.policies.createtable.CreateTableRule;
import org.continuent.sequoia.controller.loadbalancer.policies.errorchecking.ErrorCheckingAll;
import org.continuent.sequoia.controller.loadbalancer.policies.errorchecking.ErrorCheckingPolicy;
import org.continuent.sequoia.controller.loadbalancer.policies.errorchecking.ErrorCheckingRandom;
import org.continuent.sequoia.controller.loadbalancer.policies.errorchecking.ErrorCheckingRoundRobin;
import org.continuent.sequoia.controller.loadbalancer.raidb0.RAIDb0;
import org.continuent.sequoia.controller.loadbalancer.raidb1.RAIDb1_LPRF;
import org.continuent.sequoia.controller.loadbalancer.raidb1.RAIDb1_RR;
import org.continuent.sequoia.controller.loadbalancer.raidb1.RAIDb1_WRR;
import org.continuent.sequoia.controller.loadbalancer.raidb1.RAIDb1ec_RR;
import org.continuent.sequoia.controller.loadbalancer.raidb1.RAIDb1ec_WRR;
import org.continuent.sequoia.controller.loadbalancer.raidb2.RAIDb2_LPRF;
import org.continuent.sequoia.controller.loadbalancer.raidb2.RAIDb2_RR;
import org.continuent.sequoia.controller.loadbalancer.raidb2.RAIDb2_WRR;
import org.continuent.sequoia.controller.loadbalancer.raidb2.RAIDb2ec_RR;
import org.continuent.sequoia.controller.loadbalancer.raidb2.RAIDb2ec_WRR;
import org.continuent.sequoia.controller.loadbalancer.singledb.SingleDB;
import org.continuent.sequoia.controller.monitoring.SQLMonitoring;
import org.continuent.sequoia.controller.monitoring.SQLMonitoringRule;
import org.continuent.sequoia.controller.recoverylog.RecoveryLog;
import org.continuent.sequoia.controller.recoverylog.RecoveryLogControl;
import org.continuent.sequoia.controller.requestmanager.RAIDbLevels;
import org.continuent.sequoia.controller.requestmanager.RequestManager;
import org.continuent.sequoia.controller.requestmanager.distributed.RAIDb1DistributedRequestManager;
import org.continuent.sequoia.controller.requestmanager.distributed.RAIDb2DistributedRequestManager;
import org.continuent.sequoia.controller.scheduler.AbstractScheduler;
import org.continuent.sequoia.controller.scheduler.AbstractSchedulerControl;
import org.continuent.sequoia.controller.scheduler.raidb0.RAIDb0PassThroughLevelScheduler;
import org.continuent.sequoia.controller.scheduler.raidb0.RAIDb0PessimisticTransactionLevelScheduler;
import org.continuent.sequoia.controller.scheduler.raidb1.RAIDb1OptimisticQueryLevelScheduler;
import org.continuent.sequoia.controller.scheduler.raidb1.RAIDb1OptimisticTransactionLevelScheduler;
import org.continuent.sequoia.controller.scheduler.raidb1.RAIDb1PassThroughScheduler;
import org.continuent.sequoia.controller.scheduler.raidb1.RAIDb1PessimisticTransactionLevelScheduler;
import org.continuent.sequoia.controller.scheduler.raidb1.RAIDb1QueryLevelScheduler;
import org.continuent.sequoia.controller.scheduler.raidb2.RAIDb2PassThroughScheduler;
import org.continuent.sequoia.controller.scheduler.raidb2.RAIDb2PessimisticTransactionLevelScheduler;
import org.continuent.sequoia.controller.scheduler.raidb2.RAIDb2QueryLevelScheduler;
import org.continuent.sequoia.controller.scheduler.singledb.SingleDBPassThroughScheduler;
import org.continuent.sequoia.controller.scheduler.singledb.SingleDBPessimisticTransactionLevelScheduler;
import org.continuent.sequoia.controller.virtualdatabase.DistributedVirtualDatabase;
import org.continuent.sequoia.controller.virtualdatabase.VirtualDatabase;
import org.continuent.sequoia.controller.virtualdatabase.protocol.MessageTimeouts;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;
import org.xml.sax.helpers.XMLReaderFactory;

/**
 * Parses an XML content conforming to sequoia.dtd and configure the given
 * Sequoia Controller accordingly.
 * 
 * @author <a href="mailto:Emmanuel.Cecchet@inria.fr">Emmanuel Cecchet </a>
 * @author <a href="mailto:Mathieu.Peltier@inrialpes.fr">Mathieu Peltier </a>
 * @author <a href="mailto:Nicolas.Modrzyk@inrialpes.fr">Nicolas Modrzyk </a>
 * @version 1.0
 */
public class DatabasesParser extends DefaultHandler
{

  private static final WaitForCompletionPolicy DEFAULT_WAIT_FOR_ALL_COMPLETION = new WaitForCompletionPolicy(
                                                                                   WaitForCompletionPolicy.ALL,
                                                                                   false,
                                                                                   false,
                                                                                   30000);

  /** XML parser. */
  private XMLReader                            parser;

  /** Logger instance. */
  static Trace                                 logger                          = Trace
                                                                                   .getLogger(DatabasesParser.class
                                                                                       .getName());
  static Trace                                 endUserLogger                   = Trace
                                                                                   .getLogger("org.continuent.sequoia.enduser");

  /** Sequoia controller to setup. */
  private Controller                           controller;

  /** dbToPrepare is used if only a specified database has to be loaded */
  private Hashtable                            dbToPrepare                     = null;
  /** setter for jumping from one VirtualDatabase definition to the next one */
  private boolean                              skipDatabase                    = false;

  /**
   * Parsing of Users are only defined in Admin at the moment, but may be
   * defined somewhere else in the future.
   */
  private boolean                              parsingAdminUsers               = false;

  private VirtualDatabase                      currentVirtualDatabase          = null;
  private BackupManager                        currentBackupManager            = null;
  private DatabaseBackend                      currentBackend;
  private DatabaseBackendUser                  currentDatabaseBackendUser;
  private AuthenticationManager                currentAuthenticationManager;
  private AbstractScheduler                    currentRequestScheduler;
  private AbstractResultCache                  currentResultCache;
  private MetadataCache                        currentMetadataCache            = null;
  private ParsingCache                         currentParsingCache             = null;
  private ResultCacheRule                      currentResultCacheRule;
  private MacrosHandler                        currentMacroHandler;
  private AbstractLoadBalancer                 currentLoadBalancer;
  private RecoveryLog                          currentRecoveryLog;
  private VirtualDatabaseUser                  currentVirtualUser;
  private DatabaseSchema                       currentDatabaseSchema;
  private DatabaseTable                        currentTable;
  private DatabaseProcedure                    currentProcedure;
  private int                                  numberOfColumns;

  private String                               connectionManagerVLogin;
  private WaitForCompletionPolicy              currentWaitForCompletionPolicy  = DEFAULT_WAIT_FOR_ALL_COMPLETION;

  private long                                 beginTimeout;
  private long                                 commitTimeout;
  private long                                 rollbackTimeout;
  private int                                  requestTimeout;

  private boolean                              caseSensitiveParsing;

  private CreateTablePolicy                    currentCreateTablePolicy;
  private CreateTableRule                      currentCreateTableRule;
  private ArrayList                            backendNameList;
  private ErrorCheckingPolicy                  currentErrorCheckingPolicy;

  private int                                  currentNbOfConcurrentReads;

  private int                                  defaultTransactionIsolation;

  /**
   * We are setting the default connection manager rather than a 'normal'
   * connection manager.
   */
  private boolean                              settingDefaultConnectionManager;

  // VDB load options
  private boolean                              initialize;
  private boolean                              forceload;

  /**
   * Creates a new <code>DatabasesParser</code> instance. This method
   * Instanciates also a new <code>DatabasesParser</code>.
   * 
   * @param controller <code>Controller</code> to load the Virtual Database
   *          into
   * @throws SAXException if an error occurs
   */
  public DatabasesParser(Controller controller) throws SAXException
  {
    prepareHandler(controller);
  }

  /**
   * Creates a new <code>DatabasesParser</code> instance. This method
   * Instanciates also a new <code>DatabasesParser</code>. This instance will
   * look only for the specified database.
   * 
   * @param controller <code>Controller</code> to load the Virtual Database
   *          into
   * @param virtualName the specified <code>VirtualDatabase</code> to load.
   * @param autoLoad autoenable switch
   * @param checkPoint checkpoint information
   * @throws SAXException if an error occurs
   */
  public DatabasesParser(Controller controller, String virtualName,
      int autoLoad, String checkPoint) throws SAXException
  {
    prepareHandler(controller);
    initialize = autoLoad == ControllerConstants.AUTO_ENABLE_INIT;
    forceload = autoLoad == ControllerConstants.AUTO_ENABLE_FORCE_LOAD;

    // Test if a name has been specified. Otherwise skip.
    if (virtualName != null)
      prepareDB(virtualName, autoLoad, checkPoint);
  }

  private void prepareHandler(Controller controller) throws SAXException
  {
    // Instantiate a new parser
    parser = XMLReaderFactory.createXMLReader();

    this.controller = controller;

    // Activate validation
    parser.setFeature("http://xml.org/sax/features/validation", true);

    // Install error handler
    parser.setErrorHandler(this);

    // Install document handler
    parser.setContentHandler(this);

    // Install local entity resolver
    parser.setEntityResolver(this);
  }

  /**
   * Parses an XML content according to Sequoia DTD.
   * 
   * @param xml a <code>String</code> containing the XML content to parse
   * @exception SAXException if an error occurs
   * @exception IOException if an error occurs
   */
  public void readXML(String xml) throws IOException, SAXException
  {
    InputSource input = new InputSource(new StringReader(xml));
    parser.parse(input);
  }

  /**
   * Validate an XML content according to Sequoia DTD.
   * 
   * @param xml content
   * @param validateBeforeParsing if true validate the document before the
   *          parsing
   * @throws IOException if an error occurs
   * @throws SAXException if an error occurs
   */
  public void readXML(String xml, boolean validateBeforeParsing)
      throws IOException, SAXException
  {
    if (validateBeforeParsing)
    {
      XmlValidator validator = new XmlValidator(
          ControllerConstants.SEQUOIA_DTD_FILE, xml);
      if (logger.isDebugEnabled())
      {
        if (validator.isDtdValid())
          logger.debug(Translate.get("virtualdatabase.xml.dtd.validated"));
        if (validator.isXmlValid())
          logger.debug(Translate.get("virtualdatabase.xml.document.validated"));
      }

      if (validator.getWarnings().size() > 0)
      {
        ArrayList warnings = validator.getWarnings();
        for (int i = 0; i < warnings.size(); i++)
          logger.warn(Translate.get("virtualdatabase.xml.parsing.warning",
              warnings.get(i)));
      }

      if (!validator.isDtdValid())
        logger.error(Translate.get("virtualdatabase.xml.dtd.not.validated"));
      if (!validator.isXmlValid())
        logger.error(Translate
            .get("virtualdatabase.xml.document.not.validated"));

      ArrayList errors = validator.getExceptions();
      for (int i = 0; i < errors.size(); i++)
        logger.error(((Exception) errors.get(i)).getMessage());

      if (!validator.isValid())
        throw new SAXException(Translate
            .get("virtualdatabase.xml.document.not.valid"));
    }
    this.readXML(xml);
  }

  /**
   * Handles notification of a non-recoverable parser error.
   * 
   * @param e the warning information encoded as an exception.
   * @exception SAXException any SAX exception, possibly wrapping another
   *              exception.
   */
  public void fatalError(SAXParseException e) throws SAXException
  {
    String msg = Translate.get("virtualdatabase.xml.parsing.fatal",
        new String[]{e.getPublicId(), String.valueOf(e.getLineNumber()),
            String.valueOf(e.getColumnNumber()), e.getMessage()});
    logger.error(msg);
    endUserLogger.fatal(msg);
    throw e;
  }

  /**
   * Handles notification of a recoverable parser error.
   * 
   * @param e the warning information encoded as an exception.
   * @exception SAXException any SAX exception, possibly wrapping another
   *              exception
   */
  public void error(SAXParseException e) throws SAXException
  {
    logger.error(Translate.get("virtualdatabase.xml.parsing.error",
        new String[]{e.getPublicId(), String.valueOf(e.getLineNumber()),
            String.valueOf(e.getColumnNumber()), e.getMessage()}));
    throw e;
  }

  /**
   * Allows to parse the document with a local copy of the DTD whatever the
   * original <code>DOCTYPE</code> found. Warning, this method is called only
   * if the XML document contains a <code>DOCTYPE</code>.
   * 
   * @see org.xml.sax.EntityResolver#resolveEntity(java.lang.String,
   *      java.lang.String)
   */
  public InputSource resolveEntity(String publicId, String systemId)
      throws SAXException
  {
    InputStream stream = DatabasesXmlTags.class.getResourceAsStream("/"
        + ControllerConstants.SEQUOIA_DTD_FILE);
    if (stream == null)
      throw new SAXException("Cannot find Sequoia DTD file '"
          + ControllerConstants.SEQUOIA_DTD_FILE + "' in classpath");

    return new InputSource(stream);
  }

  /**
   * If this method is called. Only the specified DB of the Xml file will be
   * loaded.
   * 
   * @param virtualName <code>VirtualDatabase</code> name
   * @param autoLoad autoenable switch
   * @param checkPoint checkpoint for recovery
   */
  public void prepareDB(String virtualName, int autoLoad, String checkPoint)
  {
    dbToPrepare = new Hashtable(3);
    dbToPrepare.put("virtualName", virtualName);
    dbToPrepare.put("autoEnable", String.valueOf(autoLoad));
    dbToPrepare.put("checkPoint", checkPoint);
  }

  /**
   * Initializes parsing of a document.
   * 
   * @exception SAXException unspecialized error
   */
  public void startDocument() throws SAXException
  {
    logger.info(Translate.get("virtualdatabase.xml.start"));
  }

  /**
   * Finalizes parsing of a document.
   * 
   * @exception SAXException unspecialized error
   */
  public void endDocument() throws SAXException
  {
    logger.info(Translate.get("virtualdatabase.xml.done"));
  }

  /**
   * Analyzes an element first line.
   * 
   * @param uri name space URI
   * @param localName local name
   * @param name element raw name
   * @param atts element attributes
   * @exception SAXException if an error occurs
   */
  public void startElement(String uri, String localName, String name,
      Attributes atts) throws SAXException
  {
    logger.debug(Translate.get("virtualdatabase.xml.parsing.start", name));

    // Virtual database
    if (name.equals(DatabasesXmlTags.ELT_VirtualDatabase))
    {
      if (dbToPrepare == null)
      {
        // Prepare all databases
        newVirtualDatabase(atts);
      }
      else
      {
        // Only prepare one database
        String virtualName = atts.getValue(DatabasesXmlTags.ATT_name);
        if (virtualName.equalsIgnoreCase((String) dbToPrepare
            .get("virtualName")))
        {
          // This is the database that we want to prepare
          skipDatabase = false;
          newVirtualDatabase(atts);
        }
        else
        {
          // Skip to next one
          skipDatabase = true;
        }
      }
    }
    // Skip to next definition of a virtualDatabase ?
    if (skipDatabase)
      return;

    // Distribution
    else if (name.equals(DatabasesXmlTags.ELT_Distribution))
      newDistribution(atts);
    else if (name.equals(DatabasesXmlTags.ELT_MessageTimeouts))
      newMessageTimeouts(atts);

    // Monitoring
    else if (name.equals(DatabasesXmlTags.ELT_SQLMonitoring))
      newSQLMonitoring(atts);
    else if (name.equals(DatabasesXmlTags.ELT_SQLMonitoringRule))
      newSQLMonitoringRule(atts);

    // Backup
    else if (name.equals(DatabasesXmlTags.ELT_Backup))
      newBackupManager();
    else if (name.equals(DatabasesXmlTags.ELT_Backuper))
      newBackuper(atts);

    // Database backend
    else if (name.equals(DatabasesXmlTags.ELT_DatabaseBackend))
      newDatabaseBackend(atts);
    else if (name.equals(DatabasesXmlTags.ELT_RewritingRule))
      newRewritingRule(atts);

    // Authentication manager
    else if (name.equals(DatabasesXmlTags.ELT_AuthenticationManager))
      newAuthenticationManager(atts);
    else if (name.equals(DatabasesXmlTags.ELT_Admin))
      parsingAdminUsers = true;
    else if (name.equals(DatabasesXmlTags.ELT_User) && parsingAdminUsers)
      newAdminUser(atts);
    else if (name.equals(DatabasesXmlTags.ELT_VirtualLogin))
      newVirtualLogin(atts);

    // Function Broadcast List
    else if (name.equals(DatabasesXmlTags.ELT_FunctionBroadcastList))
      newFunctionList(atts);

    // Request manager
    else if (name.equals(DatabasesXmlTags.ELT_RequestManager))
      newRequestManager(atts);

    // Macro Handler
    else if (name.equals(DatabasesXmlTags.ELT_MacroHandling))
      newMacroHandler(atts);

    // Request schedulers
    else if (name.equals(DatabasesXmlTags.ELT_SingleDBScheduler))
      newSingleDBScheduler(atts);
    else if (name.equals(DatabasesXmlTags.ELT_RAIDb0Scheduler))
      newRAIDb0Scheduler(atts);
    else if (name.equals(DatabasesXmlTags.ELT_RAIDb1Scheduler))
      newRAIDb1Scheduler(atts);
    else if (name.equals(DatabasesXmlTags.ELT_RAIDb2Scheduler))
      newRAIDb2Scheduler(atts);

    // Request caches
    else if (name.equals(DatabasesXmlTags.ELT_MetadataCache))
      newMetadataCache(atts);
    else if (name.equals(DatabasesXmlTags.ELT_ParsingCache))
      newParsingCache(atts);
    else if (name.equals(DatabasesXmlTags.ELT_ResultCache))
      newResultCache(atts);
    else if (name.equals(DatabasesXmlTags.ELT_DefaultResultCacheRule))
      newDefaultResultCacheRule(atts);
    else if (name.equals(DatabasesXmlTags.ELT_ResultCacheRule))
      newResultCacheRule(atts);
    else if (name.equals(DatabasesXmlTags.ELT_NoCaching))
      currentResultCacheRule.setCacheBehavior(ResultCacheFactory
          .getCacheBehaviorInstance(DatabasesXmlTags.ELT_NoCaching, null));
    else if (name.equals(DatabasesXmlTags.ELT_EagerCaching))
      newEagerCaching(atts);
    else if (name.equals(DatabasesXmlTags.ELT_RelaxedCaching))
      newRelaxedCaching(atts);

    // Request load balancers
    else if (name.equals(DatabasesXmlTags.ELT_LoadBalancer))
      newLoadBalancer(atts);
    else if (name.equals(DatabasesXmlTags.ELT_SingleDB))
      newSingleDBRequestLoadBalancer();
    else if (name.equals(DatabasesXmlTags.ELT_ParallelDB_RoundRobin))
      newParallelDBRoundRobinLoadBalancer();
    else if (name
        .equals(DatabasesXmlTags.ELT_ParallelDB_LeastPendingRequestsFirst))
      newParallelDBLeastPendingRequestsFirst();
    else if (name.equals(DatabasesXmlTags.ELT_RAIDb_0))
      newRAIDb0LoadBalancer();
    else if (name.equals(DatabasesXmlTags.ELT_RAIDb_1))
      newRAIDb1LoadBalancer();
    else if (name.equals(DatabasesXmlTags.ELT_RAIDb_1_RoundRobin))
      newRAIDb1RoundRobinLoadBalancer();
    else if (name.equals(DatabasesXmlTags.ELT_RAIDb_1_WeightedRoundRobin))
      newRAIDb1WeightedRoundRobinLoadBalancer();
    else if (name
        .equals(DatabasesXmlTags.ELT_RAIDb_1_LeastPendingRequestsFirst))
      newRAIDb1LeastPendingRequestsFirst();
    else if (name.equals(DatabasesXmlTags.ELT_RAIDb_1ec))
      newRAIDb1ecLoadBalancer(atts);
    else if (name.equals(DatabasesXmlTags.ELT_RAIDb_1ec_RoundRobin))
      newRAIDb1ecRoundRobinLoadBalancer();
    else if (name.equals(DatabasesXmlTags.ELT_RAIDb_1ec_WeightedRoundRobin))
      newRAIDb1ecWeightedRoundRobinLoadBalancer();
    else if (name.equals(DatabasesXmlTags.ELT_RAIDb_2))
      newRAIDb2LoadBalancer();
    else if (name.equals(DatabasesXmlTags.ELT_RAIDb_2_RoundRobin))
      newRAIDb2RoundRobinLoadBalancer();
    else if (name.equals(DatabasesXmlTags.ELT_RAIDb_2_WeightedRoundRobin))
      newRAIDb2WeightedRoundRobinLoadBalancer();
    else if (name
        .equals(DatabasesXmlTags.ELT_RAIDb_2_LeastPendingRequestsFirst))
      newRAIDb2LeastPendingRequestsFirst();
    else if (name.equals(DatabasesXmlTags.ELT_RAIDb_2ec))
      newRAIDb2ecLoadBalancer();
    else if (name.equals(DatabasesXmlTags.ELT_RAIDb_2ec_RoundRobin))
      newRAIDb2ecRoundRobinLoadBalancer();
    else if (name.equals(DatabasesXmlTags.ELT_RAIDb_2ec_WeightedRoundRobin))
      newRAIDb2ecWeightedRoundRobinLoadBalancer();

    // Policies
    else if (name.equals(DatabasesXmlTags.ELT_WaitForCompletion))
      newWaitForCompletion(atts);
    else if (name.equals(DatabasesXmlTags.ELT_ErrorChecking))
      newErrorChecking(atts);
    else if (name.equals(DatabasesXmlTags.ELT_CreateTable))
      newCreateTable(atts);
    else if (name.equals(DatabasesXmlTags.ELT_BackendName))
      newBackendName(atts);
    else if (name.equals(DatabasesXmlTags.ELT_BackendWeight))
      newBackendWeight(atts);

    // Recovery log
    else if (name.equals(DatabasesXmlTags.ELT_RecoveryLog))
      newRecoveryLog(atts);
    else if (name.equals(DatabasesXmlTags.ELT_RecoveryLogTable))
      newRecoveryLogTable(atts);
    else if (name.equals(DatabasesXmlTags.ELT_CheckpointTable))
      newRecoveryCheckpointTable(atts);
    else if (name.equals(DatabasesXmlTags.ELT_BackendTable))
      newRecoveryBackendTable(atts);
    else if (name.equals(DatabasesXmlTags.ELT_DumpTable))
      newRecoveryDumpTable(atts);

    // Connection managers
    else if (name.equals(DatabasesXmlTags.ELT_ConnectionManager))
      newConnectionManager(atts);
    else if (name.equals(DatabasesXmlTags.ELT_DefaultConnectionManager))
      newDefaultConnectionManager();
    else if (name.equals(DatabasesXmlTags.ELT_SimpleConnectionManager))
      newSimpleConnectionManager();
    else if (name.equals(DatabasesXmlTags.ELT_FailFastPoolConnectionManager))
      newFailFastPoolConnectionManager(atts);
    else if (name.equals(DatabasesXmlTags.ELT_RandomWaitPoolConnectionManager))
      newRandomWaitPoolConnectionManager(atts);
    else if (name.equals(DatabasesXmlTags.ELT_VariablePoolConnectionManager))
      newVariablePoolConnectionManager(atts);

    // Database schema
    else if (name.equals(DatabasesXmlTags.ELT_DatabaseSchema))
      newDatabaseSchema(atts);
    else if (name.equals(DatabasesXmlTags.ELT_DefaultStoredProcedureSemantic))
      newDefaultStoredProcedureSemantic(atts);
    else if (name.equals(DatabasesXmlTags.ELT_StoredProcedureSemantic))
      newStoredProcedureSemantic(atts);
    else if (name.equals(DatabasesXmlTags.ELT_DatabaseStaticSchema))
    {
      if (currentBackend.getDynamicPrecision() != DatabaseBackendSchemaConstants.DynamicPrecisionStatic)
      {
        String msg = Translate.get(
            "virtualdatabase.xml.schema.static.incompatible.dynamic",
            currentBackend.getName());
        logger.error(msg);
        throw new SAXException(msg);
      }
      currentDatabaseSchema = new DatabaseSchema(currentVirtualDatabase
          .getVirtualDatabaseName());
    }

    // Database table (inside a DatabaseSchema)
    else if (name.equals(DatabasesXmlTags.ELT_DatabaseTable))
      newDatabaseTable(atts);

    // Table column (inside a DatabaseSchema/DatabaseTable)
    else if (name.equals(DatabasesXmlTags.ELT_DatabaseColumn))
      newDatabaseColumn(atts);

    else if (name.equals(DatabasesXmlTags.ELT_DatabaseProcedure))
      newDatabaseProcedure(atts);
    else if (name.equals(DatabasesXmlTags.ELT_DatabaseProcedureColumn))
      newDatabaseProcedureColumn(atts);
  }

  private void newFunctionList(Attributes atts)
  {
    String functionsList = atts.getValue(DatabasesXmlTags.ATT_functionList);
    if (functionsList != null)
    {
      List functionsToBroadcast = new ArrayList();
      StringTokenizer st = new StringTokenizer(functionsList, ",");
      while (st.hasMoreTokens())
      {
        functionsToBroadcast.add(st.nextToken());
      }
      currentVirtualDatabase.setFunctionsToBroadcastList(functionsToBroadcast);
    }

  }

  /**
   * DatabasesParser for end of element.
   * 
   * @param uri name space URI
   * @param localName local name
   * @param name element raw name
   * @exception SAXException if an error occurs
   */
  public void endElement(String uri, String localName, String name)
      throws SAXException
  {
    logger.debug(Translate.get("virtualdatabase.xml.parsing.end", name));
    // Test if skip is needed
    if (skipDatabase)
      return;

    // Virtual database
    if (name.equals(DatabasesXmlTags.ELT_VirtualDatabase))
    {
      if (logger.isDebugEnabled())
        logger.debug(Translate.get("virtualdatabase.xml.add.virtualdatabase"));

      if (currentVirtualDatabase != null)
      {
        try
        {
          RecoveryLog recoveryLog = currentVirtualDatabase.getRequestManager()
              .getRecoveryLog();
          if (recoveryLog != null)
          {
            if (initialize)
            {
              recoveryLog.resetRecoveryLog(true);
              recoveryLog.setLastManDown();
            }
            if (forceload)
            {
              recoveryLog.clearLastManDown();
              recoveryLog.setLastManDown();
            }
          }
          if (currentVirtualDatabase instanceof DistributedVirtualDatabase)
            ((DistributedVirtualDatabase) currentVirtualDatabase)
                .joinGroup(true);
          else
            currentVirtualDatabase.getRequestManager()
                .initBackendsLastKnownCheckpointFromRecoveryLog();
          if (dbToPrepare == null)
          { // Just add the virtual database without auto-enabling backends
            controller.addVirtualDatabase(currentVirtualDatabase);
          }
          else
          {
            int autoLoad = Integer.parseInt((String) dbToPrepare
                .get("autoEnable"));
            String checkPoint = (String) dbToPrepare.get("checkPoint");
            // checkPoint is store as "" in Hashtable
            // but methods to enable backend requires checkPoint to be null
            // if no recovery from checkpoint
            checkPoint = checkPoint.equalsIgnoreCase("") ? null : checkPoint;
            controller.addVirtualDatabase(currentVirtualDatabase, autoLoad,
                checkPoint);
          }
        }
        catch (Exception e)
        {
          unregisterRelatedMBeans(currentVirtualDatabase);

          String msg = Translate.get("controller.add.virtualdatabase.failed",
              new String[]{currentVirtualDatabase.getVirtualDatabaseName(),
                  e.getMessage()});
          if (logger.isErrorEnabled())
          {
            logger.error(msg, e);
          }
          throw new SAXException(msg);
        }
      }
      currentVirtualDatabase = null;
    }

    // Request manager
    else if (name.equals(DatabasesXmlTags.ELT_RequestManager))
    {
      if (logger.isDebugEnabled())
        logger.debug(Translate.get("virtualdatabase.xml.requestmanager.set"));

      if (currentVirtualDatabase != null)
      {
        RequestManager requestManager = null;

        // We consider that SingleDB and ParallelDB balancers don't need macros
        // handler
        if (currentLoadBalancer == null)
          throw new SAXException("virtualdatabase.xml.loadbalancer.not.set");
        if (!(currentLoadBalancer instanceof SingleDB || currentLoadBalancer instanceof ParallelDB))
        {
          // If no macros handling has been specified, create a default one
          // based one the dtd default values
          if (currentMacroHandler == null)
            currentMacroHandler = new MacrosHandler(MacrosHandler.RAND_FLOAT,
                1000, MacrosHandler.DATE_TIMESTAMP, MacrosHandler.DATE_DATE,
                MacrosHandler.DATE_TIME, MacrosHandler.DATE_TIMESTAMP,
                MacrosHandler.DATE_TIMESTAMP);
          currentLoadBalancer.setMacroHandler(currentMacroHandler);
        }

        try
        {
          if (currentVirtualDatabase.isDistributed())
          {
            switch (currentLoadBalancer.getRAIDbLevel())
            {
              case RAIDbLevels.SingleDB :
                String smsg = Translate.get(
                    "virtualdatabase.xml.no.single.distributed.requestmanager",
                    currentLoadBalancer.getRAIDbLevel());
                logger.error(smsg);
                throw new SAXException(smsg);
              case RAIDbLevels.RAIDb1 :
                requestManager = new RAIDb1DistributedRequestManager(
                    (DistributedVirtualDatabase) currentVirtualDatabase,
                    currentRequestScheduler, currentResultCache,
                    currentLoadBalancer, currentRecoveryLog, beginTimeout,
                    commitTimeout, rollbackTimeout);
                break;
              case RAIDbLevels.RAIDb2 :
                requestManager = new RAIDb2DistributedRequestManager(
                    (DistributedVirtualDatabase) currentVirtualDatabase,
                    currentRequestScheduler, currentResultCache,
                    currentLoadBalancer, currentRecoveryLog, beginTimeout,
                    commitTimeout, rollbackTimeout);
                break;
              default :
                String msg = Translate.get(
                    "virtualdatabase.xml.no.distributed.requestmanager",
                    currentLoadBalancer.getRAIDbLevel());
                logger.error(msg);
                throw new SAXException(msg);
            }
          }
          else
            requestManager = new RequestManager(currentVirtualDatabase,
                currentRequestScheduler, currentResultCache,
                currentLoadBalancer, currentRecoveryLog, beginTimeout,
                commitTimeout, rollbackTimeout);

          if (requestManager != null)
          {
            if (currentParsingCache != null)
              requestManager.setParsingCache(currentParsingCache);
            if (currentMetadataCache != null)
              requestManager.setMetadataCache(currentMetadataCache);
            requestManager.setCaseSensitiveParsing(caseSensitiveParsing);
          }

          currentVirtualDatabase.setRequestManager(requestManager);
          if (currentBackupManager == null)
            currentBackupManager = new BackupManager();
          requestManager.setBackupManager(currentBackupManager);
        }
        catch (Exception e)
        {
          String msg = Translate
              .get("virtualdatabase.xml.requestmanager.creation.failed");
          logger.error(msg, e);
          throw new SAXException(msg, e);
        }
      }
      if (currentRequestScheduler != null)
      {
        try
        {
          MBeanServerManager.registerMBean(new AbstractSchedulerControl(
              currentRequestScheduler), JmxConstants
              .getAbstractSchedulerObjectName(currentVirtualDatabase
                  .getVirtualDatabaseName()));
        }
        catch (Exception e)
        {
          if (logger.isWarnEnabled())
          {
            logger
                .warn(
                    Translate
                        .get(
                            "virtualdatabase.xml.scheduler.jmx.failed", currentVirtualDatabase.getVirtualDatabaseName()), e); //$NON-NLS-1$
          }
        }
        if (currentParsingCache != null)
        {
          try
          {
            MBeanServerManager.registerMBean(new ParsingCacheControl(
                currentParsingCache), JmxConstants
                .getParsingCacheObjectName(currentVirtualDatabase
                    .getVirtualDatabaseName()));
          }
          catch (Exception e)
          {
            if (logger.isWarnEnabled())
            {
              logger
                  .warn(
                      Translate
                          .get(
                              "virtualdatabase.xml.parsing.cache.jmx.failed", currentVirtualDatabase.getVirtualDatabaseName()), e); //$NON-NLS-1$
            }
          }
        }
      }
    }

    // Database backend
    else if (name.equals(DatabasesXmlTags.ELT_DatabaseBackend))
    {
      if (currentBackend != null)
      {
        try
        {
          currentVirtualDatabase.addBackend(currentBackend, false);
        }
        catch (Exception e)
        {
          String msg = Translate.get("virtualdatabase.xml.backend.add.failed");
          logger.error(msg, e);
          throw new SAXException(msg, e);
        }
      }
      currentBackend = null;
    }

    // Authentication manager
    else if (name.equals(DatabasesXmlTags.ELT_AuthenticationManager))
    {
      if (currentVirtualDatabase != null)
      {
        currentVirtualDatabase
            .setAuthenticationManager(currentAuthenticationManager);
      }
    }

    // Request cache
    else if (name.equals(DatabasesXmlTags.ELT_RequestCache))
    {
      if (currentResultCache != null)
      { // Set default result cache rule if missing
        if (currentResultCache.getDefaultRule() == null)
        {
          ResultCacheRule defaultRule = null;
          defaultRule = new ResultCacheRule("", false, false, 1000);
          defaultRule.setCacheBehavior(new EagerCaching(0));
          currentResultCache.setDefaultRule(defaultRule);
        }
      }
    }
    else if (name.equals(DatabasesXmlTags.ELT_DefaultResultCacheRule))
    {
      currentResultCache.setDefaultRule(currentResultCacheRule);
    }

    // Database schema
    else if (name.equals(DatabasesXmlTags.ELT_DatabaseStaticSchema))
    {
      if (currentDatabaseSchema != null)
      {
        if (currentBackend != null)
        {
          try
          {
            currentBackend.setDatabaseSchema(currentDatabaseSchema, true);
          }
          catch (Exception e)
          {
            logger.error(Translate
                .get("virtualdatabase.xml.backend.set.schema.failed"), e);
          }
        }
        else
        {
          try
          {
            currentVirtualDatabase
                .setStaticDatabaseSchema(currentDatabaseSchema);
          }
          catch (Exception e)
          {
            logger.error(Translate
                .get("virtualdatabase.xml.virtualdatabase.set.schema.failed"),
                e);
          }
        }
        currentDatabaseSchema = null;
      }
    }

    // Database table
    else if (name.equals(DatabasesXmlTags.ELT_DatabaseTable))
    {
      if (currentTable != null)
      {
        try
        {
          ArrayList cols = currentTable.getColumns();
          if (cols == null)
            logger.warn(Translate.get("virtualdatabase.xml.table.no.column",
                currentTable.getName()));
          else if (cols.size() != numberOfColumns)
            logger.warn(Translate.get(
                "virtualdatabase.xml.table.column.mismatch", new String[]{
                    String.valueOf(numberOfColumns), currentTable.getName(),
                    String.valueOf(cols.size())}));

          currentDatabaseSchema.addTable(currentTable);
          if (logger.isDebugEnabled())
            logger.debug(Translate.get("virtualdatabase.xml.table.add",
                currentTable.getName()));
        }
        catch (Exception e)
        {
          logger
              .error(Translate.get("virtualdatabase.xml.table.add.failed"), e);
        }
        currentTable = null;
      }
    }

    else if (name.equals(DatabasesXmlTags.ELT_DatabaseProcedure))
    {
      if (currentProcedure != null)
      {
        try
        {

          currentDatabaseSchema.addProcedure(currentProcedure);
          if (logger.isDebugEnabled())
            logger.debug(Translate.get("virtualdatabase.xml.procedure.add",
                currentProcedure.getName()));
        }
        catch (Exception e)
        {
          logger.error(Translate
              .get("virtualdatabase.xml.procedure.add.failed"), e);
        }
        currentProcedure = null;
      }
    }

    // CreateTable rule
    else if (name.equals(DatabasesXmlTags.ELT_CreateTable))
    {
      if (currentCreateTablePolicy != null)
      {
        if (logger.isDebugEnabled())
          logger.debug(Translate.get("virtualdatabase.xml.create.table.add",
              currentCreateTableRule.getInformation()));
        currentCreateTablePolicy.addRule(currentCreateTableRule);
      }
    }

    else if (name.equals(DatabasesXmlTags.ELT_LoadBalancer))
    {
      if (logger.isDebugEnabled())
        logger.debug(Translate.get(
            "virtualdatabase.xml.loadbalancer.transaction.isolation.set",
            defaultTransactionIsolation));

      currentLoadBalancer
          .setDefaultTransactionIsolationLevel(defaultTransactionIsolation);
    }

    // RAIDb-0 load balancer
    else if (name.equals(DatabasesXmlTags.ELT_RAIDb_0))
    {
      if (logger.isDebugEnabled())
        logger.debug(Translate
            .get("virtualdatabase.xml.loadbalancer.raidb0.set"));

      if (currentCreateTablePolicy.getDefaultRule() == null)
      {
        if (logger.isDebugEnabled())
          logger.debug(Translate
              .get("virtualdatabase.xml.create.table.default"));
        CreateTableRule rule = new CreateTableRoundRobin();
        currentCreateTablePolicy.addRule(rule);
      }
      try
      {
        currentLoadBalancer = new RAIDb0(currentVirtualDatabase,
            currentCreateTablePolicy);
      }
      catch (Exception e)
      {
        String msg = Translate
            .get("virtualdatabase.xml.loadbalancer.raidb0.failed");
        logger.error(msg, e);
        throw new SAXException(msg, e);
      }
    }

    // Recovery Log
    else if (name.equals(DatabasesXmlTags.ELT_RecoveryLog))
    {
      if (logger.isDebugEnabled())
        logger.debug(Translate
            .get("virtualdatabase.xml.recoverylog.cheking.tables"));
      try
      {
        currentRecoveryLog.checkRecoveryLogTables();
      }
      catch (Exception e)
      {
        String msg = Translate
            .get("virtualdatabase.xml.recoverylog.cheking.tables.failed");
        logger.error(msg, e);
        throw new SAXException(msg);
      }
      try
      {
        MBeanServerManager.registerMBean(new RecoveryLogControl(
            currentRecoveryLog), JmxConstants
            .getRecoveryLogObjectName(currentVirtualDatabase
                .getVirtualDatabaseName()));
      }
      catch (Exception e)
      {
        if (logger.isWarnEnabled())
        {
          logger
              .warn(
                  Translate
                      .get(
                          "virtualdatabase.xml.recoverylog.jmx.failed", currentVirtualDatabase.getVirtualDatabaseName()), e); //$NON-NLS-1$
        }
      }
    }
  }

  /**
   * Unregisters MBeans associated to the vdb.
   */
  private void unregisterRelatedMBeans(VirtualDatabase vdb)
  {
    try
    {
      MBeanServerManager.unregister(JmxConstants
          .getVirtualDataBaseObjectName(vdb.getVirtualDatabaseName()));
    }
    catch (Exception ee)
    {
    }
    try
    {
      MBeanServerManager.unregister(JmxConstants
          .getAbstractSchedulerObjectName(vdb.getVirtualDatabaseName()));
    }
    catch (Exception ee)
    {
    }
    try
    {
      MBeanServerManager.unregister(JmxConstants.getRecoveryLogObjectName(vdb
          .getVirtualDatabaseName()));
    }
    catch (Exception ee)
    {
    }
    try
    {
      MBeanServerManager.unregister(JmxConstants.getLoadBalancerObjectName(vdb
          .getVirtualDatabaseName()));
    }
    catch (Exception ee)
    {
    }
    try
    {
      MBeanServerManager.unregister(JmxConstants
          .getRequestManagerObjectName(vdb.getVirtualDatabaseName()));
    }
    catch (Exception ee)
    {
    }
    try
    {
      MBeanServerManager.unregister(JmxConstants.getParsingCacheObjectName(vdb
          .getVirtualDatabaseName()));
    }
    catch (Exception ee)
    {
    }
    List backendNames = new ArrayList();
    try
    {
      backendNames = vdb.getAllBackendNames();
    }
    catch (VirtualDatabaseException ee)
    {
    }
    for (int i = 0; i < backendNames.size(); i++)
    {
      String backendName = (String) backendNames.get(i);
      try
      {
        MBeanServerManager.unregister(JmxConstants
            .getDatabaseBackendObjectName(vdb.getVirtualDatabaseName(),
                backendName));
      }
      catch (Exception ee)
      {
      }
    }
  }

  /* Virtual database */

  /**
   * Sets {@link #currentVirtualDatabase}as a new <code> VirtualDatabase
   * </code>
   * using the parsed attributes. An exception is thrown in particular if a
   * virtual database with the same name is already registered in the
   * controller.
   * 
   * @param atts parsed attributes
   * @exception SAXException if an error occurs
   */
  private void newVirtualDatabase(Attributes atts) throws SAXException
  {
    String name = atts.getValue(DatabasesXmlTags.ATT_name);
    String maxNbOfConnections = atts
        .getValue(DatabasesXmlTags.ATT_maxNbOfConnections);
    String poolThreads = atts.getValue(DatabasesXmlTags.ATT_poolThreads);
    String minNbOfThreads = atts.getValue(DatabasesXmlTags.ATT_minNbOfThreads);
    String maxNbOfThreads = atts.getValue(DatabasesXmlTags.ATT_maxNbOfThreads);
    String maxThreadIdleTime = atts
        .getValue(DatabasesXmlTags.ATT_maxThreadIdleTime);
    String idleConnectionTimeout = atts
        .getValue(DatabasesXmlTags.ATT_idleConnectionTimeout);
    String sqlDumpLength = atts.getValue(DatabasesXmlTags.ATT_sqlDumpLength);
    String useStaticResultSetMetaData = atts
        .getValue(DatabasesXmlTags.ATT_useStaticResultSetMetaData);
    String enforcingTableExistenceIntoSchema = atts
        .getValue(DatabasesXmlTags.ATT_enforceTableExistenceIntoSchema);

    if (controller.hasVirtualDatabase(name))
    {
      String msg = Translate.get(
          "virtualdatabase.xml.virtualdatabase.already.exists", name);
      logger.error(msg);
      throw new SAXException(msg);
    }

    try
    {
      // Process the attributes
      int maxConnections = Integer.parseInt(maxNbOfConnections);
      boolean pool = poolThreads.equals(DatabasesXmlTags.VAL_true);
      int minThreads = Integer.parseInt(minNbOfThreads);
      int maxThreads = Integer.parseInt(maxNbOfThreads);
      // converts in ms
      long threadIdleTime = Long.parseLong(maxThreadIdleTime) * 1000L;
      int dumpLength = Integer.parseInt(sqlDumpLength);
      boolean staticMetadata = useStaticResultSetMetaData
          .equals(DatabasesXmlTags.VAL_true);
      boolean enforceTableExistenceIntoSchema = enforcingTableExistenceIntoSchema
          .equals(DatabasesXmlTags.VAL_true);
      if (logger.isDebugEnabled())
        logger.debug(Translate.get(
            "virtualdatabase.xml.virtualdatabase.create", name));
      currentVirtualDatabase = new VirtualDatabase(controller, name,
          maxConnections, pool, minThreads, maxThreads, threadIdleTime, Long
              .parseLong(idleConnectionTimeout) * 1000L, dumpLength,
          staticMetadata, enforceTableExistenceIntoSchema);
      org.continuent.sequoia.controller.virtualdatabase.management.VirtualDatabase virtualDatabase = new org.continuent.sequoia.controller.virtualdatabase.management.VirtualDatabase(
          currentVirtualDatabase);
      ObjectName objectName = JmxConstants.getVirtualDataBaseObjectName(name);
      MBeanServerManager.registerMBean(virtualDatabase, objectName);
      currentVirtualDatabase.setNotificationBroadcasterSupport(virtualDatabase
          .getBroadcaster());

    }
    catch (Exception e)
    {
      String msg = Translate.get("virtualdatabase.xml.virtualdatabase.failed");
      logger.error(msg, e);
      throw new SAXException(msg, e);
    }
  }

  /* Distribution */

  /**
   * Sets {@link #currentVirtualDatabase} as a new
   * <code>DistributedVirtalDatabase</code> using the parsed attributes.
   * 
   * @param atts parsed attributes
   */
  private void newDistribution(Attributes atts) throws SAXException
  {
    String groupName = atts.getValue(DatabasesXmlTags.ATT_groupName);
    String hederaPropertiesFile = atts
        .getValue(DatabasesXmlTags.ATT_hederaPropertiesFile);
    String clientFailoverTimeoutString = atts
        .getValue(DatabasesXmlTags.ATT_clientFailoverTimeout);

    if (groupName == null)
      groupName = currentVirtualDatabase.getVirtualDatabaseName();

    long clientFailoverTimeout;
    try
    {
      clientFailoverTimeout = Long.parseLong(clientFailoverTimeoutString);
    }
    catch (NumberFormatException e)
    {
      throw new SAXException(
          "Invalid long value for clientFailoverTimeout in Distribution element");
    }

    if (logger.isDebugEnabled())
      logger.debug(Translate.get(
          "virtualdatabase.xml.virtualdatabase.distributed.create",
          new String[]{currentVirtualDatabase.getVirtualDatabaseName(),
              groupName}));
    try
    {
      // we need to replace previous database mbean
      ObjectName objectName = JmxConstants
          .getVirtualDataBaseObjectName(currentVirtualDatabase
              .getVirtualDatabaseName());
      MBeanServerManager.unregister(objectName);

      // Create the distributed virtual database (does not join the group now)
      currentVirtualDatabase = new DistributedVirtualDatabase(controller,
          currentVirtualDatabase.getVirtualDatabaseName(), groupName,
          currentVirtualDatabase.getMaxNbOfConnections(),
          currentVirtualDatabase.isPoolConnectionThreads(),
          currentVirtualDatabase.getMinNbOfThreads(), currentVirtualDatabase
              .getMaxNbOfThreads(), currentVirtualDatabase
              .getMaxThreadIdleTime(), currentVirtualDatabase
              .getIdleConnectionTimeout(), currentVirtualDatabase
              .getSqlShortFormLength(), clientFailoverTimeout,
          currentVirtualDatabase.useStaticResultSetMetaData(),
          hederaPropertiesFile, currentVirtualDatabase
              .enforceTableExistenceIntoSchema());
      org.continuent.sequoia.controller.virtualdatabase.management.VirtualDatabase virtualDatabase = new org.continuent.sequoia.controller.virtualdatabase.management.VirtualDatabase(
          currentVirtualDatabase);
      MBeanServerManager.registerMBean(virtualDatabase, objectName);
      currentVirtualDatabase.setNotificationBroadcasterSupport(virtualDatabase
          .getBroadcaster());
    }
    catch (Exception e)
    {
      String msg = Translate
          .get("virtualdatabase.xml.virtualdatabase.distributed.failed");
      logger.error(msg, e);
      throw new SAXException(msg, e);
    }
  }

  /**
   * Sets a new <code>MessageTimeouts</code> to the current distributed
   * virtual database.
   * 
   * @param atts parsed attributes
   */
  private void newMessageTimeouts(Attributes atts)
  {
    MessageTimeouts messageTimeouts = new MessageTimeouts(getTimeout(atts
        .getValue(DatabasesXmlTags.ATT_defaultTimeout)));

    messageTimeouts.setBackendStatusTimeout(getTimeout(atts
        .getValue(DatabasesXmlTags.ATT_backendStatusTimeout)));
    messageTimeouts.setBackendTransferTimeout(getTimeout(atts
        .getValue(DatabasesXmlTags.ATT_backendTransferTimeout)));
    messageTimeouts.setCacheInvalidateTimeout(getTimeout(atts
        .getValue(DatabasesXmlTags.ATT_cacheInvalidateTimeout)));
    messageTimeouts.setCommitTimeout(getTimeout(atts
        .getValue(DatabasesXmlTags.ATT_commitTimeout)));
    messageTimeouts.setControllerNameTimeout(getTimeout(atts
        .getValue(DatabasesXmlTags.ATT_controllerNameTimeout)));
    messageTimeouts.setCopyLogEntryTimeout(getTimeout(atts
        .getValue(DatabasesXmlTags.ATT_copyLogEntryTimeout)));
    messageTimeouts.setDisableBackendTimeout(getTimeout(atts
        .getValue(DatabasesXmlTags.ATT_disableBackendTimeout)));
    messageTimeouts.setEnableBackendTimeout(getTimeout(atts
        .getValue(DatabasesXmlTags.ATT_enableBackendTimeout)));
    messageTimeouts.setExecReadRequestTimeout(getTimeout(atts
        .getValue(DatabasesXmlTags.ATT_execReadRequestTimeout)));
    messageTimeouts.setExecReadStoredProcedureTimeout(getTimeout(atts
        .getValue(DatabasesXmlTags.ATT_execReadStoredProcedureTimeout)));
    messageTimeouts.setExecWriteRequestTimeout(getTimeout(atts
        .getValue(DatabasesXmlTags.ATT_execWriteRequestTimeout)));
    messageTimeouts.setExecWriteRequestWithKeysTimeout(getTimeout(atts
        .getValue(DatabasesXmlTags.ATT_execWriteRequestWithKeysTimeout)));
    messageTimeouts.setExecWriteStoredProcedureTimeout(getTimeout(atts
        .getValue(DatabasesXmlTags.ATT_execWriteStoredProcedureTimeout)));
    messageTimeouts.setInitiateDumpCopyTimeout(getTimeout(atts
        .getValue(DatabasesXmlTags.ATT_initiateDumpCopyTimeout)));
    messageTimeouts.setNotifyCompletionTimeout(getTimeout(atts
        .getValue(DatabasesXmlTags.ATT_notifyCompletionTimeout)));
    messageTimeouts.setReleaseSavepointTimeout(getTimeout(atts
        .getValue(DatabasesXmlTags.ATT_releaseSavepointTimeout)));
    messageTimeouts.setReplicateLogEntriesTimeout(getTimeout(atts
        .getValue(DatabasesXmlTags.ATT_replicateLogEntriesTimeout)));
    messageTimeouts.setRollbackTimeout(getTimeout(atts
        .getValue(DatabasesXmlTags.ATT_rollbackTimeout)));
    messageTimeouts.setRollbackToSavepointTimeout(getTimeout(atts
        .getValue(DatabasesXmlTags.ATT_rollbackToSavepointTimeout)));
    messageTimeouts.setSetCheckpointTimeout(getTimeout(atts
        .getValue(DatabasesXmlTags.ATT_setCheckpointTimeout)));
    messageTimeouts.setSetSavepointTimeout(getTimeout(atts
        .getValue(DatabasesXmlTags.ATT_setSavepointTimeout)));
    messageTimeouts.setUnlogCommitTimeout(getTimeout(atts
        .getValue(DatabasesXmlTags.ATT_unlogCommitTimeout)));
    messageTimeouts.setUnlogRequestTimeout(getTimeout(atts
        .getValue(DatabasesXmlTags.ATT_unlogRequestTimeout)));
    messageTimeouts.setUnlogRollbackTimeout(getTimeout(atts
        .getValue(DatabasesXmlTags.ATT_unlogRollbackTimeout)));
    messageTimeouts.setVirtualDatabaseConfigurationTimeout(getTimeout(atts
        .getValue(DatabasesXmlTags.ATT_virtualDatabaseConfigurationTimeout)));

    // Set the message timeouts
    ((DistributedVirtualDatabase) currentVirtualDatabase)
        .setMessageTimeouts(messageTimeouts);
  }

  private static final int DEFAULT_TIMEOUT = 0;

  private long getTimeout(String timeoutAsString)
  {
    if (timeoutAsString == null)
      return DEFAULT_TIMEOUT;
    long timeout;
    try
    {
      timeout = Long.parseLong(timeoutAsString);
    }
    catch (NumberFormatException e1)
    {
      timeout = DEFAULT_TIMEOUT; // Default is no timeout
    }
    return timeout;
  }

  //
  // Monitoring
  //

  /**
   * Sets a new <code>SQLMonitoring</code> to the current virtual database.
   * 
   * @param atts parsed attributes
   */
  private void newSQLMonitoring(Attributes atts)
  {
    String monitoringString = atts
        .getValue(DatabasesXmlTags.ATT_defaultMonitoring);
    boolean monitoring;
    if (monitoringString != null)
      monitoring = monitoringString.equals(DatabasesXmlTags.VAL_on);
    else
      monitoring = false;

    SQLMonitoring sqlMonitor = new SQLMonitoring(currentVirtualDatabase
        .getVirtualDatabaseName());
    sqlMonitor.setDefaultRule(monitoring);
    currentVirtualDatabase.setSQLMonitor(sqlMonitor);
  }

  /**
   * Add a new <code>SQLMonitoringRule</code> to the current SQL monitor.
   * 
   * @param atts parsed attributes
   */
  private void newSQLMonitoringRule(Attributes atts)
  {
    String queryPattern = atts.getValue(DatabasesXmlTags.ATT_queryPattern);
    String caseSensitiveString = atts
        .getValue(DatabasesXmlTags.ATT_caseSensitive);
    String applyToSkeletonString = atts
        .getValue(DatabasesXmlTags.ATT_applyToSkeleton);
    String monitoringString = atts.getValue(DatabasesXmlTags.ATT_monitoring);

    boolean caseSensitive;
    if (caseSensitiveString != null)
      caseSensitive = caseSensitiveString.equals(DatabasesXmlTags.VAL_true);
    else
      caseSensitive = false;
    boolean applyToSkeleton;
    if (applyToSkeletonString != null)
      applyToSkeleton = applyToSkeletonString.equals(DatabasesXmlTags.VAL_true);
    else
      applyToSkeleton = false;
    boolean monitoring;
    if (monitoringString != null)
      monitoring = monitoringString.equals(DatabasesXmlTags.VAL_on);
    else
      monitoring = false;

    // Create the rule and add it
    SQLMonitoringRule rule = new SQLMonitoringRule(queryPattern, caseSensitive,
        applyToSkeleton, monitoring);

    if (logger.isDebugEnabled())
      logger.debug(Translate.get("virtualdatabase.xml.sqlmonitoring.rule.add",
          new String[]{queryPattern, String.valueOf(caseSensitive),
              applyToSkeletonString, String.valueOf(monitoring)}));
    currentVirtualDatabase.getSQLMonitor().addRule(rule);
  }

  //
  // Backup
  //

  /**
   * Adds a new <code>BackupManager</code>
   */
  private void newBackupManager()
  {
    currentBackupManager = new BackupManager();
  }

  /**
   * Adds a new <code>Backuper</code> to the currentBackupManager
   * 
   * @param atts parsed attributes
   */
  private void newBackuper(Attributes atts)
  {
    String name = atts.getValue(DatabasesXmlTags.ATT_backuperName);
    String className = atts.getValue(DatabasesXmlTags.ATT_className);
    String options = atts.getValue(DatabasesXmlTags.ATT_options);

    Backuper backuper = null;
    try
    {
      backuper = (Backuper) Class.forName(className).newInstance();
      backuper.setOptions(options);
    }
    catch (Exception e)
    {
      String msg = "Failed to load backuper " + name + " from class "
          + className + " (" + e + ")";
      if (logger.isDebugEnabled())
        logger.error(msg, e);
      else
        logger.error(e);
      return;
    }

    try
    {
      currentBackupManager.registerBackuper(name, backuper);
    }
    catch (BackupException e)
    {
      logger.error("Failed to load backuper " + name + "(" + e + ")");
    }
  }

  //
  // Database backend
  //

  /**
   * Sets {@link #currentBackend}as a new <code> DatabaseBackend</code> using
   * the parsed attributes.
   * 
   * @param atts parsed attributes
   * @throws SAXException
   */
  private void newDatabaseBackend(Attributes atts) throws SAXException
  {
    String name = atts.getValue(DatabasesXmlTags.ATT_name);
    String driverClassName = atts.getValue(DatabasesXmlTags.ATT_driver);
    String driverPath = atts.getValue(DatabasesXmlTags.ATT_driverPath);
    String url = atts.getValue(DatabasesXmlTags.ATT_url);
    String connectionTestStatement = atts
        .getValue(DatabasesXmlTags.ATT_connectionTestStatement);
    String nbOfWorkerThreadsString = atts
        .getValue(DatabasesXmlTags.ATT_nbOfBackendWorkerThreads);

    int nbOfWorkerThreads;
    try
    {
      nbOfWorkerThreads = Integer.parseInt(nbOfWorkerThreadsString);
    }
    catch (Exception e)
    {
      String msg = Translate.get("virtualdatabase.xml.virtualdatabase.failed");
      logger.error(msg, e);
      throw new SAXException(msg, e);
    }

    if (logger.isDebugEnabled())
    {
      logger.debug(Translate.get("virtualdatabase.xml.backend.create",
          new String[]{name, driverClassName, url, connectionTestStatement}));
      if (driverPath == null)
      {
        logger.debug("no driver path defined for backend.");
      }
      else
      {
        logger.debug("using driver path " + driverPath);
      }
    }
    currentBackend = new DatabaseBackend(name, driverPath, driverClassName,
        url, currentVirtualDatabase.getVirtualDatabaseName(), true,
        connectionTestStatement, nbOfWorkerThreads);
  }

  /**
   * Adds a <code>AbstractRewritingRule</code> to the current DatabaseBackend.
   * 
   * @param atts parsed attributes
   */
  private void newRewritingRule(Attributes atts) throws SAXException
  {
    String queryPattern = atts.getValue(DatabasesXmlTags.ATT_queryPattern);
    String rewrite = atts.getValue(DatabasesXmlTags.ATT_rewrite);
    String matchingType = atts.getValue(DatabasesXmlTags.ATT_matchingType);
    String caseSensitiveString = atts
        .getValue(DatabasesXmlTags.ATT_caseSensitive);
    String stopOnMatchString = atts.getValue(DatabasesXmlTags.ATT_stopOnMatch);

    boolean caseSensitive;
    if (caseSensitiveString != null)
      caseSensitive = caseSensitiveString.equals(DatabasesXmlTags.VAL_true);
    else
      caseSensitive = false;
    boolean stopOnMatch;
    if (stopOnMatchString != null)
      stopOnMatch = stopOnMatchString.equals(DatabasesXmlTags.VAL_true);
    else
      stopOnMatch = false;

    // Create the rule and add it
    AbstractRewritingRule rule;
    if (matchingType.equals(DatabasesXmlTags.VAL_simple))
      rule = new SimpleRewritingRule(queryPattern, rewrite, caseSensitive,
          stopOnMatch);
    else if (matchingType.equals(DatabasesXmlTags.VAL_pattern))
      rule = new PatternRewritingRule(queryPattern, rewrite, caseSensitive,
          stopOnMatch);
    else if (matchingType.equals(DatabasesXmlTags.VAL_replaceAll))
      rule = new ReplaceAllRewritingRule(queryPattern, rewrite, caseSensitive,
          stopOnMatch);
    else
      throw new SAXException(Translate.get(
          "virtualdatabase.xml.rewritingrule.unsupported.matching",
          matchingType));

    if (logger.isDebugEnabled())
      logger.debug(Translate.get("virtualdatabase.xml.rewritingrule.add",
          new String[]{queryPattern, rewrite, String.valueOf(caseSensitive),
              String.valueOf(stopOnMatch)}));
    currentBackend.addRewritingRule(rule);
  }

  /* Authentication manager */

  /**
   * Sets {@link #currentAuthenticationManager}as a new <code>
   * AuthenticationManager</code>.
   */
  private void newAuthenticationManager(Attributes atts)
  {
    String transparentLogin = atts
        .getValue(DatabasesXmlTags.ATT_transparentLogin);
    currentAuthenticationManager = new AuthenticationManager(transparentLogin
        .equals(DatabasesXmlTags.VAL_on));
  }

  /**
   * Sets the administrator user of the {@link #currentAuthenticationManager}
   * using the parsed attributs.
   * 
   * @param atts parsed attributes
   */
  private void newAdminUser(Attributes atts)
  {
    String aLogin = atts.getValue(DatabasesXmlTags.ATT_username);
    String aPassword = atts.getValue(DatabasesXmlTags.ATT_password);

    if (logger.isDebugEnabled())
      logger.debug(Translate.get(
          "virtualdatabase.xml.authentication.login.admin.add", new String[]{
              aLogin, aPassword}));
    currentAuthenticationManager.addAdminUser(new AdminUser(aLogin, aPassword));
  }

  /**
   * Sets {@link #currentVirtualUser}as a new <code> VirtualDatabaseUser
   * </code>
   * using the parsed attributes and adds this new virtual database user to the
   * {@link #currentAuthenticationManager}.
   * 
   * @param atts parsed attributes
   */
  private void newVirtualLogin(Attributes atts)
  {
    String vLogin = atts.getValue(DatabasesXmlTags.ATT_vLogin);
    String vPassword = atts.getValue(DatabasesXmlTags.ATT_vPassword);
    currentVirtualUser = new VirtualDatabaseUser(vLogin, vPassword);

    if (logger.isDebugEnabled())
      logger.debug(Translate.get(
          "virtualdatabase.xml.authentication.login.virtual.add", new String[]{
              vLogin, vPassword}));
    currentAuthenticationManager.addVirtualUser(currentVirtualUser);
  }

  /* Request manager */

  /**
   * Sets the {@link #beginTimeout},{@link #commitTimeout}and
   * {@link #rollbackTimeout}timeouts (in ms) using the parsed attributes.
   * 
   * @param atts element attributes
   * @exception SAXException if an error occurs
   */
  private void newRequestManager(Attributes atts) throws SAXException
  {
    try
    {
      String begin = atts.getValue(DatabasesXmlTags.ATT_beginTimeout);
      String commit = atts.getValue(DatabasesXmlTags.ATT_commitTimeout);
      String rollback = atts.getValue(DatabasesXmlTags.ATT_rollbackTimeout);
      String caseSensitiveParsingString = atts
          .getValue(DatabasesXmlTags.ATT_caseSensitiveParsing);

      // Convert to ms
      beginTimeout = Long.parseLong(begin) * 1000L;
      commitTimeout = Long.parseLong(commit) * 1000L;
      rollbackTimeout = Long.parseLong(rollback) * 1000L;

      if (caseSensitiveParsingString != null)
        caseSensitiveParsing = caseSensitiveParsingString
            .equals(DatabasesXmlTags.VAL_true);
      else
        caseSensitiveParsing = false;

      if (logger.isDebugEnabled())
        logger.debug(Translate.get(
            "virtualdatabase.xml.requestmanager.parameters", new String[]{
                String.valueOf(beginTimeout), String.valueOf(commitTimeout),
                String.valueOf(rollbackTimeout)}));
    }
    catch (NumberFormatException e)
    {
      String msg = Translate
          .get("virtualdatabase.xml.requestmanager.timeout.failed");
      logger.error(msg, e);
      throw new SAXException(msg, e);
    }
  }

  /* Macro Handling */

  /**
   * Adds a new <code>MacrosHandler</code> using the parsed attributes.
   * 
   * @param atts parsed attributes
   */
  private void newMacroHandler(Attributes atts)
  {
    /**
     * rand (off | int | long | float | double) "float" now (off | date | time |
     * timestamp) "timestamp" currentDate (off | date | time | timestamp) "date"
     * currentTime (off | date | time | timestamp) "time" timeOfDay (off | date |
     * time | timestamp) "timestamp" currentTimestamp (off | date | time |
     * timestamp) "timestamp" timeResolution CDATA "0"
     */
    String rand = atts.getValue(DatabasesXmlTags.ATT_rand);
    String now = atts.getValue(DatabasesXmlTags.ATT_now);
    String currentDate = atts.getValue(DatabasesXmlTags.ATT_currentDate);
    String currentTime = atts.getValue(DatabasesXmlTags.ATT_currentTime);
    String currentTimestamp = atts
        .getValue(DatabasesXmlTags.ATT_currentTimestamp);
    String timeResolution = atts.getValue(DatabasesXmlTags.ATT_timeResolution);
    String timeOfDay = atts.getValue(DatabasesXmlTags.ATT_timeOfDay);

    int icurrentDate = MacrosHandler.getIntDateLevel(currentDate);
    int icurrentTime = MacrosHandler.getIntDateLevel(currentTime);
    int icurrentTimestamp = MacrosHandler.getIntDateLevel(currentTimestamp);
    int itimeOfDay = MacrosHandler.getIntDateLevel(timeOfDay);
    int inow = MacrosHandler.getIntDateLevel(now);
    int irand = MacrosHandler.getIntRandLevel(rand);
    long ltimeResolution = Long.parseLong(timeResolution);

    try
    {
      currentMacroHandler = new MacrosHandler(irand, ltimeResolution, inow,
          icurrentDate, icurrentTime, itimeOfDay, icurrentTimestamp);
    }
    catch (RuntimeException e)
    {
      logger.warn(Translate.get(
          "virtualdatabase.xml.invalid.macroshandler.settings", e));
    }
  }

  /* Request scheduler */

  /**
   * Sets {@link #currentRequestScheduler}as a new <code>
   * SingleDBPassThroughScheduler</code>
   * using the parsed attributes.
   * 
   * @param atts parsed attributes
   * @exception SAXException if an error occurs
   */
  private void newSingleDBScheduler(Attributes atts) throws SAXException
  {
    String level = atts.getValue(DatabasesXmlTags.ATT_level);

    // SingleDB Query Level
    if (level.equals(DatabasesXmlTags.VAL_passThrough))
    {
      if (logger.isDebugEnabled())
        logger.debug(Translate
            .get("virtualdatabase.xml.scheduler.singledb.create.passthrough"));
      currentRequestScheduler = new SingleDBPassThroughScheduler();
    }

    // SingleDB Pessimistic Transaction Level
    else if (level.equals(DatabasesXmlTags.VAL_pessimisticTransaction))
    {
      if (logger.isDebugEnabled())
        logger.debug(Translate
            .get("virtualdatabase.xml.scheduler.singledb.create.pessimistic"));
      currentRequestScheduler = new SingleDBPessimisticTransactionLevelScheduler();
    }
    else
    {
      throw new SAXException(Translate.get(
          "virtualdatabase.xml.scheduler.singledb.unsupported", level));
    }
  }

  /**
   * Sets {@link #currentRequestScheduler}as a new <code>
   * RAIDb0PassThroughLevelScheduler</code>
   * or <code>RAIDb0PessimisticTransactionLevelScheduler</code> using the
   * parsed attributes.
   * 
   * @param atts parsed attributes
   * @exception SAXException if an error occurs
   */
  private void newRAIDb0Scheduler(Attributes atts) throws SAXException
  {
    String level = atts.getValue(DatabasesXmlTags.ATT_level);

    if (level.equals(DatabasesXmlTags.VAL_passThrough))
    {
      if (logger.isDebugEnabled())
        logger.debug(Translate
            .get("virtualdatabase.xml.scheduler.raidb0.create.passthrough"));
      currentRequestScheduler = new RAIDb0PassThroughLevelScheduler();
    }
    else if (level.equals(DatabasesXmlTags.VAL_pessimisticTransaction))
    {
      if (logger.isDebugEnabled())
        logger.debug(Translate
            .get("virtualdatabase.xml.scheduler.raidb0.create.pessimistic"));
      currentRequestScheduler = new RAIDb0PessimisticTransactionLevelScheduler();
    }
    else
      throw new SAXException(Translate.get(
          "virtualdatabase.xml.scheduler.raidb0.unsupported", level));
  }

  /**
   * Sets {@link #currentRequestScheduler}as a new
   * <code>RAIDb1PassThroughScheduler</code>,<code>
   * RAIDb1QueryLevelScheduler</code>,
   * <code>RAIDb1OptimisticQueryLevelScheduler</code> or
   * <code>RAIDb1PessimisticTransactionLevelScheduler</code> using the parsed
   * attributes.
   * 
   * @param atts parsed attributes
   * @exception SAXException if an error occurs
   */
  private void newRAIDb1Scheduler(Attributes atts) throws SAXException
  {
    String level = atts.getValue(DatabasesXmlTags.ATT_level);
    long waitForSuspendedTransactionsTimeout = Long.parseLong(atts
        .getValue(DatabasesXmlTags.ATT_waitForSuspendedTransactionsTimeout));
    long waitForPersistentConnectionsTimeout = Long.parseLong(atts
        .getValue(DatabasesXmlTags.ATT_waitForPersistentConnectionsTimeout));

    // RAIDb-1 Pass Through level
    if (level.equals(DatabasesXmlTags.VAL_passThrough))
    {
      if (logger.isDebugEnabled())
        logger.debug(Translate
            .get("virtualdatabase.xml.scheduler.raidb1.create.passthrough"));
      currentRequestScheduler = new RAIDb1PassThroughScheduler(
          currentVirtualDatabase, waitForSuspendedTransactionsTimeout,
          waitForPersistentConnectionsTimeout);
    }
    // RAIDb-1 Query level
    else if (level.equals(DatabasesXmlTags.VAL_query))
    {
      if (logger.isDebugEnabled())
        logger.debug(Translate
            .get("virtualdatabase.xml.scheduler.raidb1.create.query"));
      currentRequestScheduler = new RAIDb1QueryLevelScheduler();
    }
    // RAIDb-1 Optimistic Query level
    else if (level.equals(DatabasesXmlTags.VAL_optimisticQuery))
    {
      if (logger.isDebugEnabled())
        logger
            .debug(Translate
                .get("virtualdatabase.xml.scheduler.raidb1.create.query.optimistic"));
      currentRequestScheduler = new RAIDb1OptimisticQueryLevelScheduler();
    }
    // RAIDb-1 Optimistic Transaction level
    else if (level.equals(DatabasesXmlTags.VAL_optimisticTransaction))
    {
      if (logger.isDebugEnabled())
        logger.debug(Translate
            .get("virtualdatabase.xml.scheduler.raidb1.create.optimistic"));
      currentRequestScheduler = new RAIDb1OptimisticTransactionLevelScheduler();
    }
    // RAIDb-1 Pessimistic Transaction level
    else if (level.equals(DatabasesXmlTags.VAL_pessimisticTransaction))
    {
      if (logger.isDebugEnabled())
        logger.debug(Translate
            .get("virtualdatabase.xml.scheduler.raidb1.create.pessimistic"));
      currentRequestScheduler = new RAIDb1PessimisticTransactionLevelScheduler();
    }
    else
    {
      throw new SAXException(Translate.get(
          "virtualdatabase.xml.scheduler.raidb1.unsupported", level));
    }
  }

  /**
   * Sets {@link #currentRequestScheduler}as a new
   * <code>RAIDb2PassThroughScheduler</code>,<code>
   * RAIDb2QueryLevelScheduler</code>
   * or <code>RAIDb2PessimisticTransactionLevelScheduler</code> using the
   * parsed attributes.
   * 
   * @param atts parsed attributes
   * @exception SAXException if an error occurs
   */
  private void newRAIDb2Scheduler(Attributes atts) throws SAXException
  {
    String level = atts.getValue(DatabasesXmlTags.ATT_level);

    // RAIDb-2 Query level
    if (level.equals(DatabasesXmlTags.VAL_passThrough))
    {
      if (logger.isDebugEnabled())
        logger.debug(Translate
            .get("virtualdatabase.xml.scheduler.raidb2.create.passthrough"));
      currentRequestScheduler = new RAIDb2PassThroughScheduler(
          currentVirtualDatabase);
    }
    // RAIDb-2 Query level
    else if (level.equals(DatabasesXmlTags.VAL_query))
    {
      if (logger.isDebugEnabled())
        logger.debug(Translate
            .get("virtualdatabase.xml.scheduler.raidb2.create.query"));
      currentRequestScheduler = new RAIDb2QueryLevelScheduler();
    }
    // RAIDb-2 Pessimistic Transaction level
    else if (level.equals(DatabasesXmlTags.VAL_pessimisticTransaction))
    {
      if (logger.isDebugEnabled())
        logger.debug(Translate
            .get("virtualdatabase.xml.scheduler.raidb2.create.pessimistic"));
      currentRequestScheduler = new RAIDb2PessimisticTransactionLevelScheduler();
    }
    else
    {
      throw new SAXException(Translate.get(
          "virtualdatabase.xml.scheduler.raidb2.unsupported", level));
    }
  }

  /* ********************** */
  /* *** Request caches *** */
  /* ********************** */

  /**
   * Sets {@link #currentMetadataCache}as a new <code>MetadataCache</code>
   * using the parsed attributes.
   * 
   * @param atts parsed attributes
   * @exception SAXException if an error occurs
   */
  private void newMetadataCache(Attributes atts) throws SAXException
  {
    try
    {
      int maxMetadata = Integer.parseInt(atts
          .getValue(DatabasesXmlTags.ATT_maxNbOfMetadata));
      int maxField = Integer.parseInt(atts
          .getValue(DatabasesXmlTags.ATT_maxNbOfField));
      currentMetadataCache = new MetadataCache(maxMetadata, maxField);
    }
    catch (Exception e)
    {
      String msg = Translate.get(
          "virtualdatabase.xml.metadata.cache.create.failed", e);
      logger.error(msg, e);
      throw new SAXException(msg, e);
    }
  }

  /**
   * Sets {@link #currentParsingCache}as a new <code>ParsingCache</code>
   * using the parsed attributes.
   * 
   * @param atts parsed attributes
   * @exception SAXException if an error occurs
   */
  private void newParsingCache(Attributes atts) throws SAXException
  {
    String backgroundParsingString = atts
        .getValue(DatabasesXmlTags.ATT_backgroundParsing);
    boolean backgroundParsing;

    if (backgroundParsingString != null)
      backgroundParsing = backgroundParsingString
          .equals(DatabasesXmlTags.VAL_true);
    else
      backgroundParsing = false;

    String maxEntriesString = atts
        .getValue(DatabasesXmlTags.ATT_maxNbOfEntries);
    int maxEntries = Integer.parseInt(maxEntriesString);

    try
    {
      currentParsingCache = new ParsingCache(maxEntries, backgroundParsing);
    }
    catch (Exception e)
    {
      String msg = Translate.get(
          "virtualdatabase.xml.parsing.cache.create.failed", e);
      logger.error(msg, e);
      throw new SAXException(msg, e);
    }
  }

  /**
   * Sets {@link #currentResultCache}as a new <code> ResultCache</code> using
   * the parsed attributes.
   * 
   * @param atts parsed attributes
   * @exception SAXException if an error occurs
   */
  private void newResultCache(Attributes atts) throws SAXException
  {
    String granularity = atts.getValue(DatabasesXmlTags.ATT_granularity);
    String maxEntriesString = atts
        .getValue(DatabasesXmlTags.ATT_maxNbOfEntries);
    String pendingTimeoutString = atts
        .getValue(DatabasesXmlTags.ATT_pendingTimeout);

    if (logger.isDebugEnabled())
      logger.debug(Translate.get("virtualdatabase.xml.cache.create",
          granularity));

    try
    {
      int maxEntries = Integer.parseInt(maxEntriesString);
      int pendingTimeout = Integer.parseInt(pendingTimeoutString);

      int granularityValue;
      if (granularity.equals(DatabasesXmlTags.VAL_table))
        granularityValue = CachingGranularities.TABLE;
      else if (granularity.equals(DatabasesXmlTags.VAL_database))
        granularityValue = CachingGranularities.DATABASE;
      else if (granularity.equals(DatabasesXmlTags.VAL_column))
        granularityValue = CachingGranularities.COLUMN;
      else if (granularity.equals(DatabasesXmlTags.VAL_columnUnique))
        granularityValue = CachingGranularities.COLUMN_UNIQUE;
      else
        throw new InstantiationException(Translate.get(
            "virtualdatabase.xml.cache.unsupported", granularity));

      currentResultCache = ResultCacheFactory.getCacheInstance(
          granularityValue, maxEntries, pendingTimeout);

    }
    catch (Exception e)
    {
      String msg = Translate.get("virtualdatabase.xml.cache.create.failed",
          granularity);
      logger.error(msg, e);
      throw new SAXException(msg, e);
    }
  }

  /**
   * Add a new <code>ResultCacheRule</code> using the parsed attributes.
   * 
   * @param atts parsed attributes
   */
  private void newResultCacheRule(Attributes atts)
  {
    String queryString = atts.getValue(DatabasesXmlTags.ATT_queryPattern);

    String caseSensitiveString = atts
        .getValue(DatabasesXmlTags.ATT_caseSensitive);
    String applyToSkeletonString = atts
        .getValue(DatabasesXmlTags.ATT_applyToSkeleton);
    long timestampResolution;
    try
    {
      timestampResolution = Long.parseLong(atts
          .getValue(DatabasesXmlTags.ATT_timestampResolution));
      timestampResolution *= 1000;
    }
    catch (Exception e)
    {
      logger
          .warn(Translate.get("virtualdatabase.invalid.timestamp.resolution"));
      timestampResolution = 1000;
    }

    boolean caseSensitive;
    if (caseSensitiveString != null)
      caseSensitive = caseSensitiveString.equals(DatabasesXmlTags.VAL_true);
    else
      caseSensitive = false;
    boolean applyToSkeleton;
    if (applyToSkeletonString != null)
      applyToSkeleton = applyToSkeletonString.equals(DatabasesXmlTags.VAL_true);
    else
      applyToSkeleton = false;

    // Create the rule
    currentResultCacheRule = new ResultCacheRule(queryString, caseSensitive,
        applyToSkeleton, timestampResolution);

    if (logger.isDebugEnabled())
      logger.debug(Translate.get("virtualdatabase.xml.cache.rule.add",
          new String[]{atts.getValue(DatabasesXmlTags.ATT_queryPattern),
              String.valueOf(caseSensitive), applyToSkeletonString,
              String.valueOf(timestampResolution)}));
    currentResultCache.addCachingRule(currentResultCacheRule);
  }

  /**
   * Set the <code>DefaultResultCacheRule</code> using the parsed attributes.
   * 
   * @param atts parsed attributes
   */
  private void newDefaultResultCacheRule(Attributes atts)
  {
    long currentTimestampResolution;
    try
    {
      currentTimestampResolution = Long.parseLong(atts
          .getValue(DatabasesXmlTags.ATT_timestampResolution)) / 1000;
    }
    catch (Exception e)
    {
      String msg = Translate
          .get("virtualdatabase.invalid.timestamp.resolution");
      logger.warn(msg);
      currentTimestampResolution = 1000;
    }
    // Create a fake rule
    currentResultCacheRule = new ResultCacheRule("", false, false,
        currentTimestampResolution);
  }

  /**
   * Add a new <code>EagerCaching</code> behavior to the current
   * <code>ResultCacheRule</code>.
   * 
   * @param atts parsed attributes
   * @exception SAXException if an error occurs
   */
  private void newEagerCaching(Attributes atts)
  {
    Hashtable options = new Hashtable();
    for (int i = 0; i < atts.getLength(); i++)
      options.put(atts.getQName(i), atts.getValue(i));
    currentResultCacheRule.setCacheBehavior(ResultCacheFactory
        .getCacheBehaviorInstance(DatabasesXmlTags.ELT_EagerCaching, options));
  }

  /**
   * Add a new <code>RelaxedCaching</code> behavior to the current
   * <code>ResultCacheRule</code>.
   * 
   * @param atts parsed attributes
   */
  private void newRelaxedCaching(Attributes atts)
  {
    Hashtable options = new Hashtable();
    for (int i = 0; i < atts.getLength(); i++)
      options.put(atts.getQName(i), atts.getValue(i));
    currentResultCacheRule
        .setCacheBehavior(ResultCacheFactory.getCacheBehaviorInstance(
            DatabasesXmlTags.ELT_RelaxedCaching, options));
  }

  /* Load balancers */

  /**
   * Get the transaction isolation level for the load balancer. This will be set
   * at the end of the parsing of this element.
   */
  private void newLoadBalancer(Attributes atts)
  {
    String transactionIsolation = atts
        .getValue(DatabasesXmlTags.ATT_transactionIsolation);

    if (transactionIsolation.equals(DatabasesXmlTags.VAL_readUncommitted))
      defaultTransactionIsolation = java.sql.Connection.TRANSACTION_READ_UNCOMMITTED;
    else if (transactionIsolation.equals(DatabasesXmlTags.VAL_readCommitted))
      defaultTransactionIsolation = java.sql.Connection.TRANSACTION_READ_COMMITTED;
    else if (transactionIsolation.equals(DatabasesXmlTags.VAL_repeatableRead))
      defaultTransactionIsolation = java.sql.Connection.TRANSACTION_REPEATABLE_READ;
    else if (transactionIsolation.equals(DatabasesXmlTags.VAL_serializable))
      defaultTransactionIsolation = java.sql.Connection.TRANSACTION_SERIALIZABLE;
    else
      // if (transactionIsolation.equals(DatabasesXmlTags.VAL_databaseDefault))
      defaultTransactionIsolation = org.continuent.sequoia.driver.Connection.DEFAULT_TRANSACTION_ISOLATION_LEVEL;
  }

  /**
   * Sets {@link #currentLoadBalancer}as a new <code> SingleDB</code> using
   * the parsed attributes.
   * 
   * @exception SAXException if an error occurs
   */
  private void newSingleDBRequestLoadBalancer() throws SAXException
  {
    if (logger.isDebugEnabled())
      logger.debug(Translate
          .get("virtualdatabase.xml.loadbalancer.singledb.set"));

    try
    {
      currentLoadBalancer = new SingleDB(currentVirtualDatabase);
    }
    catch (Exception e)
    {
      String msg = Translate
          .get("virtualdatabase.xml.loadbalancer.singledb.failed");
      logger.error(msg, e);
      throw new SAXException(msg, e);
    }
  }

  //
  // ParallelDB load balancers
  //

  /**
   * Sets {@link #currentLoadBalancer}as a new <code>ParallelDB_RR</code>
   * using the parsed attributes.
   * 
   * @exception SAXException if an error occurs
   */
  private void newParallelDBLeastPendingRequestsFirst() throws SAXException
  {
    if (logger.isDebugEnabled())
      logger.debug(Translate
          .get("virtualdatabase.xml.loadbalancer.paralleldb_rr.set"));

    try
    {
      currentLoadBalancer = new ParallelDB_RR(currentVirtualDatabase);
    }
    catch (Exception e)
    {
      String msg = Translate
          .get("virtualdatabase.xml.loadbalancer.paralleldb_rr.failed");
      logger.error(msg, e);
      throw new SAXException(msg, e);
    }
  }

  /**
   * Sets {@link #currentLoadBalancer}as a new <code>ParallelDB_LPRF</code>
   * using the parsed attributes.
   * 
   * @exception SAXException if an error occurs
   */
  private void newParallelDBRoundRobinLoadBalancer() throws SAXException
  {
    if (logger.isDebugEnabled())
      logger.debug(Translate
          .get("virtualdatabase.xml.loadbalancer.paralleldb_lprf.set"));

    try
    {
      currentLoadBalancer = new ParallelDB_LPRF(currentVirtualDatabase);
    }
    catch (Exception e)
    {
      String msg = Translate
          .get("virtualdatabase.xml.loadbalancer.paralleldb_lprf.failed");
      logger.error(msg, e);
      throw new SAXException(msg, e);
    }
  }

  //
  // RAIDb-0 load balancers
  //

  /**
   * Sets {@link #currentLoadBalancer}as a new <code> RAIDb0</code> using the
   * parsed attributes.
   */
  private void newRAIDb0LoadBalancer()
  {
    currentCreateTablePolicy = new CreateTablePolicy();
    currentCreateTableRule = null;
  }

  //
  // RAIDb-1 load balancers
  //

  /**
   * Sets {@link #currentLoadBalancer}as a new <code> RAIDb1</code> using the
   * parsed attributes.
   */
  private void newRAIDb1LoadBalancer()
  {
    currentWaitForCompletionPolicy = DEFAULT_WAIT_FOR_ALL_COMPLETION;
  }

  /**
   * Sets {@link #currentLoadBalancer}as a new <code> RAIDb1_RR</code> using
   * the parsed attributes.
   * 
   * @exception SAXException if an error occurs
   */
  private void newRAIDb1RoundRobinLoadBalancer() throws SAXException
  {
    if (logger.isDebugEnabled())
    {
      logger.debug(Translate
          .get("virtualdatabase.xml.loadbalancer.raidb1_rr.set"));
      logger.debug(Translate.get(
          "virtualdatabase.xml.loadbalancer.waitforcompletion.rule",
          currentWaitForCompletionPolicy.getInformation()));
    }

    try
    {
      currentLoadBalancer = new RAIDb1_RR(currentVirtualDatabase,
          currentWaitForCompletionPolicy);
    }
    catch (Exception e)
    {
      String msg = Translate
          .get("virtualdatabase.xml.loadbalancer.raidb1_rr.failed");
      logger.error(msg, e);
      throw new SAXException(msg, e);
    }
  }

  /**
   * Sets {@link #currentLoadBalancer}as a new <code> RAIDb1_WRR</code> using
   * the parsed attributes.
   * 
   * @exception SAXException if an error occurs
   */
  private void newRAIDb1WeightedRoundRobinLoadBalancer() throws SAXException
  {
    if (logger.isDebugEnabled())
    {
      logger.debug(Translate
          .get("virtualdatabase.xml.loadbalancer.raidb1_wrr.set"));
      logger.debug(Translate.get(
          "virtualdatabase.xml.loadbalancer.waitforcompletion.rule",
          currentWaitForCompletionPolicy.getInformation()));
    }

    try
    {
      currentLoadBalancer = new RAIDb1_WRR(currentVirtualDatabase,
          currentWaitForCompletionPolicy);
    }
    catch (Exception e)
    {
      String msg = Translate
          .get("virtualdatabase.xml.loadbalancer.raidb1_wrr.failed");
      logger.error(msg, e);
      throw new SAXException(msg, e);
    }
  }

  /**
   * Sets {@link #currentLoadBalancer}as a new <code> RAIDb1_LPRF</code> using
   * the parsed attributes.
   * 
   * @exception SAXException if an error occurs
   */
  private void newRAIDb1LeastPendingRequestsFirst() throws SAXException
  {
    if (logger.isDebugEnabled())
    {
      logger.debug(Translate
          .get("virtualdatabase.xml.loadbalancer.raidb1_lprf.set"));
      logger.debug(Translate.get(
          "virtualdatabase.xml.loadbalancer.waitforcompletion.rule",
          currentWaitForCompletionPolicy.getInformation()));
    }

    try
    {
      currentLoadBalancer = new RAIDb1_LPRF(currentVirtualDatabase,
          currentWaitForCompletionPolicy);
    }
    catch (Exception e)
    {
      String msg = Translate
          .get("virtualdatabase.xml.loadbalancer.raidb1_lprf.failed");
      logger.error(msg, e);
      throw new SAXException(msg, e);
    }
  }

  //
  // RAIDb-1ec load balancers
  //

  /**
   * Sets {@link #currentLoadBalancer}as a new <code> RAIDb1ec</code> using
   * the parsed attributes.
   * 
   * @param atts parsed attributes
   */
  private void newRAIDb1ecLoadBalancer(Attributes atts)
  {
    String nbOfConcurrentReads = atts
        .getValue(DatabasesXmlTags.ATT_nbOfConcurrentReads);
    currentNbOfConcurrentReads = Integer.parseInt(nbOfConcurrentReads);
    currentErrorCheckingPolicy = null;
    currentWaitForCompletionPolicy = DEFAULT_WAIT_FOR_ALL_COMPLETION;
  }

  /**
   * Sets {@link #currentLoadBalancer}as a new <code> RAIDb1ec_RR</code> using
   * the parsed attributes.
   * 
   * @exception SAXException if an error occurs
   */
  private void newRAIDb1ecRoundRobinLoadBalancer() throws SAXException
  {
    if (logger.isDebugEnabled())
    {
      logger.debug(Translate
          .get("virtualdatabase.xml.loadbalancer.raidb1ec_rr.set"));
      logger.debug(Translate.get(
          "virtualdatabase.xml.loadbalancer.waitforcompletion.rule",
          currentWaitForCompletionPolicy.getInformation()));
    }

    try
    {
      currentLoadBalancer = new RAIDb1ec_RR(currentVirtualDatabase,
          currentWaitForCompletionPolicy, currentErrorCheckingPolicy,
          currentNbOfConcurrentReads);
      if (logger.isDebugEnabled())
        logger.debug(Translate.get(
            "virtualdatabase.xml.loadbalancer.errorchecking.policy",
            currentErrorCheckingPolicy.getInformation()));
    }
    catch (Exception e)
    {
      String msg = Translate
          .get("virtualdatabase.xml.loadbalancer.raidb1ec_rr.failed");
      logger.error(msg, e);
      throw new SAXException(msg, e);
    }
  }

  /**
   * Sets {@link #currentLoadBalancer}as a new <code> RAIDb1ec_WRR</code>
   * using the parsed attributes.
   * 
   * @exception SAXException if an error occurs
   */
  private void newRAIDb1ecWeightedRoundRobinLoadBalancer() throws SAXException
  {
    if (logger.isDebugEnabled())
    {
      logger.debug(Translate
          .get("virtualdatabase.xml.loadbalancer.raidb1ec_wrr.set"));
      logger.debug(Translate.get(
          "virtualdatabase.xml.loadbalancer.waitforcompletion.rule",
          currentWaitForCompletionPolicy.getInformation()));
    }

    try
    {
      currentLoadBalancer = new RAIDb1ec_WRR(currentVirtualDatabase,
          currentWaitForCompletionPolicy, currentErrorCheckingPolicy,
          currentNbOfConcurrentReads);
      if (logger.isDebugEnabled())
        logger.debug(Translate.get(
            "virtualdatabase.xml.loadbalancer.errorchecking.policy",
            currentErrorCheckingPolicy.getInformation()));
    }
    catch (Exception e)
    {
      String msg = Translate
          .get("virtualdatabase.xml.loadbalancer.raidb1ec_wrr.failed");
      logger.error(msg, e);
      throw new SAXException(msg, e);
    }
  }

  //
  // RAIDb-2 load balancers
  //

  /**
   * Sets {@link #currentLoadBalancer}as a new <code> RAIDb2</code> using the
   * parsed attributes.
   */
  private void newRAIDb2LoadBalancer()
  {
    currentWaitForCompletionPolicy = DEFAULT_WAIT_FOR_ALL_COMPLETION;
    currentCreateTablePolicy = new CreateTablePolicy();
    // Add a default rule to create table on all nodes
    currentCreateTablePolicy.addRule(new CreateTableAll());
    currentCreateTableRule = null;
  }

  /**
   * Sets {@link #currentLoadBalancer}as a new <code> RAIDb2_RR</code> using
   * the parsed attributes.
   * 
   * @exception SAXException if an error occurs
   */
  private void newRAIDb2RoundRobinLoadBalancer() throws SAXException
  {
    if (logger.isDebugEnabled())
    {
      logger.debug(Translate
          .get("virtualdatabase.xml.loadbalancer.raidb2_rr.set"));
      logger.debug(Translate.get(
          "virtualdatabase.xml.loadbalancer.waitforcompletion.rule",
          currentWaitForCompletionPolicy.getInformation()));
    }

    try
    {
      currentLoadBalancer = new RAIDb2_RR(currentVirtualDatabase,
          currentWaitForCompletionPolicy, currentCreateTablePolicy);
    }
    catch (Exception e)
    {
      String msg = Translate
          .get("virtualdatabase.xml.loadbalancer.raidb2_rr.failed");
      logger.error(msg, e);
      throw new SAXException(msg, e);
    }
  }

  /**
   * Sets {@link #currentLoadBalancer}as a new <code> RAIDb2_WRR</code> using
   * the parsed attributes.
   * 
   * @exception SAXException if an error occurs
   */
  private void newRAIDb2WeightedRoundRobinLoadBalancer() throws SAXException
  {
    if (logger.isDebugEnabled())
    {
      logger.debug(Translate
          .get("virtualdatabase.xml.loadbalancer.raidb2_wrr.set"));
      logger.debug(Translate.get(
          "virtualdatabase.xml.loadbalancer.waitforcompletion.rule",
          currentWaitForCompletionPolicy.getInformation()));
    }

    try
    {
      currentLoadBalancer = new RAIDb2_WRR(currentVirtualDatabase,
          currentWaitForCompletionPolicy, currentCreateTablePolicy);
    }
    catch (Exception e)
    {
      String msg = Translate
          .get("virtualdatabase.xml.loadbalancer.raidb2_wrr.failed");
      logger.error(msg, e);
      throw new SAXException(msg, e);
    }
  }

  /**
   * Sets {@link #currentLoadBalancer}as a new <code> RAIDb2_LPRF</code> using
   * the parsed attributes.
   * 
   * @exception SAXException if an error occurs
   */
  private void newRAIDb2LeastPendingRequestsFirst() throws SAXException
  {
    if (logger.isDebugEnabled())
    {
      logger.debug(Translate
          .get("virtualdatabase.xml.loadbalancer.raidb2_lprf.set"));
      logger.debug(Translate.get(
          "virtualdatabase.xml.loadbalancer.waitforcompletion.rule",
          currentWaitForCompletionPolicy.getInformation()));
    }

    try
    {
      currentLoadBalancer = new RAIDb2_LPRF(currentVirtualDatabase,
          currentWaitForCompletionPolicy, currentCreateTablePolicy);
    }
    catch (Exception e)
    {
      String msg = Translate
          .get("virtualdatabase.xml.loadbalancer.raidb2_lprf.failed");
      logger.error(msg, e);
      throw new SAXException(msg, e);
    }
  }

  //
  // RAIDb-2ec load balancers
  //

  /**
   * Sets {@link #currentLoadBalancer}as a new <code> RAIDb2ec</code> using
   * the parsed attributes.
   */
  private void newRAIDb2ecLoadBalancer()
  {
    currentErrorCheckingPolicy = null;
    currentWaitForCompletionPolicy = DEFAULT_WAIT_FOR_ALL_COMPLETION;
    currentCreateTablePolicy = new CreateTablePolicy();
    currentCreateTableRule = null;
  }

  /**
   * Sets {@link #currentLoadBalancer}as a new <code> RAIDb2ec_RR</code> using
   * the parsed attributes.
   * 
   * @exception SAXException if an error occurs
   */
  private void newRAIDb2ecRoundRobinLoadBalancer() throws SAXException
  {
    if (logger.isDebugEnabled())
    {
      logger.debug(Translate
          .get("virtualdatabase.xml.loadbalancer.raidb2_rr.set"));
      logger.debug(Translate.get(
          "virtualdatabase.xml.loadbalancer.waitforcompletion.rule",
          currentWaitForCompletionPolicy.getInformation()));
    }

    try
    {
      currentLoadBalancer = new RAIDb2ec_RR(currentVirtualDatabase,
          currentWaitForCompletionPolicy, currentCreateTablePolicy,
          currentErrorCheckingPolicy, currentNbOfConcurrentReads);
      if (logger.isDebugEnabled())
        logger.debug(Translate.get(
            "virtualdatabase.xml.loadbalancer.errorchecking.policy",
            currentErrorCheckingPolicy.getInformation()));
    }
    catch (Exception e)
    {
      String msg = Translate
          .get("virtualdatabase.xml.loadbalancer.raidb2ec_rr.failed");
      logger.error(msg, e);
      throw new SAXException(msg, e);
    }
  }

  /**
   * Sets {@link #currentLoadBalancer}as a new <code> RAIDb2ec_WRR</code>
   * using the parsed attributes.
   * 
   * @exception SAXException if an error occurs
   */
  private void newRAIDb2ecWeightedRoundRobinLoadBalancer() throws SAXException
  {
    if (logger.isDebugEnabled())
    {
      logger.debug(Translate
          .get("virtualdatabase.xml.loadbalancer.raidb2_wrr.set"));
      logger.debug(Translate.get(
          "virtualdatabase.xml.loadbalancer.waitforcompletion.rule",
          currentWaitForCompletionPolicy.getInformation()));
    }

    try
    {
      currentLoadBalancer = new RAIDb2ec_WRR(currentVirtualDatabase,
          currentWaitForCompletionPolicy, currentCreateTablePolicy,
          currentErrorCheckingPolicy, currentNbOfConcurrentReads);
      if (logger.isDebugEnabled())
        logger.debug(Translate.get(
            "virtualdatabase.xml.loadbalancer.errorchecking.policy",
            currentErrorCheckingPolicy.getInformation()));
    }
    catch (Exception e)
    {
      String msg = Translate
          .get("virtualdatabase.xml.loadbalancer.raidb2ec_wrr.failed");
      logger.error(msg, e);
      throw new SAXException(msg, e);
    }
  }

  //
  // Load balancer policies
  //

  /**
   * Set the WaitForCompletion policy.
   * 
   * @param atts parsed attributes
   * @exception SAXException if an error occurs
   */
  private void newWaitForCompletion(Attributes atts) throws SAXException
  {
    String policy = atts.getValue(DatabasesXmlTags.ATT_policy);
    String enforceTableLocking = atts
        .getValue(DatabasesXmlTags.ATT_enforceTableLocking);
    // Check in the case enforceTableLocking would not be used if insert
    // statements enforce table locks
    String enforceTableLockOnAutoIncrementInsert = "";
    if (!"true".equals(enforceTableLocking))
      enforceTableLockOnAutoIncrementInsert = atts
          .getValue(DatabasesXmlTags.ATT_enforceTableLockOnAutoIncrementInsert);
    String deadlockTimeoutInMsString = atts
        .getValue(DatabasesXmlTags.ATT_deadlockTimeoutInMs);
    long deadlockTimeoutInMs;
    try
    {
      deadlockTimeoutInMs = Long.parseLong(deadlockTimeoutInMsString);
    }
    catch (Exception e)
    {
      String msg = Translate.get("virtualdatabase.xml.virtualdatabase.failed");
      logger.error(msg, e);
      throw new SAXException(msg, e);
    }

    if (policy.equals(DatabasesXmlTags.VAL_first))
      currentWaitForCompletionPolicy = new WaitForCompletionPolicy(
          WaitForCompletionPolicy.FIRST, "true".equals(enforceTableLocking),
          "true".equals(enforceTableLockOnAutoIncrementInsert),
          deadlockTimeoutInMs);
    else if (policy.equals(DatabasesXmlTags.VAL_majority))
      currentWaitForCompletionPolicy = new WaitForCompletionPolicy(
          WaitForCompletionPolicy.MAJORITY, "true".equals(enforceTableLocking),
          "true".equals(enforceTableLockOnAutoIncrementInsert),
          deadlockTimeoutInMs);
    else if (policy.equals(DatabasesXmlTags.VAL_all))
      currentWaitForCompletionPolicy = new WaitForCompletionPolicy(
          WaitForCompletionPolicy.ALL, "true".equals(enforceTableLocking),
          "true".equals(enforceTableLockOnAutoIncrementInsert),
          deadlockTimeoutInMs);
    else
      throw new SAXException(Translate.get(
          "virtualdatabase.xml.loadbalancer.waitforcompletion.unsupported",
          policy));
  }

  /**
   * Add an ErrorChecking policy.
   * 
   * @param atts parsed attributes
   * @exception SAXException if an error occurs
   */
  private void newErrorChecking(Attributes atts) throws SAXException
  {
    String nbOfNodes = atts.getValue(DatabasesXmlTags.ATT_numberOfNodes);
    String policy = atts.getValue(DatabasesXmlTags.ATT_policy);
    if (policy.equals(DatabasesXmlTags.VAL_random))
      currentErrorCheckingPolicy = new ErrorCheckingRandom(Integer
          .parseInt(nbOfNodes));
    else if (policy.equals(DatabasesXmlTags.VAL_roundRobin))
      currentErrorCheckingPolicy = new ErrorCheckingRoundRobin(Integer
          .parseInt(nbOfNodes));
    else if (policy.equals(DatabasesXmlTags.VAL_all))
      currentErrorCheckingPolicy = new ErrorCheckingAll();
    else
      throw new SAXException(Translate.get(
          "virtualdatabase.xml.loadbalancer.errorchecking.unsupported", policy));
  }

  /**
   * Add a CreateTable rule.
   * 
   * @param atts parsed attributes
   * @exception SAXException if an error occurs
   */
  private void newCreateTable(Attributes atts) throws SAXException
  {
    String tableName = atts.getValue(DatabasesXmlTags.ATT_tableName);
    String nbOfNodes = atts.getValue(DatabasesXmlTags.ATT_numberOfNodes);
    String policy = atts.getValue(DatabasesXmlTags.ATT_policy);
    backendNameList = new ArrayList();
    if (policy.equals(DatabasesXmlTags.VAL_random))
      currentCreateTableRule = new CreateTableRandom(backendNameList);
    else if (policy.equals(DatabasesXmlTags.VAL_roundRobin))
      currentCreateTableRule = new CreateTableRoundRobin(backendNameList);
    else if (policy.equals(DatabasesXmlTags.VAL_all))
      currentCreateTableRule = new CreateTableAll(backendNameList);
    else
      throw new SAXException(Translate.get(
          "virtualdatabase.xml.create.table.unsupported", policy));

    currentCreateTableRule.setNumberOfNodes(Integer.parseInt(nbOfNodes));
    currentCreateTableRule.setTableName(tableName);
  }

  /**
   * Adds a backend name to the current backendNameList.
   * 
   * @param atts parsed attributes
   */
  private void newBackendName(Attributes atts)
  {
    String name = atts.getValue(DatabasesXmlTags.ATT_name);
    if (logger.isDebugEnabled())
      logger.debug(Translate
          .get("virtualdatabase.xml.backend.policy.add", name));
    backendNameList.add(name);
  }

  /**
   * Sets the weight of the {@link #currentLoadBalancer}using the parsed
   * attributes.
   * 
   * @param atts parsed attributes
   * @exception SAXException if an error occurs
   */
  private void newBackendWeight(Attributes atts) throws SAXException
  {
    String name = atts.getValue(DatabasesXmlTags.ATT_name);
    try
    {
      int weight = Integer.parseInt(atts.getValue(DatabasesXmlTags.ATT_weight));

      if (logger.isDebugEnabled())
        logger.debug(Translate.get("virtualdatabase.xml.backend.weigth.set",
            new String[]{String.valueOf(weight), name}));

      currentLoadBalancer.setWeight(name, weight);
    }
    catch (Exception e)
    {
      String msg = Translate.get("virtualdatabase.xml.backend.weigth.failed",
          name);
      logger.error(msg, e);
      throw new SAXException(msg, e);
    }
  }

  /* Log recovery */

  /**
   * Sets the currentRecoveryLog as new <code>RecoveryLog</code> using the
   * parsed attributes.
   * 
   * @param atts parsed attributes
   * @exception SAXException if an error occurs
   */
  private void newRecoveryLog(Attributes atts) throws SAXException
  {
    try
    {
      String driverClassName = atts.getValue(DatabasesXmlTags.ATT_driver);
      String driverPath = atts.getValue(DatabasesXmlTags.ATT_driverPath);
      String url = atts.getValue(DatabasesXmlTags.ATT_url);
      String login = atts.getValue(DatabasesXmlTags.ATT_login);
      String password = atts.getValue(DatabasesXmlTags.ATT_password);
      String timeout = atts.getValue(DatabasesXmlTags.ATT_requestTimeout);
      int recoveryBatchSize = Integer.parseInt(atts
          .getValue(DatabasesXmlTags.ATT_recoveryBatchSize));
      // Convert to ms
      requestTimeout = Integer.parseInt(timeout) * 1000;

      if (logger.isDebugEnabled())
        logger.debug(Translate.get(
            "virtualdatabase.xml.recoverylog.jdbc.create", new String[]{
                driverClassName, url, login, password,
                String.valueOf(requestTimeout)}));

      currentRecoveryLog = new RecoveryLog(driverPath, driverClassName, url,
          login, password, requestTimeout, recoveryBatchSize);
      // currentRecoveryLog.setBackendTableCreateStatement();

      String attr;
      attr = atts.getValue("idleConnectionTimeout");
      if (attr != null)
      {
        logger.info("Setting recoverylog idleConnectionTimeout=" + attr);
        try
        {
          currentRecoveryLog.setAutoCloseTimeout(Integer.parseInt(attr));
        }
        catch (Exception e)
        {
          logger.error(e.getMessage());
        }
      }

      attr = atts.getValue("checkConnectionValidity");
      if (attr != null && !"".equals(attr))
      {
        logger.info("Enabling recoverylog ConnectionValidity check.");
        currentRecoveryLog.setCheckConnectionValidity();
      }

    }
    catch (Exception e)
    {
      String msg = Translate.get("virtualdatabase.xml.recoverylog.jdbc.failed");
      logger.error(msg, e);
      throw new SAXException(msg, e);
    }
  }

  /**
   * Sets the recovery log table create statement for the current
   * <code>RecoveryLog</code> using the <code>RecoveryLogTable</code> parsed
   * attributes.
   * 
   * @param atts parsed attributes
   * @exception SAXException if an error occurs
   */
  private void newRecoveryLogTable(Attributes atts) throws SAXException
  {
    try
    {
      String createTable = atts.getValue(DatabasesXmlTags.ATT_createTable);
      String tableName = atts.getValue(DatabasesXmlTags.ATT_tableName);
      String idType = atts.getValue(DatabasesXmlTags.ATT_logIdColumnType);
      String vloginType = atts.getValue(DatabasesXmlTags.ATT_vloginColumnType);
      String sqlName = atts.getValue(DatabasesXmlTags.ATT_sqlColumnName);
      String sqlType = atts.getValue(DatabasesXmlTags.ATT_sqlColumnType);
      String sqlParamType = atts
          .getValue(DatabasesXmlTags.ATT_sqlParamColumnType);
      String autoConnTrans = atts
          .getValue(DatabasesXmlTags.ATT_autoConnTranColumnType);
      String transactionIdType = atts
          .getValue(DatabasesXmlTags.ATT_transactionIdColumnType);
      String requestIdColumnType = atts
          .getValue(DatabasesXmlTags.ATT_requestIdColumnType);
      String execTimeColumnType = atts
          .getValue(DatabasesXmlTags.ATT_execTimeColumnType);
      String updateCountColumnType = atts
          .getValue(DatabasesXmlTags.ATT_updateCountColumnType);
      String extraStatement = atts
          .getValue(DatabasesXmlTags.ATT_extraStatementDefinition);

      if (idType == null)
        throw new SAXException("Invalid null column type for logId column");
      if (vloginType == null)
        throw new SAXException("Invalid null column type for vlogin column");
      if (sqlType == null)
        throw new SAXException("Invalid null column type for sql column");
      if (sqlParamType == null)
        throw new SAXException("Invalid null column type for sql_param column");
      if (transactionIdType == null)
        throw new SAXException(
            "Invalid null column type for transaction_id column");
      if (requestIdColumnType == null)
        throw new SAXException("Invalid null column type for request_id column");
      if (execTimeColumnType == null)
        throw new SAXException("Invalid null column type for exec_time column");
      if (updateCountColumnType == null)
        throw new SAXException(
            "Invalid null column type for udpate_count column");

      if (currentRecoveryLog == null)
      {
        String msg = Translate
            .get("virtualdatabase.xml.recoverylog.jdbc.recoverytable.setnull");
        logger.error(msg);
        throw new SAXException(msg);
      }
      else
        currentRecoveryLog.setLogTableCreateStatement(createTable, tableName,
            idType, vloginType, sqlName, sqlType, sqlParamType, autoConnTrans,
            transactionIdType, requestIdColumnType, execTimeColumnType,
            updateCountColumnType, extraStatement);
    }
    catch (Exception e)
    {
      String msg = Translate
          .get("virtualdatabase.xml.recoverylog.jdbc.recoverytable.failed");
      logger.error(msg, e);
      throw new SAXException(msg, e);
    }
  }

  /**
   * Sets the checkpoint table create statement for the current
   * <code>RecoveryLog</code> using the <code>CheckpointTable</code> parsed
   * attributes.
   * 
   * @param atts parsed attributes
   * @exception SAXException if an error occurs
   */
  private void newRecoveryCheckpointTable(Attributes atts) throws SAXException
  {
    try
    {
      String createTable = atts.getValue(DatabasesXmlTags.ATT_createTable);
      String tableName = atts.getValue(DatabasesXmlTags.ATT_tableName);
      String nameType = atts
          .getValue(DatabasesXmlTags.ATT_checkpointNameColumnType);
      String logIdType = atts.getValue(DatabasesXmlTags.ATT_logIdColumnType);
      String extraStatement = atts
          .getValue(DatabasesXmlTags.ATT_extraStatementDefinition);

      if (currentRecoveryLog == null)
      {
        String msg = Translate
            .get("virtualdatabase.xml.recoverylog.jdbc.checkpointtable.setnull");
        logger.error(msg);
        throw new SAXException(msg);
      }
      else
        currentRecoveryLog.setCheckpointTableCreateStatement(createTable,
            tableName, nameType, logIdType, extraStatement);
    }
    catch (Exception e)
    {
      String msg = Translate
          .get("virtualdatabase.xml.recoverylog.jdbc.checkpointtable.failed");
      logger.error(msg, e);
      throw new SAXException(msg, e);
    }
  }

  /**
   * Sets the backend table create statement for the current
   * <code>RecoveryLog</code> using the <code>BackendTable</code> parsed
   * attributes.
   * 
   * @param atts parsed attributes
   * @exception SAXException if an error occurs
   */
  private void newRecoveryBackendTable(Attributes atts) throws SAXException
  {
    try
    {
      String createTable = atts.getValue(DatabasesXmlTags.ATT_createTable);
      String tableName = atts.getValue(DatabasesXmlTags.ATT_tableName);
      String checkpointNameType = atts
          .getValue(DatabasesXmlTags.ATT_checkpointNameColumnType);
      String databaseNameType = atts
          .getValue(DatabasesXmlTags.ATT_databaseNameColumnType);
      String backendNameType = atts
          .getValue(DatabasesXmlTags.ATT_backendNameColumnType);
      String backendStateType = atts
          .getValue(DatabasesXmlTags.ATT_backendStateColumnType);
      String extraStatement = atts
          .getValue(DatabasesXmlTags.ATT_extraStatementDefinition);

      if (currentRecoveryLog == null)
      {
        String msg = Translate
            .get("virtualdatabase.xml.recoverylog.jdbc.backendtable.setnull");
        logger.error(msg);
        throw new SAXException(msg);
      }
      else
        currentRecoveryLog.setBackendTableCreateStatement(createTable,
            tableName, checkpointNameType, backendNameType, backendStateType,
            databaseNameType, extraStatement);
    }
    catch (Exception e)
    {
      String msg = Translate
          .get("virtualdatabase.xml.recoverylog.jdbc.backendtable.failed");
      logger.error(msg, e);
      throw new SAXException(msg, e);
    }
  }

  /**
   * Sets the dump table create statement for the current
   * <code>RecoveryLog</code> using the <code>DumpTable</code> parsed
   * attributes.
   * 
   * @param atts parsed attributes
   * @exception SAXException if an error occurs
   */
  private void newRecoveryDumpTable(Attributes atts) throws SAXException
  {
    try
    {
      String createTable = atts.getValue(DatabasesXmlTags.ATT_createTable);
      String tableName = atts.getValue(DatabasesXmlTags.ATT_tableName);
      String dumpNameColumnType = atts
          .getValue(DatabasesXmlTags.ATT_dumpNameColumnType);
      String dumpDateColumnType = atts
          .getValue(DatabasesXmlTags.ATT_dumpDateColumnType);
      String dumpPathColumnType = atts
          .getValue(DatabasesXmlTags.ATT_dumpPathColumnType);
      String dumpTypeColumnType = atts
          .getValue(DatabasesXmlTags.ATT_dumpFormatColumnType);
      String checkpointNameColumnType = atts
          .getValue(DatabasesXmlTags.ATT_checkpointNameColumnType);
      String backendNameColumnType = atts
          .getValue(DatabasesXmlTags.ATT_backendNameColumnType);
      String tablesColumnName = atts
          .getValue(DatabasesXmlTags.ATT_tablesColumnName);
      String tablesColumnType = atts
          .getValue(DatabasesXmlTags.ATT_tablesColumnType);

      String extraStatement = atts
          .getValue(DatabasesXmlTags.ATT_extraStatementDefinition);

      if (currentRecoveryLog == null)
      {
        String msg = Translate
            .get("virtualdatabase.xml.recoverylog.jdbc.dumptable.setnull");
        logger.error(msg);
        throw new SAXException(msg);
      }
      else
        currentRecoveryLog.setDumpTableCreateStatement(createTable, tableName,
            dumpNameColumnType, dumpDateColumnType, dumpPathColumnType,
            dumpTypeColumnType, checkpointNameColumnType,
            backendNameColumnType, tablesColumnName, tablesColumnType,
            extraStatement);
    }
    catch (Exception e)
    {
      String msg = Translate
          .get("virtualdatabase.xml.recoverylog.jdbc.dumptable.failed");
      logger.error(msg, e);
      throw new SAXException(msg, e);
    }
  }

  /* Connection manager */

  /**
   * Sets the currentRecoveryLog as new <code>RecoveryLog</code> using the
   * parsed attributes.
   * 
   * @param atts parsed attributes
   * @exception SAXException if an error occurs
   */
  private void newConnectionManager(Attributes atts) throws SAXException
  {
    settingDefaultConnectionManager = false;
    connectionManagerVLogin = atts.getValue(DatabasesXmlTags.ATT_vLogin);
    String connectionManagerRLogin = atts.getValue(DatabasesXmlTags.ATT_rLogin);
    String connectionManagerRPassword = atts
        .getValue(DatabasesXmlTags.ATT_rPassword);
    String backendName = currentBackend.getName();

    // Check that the virtual login has been defined
    if (!currentAuthenticationManager
        .isValidVirtualLogin(connectionManagerVLogin))
    {
      String msg = Translate.get(
          "virtualdatabase.xml.connectionmanager.vlogin.undefined",
          new String[]{connectionManagerVLogin, currentBackend.getName()});
      logger.error(msg);
      throw new SAXException(msg);
    }

    // If values are missing, we default to the virtual login/password
    if (connectionManagerRLogin == null)
      connectionManagerRLogin = connectionManagerVLogin;
    if (connectionManagerRPassword == null)
      connectionManagerRPassword = currentAuthenticationManager
          .getVirtualPassword(connectionManagerVLogin);

    // Add Real user for the database
    currentDatabaseBackendUser = new DatabaseBackendUser(backendName,
        connectionManagerRLogin, connectionManagerRPassword);

    if (logger.isDebugEnabled())
      logger.debug(Translate
          .get("virtualdatabase.xml.authentication.login.real.add",
              new String[]{connectionManagerRLogin, connectionManagerRPassword,
                  backendName}));

    try
    {
      currentAuthenticationManager.addRealUser(connectionManagerVLogin,
          currentDatabaseBackendUser);
    }
    catch (AuthenticationManagerException e)
    {
      String msg = Translate
          .get("virtualdatabase.xml.authentication.login.real.add.failed");
      logger.error(msg, e);
      throw new SAXException(msg, e);
    }
  }

  /**
   * TODO: newDefaultConnectionManager definition.
   */
  private void newDefaultConnectionManager()
  {
    settingDefaultConnectionManager = true;
  }

  /**
   * Adds a new <code>SimpleConnectionManager</code> to
   * {@link #currentBackend}using the parsed attributes.
   */
  private void newSimpleConnectionManager()
  {
    if (settingDefaultConnectionManager)
    {
      if (logger.isDebugEnabled())
      {
        logger
            .debug("Setting a SimpleConnectionManager as default connection manager");
      }
      currentBackend.setDefaultConnectionManager(new SimpleConnectionManager(
          currentBackend.getURL(), currentBackend.getName(),
          currentDatabaseBackendUser.getLogin(), currentDatabaseBackendUser
              .getPassword(), currentBackend.getDriverPath(), currentBackend
              .getDriverClassName()));
    }
    else
    {
      if (logger.isDebugEnabled())
        logger.debug(Translate.get(
            "virtualdatabase.xml.connectionmanager.simple.add", new String[]{
                currentBackend.getName(), connectionManagerVLogin,
                currentDatabaseBackendUser.getLogin(),
                currentDatabaseBackendUser.getPassword()}));

      currentBackend.addConnectionManager(connectionManagerVLogin,
          new SimpleConnectionManager(currentBackend.getURL(), currentBackend
              .getName(), currentDatabaseBackendUser.getLogin(),
              currentDatabaseBackendUser.getPassword(), currentBackend
                  .getDriverPath(), currentBackend.getDriverClassName()));
    }
  }

  /**
   * Adds a new <code>FailFastPoolConnectionManager</code> to
   * {@link #currentBackend}using the parsed attributes.
   * 
   * @param atts parsed attributes
   * @exception SAXException if an error occurs
   */
  private void newFailFastPoolConnectionManager(Attributes atts)
      throws SAXException
  {
    try
    {
      int poolSize = Integer.parseInt(atts
          .getValue(DatabasesXmlTags.ATT_poolSize));

      // sanity check
      if (poolSize < 1)
        throw new IllegalArgumentException(
            Translate
                .get("virtualdatabase.xml.connectionmanager.failfast.failed.parameter"));

      if (settingDefaultConnectionManager)
      {
        if (logger.isDebugEnabled())
        {
          logger
              .debug("Setting a FailFastPoolConnectionManager as default connection manager");
        }
        currentBackend
            .setDefaultConnectionManager(new FailFastPoolConnectionManager(
                currentBackend.getURL(), currentBackend.getName(),
                currentDatabaseBackendUser.getLogin(),
                currentDatabaseBackendUser.getPassword(), currentBackend
                    .getDriverPath(), currentBackend.getDriverClassName(),
                poolSize));
      }
      else
      {
        if (logger.isDebugEnabled())
          logger.debug(Translate.get(
              "virtualdatabase.xml.connectionmanager.failfast.add",
              new String[]{currentBackend.getName(), connectionManagerVLogin,
                  String.valueOf(poolSize),
                  currentDatabaseBackendUser.getLogin(),
                  currentDatabaseBackendUser.getPassword()}));

        currentBackend.addConnectionManager(connectionManagerVLogin,
            new FailFastPoolConnectionManager(currentBackend.getURL(),
                currentBackend.getName(),
                currentDatabaseBackendUser.getLogin(),
                currentDatabaseBackendUser.getPassword(), currentBackend
                    .getDriverPath(), currentBackend.getDriverClassName(),
                poolSize));
      }
    }
    catch (Exception e)
    {
      String msg = Translate.get(
          "virtualdatabase.xml.connectionmanager.failfast.failed",
          currentBackend.getName());
      logger.error(msg, e);
      throw new SAXException(msg, e);
    }
  }

  /**
   * Adds a new <code>RandomWaitPoolConnectionManager</code> to
   * {@link #currentBackend}using the parsed attributes.
   * 
   * @param atts parsed attributes
   * @exception SAXException if an error occurs
   */
  private void newRandomWaitPoolConnectionManager(Attributes atts)
      throws SAXException
  {
    try
    {
      int poolSize = Integer.parseInt(atts
          .getValue(DatabasesXmlTags.ATT_poolSize));
      int timeout = Integer.parseInt(atts
          .getValue(DatabasesXmlTags.ATT_timeout));

      // sanity check
      if (timeout < 0 || poolSize < 1)
        throw new IllegalArgumentException(
            Translate
                .get("virtualdatabase.xml.connectionmanager.randomwait.failed.parameter"));

      if (settingDefaultConnectionManager)
      {
        if (logger.isDebugEnabled())
        {
          logger
              .debug("Setting a RandomWaitPoolConnectionManager as default connection manager");
        }
        currentBackend
            .setDefaultConnectionManager(new RandomWaitPoolConnectionManager(
                currentBackend.getURL(), currentBackend.getName(),
                currentDatabaseBackendUser.getLogin(),
                currentDatabaseBackendUser.getPassword(), currentBackend
                    .getDriverPath(), currentBackend.getDriverClassName(),
                poolSize, timeout));
      }
      else
      {
        if (logger.isDebugEnabled())
          logger.debug(Translate.get(
              "virtualdatabase.xml.connectionmanager.randomwait.add",
              new String[]{currentBackend.getName(), connectionManagerVLogin,
                  String.valueOf(poolSize), String.valueOf(timeout),
                  currentDatabaseBackendUser.getLogin(),
                  currentDatabaseBackendUser.getPassword()}));

        currentBackend.addConnectionManager(connectionManagerVLogin,
            new RandomWaitPoolConnectionManager(currentBackend.getURL(),
                currentBackend.getName(),
                currentDatabaseBackendUser.getLogin(),
                currentDatabaseBackendUser.getPassword(), currentBackend
                    .getDriverPath(), currentBackend.getDriverClassName(),
                poolSize, timeout));
      }
    }
    catch (Exception e)
    {
      String msg = Translate.get(
          "virtualdatabase.xml.connectionmanager.randomwait.failed",
          currentBackend.getName());
      logger.error(msg, e);
      throw new SAXException(msg, e);
    }
  }

  /**
   * Adds a new <code>VariablePoolConnectionManager</code> to
   * {@link #currentBackend}using the parsed attributes.
   * 
   * @param atts parsed attributes
   * @exception SAXException if an error occurs
   */
  private void newVariablePoolConnectionManager(Attributes atts)
      throws SAXException
  {
    try
    {
      int initPoolSize = Integer.parseInt(atts
          .getValue(DatabasesXmlTags.ATT_initPoolSize));

      int minPoolSize = initPoolSize;
      // minPoolSize is IMPLIED and may be null
      String attr = atts.getValue(DatabasesXmlTags.ATT_minPoolSize);
      if (attr != null)
        minPoolSize = Integer.parseInt(attr);

      int maxPoolSize = Integer.parseInt(atts
          .getValue(DatabasesXmlTags.ATT_maxPoolSize));
      int idleTimeout = Integer.parseInt(atts
          .getValue(DatabasesXmlTags.ATT_idleTimeout));
      int waitTimeout = Integer.parseInt(atts
          .getValue(DatabasesXmlTags.ATT_waitTimeout));

      // sanity checks
      if (minPoolSize < 0 || maxPoolSize < 0
          || (maxPoolSize != 0 && minPoolSize > maxPoolSize)
          || (maxPoolSize != 0 && initPoolSize > maxPoolSize)
          || initPoolSize < minPoolSize)
        throw new IllegalArgumentException(
            Translate
                .get("virtualdatabase.xml.connectionmanager.variable.failed.parameter"));

      if (settingDefaultConnectionManager)
      {
        if (logger.isDebugEnabled())
        {
          logger
              .debug("Setting a VariablePoolConnectionManager as default connection manager");
        }
        currentBackend
            .setDefaultConnectionManager(new VariablePoolConnectionManager(
                currentBackend.getURL(), currentBackend.getName(),
                currentDatabaseBackendUser.getLogin(),
                currentDatabaseBackendUser.getPassword(), currentBackend
                    .getDriverPath(), currentBackend.getDriverClassName(),
                initPoolSize, minPoolSize, maxPoolSize, idleTimeout,
                waitTimeout));
      }
      else
      {
        if (logger.isDebugEnabled())
          logger.debug(Translate.get(
              "virtualdatabase.xml.connectionmanager.randomwait.add",
              new String[]{currentBackend.getName(), connectionManagerVLogin,
                  String.valueOf(initPoolSize), String.valueOf(minPoolSize),
                  String.valueOf(maxPoolSize), String.valueOf(idleTimeout),
                  String.valueOf(waitTimeout),
                  currentDatabaseBackendUser.getLogin(),
                  currentDatabaseBackendUser.getPassword()}));

        currentBackend.addConnectionManager(connectionManagerVLogin,
            new VariablePoolConnectionManager(currentBackend.getURL(),
                currentBackend.getName(),
                currentDatabaseBackendUser.getLogin(),
                currentDatabaseBackendUser.getPassword(), currentBackend
                    .getDriverPath(), currentBackend.getDriverClassName(),
                initPoolSize, minPoolSize, maxPoolSize, idleTimeout,
                waitTimeout));
      }
    }
    catch (Exception e)
    {
      String msg = Translate.get(
          "virtualdatabase.xml.connectionmanager.variable.failed",
          currentBackend.getName());
      logger.error(msg, e);
      throw new SAXException(msg, e);
    }
  }

  /* Database schema */

  /**
   * Sets {@link #currentTable}as a new <code> DatabaseTable</code> using the
   * parsed attributs.
   * 
   * @param atts parsed attributes
   * @exception SAXException if error occurs
   */
  private void newDatabaseTable(Attributes atts) throws SAXException
  {
    String tableName = atts.getValue(DatabasesXmlTags.ATT_tableName);
    String nbOfColumns = atts.getValue(DatabasesXmlTags.ATT_nbOfColumns);

    if (logger.isDebugEnabled())
      logger.debug(Translate.get("virtualdatabase.xml.schema.table.add",
          new String[]{tableName, String.valueOf(nbOfColumns)}));

    numberOfColumns = Integer.parseInt(nbOfColumns);

    try
    {
      currentTable = new DatabaseTable(tableName, numberOfColumns);
    }
    catch (NumberFormatException e)
    {
      String msg = Translate.get("virtualdatabase.xml.schema.table.failed",
          tableName);
      logger.error(msg, e);
      throw new SAXException(msg, e);
    }
  }

  /**
   * Set the dynamic schema fetching options on the current backend.
   * 
   * @param atts parsed attributes
   */
  private void newDatabaseSchema(Attributes atts)
  {
    String dynamicLevel = atts.getValue(DatabasesXmlTags.ATT_dynamicPrecision);
    String gatherSystemTable = atts
        .getValue(DatabasesXmlTags.ATT_gatherSystemTables);
    String schemaName = atts.getValue(DatabasesXmlTags.ATT_schemaName);

    if (dynamicLevel == null)
      dynamicLevel = DatabasesXmlTags.VAL_all;
    if (gatherSystemTable == null)
      gatherSystemTable = DatabasesXmlTags.VAL_false;
    currentBackend.setDynamicPrecision(DatabaseBackendSchemaConstants
        .getDynamicSchemaLevel(dynamicLevel), DatabasesXmlTags.VAL_true
        .equals(gatherSystemTable), schemaName);
  }

  /**
   * Set the default stored procedure semantic on the current backend.
   * 
   * @param atts parsed attributes
   */
  private void newDefaultStoredProcedureSemantic(Attributes atts)
  {
    String writeTables = atts.getValue(DatabasesXmlTags.ATT_writeTables);
    String hasSelect = atts.getValue(DatabasesXmlTags.ATT_hasSelect);
    String hasInsert = atts.getValue(DatabasesXmlTags.ATT_hasInsert);
    String hasUpdate = atts.getValue(DatabasesXmlTags.ATT_hasUpdate);
    String hasDelete = atts.getValue(DatabasesXmlTags.ATT_hasDelete);
    String hasDDL = atts.getValue(DatabasesXmlTags.ATT_hasDDL);
    String hasTransaction = atts.getValue(DatabasesXmlTags.ATT_hasTransaction);
    String isCausallyDependent = atts
        .getValue(DatabasesXmlTags.ATT_isCausallyDependent);
    String isCommutative = atts.getValue(DatabasesXmlTags.ATT_isCommutative);

    DatabaseProcedureSemantic defaultSemantic = new DatabaseProcedureSemantic(
        DatabasesXmlTags.VAL_true.equals(hasSelect), DatabasesXmlTags.VAL_true
            .equals(hasInsert), DatabasesXmlTags.VAL_true.equals(hasUpdate),
        DatabasesXmlTags.VAL_true.equals(hasDelete), DatabasesXmlTags.VAL_true
            .equals(hasDDL), DatabasesXmlTags.VAL_true.equals(hasTransaction),
        DatabasesXmlTags.VAL_true.equals(isCausallyDependent),
        DatabasesXmlTags.VAL_true.equals(isCommutative));
    if (writeTables != null)
    {
      StringTokenizer st = new StringTokenizer(writeTables, ",");
      while (st.hasMoreTokens())
      {
        defaultSemantic.addWriteTable(st.nextToken().trim());
      }
    }
    currentBackend.setDefaultStoredProcedureSemantic(defaultSemantic);
  }

  /**
   * Set the default stored procedure semantic on the current backend.
   * 
   * @param atts parsed attributes
   * @exception SAXException if error occurs
   */
  private void newStoredProcedureSemantic(Attributes atts) throws SAXException
  {
    String procedureName = atts.getValue(DatabasesXmlTags.ATT_procedureName);
    int parameterCount;
    try
    {
      parameterCount = Integer.valueOf(
          atts.getValue(DatabasesXmlTags.ATT_parameterCount)).intValue();
    }
    catch (NumberFormatException e)
    {
      parameterCount = -1;
    }
    if (parameterCount < 0)
      throw new SAXException(
          "Invalid parameter count for stored procedure semantic "
              + procedureName);
    String proceduresReferenced = atts
        .getValue(DatabasesXmlTags.ATT_proceduresReferenced);
    String writeTables = atts.getValue(DatabasesXmlTags.ATT_writeTables);
    String hasSelect = atts.getValue(DatabasesXmlTags.ATT_hasSelect);
    String hasInsert = atts.getValue(DatabasesXmlTags.ATT_hasInsert);
    String hasUpdate = atts.getValue(DatabasesXmlTags.ATT_hasUpdate);
    String hasDelete = atts.getValue(DatabasesXmlTags.ATT_hasDelete);
    String hasDDL = atts.getValue(DatabasesXmlTags.ATT_hasDDL);
    String hasTransaction = atts.getValue(DatabasesXmlTags.ATT_hasTransaction);
    String isCausallyDependent = atts
        .getValue(DatabasesXmlTags.ATT_isCausallyDependent);
    String isCommutative = atts.getValue(DatabasesXmlTags.ATT_isCommutative);

    DatabaseProcedureSemantic defaultSemantic = new DatabaseProcedureSemantic(
        DatabasesXmlTags.VAL_true.equals(hasSelect), DatabasesXmlTags.VAL_true
            .equals(hasInsert), DatabasesXmlTags.VAL_true.equals(hasUpdate),
        DatabasesXmlTags.VAL_true.equals(hasDelete), DatabasesXmlTags.VAL_true
            .equals(hasDDL), DatabasesXmlTags.VAL_true.equals(hasTransaction),
        DatabasesXmlTags.VAL_true.equals(isCausallyDependent),
        DatabasesXmlTags.VAL_true.equals(isCommutative));
    if (writeTables != null)
    {
      StringTokenizer st = new StringTokenizer(writeTables, ",");
      while (st.hasMoreTokens())
      {
        defaultSemantic.addWriteTable(st.nextToken().trim());
      }
    }
    if (proceduresReferenced != null)
    {
      StringTokenizer st = new StringTokenizer(proceduresReferenced, ",");
      while (st.hasMoreTokens())
      {
        defaultSemantic.addProcedureRef(proceduresReferenced);
      }
    }
    currentBackend.addStoredProcedureSemantic(procedureName, parameterCount,
        defaultSemantic);
  }

  /**
   * Adds to {@link #currentTable}a new <code> DatabaseColumn</code> using the
   * parsed attributes.
   * 
   * @param atts parsed attributes
   */
  private void newDatabaseColumn(Attributes atts)
  {
    String columnName = atts.getValue(DatabasesXmlTags.ATT_columnName);
    String isUnique = atts.getValue(DatabasesXmlTags.ATT_isUnique);

    if (logger.isDebugEnabled())
      logger.debug(Translate.get("virtualdatabase.xml.schema.column.add",
          new String[]{columnName, String.valueOf(isUnique)}));

    currentTable.addColumn(new DatabaseColumn(columnName, isUnique
        .equals(DatabasesXmlTags.VAL_true)));
  }

  private void newDatabaseProcedure(Attributes atts)
  {
    String procedureName = atts.getValue(DatabasesXmlTags.ATT_name);
    String returnType = atts.getValue(DatabasesXmlTags.ATT_returnType);
    if (logger.isDebugEnabled())
      logger.debug(Translate.get("virtualdatabase.xml.schema.procedure.add",
          new String[]{procedureName, returnType}));

    currentProcedure = new DatabaseProcedure(procedureName, "",
        DatabaseProcedure.getTypeFromString(returnType));
  }

  private void newDatabaseProcedureColumn(Attributes atts)
  {
    String paramName = atts.getValue(DatabasesXmlTags.ATT_name);
    String nullable = atts.getValue(DatabasesXmlTags.ATT_nullable);
    String type = atts.getValue(DatabasesXmlTags.ATT_paramType);
    if (logger.isDebugEnabled())
      logger.debug(Translate.get(
          "virtualdatabase.xml.schema.procedure.parameter.add", new String[]{
              paramName, nullable, type}));
    currentProcedure.addParameter(new DatabaseProcedureParameter(paramName,
        DatabaseProcedureParameter.getColumnTypeFromString(type),
        DatabaseProcedureParameter.getNullFromString(nullable)));
  }
}