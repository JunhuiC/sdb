
package org.continuent.sequoia.controller.virtualdatabase;

import org.continuent.sequoia.common.log.Trace;

/**
 * Checks idle connection times at regular intervals.
 * 
 * @author <a href="mailto:damian.arregui@continuent.com">Damian Arregui</a>
 * @version 1.0
 */
public class IdleConnectionChecker extends Thread
{
  private static final int SLEEP = 5000;

  private VirtualDatabase vdb;

  private boolean         isKilled = false;
  private Trace           logger;

  /**
   * Creates a new <code>IdleConnectionChecker</code> object
   * 
   * @param vdb virtual database this thread is attached to.
   */
  public IdleConnectionChecker(VirtualDatabase vdb)
  {
    super("IdleConnectionChecker");
    this.vdb = vdb;
    logger = vdb.getLogger();
  }

  /**
   * @see java.lang.Thread#run()
   */
  public void run()
  {
    while (!isKilled)
    {
      vdb.checkActiveThreadsIdleConnectionTime();
      try
      {
        Thread.sleep(SLEEP);
      }
      catch (InterruptedException e)
      {
        if (logger.isDebugEnabled())
          logger
              .debug(
                  "Run interrupted while sleeping in idle connection checker thread",
                  e);
      }
    }
  }

  /**
   * Kills the idle connection checker thread.
   */
  public void kill()
  {
    isKilled = true;
    interrupt();
    try
    {
      this.join(1000);
    }
    catch (InterruptedException e)
    {
      if (logger.isDebugEnabled())
        logger.debug(
            "Kill interrupted while joining idle connection checker thread", e);
    }
  }
}
