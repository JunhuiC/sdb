/**
 * Sequoia: Database clustering technology.
 * Copyright (C) 2006 Continuent, Inc.
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
 * Initial developer(s): Gilles Rayrat.
 * Contributor(s): ______________________.
 */

package org.continuent.sequoia.driver;

/**
 * Provides the two methods that will be called when a controller is detected as
 * failed (does not respond to pings anymore) or as back alive (responds to
 * pings again after silence). Note that the <b>processing time</b> of these
 * two methods implementation <b>is critical</b>: if needed, thread them!
 * 
 * @author <a href="mailto:gilles.rayrat@continuent.com">Gilles Rayrat</a>
 * @version 1.0
 */
public interface ControllerStateChangedCallback
{
  /**
   * What to do when a controller stops responding to pings?<br>
   * Warning: execution time is critical here, if the method's implementation is
   * too slow, thread it!
   */
  void onControllerDown(ControllerInfo ctrl);

  /**
   * What to do when a controller responds again to pings after having been
   * detected as failed<br>
   * Warning: execution time is critical here, if the method's implementation is
   * too slow, thread it!
   */
  void onControllerUp(ControllerInfo ctrl);
}
