<!--
/*
 * Sequoia: Database clustering technology.
 * Copyright (C) 2002-2004 French National Institute For Research In Computer
 * Science And Control (INRIA).
 * Copyright 2005-2006 Continuent, Inc. 
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
 * Contributor(s): 
 */
-->

<!-- 
  Format and indent xml, we could use indent=true 
  but it does not work with out parser.
-->
<xsl:stylesheet version="1.0" 
     xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
  <xsl:output method="xml" />
  <xsl:param name="indent-increment" select="'   '" />
  
  <xsl:template match="*">
     <xsl:param name="indent" select="'&#xA;'"/>
  
     <xsl:value-of select="$indent"/>
     <xsl:copy>
       <xsl:copy-of select="@*" />
       <xsl:apply-templates>
         <xsl:with-param name="indent"
              select="concat($indent, $indent-increment)"/>
       </xsl:apply-templates>
       <xsl:if test="*">
       	<xsl:value-of select="$indent"/>
       </xsl:if>
     </xsl:copy>
  </xsl:template>
  
  <xsl:template match="comment()|processing-instruction()">
     <xsl:copy />
  </xsl:template>
  
  <!-- WARNING: this is dangerous. Handle with care -->
  <xsl:template match="text()[normalize-space(.)='']"/>
  
</xsl:stylesheet>

