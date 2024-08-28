/*------------------------------------------------------------------------------
 *******************************************************************************
 * COPYRIGHT Ericsson 2013
 *
 * The copyright to the computer program(s) herein is the property of
 * Ericsson Inc. The programs may be used and/or copied only with written
 * permission from Ericsson Inc. or in accordance with the terms and
 * conditions stipulated in the agreement/contract under which the
 * program(s) have been supplied.
 *******************************************************************************
 *----------------------------------------------------------------------------*/
package com.ericsson.eniq.events.server.resources;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;

import javax.ejb.LocalBean;
import javax.ejb.Stateless;

@Stateless
@LocalBean
public class KPIRatioBscResource extends KPIRatioResource {
    @Override
    protected String getPath() {
        final StringBuffer sb = new StringBuffer();
        sb.append(KPI_RATIO);
        if (applicationConfigManager.isSuccessRawEnabled()) {
            sb.append("/").append(KPI_RATIO_BSC_SUC_RAW).toString();
        } else {
            sb.append("/").append(KPI_RATIO_BSC).toString();
        }

        return sb.toString();
    }
}
