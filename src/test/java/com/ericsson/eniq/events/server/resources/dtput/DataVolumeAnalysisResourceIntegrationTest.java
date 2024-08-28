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
package com.ericsson.eniq.events.server.resources.dtput;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.*;

import javax.ws.rs.core.MultivaluedMap;

import org.junit.Test;

import com.ericsson.eniq.events.server.resources.DataServiceBaseTestCase;
import com.sun.jersey.core.util.MultivaluedMapImpl;

public class DataVolumeAnalysisResourceIntegrationTest extends DataServiceBaseTestCase {

    private MultivaluedMap<String, String> map;

    private DataVolumeAnalysisResource dataVolumeAnalysisResource;

    private static final String DISPLAY_TYPE = GRID_PARAM;

    private static final String MAX_ROWS_VALUE = "500";

    private static final String APN1 = "apn1";

    private static final long TAC2 = 20000000L;

    private static final String APN_GROUP1 = "apn_group1";

    private static final String TAC_GROUP1 = "tac_group1";

    private static final String IMSI_GROUP1 = "imsi_group1";

    @Override
    public void onSetUp() {
        dataVolumeAnalysisResource = new DataVolumeAnalysisResource();
        attachDependencies(dataVolumeAnalysisResource);
        dataVolumeAnalysisResource.setUriInfo(this.testUri);
        dataVolumeAnalysisResource.setTechPackCXCMappingService(techPackCXCMappingService);
        map = new MultivaluedMapImpl();
    }

    @Test
    public void testDrillDownByTAC() throws Exception {
        map.clear();
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(TAC_PARAM, String.valueOf(TAC2));
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        addTimeParameterToParameterMap();
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = dataVolumeAnalysisResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDrillDownByTAC_withDelayOffset() throws Exception {
        map.clear();
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(TAC_PARAM, String.valueOf(TAC2));
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        addTimeParameterToParameterMap();
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_MINUS_EIGHT_HOUR);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = dataVolumeAnalysisResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDrillDownByTERMINAL_GROUP() throws Exception {
        map.clear();
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(GROUP_NAME_PARAM, TAC_GROUP1);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        addTimeParameterToParameterMap();
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = dataVolumeAnalysisResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDrillDownByTERMINAL_GROUP_withDelayOffset() throws Exception {
        map.clear();
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(GROUP_NAME_PARAM, TAC_GROUP1);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        addTimeParameterToParameterMap();
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_MINUS_EIGHT_HOUR);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        final String result = dataVolumeAnalysisResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    private void addTimeParameterToParameterMap() {
        map.putSingle(TIME_QUERY_PARAM, "2440");
    }
}
