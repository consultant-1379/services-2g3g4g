/*------------------------------------------------------------------------------
 *******************************************************************************
 * COPYRIGHT Ericsson 2014
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
import static com.ericsson.eniq.events.server.common.RequestParametersUtilities.*;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.*;

import javax.ws.rs.core.MultivaluedMap;

import org.junit.Test;

import com.ericsson.eniq.events.server.resources.DataServiceBaseTestCase;
import com.sun.jersey.core.util.MultivaluedMapImpl;

public class NetworkDataVolumeResourceIntegrationTest extends DataServiceBaseTestCase {

    private MultivaluedMap<String, String> map;

    private NetworkDataVolumeResource networkDataVolumeResource;

    private static final String DISPLAY_TYPE = CHART_PARAM;

    private static final String MAX_ROWS_VALUE = "500";

    private static final String APN1 = "apn1";

    private static final long IMSI1 = 12300000000L;

    private static final long MSISDN2 = 20000000000L;

    private static final long TAC2 = 20000000L;

    private static final String APN_GROUP1 = "apn_group1";

    private static final String IMSI_GROUP1 = "imsi_group1";

    private static final String TAC_GROUP1 = "tac_group1";

    @Override
    public void onSetUp() {
        networkDataVolumeResource = new NetworkDataVolumeResource();
        attachDependencies(networkDataVolumeResource);
        networkDataVolumeResource.setUriInfo(this.testUri);
        networkDataVolumeResource.setTechPackCXCMappingService(techPackCXCMappingService);
        map = new MultivaluedMapImpl();
    }

    @Test
    public void testDrillDownByAPN_15min() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForAPN(APN1, CHART_PARAM, FIFTEEN_MINUTES_TEST,
                TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        final String result = networkDataVolumeResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDrillDownByAPN_30min() throws Exception {

        final MultivaluedMap<String, String> map = getRequestParametersForAPN(APN1, GRID_PARAM, THIRTY_MINUTES, TIME_ZONE_OFFSET_OF_MINUS_EIGHT_HOUR);
        final String result = networkDataVolumeResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDrillDownByAPN_1hour() throws Exception {
        map.putSingle(NODE_PARAM, APN1);
        final MultivaluedMap<String, String> map = getRequestParametersForAPN(APN1, CHART_PARAM, ONE_HOUR, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        final String result = networkDataVolumeResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDrillDownByAPN_2hours() throws Exception {
        map.putSingle(NODE_PARAM, APN1);
        final MultivaluedMap<String, String> map = getRequestParametersForAPN(APN1, GRID_PARAM, TWO_HOURS, TIME_ZONE_OFFSET_OF_MINUS_EIGHT_HOUR);
        final String result = networkDataVolumeResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDrillDownByAPN_6hours() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForAPN(APN1, CHART_PARAM, SIX_HOURS, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        final String result = networkDataVolumeResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDrillDownByAPN_12hours() throws Exception {

        final MultivaluedMap<String, String> map = getRequestParametersForAPN(APN1, GRID_PARAM, TWELVE_HOURS, TIME_ZONE_OFFSET_OF_MINUS_EIGHT_HOUR);
        final String result = networkDataVolumeResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDrillDownByAPN_1day() throws Exception {
        map.putSingle(NODE_PARAM, APN1);
        final MultivaluedMap<String, String> map = getRequestParametersForAPN(APN1, CHART_PARAM, ONE_DAY, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        final String result = networkDataVolumeResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDrillDownByAPN_1week() throws Exception {
        map.putSingle(NODE_PARAM, APN1);
        final MultivaluedMap<String, String> map = getRequestParametersForAPN(APN1, GRID_PARAM, ONE_WEEK, TIME_ZONE_OFFSET_OF_MINUS_EIGHT_HOUR);
        final String result = networkDataVolumeResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDrillDownByAPN_GROUP_15min() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForGroup(TYPE_APN, APN_GROUP1, CHART_PARAM, FIFTEEN_MINUTES_TEST,
                TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        final String result = networkDataVolumeResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDrillDownByAPN_GROUP_30min() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForGroup(TYPE_APN, APN_GROUP1, GRID_PARAM, THIRTY_MINUTES,
                TIME_ZONE_OFFSET_OF_MINUS_EIGHT_HOUR);
        final String result = networkDataVolumeResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDrillDownByAPN_GROUP_1hour() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForGroup(TYPE_APN, APN_GROUP1, CHART_PARAM, ONE_HOUR,
                TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        final String result = networkDataVolumeResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDrillDownByAPN_GROUP_2hours() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForGroup(TYPE_APN, APN_GROUP1, GRID_PARAM, TWO_HOURS,
                TIME_ZONE_OFFSET_OF_MINUS_EIGHT_HOUR);
        final String result = networkDataVolumeResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDrillDownByAPN_GROUP_6hours() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForGroup(TYPE_APN, APN_GROUP1, CHART_PARAM, SIX_HOURS,
                TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        final String result = networkDataVolumeResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDrillDownByAPN_GROUP_12hours() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForGroup(TYPE_APN, APN_GROUP1, GRID_PARAM, TWELVE_HOURS,
                TIME_ZONE_OFFSET_OF_MINUS_EIGHT_HOUR);
        final String result = networkDataVolumeResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDrillDownByAPN_GROUP_1day() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForGroup(TYPE_APN, APN_GROUP1, CHART_PARAM, ONE_DAY,
                TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        final String result = networkDataVolumeResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDrillDownByAPN_GROUP_1week() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForGroup(TYPE_APN, APN_GROUP1, GRID_PARAM, ONE_WEEK,
                TIME_ZONE_OFFSET_OF_MINUS_EIGHT_HOUR);
        final String result = networkDataVolumeResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDrillDownByIMSI_15min() throws Exception {

        final MultivaluedMap<String, String> map = getRequestParametersForIMSI(String.valueOf(IMSI1), CHART_PARAM, FIFTEEN_MINUTES_TEST,
                TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        final String result = networkDataVolumeResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDrillDownByIMSI_30min() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForIMSI(String.valueOf(IMSI1), GRID_PARAM, THIRTY_MINUTES,
                TIME_ZONE_OFFSET_OF_MINUS_EIGHT_HOUR);
        final String result = networkDataVolumeResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDrillDownByIMSI_1hour() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForIMSI(String.valueOf(IMSI1), CHART_PARAM, ONE_HOUR,
                TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        final String result = networkDataVolumeResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDrillDownByIMSI_2hours() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForIMSI(String.valueOf(IMSI1), GRID_PARAM, TWO_HOURS,
                TIME_ZONE_OFFSET_OF_MINUS_EIGHT_HOUR);
        final String result = networkDataVolumeResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDrillDownByIMSI_6hours() throws Exception {

        final MultivaluedMap<String, String> map = getRequestParametersForIMSI(String.valueOf(IMSI1), CHART_PARAM, SIX_HOURS,
                TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        final String result = networkDataVolumeResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDrillDownByIMSI_12hours() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForIMSI(String.valueOf(IMSI1), GRID_PARAM, TWELVE_HOURS,
                TIME_ZONE_OFFSET_OF_MINUS_EIGHT_HOUR);
        final String result = networkDataVolumeResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDrillDownByIMSI_1day() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForIMSI(String.valueOf(IMSI1), CHART_PARAM, ONE_DAY,
                TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        final String result = networkDataVolumeResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDrillDownByIMSI_1week() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForIMSI(String.valueOf(IMSI1), GRID_PARAM, ONE_WEEK,
                TIME_ZONE_OFFSET_OF_MINUS_EIGHT_HOUR);
        final String result = networkDataVolumeResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDrillDownByIMSI_GROUP_15min() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForGroup(TYPE_IMSI, IMSI_GROUP1, CHART_PARAM, FIFTEEN_MINUTES_TEST,
                TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        final String result = networkDataVolumeResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDrillDownByIMSI_GROUP_30min() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForGroup(TYPE_IMSI, IMSI_GROUP1, GRID_PARAM, THIRTY_MINUTES,
                TIME_ZONE_OFFSET_OF_MINUS_EIGHT_HOUR);
        final String result = networkDataVolumeResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDrillDownByIMSI_GROUP_1hour() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForGroup(TYPE_IMSI, IMSI_GROUP1, CHART_PARAM, ONE_HOUR,
                TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        final String result = networkDataVolumeResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDrillDownByIMSI_GROUP_2hours() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForGroup(TYPE_IMSI, IMSI_GROUP1, GRID_PARAM, TWO_HOURS,
                TIME_ZONE_OFFSET_OF_MINUS_EIGHT_HOUR);
        final String result = networkDataVolumeResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDrillDownByIMSI_GROUP_6hours() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForGroup(TYPE_IMSI, IMSI_GROUP1, CHART_PARAM, SIX_HOURS,
                TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        final String result = networkDataVolumeResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDrillDownByIMSI_GROUP_12hours() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForGroup(TYPE_IMSI, IMSI_GROUP1, GRID_PARAM, TWELVE_HOURS,
                TIME_ZONE_OFFSET_OF_MINUS_EIGHT_HOUR);
        final String result = networkDataVolumeResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDrillDownByIMSI_GROUP_1day() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForGroup(TYPE_IMSI, IMSI_GROUP1, CHART_PARAM, ONE_DAY,
                TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        final String result = networkDataVolumeResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDrillDownByIMSI_GROUP_1week() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForGroup(TYPE_IMSI, IMSI_GROUP1, GRID_PARAM, ONE_WEEK,
                TIME_ZONE_OFFSET_OF_MINUS_EIGHT_HOUR);
        final String result = networkDataVolumeResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDrillDownByTAC_15min() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForTAC(String.valueOf(TAC2), CHART_PARAM, FIFTEEN_MINUTES_TEST,
                TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        final String result = networkDataVolumeResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDrillDownByTAC_30min() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForTAC(String.valueOf(TAC2), GRID_PARAM, THIRTY_MINUTES,
                TIME_ZONE_OFFSET_OF_MINUS_EIGHT_HOUR);
        final String result = networkDataVolumeResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDrillDownByTAC_1hour() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForTAC(String.valueOf(TAC2), CHART_PARAM, ONE_HOUR,
                TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        final String result = networkDataVolumeResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDrillDownByTAC_2hours() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForTAC(String.valueOf(TAC2), GRID_PARAM, TWO_HOURS,
                TIME_ZONE_OFFSET_OF_MINUS_EIGHT_HOUR);
        final String result = networkDataVolumeResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDrillDownByTAC_6hour() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForTAC(String.valueOf(TAC2), GRID_PARAM, SIX_HOURS,
                TIME_ZONE_OFFSET_OF_MINUS_EIGHT_HOUR);
        final String result = networkDataVolumeResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDrillDownByTAC_12hour() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForTAC(String.valueOf(TAC2), GRID_PARAM, TWELVE_HOURS,
                TIME_ZONE_OFFSET_OF_MINUS_EIGHT_HOUR);
        final String result = networkDataVolumeResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDrillDownByTAC_1day() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForTAC(String.valueOf(TAC2), GRID_PARAM, ONE_DAY,
                TIME_ZONE_OFFSET_OF_MINUS_EIGHT_HOUR);
        final String result = networkDataVolumeResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDrillDownByTAC_1week() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForTAC(String.valueOf(TAC2), GRID_PARAM, ONE_WEEK,
                TIME_ZONE_OFFSET_OF_MINUS_EIGHT_HOUR);
        final String result = networkDataVolumeResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDrillDownByTAC_GROUP_15min() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForGroup(TYPE_TAC, TAC_GROUP1, CHART_PARAM, FIFTEEN_MINUTES_TEST,
                TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        final String result = networkDataVolumeResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDrillDownByTAC_GROUP_30min() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForGroup(TYPE_TAC, TAC_GROUP1, GRID_PARAM, THIRTY_MINUTES,
                TIME_ZONE_OFFSET_OF_MINUS_EIGHT_HOUR);
        final String result = networkDataVolumeResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDrillDownByTAC_GROUP_1hour() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForGroup(TYPE_TAC, TAC_GROUP1, CHART_PARAM, ONE_HOUR,
                TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        final String result = networkDataVolumeResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDrillDownByTAC_GROUP_2hours() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForGroup(TYPE_TAC, TAC_GROUP1, GRID_PARAM, TWO_HOURS,
                TIME_ZONE_OFFSET_OF_MINUS_EIGHT_HOUR);
        final String result = networkDataVolumeResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDrillDownByTAC_GROUP_6hour() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForGroup(TYPE_TAC, TAC_GROUP1, CHART_PARAM, SIX_HOURS,
                TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        final String result = networkDataVolumeResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDrillDownByTAC_GROUP_12hour() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForGroup(TYPE_TAC, TAC_GROUP1, GRID_PARAM, TWELVE_HOURS,
                TIME_ZONE_OFFSET_OF_MINUS_EIGHT_HOUR);
        final String result = networkDataVolumeResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDrillDownByTAC_GROUP_1day() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForGroup(TYPE_TAC, TAC_GROUP1, CHART_PARAM, ONE_DAY,
                TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        final String result = networkDataVolumeResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDrillDownByTAC_GROUP_1week() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForGroup(TYPE_TAC, TAC_GROUP1, CHART_PARAM, ONE_WEEK,
                TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        final String result = networkDataVolumeResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDrillDownByMSISDN_15min() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForMSISDN(String.valueOf(MSISDN2), CHART_PARAM, FIFTEEN_MINUTES_TEST,
                TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        final String result = networkDataVolumeResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDrillDownByMSISDN_30min() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForMSISDN(String.valueOf(MSISDN2), GRID_PARAM, THIRTY_MINUTES,
                TIME_ZONE_OFFSET_OF_MINUS_EIGHT_HOUR);
        final String result = networkDataVolumeResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDrillDownByMSISDN_1hour() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForMSISDN(String.valueOf(MSISDN2), CHART_PARAM, ONE_HOUR,
                TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        final String result = networkDataVolumeResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDrillDownByMSISDN_2hours() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForMSISDN(String.valueOf(MSISDN2), GRID_PARAM, TWO_HOURS,
                TIME_ZONE_OFFSET_OF_MINUS_EIGHT_HOUR);
        final String result = networkDataVolumeResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    public void testDrillDownByMSISDN_6hours() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForMSISDN(String.valueOf(MSISDN2), CHART_PARAM, SIX_HOURS,
                TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        final String result = networkDataVolumeResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDrillDownByMSISDN_12hours() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForMSISDN(String.valueOf(MSISDN2), GRID_PARAM, TWELVE_HOURS,
                TIME_ZONE_OFFSET_OF_MINUS_EIGHT_HOUR);
        final String result = networkDataVolumeResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDrillDownByMSISDN_1day() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForMSISDN(String.valueOf(MSISDN2), CHART_PARAM, ONE_DAY,
                TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        final String result = networkDataVolumeResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDrillDownByMSISDN_1week() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForMSISDN(String.valueOf(MSISDN2), GRID_PARAM, ONE_WEEK,
                TIME_ZONE_OFFSET_OF_MINUS_EIGHT_HOUR);
        final String result = networkDataVolumeResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    public void testDrillDownByNetwork_15min() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForNetwork(CHART_PARAM, FIFTEEN_MINUTES_TEST,
                TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        final String result = networkDataVolumeResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDrillDownByNetwork_30min() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForNetwork(GRID_PARAM, THIRTY_MINUTES, TIME_ZONE_OFFSET_OF_MINUS_EIGHT_HOUR);
        final String result = networkDataVolumeResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDrillDownByNetwork_1hour() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForNetwork(CHART_PARAM, ONE_HOUR, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        final String result = networkDataVolumeResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDrillDownByNetwork_2hours() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForNetwork(GRID_PARAM, TWO_HOURS, TIME_ZONE_OFFSET_OF_MINUS_EIGHT_HOUR);
        final String result = networkDataVolumeResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDrillDownByNetwork_6hours() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForNetwork(GRID_PARAM, SIX_HOURS, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        final String result = networkDataVolumeResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDrillDownByNetwork_12hours() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForNetwork(GRID_PARAM, TWELVE_HOURS, TIME_ZONE_OFFSET_OF_MINUS_EIGHT_HOUR);
        final String result = networkDataVolumeResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDrillDownByNetwork_1day() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForNetwork(GRID_PARAM, ONE_DAY, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        final String result = networkDataVolumeResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDrillDownByNetwork_1week() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForNetwork(GRID_PARAM, ONE_WEEK, TIME_ZONE_OFFSET_OF_MINUS_EIGHT_HOUR);
        final String result = networkDataVolumeResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

}
