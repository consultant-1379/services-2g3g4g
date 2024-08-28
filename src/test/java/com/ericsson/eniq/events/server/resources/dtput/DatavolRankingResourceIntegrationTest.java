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

public class DatavolRankingResourceIntegrationTest extends DataServiceBaseTestCase {

    private MultivaluedMap<String, String> map;

    private DatavolRankingResource datavolRankingResource;

    private static final String DISPLAY_TYPE = GRID_PARAM;

    private static final String MAX_ROWS_VALUE = "500";

    @Override
    public void onSetUp() {
        datavolRankingResource = new DatavolRankingResource();
        attachDependencies(datavolRankingResource);
        datavolRankingResource.setUriInfo(this.testUri);
        datavolRankingResource.setTechPackCXCMappingService(techPackCXCMappingService);
        map = new MultivaluedMapImpl();
    }

    @Test
    public void testDataVolumeRankingByIMSI_15MINS() throws Exception {
        map = getRequestParameters(TYPE_IMSI, DISPLAY_TYPE, FIFTEEN_MINUTES_TEST, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR, MAX_ROWS_VALUE);
        final String result = datavolRankingResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDataVolumeRankingByIMSI_30MINS() throws Exception {
        map = getRequestParameters(TYPE_IMSI, DISPLAY_TYPE, THIRTY_MINUTES, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR, MAX_ROWS_VALUE);
        final String result = datavolRankingResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDataVolumeRankingByIMSI_1HOUR() throws Exception {
        map = getRequestParameters(TYPE_IMSI, DISPLAY_TYPE, ONE_HOUR, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR, MAX_ROWS_VALUE);
        final String result = datavolRankingResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDataVolumeRankingByIMSI_2HOURS() throws Exception {
        map = getRequestParameters(TYPE_IMSI, DISPLAY_TYPE, TWO_HOURS, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR, MAX_ROWS_VALUE);
        final String result = datavolRankingResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDataVolumeRankingByIMSI_6HOURS() throws Exception {
        map = getRequestParameters(TYPE_IMSI, DISPLAY_TYPE, SIX_HOURS, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR, MAX_ROWS_VALUE);
        final String result = datavolRankingResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDataVolumeRankingByIMSI_12HOURS() throws Exception {
        map = getRequestParameters(TYPE_IMSI, DISPLAY_TYPE, TWELVE_HOURS, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR, MAX_ROWS_VALUE);
        final String result = datavolRankingResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDataVolumeRankingByIMSI_1DAY() throws Exception {
        map = getRequestParameters(TYPE_IMSI, DISPLAY_TYPE, ONE_DAY, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR, MAX_ROWS_VALUE);
        final String result = datavolRankingResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDataVolumeRankingByIMSI_1WEEK() throws Exception {
        map = getRequestParameters(TYPE_IMSI, DISPLAY_TYPE, ONE_WEEK, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR, MAX_ROWS_VALUE);
        final String result = datavolRankingResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDataVolumeRankingByIMSI_CustomTime() throws Exception {
        map = getRequestParameters(TYPE_IMSI, DISPLAY_TYPE, THREE_DAY, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR, MAX_ROWS_VALUE);
        final String result = datavolRankingResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDataVolumeRankingByIMSIGroup_15MINS() throws Exception {
        map = getRequestParameters(TYPE_IMSI, DISPLAY_TYPE, FIFTEEN_MINUTES_TEST, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR, MAX_ROWS_VALUE);
        final String result = datavolRankingResource.getDatavolRankingResults(REQUEST_ID, map, DATAVOL_GROUP_RANKING_ANALYSIS);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDataVolumeRankingByIMSIGroup_30MINS() throws Exception {
        map = getRequestParameters(TYPE_IMSI, DISPLAY_TYPE, THIRTY_MINUTES, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR, MAX_ROWS_VALUE);
        final String result = datavolRankingResource.getDatavolRankingResults(REQUEST_ID, map, DATAVOL_GROUP_RANKING_ANALYSIS);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDataVolumeRankingByIMSIGroup_1HOUR() throws Exception {
        map = getRequestParameters(TYPE_IMSI, DISPLAY_TYPE, ONE_HOUR, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR, MAX_ROWS_VALUE);
        final String result = datavolRankingResource.getDatavolRankingResults(REQUEST_ID, map, DATAVOL_GROUP_RANKING_ANALYSIS);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDataVolumeRankingByIMSIGroup_2HOURS() throws Exception {
        map = getRequestParameters(TYPE_IMSI, DISPLAY_TYPE, TWO_HOURS, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR, MAX_ROWS_VALUE);
        final String result = datavolRankingResource.getDatavolRankingResults(REQUEST_ID, map, DATAVOL_GROUP_RANKING_ANALYSIS);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDataVolumeRankingByIMSIGroup_6HOURS() throws Exception {
        map = getRequestParameters(TYPE_IMSI, DISPLAY_TYPE, SIX_HOURS, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR, MAX_ROWS_VALUE);
        final String result = datavolRankingResource.getDatavolRankingResults(REQUEST_ID, map, DATAVOL_GROUP_RANKING_ANALYSIS);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDataVolumeRankingByIMSIGroup_12HOURS() throws Exception {
        map = getRequestParameters(TYPE_IMSI, DISPLAY_TYPE, TWELVE_HOURS, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR, MAX_ROWS_VALUE);
        final String result = datavolRankingResource.getDatavolRankingResults(REQUEST_ID, map, DATAVOL_GROUP_RANKING_ANALYSIS);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDataVolumeRankingByIMSIGroup_1DAY() throws Exception {
        map = getRequestParameters(TYPE_IMSI, DISPLAY_TYPE, ONE_DAY, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR, MAX_ROWS_VALUE);
        final String result = datavolRankingResource.getDatavolRankingResults(REQUEST_ID, map, DATAVOL_GROUP_RANKING_ANALYSIS);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDataVolumeRankingByIMSIGroup_1WEEK() throws Exception {
        map = getRequestParameters(TYPE_IMSI, DISPLAY_TYPE, ONE_WEEK, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR, MAX_ROWS_VALUE);
        final String result = datavolRankingResource.getDatavolRankingResults(REQUEST_ID, map, DATAVOL_GROUP_RANKING_ANALYSIS);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDataVolumeRankingByIMSIGroup_CustomTime() throws Exception {
        map = getRequestParameters(TYPE_IMSI, DISPLAY_TYPE, THREE_DAY, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR, MAX_ROWS_VALUE);
        final String result = datavolRankingResource.getDatavolRankingResults(REQUEST_ID, map, DATAVOL_GROUP_RANKING_ANALYSIS);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDataVolumeRankingByTAC_15MINS() throws Exception {
        map = getRequestParameters(TYPE_TAC, DISPLAY_TYPE, FIFTEEN_MINUTES_TEST, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR, MAX_ROWS_VALUE);
        final String result = datavolRankingResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDataVolumeRankingByTAC_30MINS() throws Exception {
        map = getRequestParameters(TYPE_TAC, DISPLAY_TYPE, THIRTY_MINUTES, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR, MAX_ROWS_VALUE);
        final String result = datavolRankingResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDataVolumeRankingByTAC_1HOUR() throws Exception {
        map = getRequestParameters(TYPE_TAC, DISPLAY_TYPE, ONE_HOUR, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR, MAX_ROWS_VALUE);
        final String result = datavolRankingResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDataVolumeRankingByTAC_2HOURS() throws Exception {
        map = getRequestParameters(TYPE_TAC, DISPLAY_TYPE, TWO_HOURS, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR, MAX_ROWS_VALUE);
        final String result = datavolRankingResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDataVolumeRankingByTAC_6HOURS() throws Exception {
        map = getRequestParameters(TYPE_TAC, DISPLAY_TYPE, SIX_HOURS, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR, MAX_ROWS_VALUE);
        final String result = datavolRankingResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDataVolumeRankingByTAC_12HOURS() throws Exception {
        map = getRequestParameters(TYPE_TAC, DISPLAY_TYPE, TWELVE_HOURS, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR, MAX_ROWS_VALUE);
        final String result = datavolRankingResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDataVolumeRankingByTAC_1DAY() throws Exception {
        map = getRequestParameters(TYPE_TAC, DISPLAY_TYPE, ONE_DAY, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR, MAX_ROWS_VALUE);
        final String result = datavolRankingResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDataVolumeRankingByTAC_1WEEK() throws Exception {
        map = getRequestParameters(TYPE_TAC, DISPLAY_TYPE, ONE_WEEK, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR, MAX_ROWS_VALUE);
        final String result = datavolRankingResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDataVolumeRankingByTAC_CustomTime() throws Exception {
        map = getRequestParameters(TYPE_TAC, DISPLAY_TYPE, THREE_DAY, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR, MAX_ROWS_VALUE);
        final String result = datavolRankingResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDataVolumeRankingByTACGroup_15MINS() throws Exception {
        map = getRequestParameters(TYPE_TAC, DISPLAY_TYPE, FIFTEEN_MINUTES_TEST, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR, MAX_ROWS_VALUE);
        final String result = datavolRankingResource.getDatavolRankingResults(REQUEST_ID, map, DATAVOL_GROUP_RANKING_ANALYSIS);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDataVolumeRankingByTACGroup_30MINS() throws Exception {
        map = getRequestParameters(TYPE_TAC, DISPLAY_TYPE, THIRTY_MINUTES, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR, MAX_ROWS_VALUE);
        final String result = datavolRankingResource.getDatavolRankingResults(REQUEST_ID, map, DATAVOL_GROUP_RANKING_ANALYSIS);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDataVolumeRankingByTACGroup_1HOUR() throws Exception {
        map = getRequestParameters(TYPE_TAC, DISPLAY_TYPE, ONE_HOUR, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR, MAX_ROWS_VALUE);
        final String result = datavolRankingResource.getDatavolRankingResults(REQUEST_ID, map, DATAVOL_GROUP_RANKING_ANALYSIS);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDataVolumeRankingByTACGroup_2HOURS() throws Exception {
        map = getRequestParameters(TYPE_TAC, DISPLAY_TYPE, TWO_HOURS, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR, MAX_ROWS_VALUE);
        final String result = datavolRankingResource.getDatavolRankingResults(REQUEST_ID, map, DATAVOL_GROUP_RANKING_ANALYSIS);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDataVolumeRankingByTACGroup_6HOURS() throws Exception {
        map = getRequestParameters(TYPE_TAC, DISPLAY_TYPE, SIX_HOURS, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR, MAX_ROWS_VALUE);
        final String result = datavolRankingResource.getDatavolRankingResults(REQUEST_ID, map, DATAVOL_GROUP_RANKING_ANALYSIS);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDataVolumeRankingByTACGroup_12HOURS() throws Exception {
        map = getRequestParameters(TYPE_TAC, DISPLAY_TYPE, TWELVE_HOURS, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR, MAX_ROWS_VALUE);
        final String result = datavolRankingResource.getDatavolRankingResults(REQUEST_ID, map, DATAVOL_GROUP_RANKING_ANALYSIS);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDataVolumeRankingByTACGroup_1DAY() throws Exception {
        map = getRequestParameters(TYPE_TAC, DISPLAY_TYPE, ONE_DAY, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR, MAX_ROWS_VALUE);
        final String result = datavolRankingResource.getDatavolRankingResults(REQUEST_ID, map, DATAVOL_GROUP_RANKING_ANALYSIS);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDataVolumeRankingByTACGroup_1WEEK() throws Exception {
        map = getRequestParameters(TYPE_TAC, DISPLAY_TYPE, ONE_WEEK, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR, MAX_ROWS_VALUE);
        final String result = datavolRankingResource.getDatavolRankingResults(REQUEST_ID, map, DATAVOL_GROUP_RANKING_ANALYSIS);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDataVolumeRankingByTACGroup_CustomTime() throws Exception {
        map = getRequestParameters(TYPE_TAC, DISPLAY_TYPE, THREE_DAY, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR, MAX_ROWS_VALUE);
        final String result = datavolRankingResource.getDatavolRankingResults(REQUEST_ID, map, DATAVOL_GROUP_RANKING_ANALYSIS);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDataVolumeRankingByGGSN_15MINS() throws Exception {
        map = getRequestParameters(TYPE_GGSN, DISPLAY_TYPE, FIFTEEN_MINUTES_TEST, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR, MAX_ROWS_VALUE);
        final String result = datavolRankingResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDataVolumeRankingByGGSN_30MINS() throws Exception {
        map = getRequestParameters(TYPE_GGSN, DISPLAY_TYPE, THIRTY_MINUTES, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR, MAX_ROWS_VALUE);
        final String result = datavolRankingResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDataVolumeRankingByGGSN_1HOUR() throws Exception {
        map = getRequestParameters(TYPE_GGSN, DISPLAY_TYPE, ONE_HOUR, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR, MAX_ROWS_VALUE);
        final String result = datavolRankingResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDataVolumeRankingByGGSN_2HOURS() throws Exception {
        map = getRequestParameters(TYPE_GGSN, DISPLAY_TYPE, TWO_HOURS, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR, MAX_ROWS_VALUE);
        final String result = datavolRankingResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDataVolumeRankingByGGSN_6HOURS() throws Exception {
        map = getRequestParameters(TYPE_GGSN, DISPLAY_TYPE, SIX_HOURS, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR, MAX_ROWS_VALUE);
        final String result = datavolRankingResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDataVolumeRankingByGGSN_12HOURS() throws Exception {
        map = getRequestParameters(TYPE_GGSN, DISPLAY_TYPE, TWELVE_HOURS, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR, MAX_ROWS_VALUE);
        final String result = datavolRankingResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDataVolumeRankingByGGSN_1DAY() throws Exception {
        map = getRequestParameters(TYPE_GGSN, DISPLAY_TYPE, ONE_DAY, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR, MAX_ROWS_VALUE);
        final String result = datavolRankingResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDataVolumeRankingByGGSN_1WEEK() throws Exception {
        map = getRequestParameters(TYPE_GGSN, DISPLAY_TYPE, ONE_WEEK, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR, MAX_ROWS_VALUE);
        final String result = datavolRankingResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDataVolumeRankingByGGSN_CustomTime() throws Exception {
        map = getRequestParameters(TYPE_GGSN, DISPLAY_TYPE, THREE_DAY, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR, MAX_ROWS_VALUE);
        final String result = datavolRankingResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDataVolumeRankingByAPN_15MINS() throws Exception {
        map = getRequestParameters(TYPE_APN, DISPLAY_TYPE, FIFTEEN_MINUTES_TEST, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR, MAX_ROWS_VALUE);
        final String result = datavolRankingResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDataVolumeRankingByAPN_30MINS() throws Exception {
        map = getRequestParameters(TYPE_APN, DISPLAY_TYPE, THIRTY_MINUTES, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR, MAX_ROWS_VALUE);
        final String result = datavolRankingResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDataVolumeRankingByAPN_1HOUR() throws Exception {
        map = getRequestParameters(TYPE_APN, DISPLAY_TYPE, ONE_HOUR, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR, MAX_ROWS_VALUE);
        final String result = datavolRankingResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDataVolumeRankingByAPN_2HOURS() throws Exception {
        map = getRequestParameters(TYPE_APN, DISPLAY_TYPE, TWO_HOURS, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR, MAX_ROWS_VALUE);
        final String result = datavolRankingResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDataVolumeRankingByAPN_6HOURS() throws Exception {
        map = getRequestParameters(TYPE_APN, DISPLAY_TYPE, SIX_HOURS, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR, MAX_ROWS_VALUE);
        final String result = datavolRankingResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDataVolumeRankingByAPN_12HOURS() throws Exception {
        map = getRequestParameters(TYPE_APN, DISPLAY_TYPE, TWELVE_HOURS, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR, MAX_ROWS_VALUE);
        final String result = datavolRankingResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDataVolumeRankingByAPN_1DAY() throws Exception {
        map = getRequestParameters(TYPE_APN, DISPLAY_TYPE, ONE_DAY, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR, MAX_ROWS_VALUE);
        final String result = datavolRankingResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDataVolumeRankingByAPN_1WEEK() throws Exception {
        map = getRequestParameters(TYPE_APN, DISPLAY_TYPE, ONE_WEEK, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR, MAX_ROWS_VALUE);
        final String result = datavolRankingResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDataVolumeRankingByAPN_CustomTime() throws Exception {
        map = getRequestParameters(TYPE_APN, DISPLAY_TYPE, THREE_DAY, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR, MAX_ROWS_VALUE);
        final String result = datavolRankingResource.getData(REQUEST_ID, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDataVolumeRankingByApnGroup_15MINS() throws Exception {
        map = getRequestParameters(TYPE_APN, DISPLAY_TYPE, FIFTEEN_MINUTES_TEST, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR, MAX_ROWS_VALUE);
        final String result = datavolRankingResource.getDatavolRankingResults(REQUEST_ID, map, DATAVOL_GROUP_RANKING_ANALYSIS);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDataVolumeRankingByApnGroup_30MINS() throws Exception {
        map = getRequestParameters(TYPE_APN, DISPLAY_TYPE, THIRTY_MINUTES, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR, MAX_ROWS_VALUE);
        final String result = datavolRankingResource.getDatavolRankingResults(REQUEST_ID, map, DATAVOL_GROUP_RANKING_ANALYSIS);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDataVolumeRankingByApnGroup_1HOUR() throws Exception {
        map = getRequestParameters(TYPE_APN, DISPLAY_TYPE, ONE_HOUR, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR, MAX_ROWS_VALUE);
        final String result = datavolRankingResource.getDatavolRankingResults(REQUEST_ID, map, DATAVOL_GROUP_RANKING_ANALYSIS);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDataVolumeRankingByApnGroup_2HOURS() throws Exception {
        map = getRequestParameters(TYPE_APN, DISPLAY_TYPE, TWO_HOURS, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR, MAX_ROWS_VALUE);
        final String result = datavolRankingResource.getDatavolRankingResults(REQUEST_ID, map, DATAVOL_GROUP_RANKING_ANALYSIS);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDataVolumeRankingByApnGroup_6HOURS() throws Exception {
        map = getRequestParameters(TYPE_APN, DISPLAY_TYPE, SIX_HOURS, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR, MAX_ROWS_VALUE);
        final String result = datavolRankingResource.getDatavolRankingResults(REQUEST_ID, map, DATAVOL_GROUP_RANKING_ANALYSIS);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDataVolumeRankingByApnGroup_12HOURS() throws Exception {
        map = getRequestParameters(TYPE_APN, DISPLAY_TYPE, TWELVE_HOURS, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR, MAX_ROWS_VALUE);
        final String result = datavolRankingResource.getDatavolRankingResults(REQUEST_ID, map, DATAVOL_GROUP_RANKING_ANALYSIS);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDataVolumeRankingByApnGroup_1DAY() throws Exception {
        map = getRequestParameters(TYPE_APN, DISPLAY_TYPE, ONE_DAY, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR, MAX_ROWS_VALUE);
        final String result = datavolRankingResource.getDatavolRankingResults(REQUEST_ID, map, DATAVOL_GROUP_RANKING_ANALYSIS);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDataVolumeRankingByApnGroup_1WEEK() throws Exception {
        map = getRequestParameters(TYPE_APN, DISPLAY_TYPE, ONE_WEEK, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR, MAX_ROWS_VALUE);
        final String result = datavolRankingResource.getDatavolRankingResults(REQUEST_ID, map, DATAVOL_GROUP_RANKING_ANALYSIS);
        assertJSONSucceeds(result);
    }

    @Test
    public void testDataVolumeRankingByApnGroup_CustomTime() throws Exception {
        map = getRequestParameters(TYPE_APN, DISPLAY_TYPE, THREE_DAY, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR, MAX_ROWS_VALUE);
        final String result = datavolRankingResource.getDatavolRankingResults(REQUEST_ID, map, DATAVOL_GROUP_RANKING_ANALYSIS);
        assertJSONSucceeds(result);
    }

    private MultivaluedMap<String, String> getRequestParameters(String type, String display, String time, String timeZoneOffset, String maxRows) {
        map.clear();
        map.putSingle(TYPE_PARAM, type);
        map.putSingle(DISPLAY_PARAM, display);
        map.putSingle(TIME_QUERY_PARAM, time);
        map.putSingle(TZ_OFFSET, timeZoneOffset);
        map.putSingle(MAX_ROWS, maxRows);
        return map;
    }
}