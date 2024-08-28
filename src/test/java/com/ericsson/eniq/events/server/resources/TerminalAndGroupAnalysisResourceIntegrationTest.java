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
import static com.ericsson.eniq.events.server.common.MessageConstants.*;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.*;

import javax.ws.rs.core.MultivaluedMap;

import org.junit.Test;

import com.sun.jersey.core.util.MultivaluedMapImpl;

public class TerminalAndGroupAnalysisResourceIntegrationTest extends DataServiceBaseTestCase {

    private MultivaluedMap<String, String> map;

    private TerminalAndGroupAnalysisResource terminalAndGroupAnalysisResource;

    private static final String GROUP_DISPLAY_TYPE = CHART_PARAM;

    private static final String DISPLAY_TYPE = GRID_PARAM;

    private static final String TIME_FROM = "1500";

    private static final String TIME_TO = "1600";

    private static final String DATE_FROM = "14112009";

    private static final String DATE_TO = "17112009";

    private static final String TEST_GROUP_NAME = "someTestGroup-TAC";

    private static final String DRILLTYPE = null;

    @Override
    public void onSetUp() {
        terminalAndGroupAnalysisResource = new TerminalAndGroupAnalysisResource();
        attachDependencies(terminalAndGroupAnalysisResource);
        techPackCXCMappingService.readTechPackLicenseNumbersFromDB();
        terminalAndGroupAnalysisResource.setTechPackCXCMapping(techPackCXCMappingService);
        map = new MultivaluedMapImpl();
    }

    @Test
    public void testGetGroupMostPopularDataByTimerange() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, GROUP_DISPLAY_TYPE);
        map.putSingle(TIME_FROM_QUERY_PARAM, TIME_FROM);
        map.putSingle(TIME_TO_QUERY_PARAM, TIME_TO);
        map.putSingle(DATE_FROM_QUERY_PARAM, DATE_FROM);
        map.putSingle(DATE_TO_QUERY_PARAM, DATE_TO);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        final String uriPath = TERMINAL_GROUP_ANALYSIS + "/" + MOST_POPULAR;
        final String result = terminalAndGroupAnalysisResource.getTerminalAndGroupAnalysisResults("requestID", map, null, uriPath);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetGroupMostPopularDataByTimerange_withDelayOffset() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, GROUP_DISPLAY_TYPE);
        map.putSingle(TIME_FROM_QUERY_PARAM, TIME_FROM);
        map.putSingle(TIME_TO_QUERY_PARAM, TIME_TO);
        map.putSingle(DATE_FROM_QUERY_PARAM, DATE_FROM);
        map.putSingle(DATE_TO_QUERY_PARAM, DATE_TO);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_MINUS_EIGHT_HOUR);
        final String uriPath = TERMINAL_GROUP_ANALYSIS + "/" + MOST_POPULAR;
        final String result = terminalAndGroupAnalysisResource.getTerminalAndGroupAnalysisResults("requestID", map, null, uriPath);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetTerminalMostPopularDataByTimerange() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TIME_FROM_QUERY_PARAM, TIME_FROM);
        map.putSingle(TIME_TO_QUERY_PARAM, TIME_TO);
        map.putSingle(DATE_FROM_QUERY_PARAM, DATE_FROM);
        map.putSingle(DATE_TO_QUERY_PARAM, DATE_TO);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        final String uriPath = TERMINAL_ANALYSIS + "/" + MOST_POPULAR;

        final String result = terminalAndGroupAnalysisResource.getTerminalAndGroupAnalysisResults("requestID", map, null, uriPath);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetGroupMostPopularDataDrilldownByTimeRange() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TIME_FROM_QUERY_PARAM, TIME_FROM);
        map.putSingle(TIME_TO_QUERY_PARAM, TIME_TO);
        map.putSingle(DATE_FROM_QUERY_PARAM, DATE_FROM);
        map.putSingle(DATE_TO_QUERY_PARAM, DATE_TO);
        map.putSingle(GROUP_NAME_PARAM, TEST_GROUP_NAME);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        final String uriPath = TERMINAL_GROUP_ANALYSIS + "/" + MOST_POPULAR;

        final String result = terminalAndGroupAnalysisResource.getTerminalAndGroupAnalysisResults("requestID", map, null, uriPath);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetGroupMostPopularDataDrilldownByTime_FiveMinutes() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(TIME_QUERY_PARAM, FIVE_MINUTES);
        map.putSingle(GROUP_NAME_PARAM, TEST_GROUP_NAME);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        final String uriPath = TERMINAL_GROUP_ANALYSIS + "/" + MOST_POPULAR;

        final String result = terminalAndGroupAnalysisResource.getTerminalAndGroupAnalysisResults("requestID", map, null, uriPath);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetGroupMostPopularEventSummaryDataDrilldown_OneDay() {
        map.clear();
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(TIME_QUERY_PARAM, ONE_DAY);
        map.putSingle(GROUP_NAME_PARAM, TYPE_TAC_GROUP);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        final String uriPath = TERMINAL_GROUP_ANALYSIS + "/" + MOST_POPULAR_EVENT_SUMMARY;

        final String result = terminalAndGroupAnalysisResource.getTerminalAndGroupAnalysisResults("requestID", map, null, uriPath);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetGroupMostPopularEventSummaryDataDrilldown_FiveMinutes() {
        map.clear();
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(TIME_QUERY_PARAM, FIVE_MINUTES);
        map.putSingle(GROUP_NAME_PARAM, TYPE_TAC_GROUP);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        final String uriPath = TERMINAL_GROUP_ANALYSIS + "/" + MOST_POPULAR_EVENT_SUMMARY;

        final String result = terminalAndGroupAnalysisResource.getTerminalAndGroupAnalysisResults("requestID", map, null, uriPath);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetTerminalMostAttachedFailuresDataByTimerange() {
        map.clear();
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(TIME_FROM_QUERY_PARAM, TIME_FROM);
        map.putSingle(TIME_TO_QUERY_PARAM, TIME_TO);
        map.putSingle(DATE_FROM_QUERY_PARAM, DATE_FROM);
        map.putSingle(DATE_TO_QUERY_PARAM, DATE_TO);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        final String uriPath = TERMINAL_ANALYSIS + "/" + MOST_ATTACHED_FAILURES;

        final String result = terminalAndGroupAnalysisResource.getTerminalAndGroupAnalysisResults("requestID", map, ATTACHED_KEY_VALUE, uriPath);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetGroupMostAttachedFailuresDataByTimerange() {
        map.clear();
        map.putSingle(DISPLAY_PARAM, GROUP_DISPLAY_TYPE);
        map.putSingle(TIME_FROM_QUERY_PARAM, TIME_FROM);
        map.putSingle(TIME_TO_QUERY_PARAM, TIME_TO);
        map.putSingle(DATE_FROM_QUERY_PARAM, DATE_FROM);
        map.putSingle(DATE_TO_QUERY_PARAM, DATE_TO);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        final String uriPath = TERMINAL_GROUP_ANALYSIS + "/" + MOST_ATTACHED_FAILURES;

        final String result = terminalAndGroupAnalysisResource.getTerminalAndGroupAnalysisResults("requestID", map, ATTACHED_KEY_VALUE, uriPath);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetGroupMostAttachedFailuresDataDrilldownByTimerange() {
        map.clear();
        map.putSingle(DISPLAY_PARAM, GROUP_DISPLAY_TYPE);
        map.putSingle(TIME_FROM_QUERY_PARAM, TIME_FROM);
        map.putSingle(TIME_TO_QUERY_PARAM, TIME_TO);
        map.putSingle(DATE_FROM_QUERY_PARAM, DATE_FROM);
        map.putSingle(DATE_TO_QUERY_PARAM, DATE_TO);
        map.putSingle(GROUP_NAME_PARAM, TEST_GROUP_NAME);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        final String uriPath = TERMINAL_GROUP_ANALYSIS + "/" + MOST_ATTACHED_FAILURES;

        final String result = terminalAndGroupAnalysisResource.getTerminalAndGroupAnalysisResults("requestID", map, ATTACHED_KEY_VALUE, uriPath);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetGroupMostAttachedFailuresDataDrilldownByTime_FiveMinutes() {
        map.clear();
        map.putSingle(DISPLAY_PARAM, GROUP_DISPLAY_TYPE);
        map.putSingle(TIME_QUERY_PARAM, FIVE_MINUTES);
        map.putSingle(GROUP_NAME_PARAM, TEST_GROUP_NAME);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        final String uriPath = TERMINAL_GROUP_ANALYSIS + "/" + MOST_ATTACHED_FAILURES;

        final String result = terminalAndGroupAnalysisResource.getTerminalAndGroupAnalysisResults("requestID", map, ATTACHED_KEY_VALUE, uriPath);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetGroupMostPDPSessionSetupFailuresDataByTimerange() {
        map.clear();
        map.putSingle(DISPLAY_PARAM, GROUP_DISPLAY_TYPE);
        map.putSingle(TIME_FROM_QUERY_PARAM, TIME_FROM);
        map.putSingle(TIME_TO_QUERY_PARAM, TIME_TO);
        map.putSingle(DATE_FROM_QUERY_PARAM, DATE_FROM);
        map.putSingle(DATE_TO_QUERY_PARAM, DATE_TO);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        final String uriPath = TERMINAL_GROUP_ANALYSIS + "/" + MOST_PDP_SESSION_SETUP_FAILURES;

        final String result = terminalAndGroupAnalysisResource.getTerminalAndGroupAnalysisResults("requestID", map, PDP_KEY_VALUE, uriPath);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetGroupMostMobilityIssuesDataByTimerange() {
        map.clear();
        map.putSingle(DISPLAY_PARAM, GROUP_DISPLAY_TYPE);
        map.putSingle(TIME_FROM_QUERY_PARAM, TIME_FROM);
        map.putSingle(TIME_TO_QUERY_PARAM, TIME_TO);
        map.putSingle(DATE_FROM_QUERY_PARAM, DATE_FROM);
        map.putSingle(DATE_TO_QUERY_PARAM, DATE_TO);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        final String uriPath = TERMINAL_GROUP_ANALYSIS + "/" + MOST_MOBILITY_ISSUES;

        final String result = terminalAndGroupAnalysisResource.getTerminalAndGroupAnalysisResults("requestID", map, MOBILITY_KEY_VALUE, uriPath);
        assertJSONSucceeds(result);
    }

    @Test
    public void testNoSuchDisplayType() throws Exception {
        final String invalidDisplayType = "error";

        map.clear();
        map.putSingle(DISPLAY_PARAM, invalidDisplayType);
        map.putSingle(TIME_FROM_QUERY_PARAM, TIME_FROM);
        map.putSingle(TIME_TO_QUERY_PARAM, TIME_TO);
        map.putSingle(DATE_FROM_QUERY_PARAM, DATE_FROM);
        map.putSingle(DATE_TO_QUERY_PARAM, DATE_TO);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        final String uriPath = TERMINAL_GROUP_ANALYSIS + "/" + MOST_MOBILITY_ISSUES;

        final String result = terminalAndGroupAnalysisResource.getTerminalAndGroupAnalysisResults("requestID", map, MOBILITY_KEY_VALUE, uriPath);
        assertJSONErrorResult(result);
        assertResultContains(result, E_NO_SUCH_DISPLAY_TYPE);
    }

    @Test
    public void testGetGroupHighestDavavolDrilldownDataByTime() {
        map.clear();
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(TIME_QUERY_PARAM, THIRTY_MINUTES);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(GROUP_NAME_PARAM, TEST_GROUP_NAME);
        final String uriPath = TERMINAL_GROUP_ANALYSIS + "/" + HIGHEST_DATAVOL;

        final String result = terminalAndGroupAnalysisResource.getTerminalAndGroupAnalysisResults("requestID", map, null, uriPath);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetHighestDavavolDataByTimeByTerminalType() {
        map.clear();
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(TIME_QUERY_PARAM, THIRTY_MINUTES);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        final String uriPath = TERMINAL_ANALYSIS + "/" + HIGHEST_DATAVOL;
        final String result = terminalAndGroupAnalysisResource.getTerminalAndGroupAnalysisResults("requestID", map, null, uriPath);
        assertJSONSucceeds(result);
    }

}
