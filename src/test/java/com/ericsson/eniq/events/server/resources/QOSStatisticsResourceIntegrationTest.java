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
package com.ericsson.eniq.events.server.resources;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.*;

import javax.ws.rs.core.MultivaluedMap;

import org.junit.Test;

import com.sun.jersey.core.util.MultivaluedMapImpl;

/**
 * See previous versions of this class - running the query for each type for
 * groups and non groups takes 34 minutes, so just running one group and one non
 * group query
 */
public class QOSStatisticsResourceIntegrationTest extends
        DataServiceBaseTestCase {

    private static final String AND_TIME_PERIOD = " and time period ";

    private static final String RUNNING_QUERY_FOR = "Running query for ";

    private QOSStatisticsResource qosStatisticsResource;

    @Override
    protected void onSetUp() throws Exception {
        super.onSetUp();
        qosStatisticsResource = new QOSStatisticsResource();
        attachDependencies(qosStatisticsResource);
    }

    @Test
    public void testGetQOSStatisticsGroupSummary_TAC_FiveMinutes() {
        printTestDetails(THIRTY_MINUTES, SAMPLE_TAC_GROUP);
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(GROUP_NAME_PARAM, SAMPLE_TAC_GROUP);
        getQOSStatisticsSummary(map, TYPE_TAC, THIRTY_MINUTES);
    }

    private void printTestDetails(final String time,
            final String groupOrNodeType) {
        System.out.println(RUNNING_QUERY_FOR + groupOrNodeType
                + AND_TIME_PERIOD + time);
    }

    @Test
    public void testGetQOSStatisticsSummary_CELL_OneWeek() {
        printTestDetails(ONE_WEEK, TYPE_CELL);
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(NODE_PARAM, LTECELL1 + COMMA + ERBS1 + COMMA + ERICSSON
                + COMMA + LTE + COMMA + "X");
        getQOSStatisticsSummary(map, TYPE_CELL, ONE_WEEK);
    }

    private void getQOSStatisticsSummary(
            final MultivaluedMap<String, String> map, final String type,
            final String time) {
        map.putSingle(TYPE_PARAM, type);
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(TIME_QUERY_PARAM, time);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        final String result = qosStatisticsResource.getData(SAMPLE_REQUEST_ID,
                map);
        System.out.println(result);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetQOSStatisticsSummary_TAC_OneWeek() {
        printTestDetails(ONE_WEEK, TYPE_TAC);

        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(NODE_PARAM, MARKETING_NAME_FOR_SAMPLE_EXCLUSIVE_TAC
                + COMMA + SAMPLE_EXCLUSIVE_TAC);
        getQOSStatisticsSummary(map, TYPE_TAC, ONE_WEEK);
    }

    @Test
    public void testGetQOSStatisticsGroupSummary_EXCLUSIVE_TAC_OneWeek() {
        printTestDetails(ONE_WEEK, "EXCLUSIVE_TAC");
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(GROUP_NAME_PARAM, "EXCLUSIVE_TAC");
        getQOSStatisticsSummary(map, TYPE_TAC, ONE_WEEK);
    }

    @Test
    public void testGetQOSStatisticsGroupSummary_HIER3_OneWeek() {
        printTestDetails(ONE_WEEK, SAMPLE_BSC);
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(GROUP_NAME_PARAM, SAMPLE_BSC);
        getQOSStatisticsSummary(map, TYPE_BSC, ONE_WEEK);
    }

}
