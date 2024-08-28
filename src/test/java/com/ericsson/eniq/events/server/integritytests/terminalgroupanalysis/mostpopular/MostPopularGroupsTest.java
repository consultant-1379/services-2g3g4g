/*------------------------------------------------------------------------------
 *******************************************************************************
 * COPYRIGHT Ericsson 2015
 *
 * The copyright to the computer program(s) herein is the property of
 * Ericsson Inc. The programs may be used and/or copied only with written
 * permission from Ericsson Inc. or in accordance with the terms and
 * conditions stipulated in the agreement/contract under which the
 * program(s) have been supplied.
 *******************************************************************************
 *----------------------------------------------------------------------------*/
package com.ericsson.eniq.events.server.integritytests.terminalgroupanalysis.mostpopular;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.integritytests.terminalgroupanalysis.mostpopulareventsummary.TablesPopulatorForOneWeekQuery.*;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.*;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.util.List;

import javax.ws.rs.core.MultivaluedMap;

import org.junit.Test;

import com.ericsson.eniq.events.server.integritytests.terminalgroupanalysis.mostpopulareventsummary.TablesPopulatorForOneWeekQuery;
import com.ericsson.eniq.events.server.resources.TerminalAndGroupAnalysisResource;
import com.ericsson.eniq.events.server.resources.TestsWithTemporaryTablesBaseTestCase;
import com.ericsson.eniq.events.server.test.queryresults.MostPopularGroupsResult;
import com.ericsson.eniq.events.server.test.stubs.DummyUriInfoImpl;
import com.sun.jersey.core.util.MultivaluedMapImpl;

public class MostPopularGroupsTest extends TestsWithTemporaryTablesBaseTestCase<MostPopularGroupsResult> {

    private TerminalAndGroupAnalysisResource terminalAndGroupAnalysisResource;

    @Override
    public void onSetUp() throws Exception {
        super.onSetUp();
        terminalAndGroupAnalysisResource = new TerminalAndGroupAnalysisResource();
        attachDependencies(terminalAndGroupAnalysisResource);
        terminalAndGroupAnalysisResource.setTechPackCXCMapping(techPackCXCMappingService);
        final TablesPopulatorForOneWeekQuery populator = new TablesPopulatorForOneWeekQuery(connection);
        populator.createTemporaryTables();
        populator.populateTemporaryTables();
    }

    @Test
    public void testGetGroupMostPopularEvents_OneWeek() throws Exception {
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(TIME_QUERY_PARAM, ONE_WEEK);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        DummyUriInfoImpl.setUriInfo(map, terminalAndGroupAnalysisResource, SAMPLE_BASE_URI, TERMINAL_GROUP_ANALYSIS + "/" + MOST_POPULAR);
        final String result = terminalAndGroupAnalysisResource.getMostPopularData();
        System.out.println(result);
        validateResultForOneWeekPeriod(result);

    }

    @Test
    public void testGetGroupMostPopularEvents_6Hours() throws Exception {
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(TIME_QUERY_PARAM, SIX_HOURS);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        DummyUriInfoImpl.setUriInfo(map, terminalAndGroupAnalysisResource, SAMPLE_BASE_URI, TERMINAL_GROUP_ANALYSIS + "/" + MOST_POPULAR);
        final String result = terminalAndGroupAnalysisResource.getMostPopularData();
        System.out.println(result);
        validateResultFor6WeekPeriod(result);

    }

    @Test
    public void testGetGroupMostPopularEvents_OneWeek_UsingIMSISucRaw() throws Exception {
        jndiProperties.disableSucRawJNDIProperty();
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(TIME_QUERY_PARAM, ONE_WEEK);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        DummyUriInfoImpl.setUriInfo(map, terminalAndGroupAnalysisResource, SAMPLE_BASE_URI, TERMINAL_GROUP_ANALYSIS + "/" + MOST_POPULAR);
        final String result = terminalAndGroupAnalysisResource.getMostPopularData();
        System.out.println(result);
        validateResultForOneWeekPeriod(result);
        jndiProperties.setUpJNDIPropertiesForTest();
    }

    @Test
    public void testGetGroupMostPopularEvents_6Hours_UsingIMSISucRaw() throws Exception {
        jndiProperties.disableSucRawJNDIProperty();
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(TIME_QUERY_PARAM, SIX_HOURS);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        DummyUriInfoImpl.setUriInfo(map, terminalAndGroupAnalysisResource, SAMPLE_BASE_URI, TERMINAL_GROUP_ANALYSIS + "/" + MOST_POPULAR);
        final String result = terminalAndGroupAnalysisResource.getMostPopularData();
        System.out.println(result);
        validateResultFor6WeekPeriod(result);
        jndiProperties.setUpJNDIPropertiesForTest();
    }

    private void validateResultForOneWeekPeriod(final String result) throws Exception {
        final List<MostPopularGroupsResult> results = getTranslator().translateResult(result, MostPopularGroupsResult.class);
        assertThat(results.size(), is(3));
        final MostPopularGroupsResult mostPopularTacGroup = results.get(0);
        assertThat(mostPopularTacGroup.getTacGroupName(), is(MOST_POPULAR_TAC_GROUP));
        assertThat(mostPopularTacGroup.getNoEvents(), is(noErrorsForMostPopularTacInSgehDayTable + noErrorsForMostPopularTacInLTEDayTable
                + noSuccessesForMostPopularTacInLTEDayTable + noSuccessesForMostPopularTacInSgehDayTable));
        assertThat(mostPopularTacGroup.getNoTotalErrSubscribers(), is(1));

        final MostPopularGroupsResult secondMostPopularTacGroup = results.get(2);
        assertThat(secondMostPopularTacGroup.getTacGroupName(), is(SECOND_MOST_POPULAR_TAC_GROUP));
        assertThat(secondMostPopularTacGroup.getNoEvents(), is(noSuccessesForSecondMostPopularTacInSgehDayTable
                + noSuccessesForSecondMostPopularTacInLTEDayTable));
        assertThat(secondMostPopularTacGroup.getNoTotalErrSubscribers(), is(0));

        final MostPopularGroupsResult thirdMostPopularTacGroup = results.get(1);
        assertThat(thirdMostPopularTacGroup.getTacGroupName(), is(THIRD_MOST_POPULAR_TAC_GROUP));
        assertThat(thirdMostPopularTacGroup.getNoEvents(), is(noErrorsForThirdMostPopularTacInLTEDayTable));
        assertThat(thirdMostPopularTacGroup.getNoTotalErrSubscribers(), is(1));
    }

    private void validateResultFor6WeekPeriod(final String result) throws Exception {
        final List<MostPopularGroupsResult> results = getTranslator().translateResult(result, MostPopularGroupsResult.class);
        assertThat(results.size(), is(3));
        final MostPopularGroupsResult mostPopularTacGroup = results.get(1);
        assertThat(mostPopularTacGroup.getTacGroupName(), is(MOST_POPULAR_TAC_GROUP));
        assertThat(mostPopularTacGroup.getNoEvents(), is(noErrorsForMostPopularTacInSgeh15MinTable + noErrorsForMostPopularTacInLTE15MinTable
                + noSuccessesForMostPopularTacInLTE15MinTable + noSuccessesForMostPopularTacInSgeh15MinTable));
        assertThat(mostPopularTacGroup.getNoTotalErrSubscribers(), is(1));

        final MostPopularGroupsResult secondMostPopularTacGroup = results.get(0);
        assertThat(secondMostPopularTacGroup.getTacGroupName(), is(SECOND_MOST_POPULAR_TAC_GROUP));
        assertThat(secondMostPopularTacGroup.getNoEvents(), is(noSuccessesForSecondMostPopularTacInSgeh15MinTable
                + noSuccessesForSecondMostPopularTacInLTE15MinTable));
        assertThat(secondMostPopularTacGroup.getNoTotalErrSubscribers(), is(0));

        final MostPopularGroupsResult thirdMostPopularTacGroup = results.get(2);
        assertThat(thirdMostPopularTacGroup.getTacGroupName(), is(THIRD_MOST_POPULAR_TAC_GROUP));
        assertThat(thirdMostPopularTacGroup.getNoEvents(), is(noErrorsForThirdMostPopularTacInLTE15MinTable));
        assertThat(thirdMostPopularTacGroup.getNoTotalErrSubscribers(), is(1));

    }
}
