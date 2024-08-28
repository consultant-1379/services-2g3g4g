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
import static com.ericsson.eniq.events.server.integritytests.terminalgroupanalysis.mostpopulareventsummary.RawTablesPopulatorForTerminalGroupAnalysis.*;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.*;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.util.List;

import javax.ws.rs.core.MultivaluedMap;

import org.junit.Test;

import com.ericsson.eniq.events.server.integritytests.terminalgroupanalysis.mostpopulareventsummary.RawTablesPopulatorForTerminalGroupAnalysis;
import com.ericsson.eniq.events.server.resources.TerminalAndGroupAnalysisResource;
import com.ericsson.eniq.events.server.resources.TestsWithTemporaryTablesBaseTestCase;
import com.ericsson.eniq.events.server.test.queryresults.MostPopularGroupsResult;
import com.ericsson.eniq.events.server.test.stubs.DummyUriInfoImpl;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;
import com.sun.jersey.core.util.MultivaluedMapImpl;

public class MostPopularGroupsRawTest extends TestsWithTemporaryTablesBaseTestCase<MostPopularGroupsResult> {

    private TerminalAndGroupAnalysisResource terminalAndGroupAnalysisResource;

    @Override
    public void onSetUp() throws Exception {
        super.onSetUp();
        terminalAndGroupAnalysisResource = new TerminalAndGroupAnalysisResource();
        attachDependencies(terminalAndGroupAnalysisResource);
        terminalAndGroupAnalysisResource.setTechPackCXCMapping(techPackCXCMappingService);
    }

    @Test
    public void testGetGroupMostPopularEvents_FiveMinutes() throws Exception {
        final RawTablesPopulatorForTerminalGroupAnalysis populator = new RawTablesPopulatorForTerminalGroupAnalysis(connection);
        populator.createTemporaryTables();
        populator.populateTemporaryTables(DateTimeUtilities.getDateTimeMinus2Minutes());

        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(TIME_QUERY_PARAM, FIVE_MINUTES);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        DummyUriInfoImpl.setUriInfo(map, terminalAndGroupAnalysisResource, SAMPLE_BASE_URI, TERMINAL_GROUP_ANALYSIS + "/" + MOST_POPULAR);
        final String result = terminalAndGroupAnalysisResource.getMostPopularData();
        System.out.println(result);
        validateResultFor5Minutes(result);

    }

    @Test
    public void testGetGroupMostPopularEvents_WithDataTieringOn30Min() throws Exception {
        final RawTablesPopulatorForTerminalGroupAnalysis populator = new RawTablesPopulatorForTerminalGroupAnalysis(connection);
        populator.createTemporaryTables();
        populator.populateTemporaryTables(DateTimeUtilities.getDateTimeMinus30Minutes());

        jndiProperties.setUpDataTieringJNDIProperty();
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(TIME_QUERY_PARAM, THIRTY_MINUTES);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        DummyUriInfoImpl.setUriInfo(map, terminalAndGroupAnalysisResource, SAMPLE_BASE_URI, TERMINAL_GROUP_ANALYSIS + "/" + MOST_POPULAR);
        final String result = terminalAndGroupAnalysisResource.getMostPopularData();
        System.out.println(result);
        validateResultFor30Minutes(result);
        jndiProperties.setUpJNDIPropertiesForTest();
    }

    @Test
    public void testGetGroupMostPopularEvents_WithDataTieringOn30Min_UsingIMSISucRaw() throws Exception {
        final RawTablesPopulatorForTerminalGroupAnalysis populator = new RawTablesPopulatorForTerminalGroupAnalysis(connection);
        populator.createTemporaryTables();
        populator.populateTemporaryTables(DateTimeUtilities.getDateTimeMinus30Minutes());

        jndiProperties.setupDataTieringAndDisableSucRawJNDIProperties();
        ;
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(TIME_QUERY_PARAM, THIRTY_MINUTES);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        DummyUriInfoImpl.setUriInfo(map, terminalAndGroupAnalysisResource, SAMPLE_BASE_URI, TERMINAL_GROUP_ANALYSIS + "/" + MOST_POPULAR);
        final String result = terminalAndGroupAnalysisResource.getMostPopularData();
        System.out.println(result);
        validateResultFor30Minutes(result);
        jndiProperties.setUpJNDIPropertiesForTest();
    }

    private void validateResultFor30Minutes(final String result) throws Exception {
        final List<MostPopularGroupsResult> results = getTranslator().translateResult(result, MostPopularGroupsResult.class);
        assertThat(results.size(), is(3));

        final MostPopularGroupsResult mostPopularTacGroup = results.get(0);
        assertThat(mostPopularTacGroup.getTacGroupName(), is(MOST_POPULAR_TAC_GROUP));
        assertThat(mostPopularTacGroup.getNoEvents(), is(4));
        assertThat(mostPopularTacGroup.getNoTotalErrSubscribers(), is(2));

        final MostPopularGroupsResult secondMostPopularTacGroup = results.get(1);
        assertThat(secondMostPopularTacGroup.getTacGroupName(), is(SECOND_MOST_POPULAR_TAC_GROUP));
        assertThat(secondMostPopularTacGroup.getNoEvents(), is(3));
        assertThat(secondMostPopularTacGroup.getNoTotalErrSubscribers(), is(0));

        final MostPopularGroupsResult thirdMostPopularTacGroup = results.get(2);
        assertThat(thirdMostPopularTacGroup.getTacGroupName(), is(THIRD_MOST_POPULAR_TAC_GROUP));
        assertThat(thirdMostPopularTacGroup.getNoEvents(), is(2));
        assertThat(thirdMostPopularTacGroup.getNoTotalErrSubscribers(), is(1));

    }

    private void validateResultFor5Minutes(final String result) throws Exception {
        final List<MostPopularGroupsResult> results = getTranslator().translateResult(result, MostPopularGroupsResult.class);
        assertThat(results.size(), is(3));
        final MostPopularGroupsResult mostPopularTacGroup = results.get(0);
        assertThat(mostPopularTacGroup.getTacGroupName(), is(MOST_POPULAR_TAC_GROUP));
        assertThat(mostPopularTacGroup.getNoEvents(), is(4));
        assertThat(mostPopularTacGroup.getNoTotalErrSubscribers(), is(2));

        final MostPopularGroupsResult secondMostPopularTacGroup = results.get(1);
        assertThat(secondMostPopularTacGroup.getTacGroupName(), is(SECOND_MOST_POPULAR_TAC_GROUP));
        assertThat(secondMostPopularTacGroup.getNoEvents(), is(3));
        assertThat(secondMostPopularTacGroup.getNoTotalErrSubscribers(), is(0));

        final MostPopularGroupsResult thirdMostPopularTacGroup = results.get(2);
        validResultsWithSameNoOfEvents(thirdMostPopularTacGroup);

    }

    /**
     * @param results
     * @param thirdMostPopularTacGroup
     */
    private void validResultsWithSameNoOfEvents(final MostPopularGroupsResult result) {
        if (result.getTacGroupName().equals(THIRD_MOST_POPULAR_TAC_GROUP)) {
            assertThat(result.getTacGroupName(), is(THIRD_MOST_POPULAR_TAC_GROUP));
            assertThat(result.getNoEvents(), is(2));
            assertThat(result.getNoTotalErrSubscribers(), is(1));
        } else {
            fail("TAC group should not be in results");
        }
    }
}
