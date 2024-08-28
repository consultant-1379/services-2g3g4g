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
package com.ericsson.eniq.events.server.integritytests.terminalgroupanalysis.mostpopulareventsummary;

import com.ericsson.eniq.events.server.resources.TerminalAndGroupAnalysisResource;
import com.ericsson.eniq.events.server.resources.TestsWithTemporaryTablesBaseTestCase;
import com.ericsson.eniq.events.server.test.queryresults.TerminalMostPopularEventSummaryDrilldownResult;
import com.ericsson.eniq.events.server.test.stubs.DummyUriInfoImpl;
import com.sun.jersey.core.util.MultivaluedMapImpl;
import org.junit.Test;

import javax.ws.rs.core.MultivaluedMap;
import java.net.URISyntaxException;
import java.util.List;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.integritytests.terminalgroupanalysis.mostpopulareventsummary.TablesPopulatorForOneWeekQuery.*;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.*;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class MostPopularEventSummaryDrilldownTest extends TestsWithTemporaryTablesBaseTestCase<TerminalMostPopularEventSummaryDrilldownResult> {

    private TerminalAndGroupAnalysisResource terminalAndGroupAnalysisResource;

    private TablesPopulatorForOneWeekQuery populator;

    /*
     * (non-Javadoc)
     * 
     * @see com.ericsson.eniq.events.server.resources.TestsWithTemporaryTablesBaseTestCase#onSetUp()
     */
    @Override
    public void onSetUp() throws Exception {
        super.onSetUp();
        terminalAndGroupAnalysisResource = new TerminalAndGroupAnalysisResource();
        attachDependencies(terminalAndGroupAnalysisResource);
        terminalAndGroupAnalysisResource.setTechPackCXCMapping(techPackCXCMappingService);
        populator = new TablesPopulatorForOneWeekQuery(connection);
        populator.createTemporaryTables();
        populator.populateTemporaryTables();
    }

    @Test
    public void testGetGroupMostPopularEventSummaryDrilldown_ExclusiveTacGroup_OneWeek() throws Exception {
        final String result = runQuery(EXCLUSIVE_TAC_GROUP);
        validateResultForExclusiveTacGroup(result);
    }

    @Test
    public void testGetGroupMostPopularEventSummaryDrilldown_ExclusiveTacGroup_OneWeek_UnknownModel()
            throws Exception {
        populator.populateTemporaryTablesForUnknownTAC();
        final String result = runQuery(EXCLUSIVE_TAC_GROUP);
        validateResultForExclusiveTacGroup_UnknownModel(result);
    }

    @Test
    public void testGetGroupMostPopularEventSummaryDrilldown_ExclusiveTacGroup_OneWeek_UsingIMSISucRaw() throws Exception {
        jndiProperties.disableSucRawJNDIProperty();
        final String result = runQuery(EXCLUSIVE_TAC_GROUP);
        validateResultForExclusiveTacGroupUsingIMSISucRaw(result);
        jndiProperties.setUpJNDIPropertiesForTest();
    }

    @Test
    public void testGetGroupMostPopularEventSummaryDrilldown_ExclusiveTacGroup_OneWeek_UsingIMSISucRaw_NoModel()
            throws Exception {
        jndiProperties.disableSucRawJNDIProperty();
        populator.populateTemporaryTablesForUnknownTAC();
        final String result = runQuery(EXCLUSIVE_TAC_GROUP);
        validateResultForExclusiveTacGroupUsingIMSISucRaw_NoModel(result);
        jndiProperties.setUpJNDIPropertiesForTest();
    }

    @Test
    public void testGetGroupMostPopularEventSummaryDrilldown_OneWeek() throws Exception {
        final String result = runQuery(MOST_POPULAR_TAC_GROUP);
        validateResultForNormalTacGroup(result);
    }

    @Test
    public void testGetGroupMostPopularEventSummaryDrilldown_OneWeek_NoModel() throws Exception {
        final String result = runQuery(THIRD_MOST_POPULAR_TAC_GROUP);
        validateResultForNormalTacGroup_NoModel(result);
    }

    private String runQuery(final String tacGroupName) throws URISyntaxException {
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(TIME_QUERY_PARAM, ONE_WEEK);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(GROUP_NAME_PARAM, tacGroupName);
        DummyUriInfoImpl.setUriInfo(map, terminalAndGroupAnalysisResource, SAMPLE_BASE_URI, TERMINAL_GROUP_ANALYSIS + "/"
                + MOST_POPULAR_EVENT_SUMMARY);
        final String result = terminalAndGroupAnalysisResource.getMostPopularEventSummaryData();
        System.out.println(result);
        return result;
    }

    private void validateResultForExclusiveTacGroup(final String result) throws Exception {
        final List<TerminalMostPopularEventSummaryDrilldownResult> results = getTranslator().translateResult(result,
                TerminalMostPopularEventSummaryDrilldownResult.class);
        assertThat(results.size(), is(1));
        final TerminalMostPopularEventSummaryDrilldownResult tacInExclusiveTacGroup = results.get(0);
        assertThat(tacInExclusiveTacGroup.getTac(), is(SAMPLE_EXCLUSIVE_TAC));
        assertThat(tacInExclusiveTacGroup.getManufacturer(), is(MANUFACTURER_FOR_SAMPLE_EXCLUSIVE_TAC));
        assertThat(tacInExclusiveTacGroup.getMarketingName(), is(MARKETING_NAME_FOR_SAMPLE_EXCLUSIVE_TAC));
        assertThat(tacInExclusiveTacGroup.getNoErrors(), is(2));
        assertThat(tacInExclusiveTacGroup.getNoSuccess(), is(1));
        assertThat(tacInExclusiveTacGroup.getOccurrences(), is(3));
        assertThat(tacInExclusiveTacGroup.getSuccessRatio(), is(tacInExclusiveTacGroup.calculateExpectedSuccessRatio()));

    }

    private void validateResultForExclusiveTacGroup_UnknownModel(final String result) throws Exception {
        final List<TerminalMostPopularEventSummaryDrilldownResult> results = getTranslator().translateResult(result,
                TerminalMostPopularEventSummaryDrilldownResult.class);
        assertThat(results.size(), is(2));
        final TerminalMostPopularEventSummaryDrilldownResult tacInExclusiveTacGroup = results.get(1);
        assertThat(tacInExclusiveTacGroup.getTac(), is(UNKNOWN_TAC));
        assertThat(tacInExclusiveTacGroup.getManufacturer(), is(String.valueOf(UNKNOWN_TAC)));
        assertThat(tacInExclusiveTacGroup.getMarketingName(), is(String.valueOf(UNKNOWN_TAC)));
        assertThat(tacInExclusiveTacGroup.getNoErrors(), is(1));
        assertThat(tacInExclusiveTacGroup.getNoSuccess(), is(5));
        assertThat(tacInExclusiveTacGroup.getOccurrences(), is(6));
        assertThat(tacInExclusiveTacGroup.getSuccessRatio(), is(tacInExclusiveTacGroup.calculateExpectedSuccessRatio()));

    }

    private void validateResultForExclusiveTacGroupUsingIMSISucRaw(final String result) throws Exception {
        final List<TerminalMostPopularEventSummaryDrilldownResult> results = getTranslator().translateResult(result,
                TerminalMostPopularEventSummaryDrilldownResult.class);
        assertThat(results.size(), is(1));
        final TerminalMostPopularEventSummaryDrilldownResult tacInExclusiveTacGroup = results.get(0);
        assertThat(tacInExclusiveTacGroup.getTac(), is(SAMPLE_EXCLUSIVE_TAC));
        assertThat(tacInExclusiveTacGroup.getManufacturer(), is(MANUFACTURER_FOR_SAMPLE_EXCLUSIVE_TAC));
        assertThat(tacInExclusiveTacGroup.getMarketingName(), is(MARKETING_NAME_FOR_SAMPLE_EXCLUSIVE_TAC));
        assertThat(tacInExclusiveTacGroup.getNoErrors(), is(2));
        assertThat(tacInExclusiveTacGroup.getNoSuccess(), is(noOfSuccessesForExclusiveTacInSGEHIMSISucRawForOneWeek
                + noOfSuccessesForExclusiveTacInLTEIMSISucRawForOneWeek));
        assertThat(tacInExclusiveTacGroup.getOccurrences(), is(2 + noOfSuccessesForExclusiveTacInSGEHIMSISucRawForOneWeek
                + noOfSuccessesForExclusiveTacInLTEIMSISucRawForOneWeek));
        assertThat(tacInExclusiveTacGroup.getSuccessRatio(), is(tacInExclusiveTacGroup.calculateExpectedSuccessRatio()));

    }

    private void validateResultForExclusiveTacGroupUsingIMSISucRaw_NoModel(final String result) throws Exception {
        final List<TerminalMostPopularEventSummaryDrilldownResult> results = getTranslator().translateResult(result,
                TerminalMostPopularEventSummaryDrilldownResult.class);
        assertThat(results.size(), is(2));
        final TerminalMostPopularEventSummaryDrilldownResult tacInExclusiveTacGroup = results.get(1);
        assertThat(tacInExclusiveTacGroup.getTac(), is(UNKNOWN_TAC));
        assertThat(tacInExclusiveTacGroup.getManufacturer(), is(String.valueOf(UNKNOWN_TAC)));
        assertThat(tacInExclusiveTacGroup.getMarketingName(), is(String.valueOf(UNKNOWN_TAC)));
        assertThat(tacInExclusiveTacGroup.getNoErrors(), is(1));
        assertThat(tacInExclusiveTacGroup.getNoSuccess(), is(15));
        assertThat(tacInExclusiveTacGroup.getOccurrences(), is(16));
        assertThat(tacInExclusiveTacGroup.getSuccessRatio(), is(tacInExclusiveTacGroup.calculateExpectedSuccessRatio()));

    }

    private void validateResultForNormalTacGroup(final String result) throws Exception {
        final List<TerminalMostPopularEventSummaryDrilldownResult> results = getTranslator().translateResult(result,
                TerminalMostPopularEventSummaryDrilldownResult.class);
        assertThat(results.size(), is(1));

        final TerminalMostPopularEventSummaryDrilldownResult tacInMostPopularTacGroup = results.get(0);
        assertThat(tacInMostPopularTacGroup.getTac(), is(MOST_POPULAR_TAC));
        assertThat(tacInMostPopularTacGroup.getManufacturer(), is(MANUFACTURER_FOR_MOST_POPULAR_TAC));
        assertThat(tacInMostPopularTacGroup.getMarketingName(), is(MARKETING_NAME_FOR_MOST_POPULAR_TAC));
        final int expectedNoErrors = noErrorsForMostPopularTacInSgehDayTable + noErrorsForMostPopularTacInLTEDayTable;
        assertThat(tacInMostPopularTacGroup.getNoErrors(), is(expectedNoErrors));
        final int expectedNoSuccesses = noSuccessesForMostPopularTacInLTEDayTable + noSuccessesForMostPopularTacInSgehDayTable;
        assertThat(tacInMostPopularTacGroup.getNoSuccess(), is(expectedNoSuccesses));
        final int occurrences = expectedNoErrors + expectedNoSuccesses;
        assertThat(tacInMostPopularTacGroup.getOccurrences(), is(occurrences));
        assertThat(tacInMostPopularTacGroup.getSuccessRatio(), is(tacInMostPopularTacGroup.calculateExpectedSuccessRatio()));

    }

    private void validateResultForNormalTacGroup_NoModel(final String result) throws Exception {
        final List<TerminalMostPopularEventSummaryDrilldownResult> results = getTranslator().translateResult(result,
                TerminalMostPopularEventSummaryDrilldownResult.class);
        assertThat(results.size(), is(1));

        final TerminalMostPopularEventSummaryDrilldownResult tacInMostPopularTacGroup = results.get(0);
        assertThat(tacInMostPopularTacGroup.getTac(), is(THIRD_MOST_POPULAR_TAC));
        assertThat(tacInMostPopularTacGroup.getManufacturer(), is(String.valueOf(THIRD_MOST_POPULAR_TAC)));
        assertThat(tacInMostPopularTacGroup.getMarketingName(), is(String.valueOf(THIRD_MOST_POPULAR_TAC)));
        assertThat(tacInMostPopularTacGroup.getNoErrors(), is(8));
        assertThat(tacInMostPopularTacGroup.getNoSuccess(), is(0));
        assertThat(tacInMostPopularTacGroup.getOccurrences(), is(8));
        assertThat(tacInMostPopularTacGroup.getSuccessRatio(), is(tacInMostPopularTacGroup.calculateExpectedSuccessRatio()));

    }

}
