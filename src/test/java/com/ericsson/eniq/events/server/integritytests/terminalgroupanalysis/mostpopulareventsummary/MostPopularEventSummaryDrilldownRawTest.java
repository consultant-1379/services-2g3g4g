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
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;
import com.sun.jersey.core.util.MultivaluedMapImpl;
import org.junit.Test;

import javax.ws.rs.core.MultivaluedMap;
import java.net.URISyntaxException;
import java.util.List;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.integritytests.terminalgroupanalysis.mostpopulareventsummary.RawTablesPopulatorForTerminalGroupAnalysis.*;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.*;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class MostPopularEventSummaryDrilldownRawTest extends TestsWithTemporaryTablesBaseTestCase<TerminalMostPopularEventSummaryDrilldownResult> {

    private TerminalAndGroupAnalysisResource terminalAndGroupAnalysisResource;
    private RawTablesPopulatorForTerminalGroupAnalysis populator;

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

        populator = new RawTablesPopulatorForTerminalGroupAnalysis(connection);
        populator.createTemporaryTables();
    }

    @Test
    public void testGetGroupMostPopularEventSummaryDrilldown_FiveMinutes() throws Exception {
        populator.populateTemporaryTables(DateTimeUtilities.getDateTimeMinus2Minutes());
        final String result = runQuery(MOST_POPULAR_TAC_GROUP, FIVE_MINUTES);
        validateResultForRegularTacGroup(result);
    }

    @Test
    public void testGetGroupMostPopularEventSummaryDrilldown_FiveMinutes_UnknownModel() throws Exception {
        populator.populateTemporaryTables(DateTimeUtilities.getDateTimeMinus2Minutes());
        final String result = runQuery(THIRD_MOST_POPULAR_TAC_GROUP, FIVE_MINUTES);
        validateResultForRegularTacGroup_UnknownModel(result);
    }

    @Test
    public void testGetGroupMostPopularEventSummaryDrilldown_ExclusiveTacGroup_FiveMinutes() throws Exception {
        populator.populateTemporaryTables(DateTimeUtilities.getDateTimeMinus2Minutes());
        final String result = runQuery(EXCLUSIVE_TAC_GROUP, FIVE_MINUTES);
        validateResultForExclusiveTacGroup(result);
    }

    @Test
    public void testGetGroupMostPopularEventSummaryDrilldown_30Minutes() throws Exception {
        jndiProperties.setUpDataTieringJNDIProperty();
        populator.populateTemporaryTables(DateTimeUtilities.getDateTimeMinus25Minutes());
        final String result = runQuery(MOST_POPULAR_TAC_GROUP, THIRTY_MINUTES);
        validateResultForRegularTacGroup(result);
        jndiProperties.setUpJNDIPropertiesForTest();
    }

    @Test
    public void testGetGroupMostPopularEventSummaryDrilldown_30Minutes_UnknownModel() throws Exception {
        jndiProperties.setUpDataTieringJNDIProperty();
        populator.populateTemporaryTables(DateTimeUtilities.getDateTimeMinus25Minutes());
        final String result = runQuery(THIRD_MOST_POPULAR_TAC_GROUP, THIRTY_MINUTES);
        validateResultForRegularTacGroup_UnknownModel(result);
        jndiProperties.setUpJNDIPropertiesForTest();
    }

    @Test
    public void testGetGroupMostPopularEventSummaryDrilldown_ExclusiveTacGroup_30Minutes() throws Exception {
        jndiProperties.setUpDataTieringJNDIProperty();
        populator.populateTemporaryTables(DateTimeUtilities.getDateTimeMinus25Minutes());
        final String result = runQuery(EXCLUSIVE_TAC_GROUP, THIRTY_MINUTES);
        validateResultForExclusiveTacGroup(result);
        jndiProperties.setUpJNDIPropertiesForTest();
    }

    @Test
    public void testGetGroupMostPopularEventSummaryDrilldown_ExclusiveTacGroup_30Minutes_UnknownModel()
            throws Exception {
        jndiProperties.setUpDataTieringJNDIProperty();
        populator.populateTemporaryTables(DateTimeUtilities.getDateTimeMinus25Minutes());
        populator.populateTemporaryTablesForUnknownTAC(DateTimeUtilities.getDateTimeMinus25Minutes());
        final String result = runQuery(EXCLUSIVE_TAC_GROUP, THIRTY_MINUTES);
        validateResultForExclusiveTacGroup_UnknownModel(result);
        jndiProperties.setUpJNDIPropertiesForTest();
    }

    @Test
    public void testGetGroupMostPopularEventSummaryDrilldown_ExclusiveTacGroup_30Minutes_UsingIMSISucRaw() throws Exception {
        jndiProperties.setupDataTieringAndDisableSucRawJNDIProperties();
        populator.populateTemporaryTables(DateTimeUtilities.getDateTimeMinus25Minutes());
        final String result = runQuery(EXCLUSIVE_TAC_GROUP, THIRTY_MINUTES);
        validateResultForExclusiveTacGroupUsingIMSISucRaw(result);
        jndiProperties.setUpJNDIPropertiesForTest();
    }

    @Test
    public void testGetGroupMostPopularEventSummaryDrilldown_ExclusiveTacGroup_30Minutes_UsingIMSISucRaw_UnknownModel()
            throws Exception {
        jndiProperties.setupDataTieringAndDisableSucRawJNDIProperties();
        populator.populateTemporaryTables(DateTimeUtilities.getDateTimeMinus25Minutes());
        populator.populateTemporaryTablesForUnknownTAC(DateTimeUtilities.getDateTimeMinus25Minutes());
        final String result = runQuery(EXCLUSIVE_TAC_GROUP, THIRTY_MINUTES);
        validateResultForExclusiveTacGroupUsingIMSISucRaw_NoModel(result);
        jndiProperties.setUpJNDIPropertiesForTest();
    }

    private String runQuery(final String tacGroup, final String time) throws URISyntaxException {
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(TIME_QUERY_PARAM, time);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(GROUP_NAME_PARAM, tacGroup);
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
        final TerminalMostPopularEventSummaryDrilldownResult resultForExclusiveTac = results.get(0);
        assertThat(resultForExclusiveTac.getTac(), is(SAMPLE_EXCLUSIVE_TAC));
        assertThat(resultForExclusiveTac.getManufacturer(), is(MANUFACTURER_FOR_SAMPLE_EXCLUSIVE_TAC));
        assertThat(resultForExclusiveTac.getMarketingName(), is(MARKETING_NAME_FOR_SAMPLE_EXCLUSIVE_TAC));
        assertThat(resultForExclusiveTac.getNoErrors(), is(1));
        assertThat(resultForExclusiveTac.getNoSuccess(), is(1));
        assertThat(resultForExclusiveTac.getOccurrences(), is(2));
        assertThat(resultForExclusiveTac.getSuccessRatio(), is(resultForExclusiveTac.calculateExpectedSuccessRatio()));
    }

    private void validateResultForExclusiveTacGroup_UnknownModel(final String result) throws Exception {
        final List<TerminalMostPopularEventSummaryDrilldownResult> results = getTranslator().translateResult(result,
                TerminalMostPopularEventSummaryDrilldownResult.class);
        assertThat(results.size(), is(2));
        final TerminalMostPopularEventSummaryDrilldownResult resultForExclusiveTac = results.get(1);
        assertThat(resultForExclusiveTac.getTac(), is(UNKNOWN_TAC));
        assertThat(resultForExclusiveTac.getManufacturer(), is(String.valueOf(UNKNOWN_TAC)));
        assertThat(resultForExclusiveTac.getMarketingName(), is(String.valueOf(UNKNOWN_TAC)));
        assertThat(resultForExclusiveTac.getNoErrors(), is(1));
        assertThat(resultForExclusiveTac.getNoSuccess(), is(5));
        assertThat(resultForExclusiveTac.getOccurrences(), is(6));
        assertThat(resultForExclusiveTac.getSuccessRatio(), is(resultForExclusiveTac.calculateExpectedSuccessRatio()));
    }

    private void validateResultForRegularTacGroup(final String result) throws Exception {
        final List<TerminalMostPopularEventSummaryDrilldownResult> results = getTranslator().translateResult(result,
                TerminalMostPopularEventSummaryDrilldownResult.class);
        assertThat(results.size(), is(1));
        final TerminalMostPopularEventSummaryDrilldownResult tacInMostPopularTacGroup = results.get(0);
        assertThat(tacInMostPopularTacGroup.getTac(), is(MOST_POPULAR_TAC));
        assertThat(tacInMostPopularTacGroup.getManufacturer(), is(MANUFACTURER_FOR_MOST_POPULAR_TAC));
        assertThat(tacInMostPopularTacGroup.getMarketingName(), is(MARKETING_NAME_FOR_MOST_POPULAR_TAC));
        assertThat(tacInMostPopularTacGroup.getNoErrors(), is(2));
        assertThat(tacInMostPopularTacGroup.getNoSuccess(), is(2));
        assertThat(tacInMostPopularTacGroup.getOccurrences(), is(4));
        assertThat(tacInMostPopularTacGroup.getSuccessRatio(), is(tacInMostPopularTacGroup.calculateExpectedSuccessRatio()));
    }

    private void validateResultForRegularTacGroup_UnknownModel(final String result) throws Exception {
        final List<TerminalMostPopularEventSummaryDrilldownResult> results = getTranslator().translateResult(result,
                TerminalMostPopularEventSummaryDrilldownResult.class);
        assertThat(results.size(), is(1));
        final TerminalMostPopularEventSummaryDrilldownResult tacInMostPopularTacGroup = results.get(0);
        assertThat(tacInMostPopularTacGroup.getTac(), is(THIRD_MOST_POPULAR_TAC));
        assertThat(tacInMostPopularTacGroup.getManufacturer(), is(String.valueOf(THIRD_MOST_POPULAR_TAC)));
        assertThat(tacInMostPopularTacGroup.getMarketingName(), is(String.valueOf(THIRD_MOST_POPULAR_TAC)));
        assertThat(tacInMostPopularTacGroup.getNoErrors(), is(1));
        assertThat(tacInMostPopularTacGroup.getNoSuccess(), is(1));
        assertThat(tacInMostPopularTacGroup.getOccurrences(), is(2));
        assertThat(tacInMostPopularTacGroup.getSuccessRatio(), is(tacInMostPopularTacGroup.calculateExpectedSuccessRatio()));
    }

    private void validateResultForExclusiveTacGroupUsingIMSISucRaw(final String result) throws Exception {
        final List<TerminalMostPopularEventSummaryDrilldownResult> results = getTranslator().translateResult(result,
                TerminalMostPopularEventSummaryDrilldownResult.class);
        assertThat(results.size(), is(1));
        final TerminalMostPopularEventSummaryDrilldownResult resultForExclusiveTac = results.get(0);
        assertThat(resultForExclusiveTac.getTac(), is(SAMPLE_EXCLUSIVE_TAC));
        assertThat(resultForExclusiveTac.getManufacturer(), is(MANUFACTURER_FOR_SAMPLE_EXCLUSIVE_TAC));
        assertThat(resultForExclusiveTac.getMarketingName(), is(MARKETING_NAME_FOR_SAMPLE_EXCLUSIVE_TAC));
        assertThat(resultForExclusiveTac.getNoErrors(), is(1));
        assertThat(resultForExclusiveTac.getNoSuccess(), is(noSuccessesForMostPopularTacInSGEHIMSISucRaw
                + noSuccessesForMostPopularTacInLTEIMSISucRaw));
        assertThat(resultForExclusiveTac.getOccurrences(), is(1 + noSuccessesForMostPopularTacInSGEHIMSISucRaw
                + noSuccessesForMostPopularTacInLTEIMSISucRaw));
        assertThat(resultForExclusiveTac.getSuccessRatio(), is(resultForExclusiveTac.calculateExpectedSuccessRatio()));
    }

    private void validateResultForExclusiveTacGroupUsingIMSISucRaw_NoModel(final String result) throws Exception {
        final List<TerminalMostPopularEventSummaryDrilldownResult> results = getTranslator().translateResult(result,
                TerminalMostPopularEventSummaryDrilldownResult.class);
        assertThat(results.size(), is(2));
        final TerminalMostPopularEventSummaryDrilldownResult resultForExclusiveTac = results.get(1);
        assertThat(resultForExclusiveTac.getTac(), is(UNKNOWN_TAC));
        assertThat(resultForExclusiveTac.getManufacturer(), is(String.valueOf(UNKNOWN_TAC)));
        assertThat(resultForExclusiveTac.getMarketingName(), is(String.valueOf(UNKNOWN_TAC)));
        assertThat(resultForExclusiveTac.getNoErrors(), is(1));
        assertThat(resultForExclusiveTac.getNoSuccess(), is(15));
        assertThat(resultForExclusiveTac.getOccurrences(), is(16));
        assertThat(resultForExclusiveTac.getSuccessRatio(), is(resultForExclusiveTac.calculateExpectedSuccessRatio()));
    }
}
