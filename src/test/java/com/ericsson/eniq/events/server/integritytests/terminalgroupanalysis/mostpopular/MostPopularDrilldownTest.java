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
package com.ericsson.eniq.events.server.integritytests.terminalgroupanalysis.mostpopular;

import com.ericsson.eniq.events.server.integritytests.terminalgroupanalysis.mostpopulareventsummary.TablesPopulatorForOneWeekQuery;
import com.ericsson.eniq.events.server.resources.TerminalAndGroupAnalysisResource;
import com.ericsson.eniq.events.server.resources.TestsWithTemporaryTablesBaseTestCase;
import com.ericsson.eniq.events.server.test.queryresults.TerminalMostPopularDrilldownResult;
import com.ericsson.eniq.events.server.test.stubs.DummyUriInfoImpl;
import com.sun.jersey.core.util.MultivaluedMapImpl;
import org.junit.Test;

import javax.ws.rs.core.MultivaluedMap;
import java.util.List;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.integritytests.terminalgroupanalysis.mostpopulareventsummary.TablesPopulatorForOneWeekQuery.*;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.*;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class MostPopularDrilldownTest extends TestsWithTemporaryTablesBaseTestCase<TerminalMostPopularDrilldownResult> {

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
    public void testGetGroupMostPopularEventDrilldown_OneWeek() throws Exception {
        final List<TerminalMostPopularDrilldownResult> result = runQuery(MOST_POPULAR_TAC_GROUP);
        validateResultForRegularTacGroup(result);
    }

    @Test
    public void testGetGroupMostPopularEventDrilldown_OneWeek_NoModel() throws Exception {
        final List<TerminalMostPopularDrilldownResult> result = runQuery(THIRD_MOST_POPULAR_TAC_GROUP);
        validateResultForRegularTacGroup_NoModel(result);
    }

    @Test
    public void testGetGroupMostPopularEventDrilldown_ExclusiveTacGroup_OneWeek() throws Exception {
        final List<TerminalMostPopularDrilldownResult> results = runQuery(EXCLUSIVE_TAC_GROUP);
        validateResultForExclusiveTacGroup(results);
    }

    @Test
    public void testGetGroupMostPopularEventDrilldown_ExclusiveTacGroup_OneWeek_NoModel() throws Exception {
        populator.populateTemporaryTablesForUnknownTAC();
        final List<TerminalMostPopularDrilldownResult> results = runQuery(EXCLUSIVE_TAC_GROUP);
        validateResultForExclusiveTacGroup_NoModel(results);
    }


    @Test
    public void testGetGroupMostPopularEventDrilldown_ExclusiveTacGroup_OneWeek_UsingIMSISucRaw() throws Exception {
        jndiProperties.disableSucRawJNDIProperty();
        final List<TerminalMostPopularDrilldownResult> results = runQuery(EXCLUSIVE_TAC_GROUP);
        validateResultForExclusiveTacGroupUsingIMSISucRaw(results);
        jndiProperties.setUpJNDIPropertiesForTest();
    }

    @Test
    public void testGetGroupMostPopularEventDrilldown_ExclusiveTacGroup_OneWeek_UsingIMSISucRaw_NoModel() throws Exception {
        jndiProperties.disableSucRawJNDIProperty();
        populator.populateTemporaryTablesForUnknownTAC();
        final List<TerminalMostPopularDrilldownResult> results = runQuery(EXCLUSIVE_TAC_GROUP);
        validateResultForExclusiveTacGroupUsingIMSISucRaw_NoModel(results);
        jndiProperties.setUpJNDIPropertiesForTest();
    }

    private List<TerminalMostPopularDrilldownResult> runQuery(final String tacGroup) throws Exception {
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(TIME_QUERY_PARAM, ONE_WEEK);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(GROUP_NAME_PARAM, tacGroup);
        DummyUriInfoImpl.setUriInfo(map, terminalAndGroupAnalysisResource, SAMPLE_BASE_URI, TERMINAL_GROUP_ANALYSIS + "/" + MOST_POPULAR);
        final String result = terminalAndGroupAnalysisResource.getMostPopularData();
        System.out.println(result);
        final List<TerminalMostPopularDrilldownResult> results = getTranslator().translateResult(result, TerminalMostPopularDrilldownResult.class);
        return results;
    }

    private void validateResultForRegularTacGroup(final List<TerminalMostPopularDrilldownResult> results) {
        assertThat(results.size(), is(1));

        final TerminalMostPopularDrilldownResult mostPopularTacGroup = results.get(0);
        assertThat(mostPopularTacGroup.getManufacturer(), is(MANUFACTURER_FOR_MOST_POPULAR_TAC));
        assertThat(mostPopularTacGroup.getMarketingName(), is(MARKETING_NAME_FOR_MOST_POPULAR_TAC));
        assertThat(mostPopularTacGroup.getTAC(), is(MOST_POPULAR_TAC));
        assertThat(mostPopularTacGroup.getTotalEvents(), is(noErrorsForMostPopularTacInSgehDayTable + noSuccessesForMostPopularTacInLTEDayTable
                + noErrorsForMostPopularTacInLTEDayTable + noSuccessesForMostPopularTacInSgehDayTable));
    }

    private void validateResultForRegularTacGroup_NoModel(final List<TerminalMostPopularDrilldownResult> results) {
        assertThat(results.size(), is(1));

        final TerminalMostPopularDrilldownResult mostPopularTacGroup = results.get(0);
        assertThat(mostPopularTacGroup.getManufacturer(), is(String.valueOf(THIRD_MOST_POPULAR_TAC)));
        assertThat(mostPopularTacGroup.getMarketingName(), is(String.valueOf(THIRD_MOST_POPULAR_TAC)));
        assertThat(mostPopularTacGroup.getTAC(), is(THIRD_MOST_POPULAR_TAC));
        assertThat(mostPopularTacGroup.getTotalEvents(), is(8));
    }

    private void validateResultForExclusiveTacGroup(final List<TerminalMostPopularDrilldownResult> results) {
        assertThat(results.size(), is(1));
        final TerminalMostPopularDrilldownResult mostPopularTacGroup = results.get(0);
        assertThat(mostPopularTacGroup.getManufacturer(), is(MANUFACTURER_FOR_SAMPLE_EXCLUSIVE_TAC));
        assertThat(mostPopularTacGroup.getMarketingName(), is(MARKETING_NAME_FOR_SAMPLE_EXCLUSIVE_TAC));
        assertThat(mostPopularTacGroup.getTAC(), is(SAMPLE_EXCLUSIVE_TAC));
        assertThat(mostPopularTacGroup.getTotalEvents(), is(3));

    }

    private void validateResultForExclusiveTacGroup_NoModel(final List<TerminalMostPopularDrilldownResult> results) {
        assertThat(results.size(), is(2));
        final TerminalMostPopularDrilldownResult mostPopularTacGroup = results.get(1);
        assertThat(mostPopularTacGroup.getManufacturer(), is(String.valueOf(UNKNOWN_TAC)));
        assertThat(mostPopularTacGroup.getMarketingName(), is(String.valueOf(UNKNOWN_TAC)));
        assertThat(mostPopularTacGroup.getTAC(), is(UNKNOWN_TAC));
        assertThat(mostPopularTacGroup.getTotalEvents(), is(6));

    }

    private void validateResultForExclusiveTacGroupUsingIMSISucRaw(final List<TerminalMostPopularDrilldownResult> results) {
        assertThat(results.size(), is(1));
        final TerminalMostPopularDrilldownResult mostPopularTacGroup = results.get(0);
        assertThat(mostPopularTacGroup.getManufacturer(), is(MANUFACTURER_FOR_SAMPLE_EXCLUSIVE_TAC));
        assertThat(mostPopularTacGroup.getMarketingName(), is(MARKETING_NAME_FOR_SAMPLE_EXCLUSIVE_TAC));
        assertThat(mostPopularTacGroup.getTAC(), is(SAMPLE_EXCLUSIVE_TAC));
        assertThat(mostPopularTacGroup.getTotalEvents(), is(2 + noOfSuccessesForExclusiveTacInSGEHIMSISucRawForOneWeek
                + noOfSuccessesForExclusiveTacInLTEIMSISucRawForOneWeek));

    }

    private void validateResultForExclusiveTacGroupUsingIMSISucRaw_NoModel(
            final List<TerminalMostPopularDrilldownResult> results) {
        assertThat(results.size(), is(2));
        final TerminalMostPopularDrilldownResult mostPopularTacGroup = results.get(1);
        assertThat(mostPopularTacGroup.getManufacturer(), is(String.valueOf(UNKNOWN_TAC)));
        assertThat(mostPopularTacGroup.getMarketingName(), is(String.valueOf(UNKNOWN_TAC)));
        assertThat(mostPopularTacGroup.getTAC(), is(UNKNOWN_TAC));
        assertThat(mostPopularTacGroup.getTotalEvents(), is(16));
    }
}