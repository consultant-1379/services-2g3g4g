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

import com.ericsson.eniq.events.server.integritytests.terminalgroupanalysis.mostpopulareventsummary.RawTablesPopulatorForTerminalGroupAnalysis;
import com.ericsson.eniq.events.server.resources.TerminalAndGroupAnalysisResource;
import com.ericsson.eniq.events.server.resources.TestsWithTemporaryTablesBaseTestCase;
import com.ericsson.eniq.events.server.test.queryresults.TerminalMostPopularDrilldownResult;
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

public class MostPopularDrilldownRawTest extends TestsWithTemporaryTablesBaseTestCase<TerminalMostPopularDrilldownResult> {

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
    public void testGetGroupMostPopularEventDrilldown_ExclusiveTacGroup_FiveMinutes() throws Exception {
        populator.populateTemporaryTables(DateTimeUtilities.getDateTimeMinus2Minutes());
        final String result = runQuery(EXCLUSIVE_TAC_GROUP, FIVE_MINUTES);
        validateResultForExclusiveTacGroup(result);
    }

    @Test
    public void testGetGroupMostPopularEventDrilldown_ExclusiveTacGroup_FiveMinutes_NoModel() throws Exception {
        populator.populateTemporaryTables(DateTimeUtilities.getDateTimeMinus2Minutes());
        populator.populateTemporaryTablesForUnknownTAC(DateTimeUtilities.getDateTimeMinus2Minutes());
        final String result = runQuery(EXCLUSIVE_TAC_GROUP, FIVE_MINUTES);
        validateResultForExclusiveTacGroup_NoModel(result);
    }

    @Test
    public void testGetGroupMostPopularEventDrilldown_FiveMinutes() throws Exception {
        populator.populateTemporaryTables(DateTimeUtilities.getDateTimeMinus2Minutes());
        final String result = runQuery(MOST_POPULAR_TAC_GROUP, FIVE_MINUTES);
        validateResultForRegularTacGroup(result);
    }

    @Test
    public void testGetGroupMostPopularEventDrilldown_FiveMinutes_NoModel() throws Exception {
        populator.populateTemporaryTables(DateTimeUtilities.getDateTimeMinus2Minutes());
        final String result = runQuery(THIRD_MOST_POPULAR_TAC_GROUP, FIVE_MINUTES);
        validateResultForRegularTacGroup_NoModel(result);
    }

    @Test
    public void testGetGroupMostPopularEventDrilldown_30Minutes() throws Exception {
        populator.populateTemporaryTables(DateTimeUtilities.getDateTimeMinus25Minutes());
        jndiProperties.setUpDataTieringJNDIProperty();
        final String result = runQuery(MOST_POPULAR_TAC_GROUP, THIRTY_MINUTES);
        validateResultForRegularTacGroup(result);
        jndiProperties.setUpJNDIPropertiesForTest();
    }

    @Test
    public void testGetGroupMostPopularEventDrilldown_30Minutes_NoModel() throws Exception {
        populator.populateTemporaryTables(DateTimeUtilities.getDateTimeMinus25Minutes());
        jndiProperties.setUpDataTieringJNDIProperty();
        final String result = runQuery(THIRD_MOST_POPULAR_TAC_GROUP, THIRTY_MINUTES);
        validateResultForRegularTacGroup_NoModel(result);
        jndiProperties.setUpJNDIPropertiesForTest();
    }

    @Test
    public void testGetGroupMostPopularEventDrilldown_30Minutes_UsingIMSISucRaw() throws Exception {
        jndiProperties.setupDataTieringAndDisableSucRawJNDIProperties();
        populator.populateTemporaryTables(DateTimeUtilities.getDateTimeMinus25Minutes());
        final String result = runQuery(MOST_POPULAR_TAC_GROUP, THIRTY_MINUTES);
        validateResultForRegularTacGroup(result);
        jndiProperties.setUpJNDIPropertiesForTest();
    }

    @Test
    public void testGetGroupMostPopularEventDrilldown_30Minutes_UsingIMSISucRaw_NoModel() throws Exception {
        jndiProperties.setupDataTieringAndDisableSucRawJNDIProperties();
        populator.populateTemporaryTables(DateTimeUtilities.getDateTimeMinus25Minutes());
        final String result = runQuery(THIRD_MOST_POPULAR_TAC_GROUP, THIRTY_MINUTES);
        validateResultForRegularTacGroup_NoModel(result);
        jndiProperties.setUpJNDIPropertiesForTest();
    }

    @Test
    public void testGetGroupMostPopularEventDrilldown_30Minutes_ForExclusiveTAC() throws Exception {
        populator.populateTemporaryTables(DateTimeUtilities.getDateTimeMinus25Minutes());
        jndiProperties.setUpDataTieringJNDIProperty();
        final String result = runQuery(EXCLUSIVE_TAC_GROUP, THIRTY_MINUTES);
        validateResultForExclusiveTacGroup(result);
        jndiProperties.setUpJNDIPropertiesForTest();
    }

    @Test
    public void testGetGroupMostPopularEventDrilldown_30Minutes_ForExclusiveTAC_NoModel() throws Exception {
        populator.populateTemporaryTables(DateTimeUtilities.getDateTimeMinus25Minutes());
        populator.populateTemporaryTablesForUnknownTAC(DateTimeUtilities.getDateTimeMinus25Minutes());
        jndiProperties.setUpDataTieringJNDIProperty();
        final String result = runQuery(EXCLUSIVE_TAC_GROUP, THIRTY_MINUTES);
        validateResultForExclusiveTacGroup_NoModel(result);
        jndiProperties.setUpJNDIPropertiesForTest();
    }

    @Test
    public void testGetGroupMostPopularEventDrilldown_30Minutes_UsingIMSISucRaw_ForExclusiveTAC() throws Exception {
        jndiProperties.setupDataTieringAndDisableSucRawJNDIProperties();
        populator.populateTemporaryTables(DateTimeUtilities.getDateTimeMinus25Minutes());
        final String result = runQuery(EXCLUSIVE_TAC_GROUP, THIRTY_MINUTES);
        validateResultForExclusiveTacGroupFor30MinuteQuery(result);
        jndiProperties.setUpJNDIPropertiesForTest();
    }

    private String runQuery(final String tacGroup, final String time) throws URISyntaxException {
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(TIME_QUERY_PARAM, time);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_MINUS_ONE_HOUR);
        map.putSingle(GROUP_NAME_PARAM, tacGroup);
        DummyUriInfoImpl.setUriInfo(map, terminalAndGroupAnalysisResource, SAMPLE_BASE_URI, TERMINAL_GROUP_ANALYSIS + "/" + MOST_POPULAR);
        final String result = terminalAndGroupAnalysisResource.getMostPopularData();
        System.out.println(result);
        return result;
    }

    private void validateResultForExclusiveTacGroup(final String result) throws Exception {
        final List<TerminalMostPopularDrilldownResult> results = getTranslator().translateResult(result, TerminalMostPopularDrilldownResult.class);
        assertThat(results.size(), is(1));
        final TerminalMostPopularDrilldownResult mostPopularTacGroup = results.get(0);
        assertThat(mostPopularTacGroup.getManufacturer(), is(MANUFACTURER_FOR_SAMPLE_EXCLUSIVE_TAC));
        assertThat(mostPopularTacGroup.getMarketingName(), is(MARKETING_NAME_FOR_SAMPLE_EXCLUSIVE_TAC));
        assertThat(mostPopularTacGroup.getTAC(), is(SAMPLE_EXCLUSIVE_TAC));
        assertThat(mostPopularTacGroup.getTotalEvents(), is(2));
    }

    private void validateResultForExclusiveTacGroup_NoModel(final String result) throws Exception {
        final List<TerminalMostPopularDrilldownResult> results = getTranslator().translateResult(result,
                TerminalMostPopularDrilldownResult.class);
        assertThat(results.size(), is(2));
        final TerminalMostPopularDrilldownResult mostPopularTacGroup = results.get(1);
        assertThat(mostPopularTacGroup.getManufacturer(), is(String.valueOf(UNKNOWN_TAC)));
        assertThat(mostPopularTacGroup.getMarketingName(), is(String.valueOf(UNKNOWN_TAC)));
        assertThat(mostPopularTacGroup.getTAC(), is(UNKNOWN_TAC));
        assertThat(mostPopularTacGroup.getTotalEvents(), is(6));
    }

    private void validateResultForRegularTacGroup(final String result) throws Exception {
        final List<TerminalMostPopularDrilldownResult> results = getTranslator().translateResult(result, TerminalMostPopularDrilldownResult.class);
        assertThat(results.size(), is(1));
        final TerminalMostPopularDrilldownResult mostPopularTacGroup = results.get(0);
        assertThat(mostPopularTacGroup.getManufacturer(), is(MANUFACTURER_FOR_MOST_POPULAR_TAC));
        assertThat(mostPopularTacGroup.getMarketingName(), is(MARKETING_NAME_FOR_MOST_POPULAR_TAC));
        assertThat(mostPopularTacGroup.getTAC(), is(MOST_POPULAR_TAC));
        assertThat(mostPopularTacGroup.getTotalEvents(), is(4));
    }

    private void validateResultForRegularTacGroup_NoModel(final String result) throws Exception {
        final List<TerminalMostPopularDrilldownResult> results = getTranslator().translateResult(result, TerminalMostPopularDrilldownResult.class);
        assertThat(results.size(), is(1));
        final TerminalMostPopularDrilldownResult mostPopularTacGroup = results.get(0);
        assertThat(mostPopularTacGroup.getManufacturer(), is(String.valueOf(THIRD_MOST_POPULAR_TAC)));
        assertThat(mostPopularTacGroup.getMarketingName(), is(String.valueOf(THIRD_MOST_POPULAR_TAC)));
        assertThat(mostPopularTacGroup.getTAC(), is(THIRD_MOST_POPULAR_TAC));
        assertThat(mostPopularTacGroup.getTotalEvents(), is(2));
    }

    private void validateResultForExclusiveTacGroupFor30MinuteQuery(final String result) throws Exception {
        final List<TerminalMostPopularDrilldownResult> results = getTranslator().translateResult(result, TerminalMostPopularDrilldownResult.class);
        assertThat(results.size(), is(1));
        final TerminalMostPopularDrilldownResult mostPopularTacGroup = results.get(0);
        assertThat(mostPopularTacGroup.getManufacturer(), is(MANUFACTURER_FOR_SAMPLE_EXCLUSIVE_TAC));
        assertThat(mostPopularTacGroup.getMarketingName(), is(MARKETING_NAME_FOR_SAMPLE_EXCLUSIVE_TAC));
        assertThat(mostPopularTacGroup.getTAC(), is(SAMPLE_EXCLUSIVE_TAC));
        assertThat(mostPopularTacGroup.getTotalEvents(), is(1 + noSuccessesForMostPopularTacInSGEHIMSISucRaw
                + noSuccessesForMostPopularTacInLTEIMSISucRaw));
    }
}
