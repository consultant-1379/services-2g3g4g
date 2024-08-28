/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.integritytests.terminalgroupanalysis.mostattachedfailures;

import com.ericsson.eniq.events.server.resources.TerminalAndGroupAnalysisResource;
import com.ericsson.eniq.events.server.resources.TestsWithTemporaryTablesBaseTestCase;
import com.ericsson.eniq.events.server.test.queryresults.TerminalMostAttachedFailuresDrilldownResult;
import com.ericsson.eniq.events.server.test.stubs.DummyUriInfoImpl;
import com.sun.jersey.core.util.MultivaluedMapImpl;
import org.junit.Test;

import javax.ws.rs.core.MultivaluedMap;
import java.util.List;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.*;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * @author eemecoy
 *
 */
public class MostAttachedFailuresDrilldownRawTest extends
        TestsWithTemporaryTablesBaseTestCase<TerminalMostAttachedFailuresDrilldownResult> {

    private TerminalAndGroupAnalysisResource terminalAndGroupAnalysisResource;

    private MostAttachedFailuresTablesPopulator populator;

    /* (non-Javadoc)
     * @see com.ericsson.eniq.events.server.resources.TestsWithTemporaryTablesBaseTestCase#onSetUp()
     */
    @Override
    public void onSetUp() throws Exception {
        super.onSetUp();
        terminalAndGroupAnalysisResource = new TerminalAndGroupAnalysisResource();
        attachDependencies(terminalAndGroupAnalysisResource);
        terminalAndGroupAnalysisResource.setTechPackCXCMapping(techPackCXCMappingService);
        populator = new MostAttachedFailuresTablesPopulator(connection);
        populator.createTemporaryTables();
        populator.populateTemporaryGroupTable();
    }

    @Test
    public void testGetDrillDownOnAttachedFailuresData_ExclusiveTacGroup_48Hours() throws Exception {
        populator.populateTemporaryTablesForLast3Minutes();
        populator.populateTemporaryTablesForLast48Hours();
        final List<TerminalMostAttachedFailuresDrilldownResult> translatedResults = runQuery(EXCLUSIVE_TAC_GROUP,
                FORTY_EIGHT_HOURS);
        validateResultForExclusiveTacGroup(translatedResults);
    }

    @Test
    public void testGetDrillDownOnAttachedFailuresData_ExclusiveTacGroup_48Hours_NoModel() throws Exception {
        populator.populateTemporaryTablesForLast48HoursWithUnknownTac();
        populator.populateTACGroupTable(EXCLUSIVE_TAC_GROUP,MostAttachedFailuresTablesPopulator.UNKNOWN_TAC);
        final List<TerminalMostAttachedFailuresDrilldownResult> translatedResults = runQuery(EXCLUSIVE_TAC_GROUP,
                FORTY_EIGHT_HOURS);
        validateResultForExclusiveTacGroup_NoModel(translatedResults);
    }

    @Test
    public void testGetDrillDownOnAttachedFailuresData_FiveMinutes() throws Exception {
        populator.populateTemporaryTablesForLast3Minutes();
        populator.populateTemporaryTablesForLast48Hours();
        final List<TerminalMostAttachedFailuresDrilldownResult> translatedResults = runQuery(
                MostAttachedFailuresTablesPopulator.WORST_TAC_GROUP, FIVE_MINUTES);
        validateResultForRegularTacGroup(translatedResults);
    }

    @Test
    public void testGetDrillDownOnAttachedFailuresData_FiveMinutes_NoModel() throws Exception {
        populator.populateTemporaryTablesForLast3MinutesWithUnknownTac();
        populator.populateTemporaryTablesForLast48HoursWithUnknownTac();
        populator.populateTACGroupTable(MostAttachedFailuresTablesPopulator.WORST_TAC_GROUP,
                MostAttachedFailuresTablesPopulator.UNKNOWN_TAC);
        final List<TerminalMostAttachedFailuresDrilldownResult> translatedResults = runQuery(
                MostAttachedFailuresTablesPopulator.WORST_TAC_GROUP, FIVE_MINUTES);
        validateResultForRegularTacGroup_NoModel(translatedResults);
    }

    @Test
    public void testGetDrillDownOnAttachedFailuresData_ExclusiveTacGroup_FiveMinutes() throws Exception {
        populator.populateTemporaryTablesForLast3Minutes();
        populator.populateTemporaryTablesForLast48Hours();
        final List<TerminalMostAttachedFailuresDrilldownResult> translatedResults = runQuery(EXCLUSIVE_TAC_GROUP,
                FIVE_MINUTES);
        validateResultForExclusiveTacGroup(translatedResults);
    }

    @Test
    public void testGetDrillDownOnAttachedFailuresData_ExclusiveTacGroup_FiveMinutes_NoModel() throws Exception {
        populator.populateTemporaryTablesForLast3MinutesWithUnknownTac();
        populator.populateTemporaryTablesForLast48HoursWithUnknownTac();
        populator.populateTACGroupTable(EXCLUSIVE_TAC_GROUP, MostAttachedFailuresTablesPopulator.UNKNOWN_TAC);
        final List<TerminalMostAttachedFailuresDrilldownResult> translatedResults = runQuery(EXCLUSIVE_TAC_GROUP,
                FIVE_MINUTES);
        validateResultForExclusiveTacGroup_NoModel(translatedResults);
    }

    private void validateResultForExclusiveTacGroup(final List<TerminalMostAttachedFailuresDrilldownResult> results) {
        assertThat(results.size(), is(1));
        final TerminalMostAttachedFailuresDrilldownResult resultsForTac = results.get(0);
        assertThat(resultsForTac.getTAC(), is(SAMPLE_EXCLUSIVE_TAC));
        assertThat(resultsForTac.getManufacturer(), is(MANUFACTURER_FOR_SAMPLE_EXCLUSIVE_TAC));
        assertThat(resultsForTac.getMarketingName(), is(MARKETING_NAME_FOR_SAMPLE_EXCLUSIVE_TAC));
        assertThat(resultsForTac.getNoErrors(), is(1));
    }

    private void validateResultForExclusiveTacGroup_NoModel(
            final List<TerminalMostAttachedFailuresDrilldownResult> results) {
        assertThat(results.size(), is(1));
        final TerminalMostAttachedFailuresDrilldownResult resultsForTac = results.get(0);
        assertThat(resultsForTac.getTAC(), is(MostAttachedFailuresTablesPopulator.UNKNOWN_TAC));
        assertThat(resultsForTac.getManufacturer(), is(String.valueOf(
                MostAttachedFailuresTablesPopulator.UNKNOWN_TAC)));
        assertThat(resultsForTac.getMarketingName(), is(String.valueOf(
                MostAttachedFailuresTablesPopulator.UNKNOWN_TAC)));
        assertThat(resultsForTac.getNoErrors(), is(2));
    }

    private List<TerminalMostAttachedFailuresDrilldownResult> runQuery(final String tacGroup, final String time)
            throws Exception {
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(TIME_QUERY_PARAM, time);
        map.putSingle(GROUP_NAME_PARAM, tacGroup);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        final String uriPath = TERMINAL_GROUP_ANALYSIS + "/" + MOST_ATTACHED_FAILURES;
        DummyUriInfoImpl.setUriInfo(map, terminalAndGroupAnalysisResource, SAMPLE_BASE_URI, uriPath);

        final String result = terminalAndGroupAnalysisResource.getMostAttachedFailuresData();
        System.out.println(result);
        final List<TerminalMostAttachedFailuresDrilldownResult> translatedResults = getTranslator().translateResult(
                result, TerminalMostAttachedFailuresDrilldownResult.class);
        return translatedResults;
    }

    private void validateResultForRegularTacGroup(final List<TerminalMostAttachedFailuresDrilldownResult> results) {
        assertThat(results.size(), is(1));
        final TerminalMostAttachedFailuresDrilldownResult resultsForTac = results.get(0);
        assertThat(resultsForTac.getTAC(), is(MostAttachedFailuresTablesPopulator.WORST_TAC));
        assertThat(resultsForTac.getManufacturer(), is(MostAttachedFailuresTablesPopulator.MANFACTURER_FOR_WORST_TAC));
        assertThat(resultsForTac.getMarketingName(),
                is(MostAttachedFailuresTablesPopulator.MARKETING_NAME_FOR_WORST_TAC));
        assertThat(resultsForTac.getNoErrors(), is(2));
    }

    private void validateResultForRegularTacGroup_NoModel(
            final List<TerminalMostAttachedFailuresDrilldownResult> results) {
        assertThat(results.size(), is(1));
        final TerminalMostAttachedFailuresDrilldownResult resultsForTac = results.get(0);
        assertThat(resultsForTac.getTAC(), is(MostAttachedFailuresTablesPopulator.UNKNOWN_TAC));
        assertThat(resultsForTac.getManufacturer(),
                is(String.valueOf(MostAttachedFailuresTablesPopulator.UNKNOWN_TAC)));
        assertThat(resultsForTac.getMarketingName(),
                is(String.valueOf(MostAttachedFailuresTablesPopulator.UNKNOWN_TAC)));
        assertThat(resultsForTac.getNoErrors(), is(2));
    }

}
