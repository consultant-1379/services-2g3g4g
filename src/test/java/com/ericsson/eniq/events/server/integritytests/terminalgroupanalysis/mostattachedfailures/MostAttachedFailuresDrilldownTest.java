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
public class MostAttachedFailuresDrilldownTest extends
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
        populator.populateTemporaryGroupTable();
        populator.createTemporaryTables();

    }

    @Test
    public void testGetDrillDownOnAttachedFailuresData_ExclusiveTacGroup_OneWeek() throws Exception {
        populator.populateTemporaryTablesForLast48Hours();
        final List<TerminalMostAttachedFailuresDrilldownResult> results = runQuery(EXCLUSIVE_TAC_GROUP);
        validateResultForExclusiveTacGroup(results);
    }

    @Test
    public void testGetDrillDownOnAttachedFailuresData_ExclusiveTacGroup_OneWeek_NoModel() throws Exception {
        populator.populateTemporaryTablesForLast48HoursWithUnknownTac();
        populator.populateTACGroupTable(EXCLUSIVE_TAC_GROUP, MostAttachedFailuresTablesPopulator.UNKNOWN_TAC);
        final List<TerminalMostAttachedFailuresDrilldownResult> results = runQuery(EXCLUSIVE_TAC_GROUP);
        validateResultForExclusiveTacGroup_NoModel(results);
    }

    private void validateResultForExclusiveTacGroup(final List<TerminalMostAttachedFailuresDrilldownResult> results) {
        assertThat(results.size(), is(1));
        final TerminalMostAttachedFailuresDrilldownResult resultsForTac = results.get(0);
        assertThat(resultsForTac.getTAC(), is(SAMPLE_EXCLUSIVE_TAC));
        assertThat(resultsForTac.getManufacturer(), is(MANUFACTURER_FOR_SAMPLE_EXCLUSIVE_TAC));
        assertThat(resultsForTac.getMarketingName(), is(MARKETING_NAME_FOR_SAMPLE_EXCLUSIVE_TAC));
        assertThat(resultsForTac.getNoErrors(), is(1));
    }

    private void validateResultForExclusiveTacGroup_NoModel(final List<TerminalMostAttachedFailuresDrilldownResult> results) {
        assertThat(results.size(), is(1));
        final TerminalMostAttachedFailuresDrilldownResult resultsForTac = results.get(0);
        assertThat(resultsForTac.getTAC(), is(MostAttachedFailuresTablesPopulator.UNKNOWN_TAC));
        assertThat(resultsForTac.getManufacturer(),
                is(String.valueOf(MostAttachedFailuresTablesPopulator.UNKNOWN_TAC)));
        assertThat(resultsForTac.getMarketingName(),
                is(String.valueOf(MostAttachedFailuresTablesPopulator.UNKNOWN_TAC)));
        assertThat(resultsForTac.getNoErrors(), is(2));
    }

    @Test
    public void testGetDrillDownOnAttachedFailuresData_OneWeek() throws Exception {
        populator.populateTemporaryTablesForLast48Hours();
        final List<TerminalMostAttachedFailuresDrilldownResult> results = runQuery(MostAttachedFailuresTablesPopulator.WORST_TAC_GROUP);
        validateResultForRegularTacGroup(results);
    }

    @Test
    public void testGetDrillDownOnAttachedFailuresData_OneWeek_NoModel() throws Exception {
        populator.populateTemporaryTablesForLast48HoursWithUnknownTac();
        populator.populateTACGroupTable(MostAttachedFailuresTablesPopulator.WORST_TAC_GROUP,
                MostAttachedFailuresTablesPopulator.UNKNOWN_TAC);
        final List<TerminalMostAttachedFailuresDrilldownResult> results = runQuery(MostAttachedFailuresTablesPopulator.WORST_TAC_GROUP);
        validateResultForRegularTacGroup_NoModel(results);
    }

    private List<TerminalMostAttachedFailuresDrilldownResult> runQuery(final String tacGroup) throws Exception {
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(TIME_QUERY_PARAM, ONE_WEEK);
        map.putSingle(GROUP_NAME_PARAM, tacGroup);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        final String uriPath = TERMINAL_GROUP_ANALYSIS + "/" + MOST_ATTACHED_FAILURES;
        DummyUriInfoImpl.setUriInfo(map, terminalAndGroupAnalysisResource, SAMPLE_BASE_URI, uriPath);

        final String result = terminalAndGroupAnalysisResource.getMostAttachedFailuresData();
        System.out.println(result);
        return getTranslator().translateResult(result, TerminalMostAttachedFailuresDrilldownResult.class);
    }

    private void validateResultForRegularTacGroup(final List<TerminalMostAttachedFailuresDrilldownResult> results) {
        assertThat(results.size(), is(1));
        final TerminalMostAttachedFailuresDrilldownResult resultsForTac = results.get(0);
        assertThat(resultsForTac.getTAC(), is(MostAttachedFailuresTablesPopulator.WORST_TAC));
        assertThat(resultsForTac.getManufacturer(), is(MostAttachedFailuresTablesPopulator.MANFACTURER_FOR_WORST_TAC));
        assertThat(resultsForTac.getMarketingName(),
                is(MostAttachedFailuresTablesPopulator.MARKETING_NAME_FOR_WORST_TAC));
        assertThat(resultsForTac.getNoErrors(), is(MostAttachedFailuresTablesPopulator.noErrorsForWorstTacInDayTables * 2));
    }

    private void validateResultForRegularTacGroup_NoModel(final List<TerminalMostAttachedFailuresDrilldownResult> results) {
        assertThat(results.size(), is(1));
        final TerminalMostAttachedFailuresDrilldownResult resultsForTac = results.get(0);
        assertThat(resultsForTac.getTAC(), is(MostAttachedFailuresTablesPopulator.UNKNOWN_TAC));
        assertThat(resultsForTac.getManufacturer(),
                is(String.valueOf(MostAttachedFailuresTablesPopulator.UNKNOWN_TAC)));
        assertThat(resultsForTac.getMarketingName(),
                is(String.valueOf(MostAttachedFailuresTablesPopulator.UNKNOWN_TAC)));
        assertThat(resultsForTac.getNoErrors(), is(10));
    }

}
