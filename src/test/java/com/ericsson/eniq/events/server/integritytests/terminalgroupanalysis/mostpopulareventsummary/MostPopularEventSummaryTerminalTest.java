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
import com.ericsson.eniq.events.server.test.queryresults.MostPopularEventSummaryTerminalResult;
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

public class MostPopularEventSummaryTerminalTest extends TestsWithTemporaryTablesBaseTestCase<MostPopularEventSummaryTerminalResult> {

    private TerminalAndGroupAnalysisResource terminalAndGroupAnalysisResource;

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
        final TablesPopulatorForOneWeekQuery populator = new TablesPopulatorForOneWeekQuery(connection);
        populator.createTemporaryTables();
        populator.populateTemporaryTables();
    }

    @Test
    public void testGetTerminalMostPopularEventSummary_OneWeek() throws Exception {
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(TIME_QUERY_PARAM, ONE_WEEK);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        DummyUriInfoImpl.setUriInfo(map, terminalAndGroupAnalysisResource, SAMPLE_BASE_URI, TERMINAL_ANALYSIS + "/" + MOST_POPULAR_EVENT_SUMMARY);
        final String result = terminalAndGroupAnalysisResource.getMostPopularEventSummaryData();
        System.out.println(result);
        validateResult(result);

    }

    private void validateResult(final String result) throws Exception {
        final List<MostPopularEventSummaryTerminalResult> results = getTranslator().translateResult(result,
                MostPopularEventSummaryTerminalResult.class);
        assertThat(results.size(), is(3));

        final MostPopularEventSummaryTerminalResult mostPopularTac = results.get(0);
        assertThat(mostPopularTac.getTac(), is(MOST_POPULAR_TAC));
        assertThat(mostPopularTac.getManufacturer(), is(MANUFACTURER_FOR_MOST_POPULAR_TAC));
        assertThat(mostPopularTac.getMarketingName(), is(MARKETING_NAME_FOR_MOST_POPULAR_TAC));
        final int expectedNoErrors = noErrorsForMostPopularTacInSgehDayTable + noErrorsForMostPopularTacInLTEDayTable;
        assertThat(mostPopularTac.getNoErrors(), is(expectedNoErrors));
        final int expectedNoSuccesses = noSuccessesForMostPopularTacInLTEDayTable + noSuccessesForMostPopularTacInSgehDayTable;
        assertThat(mostPopularTac.getNoSuccesses(), is(expectedNoSuccesses));
        assertThat(mostPopularTac.getOccurrences(), is(expectedNoErrors + expectedNoSuccesses));
        assertThat(mostPopularTac.getSuccessRatio(), is(mostPopularTac.calculateExpectedSuccessRatio()));

        final MostPopularEventSummaryTerminalResult thirdMostPopularTac = results.get(1);
        assertThat(thirdMostPopularTac.getTac(), is(THIRD_MOST_POPULAR_TAC));
        assertThat(thirdMostPopularTac.getManufacturer(), is(Integer.toString(THIRD_MOST_POPULAR_TAC)));
        assertThat(thirdMostPopularTac.getMarketingName(), is(Integer.toString(THIRD_MOST_POPULAR_TAC)));
        assertThat(thirdMostPopularTac.getNoErrors(), is(noErrorsForThirdMostPopularTacInLTEDayTable));
        assertThat(thirdMostPopularTac.getNoSuccesses(), is(0));
        assertThat(thirdMostPopularTac.getOccurrences(), is(noErrorsForThirdMostPopularTacInLTEDayTable));
        assertThat(thirdMostPopularTac.getSuccessRatio(), is(thirdMostPopularTac.calculateExpectedSuccessRatio()));

        final MostPopularEventSummaryTerminalResult secondMostPopularTac = results.get(2);
        assertThat(secondMostPopularTac.getTac(), is(SECOND_MOST_POPULAR_TAC));
        assertThat(secondMostPopularTac.getManufacturer(), is(MANUFACTURER_FOR_SECOND_MOST_POPULAR_TAC));
        assertThat(secondMostPopularTac.getMarketingName(), is(MARKETING_NAME_FOR_SECOND_MOST_POPULAR_TAC));
        assertThat(secondMostPopularTac.getNoErrors(), is(0));
        assertThat(secondMostPopularTac.getNoSuccesses(), is(noSuccessesForSecondMostPopularTacInLTEDayTable
                + noSuccessesForSecondMostPopularTacInSgehDayTable));
        assertThat(secondMostPopularTac.getOccurrences(), is(noSuccessesForSecondMostPopularTacInLTEDayTable
                + noSuccessesForSecondMostPopularTacInSgehDayTable));
        assertThat(secondMostPopularTac.getSuccessRatio(), is(secondMostPopularTac.calculateExpectedSuccessRatio()));
    }

}
