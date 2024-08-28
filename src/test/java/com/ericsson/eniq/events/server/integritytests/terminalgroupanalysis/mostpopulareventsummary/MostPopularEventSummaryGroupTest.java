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

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.*;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.net.URISyntaxException;
import java.util.List;

import static com.ericsson.eniq.events.server.integritytests.terminalgroupanalysis.mostpopulareventsummary.TablesPopulatorForOneWeekQuery.*;

import javax.ws.rs.core.MultivaluedMap;

import org.junit.Test;

import com.ericsson.eniq.events.server.resources.TerminalAndGroupAnalysisResource;
import com.ericsson.eniq.events.server.resources.TestsWithTemporaryTablesBaseTestCase;
import com.ericsson.eniq.events.server.test.queryresults.MostPopularEventSummaryGroupResult;
import com.ericsson.eniq.events.server.test.stubs.DummyUriInfoImpl;
import com.sun.jersey.core.util.MultivaluedMapImpl;

public class MostPopularEventSummaryGroupTest
        extends
        TestsWithTemporaryTablesBaseTestCase<MostPopularEventSummaryGroupResult> {

    private TerminalAndGroupAnalysisResource terminalAndGroupAnalysisResource;

    /*
     * (non-Javadoc)
     * 
     * @see com.ericsson.eniq.events.server.resources.
     * TestsWithTemporaryTablesBaseTestCase#onSetUp()
     */
    @Override
    public void onSetUp() throws Exception {
        super.onSetUp();
        terminalAndGroupAnalysisResource = new TerminalAndGroupAnalysisResource();
        attachDependencies(terminalAndGroupAnalysisResource);
        terminalAndGroupAnalysisResource
                .setTechPackCXCMapping(techPackCXCMappingService);
        final TablesPopulatorForOneWeekQuery populator = new TablesPopulatorForOneWeekQuery(
                connection);
        populator.createTemporaryTables();
        populator.populateTemporaryTables();
    }

    private String getQueryParams(final String timeParam) {
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        String result = null;
        map.putSingle(DISPLAY_PARAM_TEST, GRID);
        map.putSingle(TIME_QUERY_PARAM_TEST, timeParam);
        map.putSingle(TZ_OFFSET_TEST, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        try {
            DummyUriInfoImpl.setUriInfo(map,
                    terminalAndGroupAnalysisResource, SAMPLE_BASE_URI,
                    TERMINAL_GROUP_ANALYSIS + "/" + MOST_POPULAR_EVENT_SUMMARY);        
            result = terminalAndGroupAnalysisResource
                .getMostPopularEventSummaryData();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        return result;
    }

    @Test
    public void testGetGroupMostPopularEventSummary_OneWeek() throws Exception {        
        final String result = getQueryParams(ONE_WEEK);        
        validateResultForWeekPeriod(result);

    }

    @Test
    public void testGetGroupMostPopularEventSummary_6Hours() throws Exception {
        final String result = getQueryParams(SIX_HOURS);
        validateResultFor6HourPeriod(result);

    }

    @Test
    public void testGetGroupMostPopularEventSummary_OneWeek_UsingIMSISucRaw()
            throws Exception {
        jndiProperties.disableSucRawJNDIProperty();
        final String result = getQueryParams(ONE_WEEK);
        validateResultForWeekPeriod(result);
        jndiProperties.setUpJNDIPropertiesForTest();
    }

    @Test
    public void testGetGroupMostPopularEventSummary_6Hours_UsingIMSISucRaw()
            throws Exception {
        jndiProperties.disableSucRawJNDIProperty();
        final String result = getQueryParams(SIX_HOURS);
        validateResultFor6HourPeriod(result);
        jndiProperties.setUpJNDIPropertiesForTest();
    }


    private void validateResultForWeekPeriod(final String result)
            throws Exception {
        final List<MostPopularEventSummaryGroupResult> results = getTranslator()
                .translateResult(result,
                        MostPopularEventSummaryGroupResult.class);
        assertThat(results.size(), is(3));

        final MostPopularEventSummaryGroupResult mostPopularTacGroup = results
                .get(0);
        assertThat(mostPopularTacGroup.getTacGroupName(),
                is(MOST_POPULAR_TAC_GROUP));
        final int expectedNoErrors = noErrorsForMostPopularTacInSgehDayTable
                + noErrorsForMostPopularTacInLTEDayTable;
        assertThat(mostPopularTacGroup.getNoErrors(), is(expectedNoErrors));
        final int expectedNoSuccesses = noSuccessesForMostPopularTacInLTEDayTable
                + noSuccessesForMostPopularTacInSgehDayTable;
        assertThat(mostPopularTacGroup.getNoSuccess(), is(expectedNoSuccesses));
        final int occurrences = expectedNoErrors + expectedNoSuccesses;
        assertThat(mostPopularTacGroup.getOccurrences(), is(occurrences));
        assertThat(mostPopularTacGroup.getSuccessRatio(),
                is(mostPopularTacGroup.calculateExpectedSuccessRatio()));
        assertThat(mostPopularTacGroup.getNoTotalErrSubscribers(), is(1));

        final MostPopularEventSummaryGroupResult thirdMostPopularTacGroup = results
                .get(1);
        assertThat(thirdMostPopularTacGroup.getTacGroupName(),
                is(THIRD_MOST_POPULAR_TAC_GROUP));
        assertThat(thirdMostPopularTacGroup.getNoErrors(),
                is(noErrorsForThirdMostPopularTacInLTEDayTable));
        assertThat(thirdMostPopularTacGroup.getNoSuccess(), is(0));
        assertThat(thirdMostPopularTacGroup.getOccurrences(),
                is(noErrorsForThirdMostPopularTacInLTEDayTable));
        assertThat(thirdMostPopularTacGroup.getSuccessRatio(),
                is(thirdMostPopularTacGroup.calculateExpectedSuccessRatio()));
        assertThat(thirdMostPopularTacGroup.getNoTotalErrSubscribers(), is(1));

        final MostPopularEventSummaryGroupResult secondMostPopularTacGroup = results
                .get(2);
        assertThat(secondMostPopularTacGroup.getTacGroupName(),
                is(SECOND_MOST_POPULAR_TAC_GROUP));
        assertThat(secondMostPopularTacGroup.getNoErrors(), is(0));
        assertThat(secondMostPopularTacGroup.getNoSuccess(),
                is(noSuccessesForSecondMostPopularTacInLTEDayTable
                        + noSuccessesForSecondMostPopularTacInSgehDayTable));
        assertThat(secondMostPopularTacGroup.getOccurrences(),
                is(noSuccessesForSecondMostPopularTacInLTEDayTable
                        + noSuccessesForSecondMostPopularTacInSgehDayTable));
        assertThat(secondMostPopularTacGroup.getSuccessRatio(),
                is(secondMostPopularTacGroup.calculateExpectedSuccessRatio()));
        assertThat(secondMostPopularTacGroup.getNoTotalErrSubscribers(), is(0));
    }

    private void validateResultFor6HourPeriod(final String result)
            throws Exception {
        final List<MostPopularEventSummaryGroupResult> results = getTranslator()
                .translateResult(result,
                        MostPopularEventSummaryGroupResult.class);
        assertThat(results.size(), is(3));

        final MostPopularEventSummaryGroupResult mostPopularTacGroup = results
                .get(1);
        assertThat(mostPopularTacGroup.getTacGroupName(),
                is(MOST_POPULAR_TAC_GROUP));
        final int expectedNoErrors = noErrorsForMostPopularTacInSgeh15MinTable
                + noErrorsForMostPopularTacInLTE15MinTable;
        assertThat(mostPopularTacGroup.getNoErrors(), is(expectedNoErrors));
        final int expectedNoSuccesses = noSuccessesForMostPopularTacInLTE15MinTable
                + noSuccessesForMostPopularTacInSgeh15MinTable;
        assertThat(mostPopularTacGroup.getNoSuccess(), is(expectedNoSuccesses));
        final int occurrences = expectedNoErrors + expectedNoSuccesses;
        assertThat(mostPopularTacGroup.getOccurrences(), is(occurrences));
        assertThat(mostPopularTacGroup.getSuccessRatio(),
                is(mostPopularTacGroup.calculateExpectedSuccessRatio()));
        assertThat(mostPopularTacGroup.getNoTotalErrSubscribers(), is(1));

        final MostPopularEventSummaryGroupResult thirdMostPopularTacGroup = results
                .get(2);
        assertThat(thirdMostPopularTacGroup.getTacGroupName(),
                is(THIRD_MOST_POPULAR_TAC_GROUP));
        assertThat(thirdMostPopularTacGroup.getNoErrors(),
                is(noErrorsForThirdMostPopularTacInLTE15MinTable));
        assertThat(thirdMostPopularTacGroup.getNoSuccess(), is(0));
        assertThat(thirdMostPopularTacGroup.getOccurrences(),
                is(noErrorsForThirdMostPopularTacInLTE15MinTable));
        assertThat(thirdMostPopularTacGroup.getSuccessRatio(),
                is(thirdMostPopularTacGroup.calculateExpectedSuccessRatio()));
        assertThat(thirdMostPopularTacGroup.getNoTotalErrSubscribers(), is(1));

        final MostPopularEventSummaryGroupResult secondMostPopularTacGroup = results
                .get(0);
        assertThat(secondMostPopularTacGroup.getTacGroupName(),
                is(SECOND_MOST_POPULAR_TAC_GROUP));
        assertThat(secondMostPopularTacGroup.getNoErrors(), is(0));
        assertThat(secondMostPopularTacGroup.getNoSuccess(),
                is(noSuccessesForSecondMostPopularTacInLTE15MinTable
                        + noSuccessesForSecondMostPopularTacInSgeh15MinTable));
        assertThat(secondMostPopularTacGroup.getOccurrences(),
                is(noSuccessesForSecondMostPopularTacInLTE15MinTable
                        + noSuccessesForSecondMostPopularTacInSgeh15MinTable));
        assertThat(secondMostPopularTacGroup.getSuccessRatio(),
                is(secondMostPopularTacGroup.calculateExpectedSuccessRatio()));
        assertThat(secondMostPopularTacGroup.getNoTotalErrSubscribers(), is(0));

    }

    private void validateResultForWeekPeriodUsingIMSISucRaw(final String result)
            throws Exception {
        final List<MostPopularEventSummaryGroupResult> results = getTranslator()
                .translateResult(result,
                        MostPopularEventSummaryGroupResult.class);
        assertThat(results.size(), is(4));

        final MostPopularEventSummaryGroupResult mostPopularTacGroup = results
                .get(1);
        assertThat(mostPopularTacGroup.getTacGroupName(),
                is(MOST_POPULAR_TAC_GROUP));
        final int expectedNoErrors = noErrorsForMostPopularTacInSgehDayTable
                + noErrorsForMostPopularTacInLTEDayTable;
        assertThat(mostPopularTacGroup.getNoErrors(), is(expectedNoErrors));
        final int expectedNoSuccesses = noSuccessesForMostPopularTacInLTEDayTable
                + noSuccessesForMostPopularTacInSgehDayTable;
        assertThat(mostPopularTacGroup.getNoSuccess(), is(expectedNoSuccesses));
        final int occurrences = expectedNoErrors + expectedNoSuccesses;
        assertThat(mostPopularTacGroup.getOccurrences(), is(occurrences));
        assertThat(mostPopularTacGroup.getSuccessRatio(),
                is(mostPopularTacGroup.calculateExpectedSuccessRatio()));
        assertThat(mostPopularTacGroup.getNoTotalErrSubscribers(), is(1));

        final MostPopularEventSummaryGroupResult thirdMostPopularTacGroup = results
                .get(2);
        assertThat(thirdMostPopularTacGroup.getTacGroupName(),
                is(THIRD_MOST_POPULAR_TAC_GROUP));
        assertThat(thirdMostPopularTacGroup.getNoErrors(),
                is(noErrorsForThirdMostPopularTacInLTEDayTable));
        assertThat(thirdMostPopularTacGroup.getNoSuccess(), is(0));
        assertThat(thirdMostPopularTacGroup.getOccurrences(),
                is(noErrorsForThirdMostPopularTacInLTEDayTable));
        assertThat(thirdMostPopularTacGroup.getSuccessRatio(),
                is(thirdMostPopularTacGroup.calculateExpectedSuccessRatio()));
        assertThat(thirdMostPopularTacGroup.getNoTotalErrSubscribers(), is(1));

        final MostPopularEventSummaryGroupResult secondMostPopularTacGroup = results
                .get(3);
        assertThat(secondMostPopularTacGroup.getTacGroupName(),
                is(SECOND_MOST_POPULAR_TAC_GROUP));
        assertThat(secondMostPopularTacGroup.getNoErrors(), is(0));
        assertThat(secondMostPopularTacGroup.getNoSuccess(),
                is(noSuccessesForSecondMostPopularTacInLTEDayTable
                        + noSuccessesForSecondMostPopularTacInSgehDayTable));
        assertThat(secondMostPopularTacGroup.getOccurrences(),
                is(noSuccessesForSecondMostPopularTacInLTEDayTable
                        + noSuccessesForSecondMostPopularTacInSgehDayTable));
        assertThat(secondMostPopularTacGroup.getSuccessRatio(),
                is(secondMostPopularTacGroup.calculateExpectedSuccessRatio()));
        assertThat(secondMostPopularTacGroup.getNoTotalErrSubscribers(), is(0));

        final MostPopularEventSummaryGroupResult fourthMostPopularTacGroup = results
                .get(0);
        assertThat(fourthMostPopularTacGroup.getTacGroupName(),
                is(EXCLUSIVE_TAC_GROUP));
        assertThat(fourthMostPopularTacGroup.getNoErrors(), is(2));
        assertThat(
                fourthMostPopularTacGroup.getNoSuccess(),
                is(noOfSuccessesForExclusiveTacInLTEIMSISucRawForOneWeek
                        + noOfSuccessesForExclusiveTacInSGEHIMSISucRawForOneWeek));
        assertThat(fourthMostPopularTacGroup.getOccurrences(), is(2
                + noOfSuccessesForExclusiveTacInLTEIMSISucRawForOneWeek
                + noOfSuccessesForExclusiveTacInSGEHIMSISucRawForOneWeek));
        assertThat(fourthMostPopularTacGroup.getSuccessRatio(),
                is(fourthMostPopularTacGroup.calculateExpectedSuccessRatio()));
        assertThat(fourthMostPopularTacGroup.getNoTotalErrSubscribers(), is(1));

    }

    private void validateResultFor6HourPeriodUsingIMSISucRaw(final String result)
            throws Exception {
        final List<MostPopularEventSummaryGroupResult> results = getTranslator()
                .translateResult(result,
                        MostPopularEventSummaryGroupResult.class);
        assertThat(results.size(), is(4));

        final MostPopularEventSummaryGroupResult mostPopularTacGroup = results
                .get(2);
        assertThat(mostPopularTacGroup.getTacGroupName(),
                is(MOST_POPULAR_TAC_GROUP));
        final int expectedNoErrors = noErrorsForMostPopularTacInSgeh15MinTable
                + noErrorsForMostPopularTacInLTE15MinTable;
        assertThat(mostPopularTacGroup.getNoErrors(), is(expectedNoErrors));
        final int expectedNoSuccesses = noSuccessesForMostPopularTacInLTE15MinTable
                + noSuccessesForMostPopularTacInSgeh15MinTable;
        assertThat(mostPopularTacGroup.getNoSuccess(), is(expectedNoSuccesses));
        final int occurrences = expectedNoErrors + expectedNoSuccesses;
        assertThat(mostPopularTacGroup.getOccurrences(), is(occurrences));
        assertThat(mostPopularTacGroup.getSuccessRatio(),
                is(mostPopularTacGroup.calculateExpectedSuccessRatio()));
        assertThat(mostPopularTacGroup.getNoTotalErrSubscribers(), is(1));

        final MostPopularEventSummaryGroupResult thirdMostPopularTacGroup = results
                .get(3);
        assertThat(thirdMostPopularTacGroup.getTacGroupName(),
                is(THIRD_MOST_POPULAR_TAC_GROUP));
        assertThat(thirdMostPopularTacGroup.getNoErrors(),
                is(noErrorsForThirdMostPopularTacInLTE15MinTable));
        assertThat(thirdMostPopularTacGroup.getNoSuccess(), is(0));
        assertThat(thirdMostPopularTacGroup.getOccurrences(),
                is(noErrorsForThirdMostPopularTacInLTE15MinTable));
        assertThat(thirdMostPopularTacGroup.getSuccessRatio(),
                is(thirdMostPopularTacGroup.calculateExpectedSuccessRatio()));
        assertThat(thirdMostPopularTacGroup.getNoTotalErrSubscribers(), is(1));

        final MostPopularEventSummaryGroupResult secondMostPopularTacGroup = results
                .get(1);
        assertThat(secondMostPopularTacGroup.getTacGroupName(),
                is(SECOND_MOST_POPULAR_TAC_GROUP));
        assertThat(secondMostPopularTacGroup.getNoErrors(), is(0));
        assertThat(secondMostPopularTacGroup.getNoSuccess(),
                is(noSuccessesForSecondMostPopularTacInLTE15MinTable
                        + noSuccessesForSecondMostPopularTacInSgeh15MinTable));
        assertThat(secondMostPopularTacGroup.getOccurrences(),
                is(noSuccessesForSecondMostPopularTacInLTE15MinTable
                        + noSuccessesForSecondMostPopularTacInSgeh15MinTable));
        assertThat(secondMostPopularTacGroup.getSuccessRatio(),
                is(secondMostPopularTacGroup.calculateExpectedSuccessRatio()));
        assertThat(secondMostPopularTacGroup.getNoTotalErrSubscribers(), is(0));

        final MostPopularEventSummaryGroupResult fourthMostPopularTacGroup = results
                .get(0);
        assertThat(fourthMostPopularTacGroup.getTacGroupName(),
                is(EXCLUSIVE_TAC_GROUP));
        assertThat(fourthMostPopularTacGroup.getNoErrors(), is(3));
        assertThat(
                fourthMostPopularTacGroup.getNoSuccess(),
                is(noOfSuccessesForExclusiveTacInLTEIMSISucRawFor30Minutes
                        + noOfSuccessesForExclusiveTacInSGEHIMSISucRawFor30Minutes));
        assertThat(fourthMostPopularTacGroup.getOccurrences(), is(3
                + noOfSuccessesForExclusiveTacInLTEIMSISucRawFor30Minutes
                + noOfSuccessesForExclusiveTacInSGEHIMSISucRawFor30Minutes));
        assertThat(fourthMostPopularTacGroup.getSuccessRatio(),
                is(fourthMostPopularTacGroup.calculateExpectedSuccessRatio()));
        assertThat(fourthMostPopularTacGroup.getNoTotalErrSubscribers(), is(1));

    }
}
