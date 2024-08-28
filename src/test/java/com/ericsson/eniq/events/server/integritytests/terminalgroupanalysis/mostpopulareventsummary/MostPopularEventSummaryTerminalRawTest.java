/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.integritytests.terminalgroupanalysis.mostpopulareventsummary;

import com.ericsson.eniq.events.server.resources.TerminalAndGroupAnalysisResource;
import com.ericsson.eniq.events.server.resources.TestsWithTemporaryTablesBaseTestCase;
import com.ericsson.eniq.events.server.test.queryresults.MostPopularEventSummaryTerminalResult;
import com.ericsson.eniq.events.server.test.stubs.DummyUriInfoImpl;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;
import com.sun.jersey.core.util.MultivaluedMapImpl;
import org.junit.Test;

import javax.ws.rs.core.MultivaluedMap;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.List;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.integritytests.terminalgroupanalysis.mostpopulareventsummary.RawTablesPopulatorForTerminalGroupAnalysis.*;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.*;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * @author eemecoy
 *
 */
public class MostPopularEventSummaryTerminalRawTest extends
        TestsWithTemporaryTablesBaseTestCase<MostPopularEventSummaryTerminalResult> {

    private TerminalAndGroupAnalysisResource terminalAndGroupAnalysisResource;

    /* (non-Javadoc)
     * @see com.ericsson.eniq.events.server.resources.TestsWithTemporaryTablesBaseTestCase#onSetUp()
     */
    @Override
    public void onSetUp() throws Exception {
        super.onSetUp();
        terminalAndGroupAnalysisResource = new TerminalAndGroupAnalysisResource();
        attachDependencies(terminalAndGroupAnalysisResource);
        terminalAndGroupAnalysisResource.setTechPackCXCMapping(techPackCXCMappingService);
    }

    @Test
    public void testGetTerminalMostPopularEventSummary_FiveMinutes() throws Exception {
        populateTablesAndData(DateTimeUtilities.getDateTimeMinus2Minutes());
        final String result = getData(FIVE_MINUTES);
        validateResult(result);
    }

    @Test
    public void testGetTerminalMostPopularEventSummaryWithDataTieredOn30Min() throws Exception {
        jndiProperties.setUpDataTieringJNDIProperty();
        populateTablesAndData(DateTimeUtilities.getDateTimeMinus25Minutes());
        final String result = getData(THIRTY_MINUTES);
        validateResult(result);
        jndiProperties.setUpJNDIPropertiesForTest();
    }

    private void validateResult(final String result) throws Exception {
        final List<MostPopularEventSummaryTerminalResult> results = getTranslator().translateResult(result,
                MostPopularEventSummaryTerminalResult.class);
        assertThat(results.size(), is(3));
        final MostPopularEventSummaryTerminalResult mostPopularTac = results.get(0);
        assertThat(mostPopularTac.getTac(), is(MOST_POPULAR_TAC));
        assertThat(mostPopularTac.getManufacturer(), is(MANUFACTURER_FOR_MOST_POPULAR_TAC));
        assertThat(mostPopularTac.getMarketingName(), is(MARKETING_NAME_FOR_MOST_POPULAR_TAC));
        assertThat(mostPopularTac.getNoErrors(), is(2));
        assertThat(mostPopularTac.getNoSuccesses(), is(2));
        assertThat(mostPopularTac.getOccurrences(), is(4));
        assertThat(mostPopularTac.getSuccessRatio(), is(mostPopularTac.calculateExpectedSuccessRatio()));

        final MostPopularEventSummaryTerminalResult thirdMostPopularTac = results.get(2);
        assertThat(thirdMostPopularTac.getManufacturer(), is(String.valueOf(THIRD_MOST_POPULAR_TAC)));
        assertThat(thirdMostPopularTac.getMarketingName(), is(String.valueOf(THIRD_MOST_POPULAR_TAC)));
        assertThat(thirdMostPopularTac.getNoErrors(), is(1));
        assertThat(thirdMostPopularTac.getNoSuccesses(), is(1));
        assertThat(thirdMostPopularTac.getOccurrences(), is(2));
        assertThat(thirdMostPopularTac.getSuccessRatio(), is(thirdMostPopularTac.calculateExpectedSuccessRatio()));

    }

    private void populateTablesAndData(final String time) throws Exception, SQLException {
        final RawTablesPopulatorForTerminalGroupAnalysis populator = new RawTablesPopulatorForTerminalGroupAnalysis(
                connection);
        populator.createTemporaryTables();
        populator.populateTemporaryTables(time);
    }

    private String getData(final String time) throws URISyntaxException {
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(TIME_QUERY_PARAM, time);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        DummyUriInfoImpl.setUriInfo(map, terminalAndGroupAnalysisResource, SAMPLE_BASE_URI, TERMINAL_ANALYSIS + "/"
                + MOST_POPULAR_EVENT_SUMMARY);
        final String result = terminalAndGroupAnalysisResource.getMostPopularEventSummaryData();
        System.out.println(result);
        return result;
    }

}
