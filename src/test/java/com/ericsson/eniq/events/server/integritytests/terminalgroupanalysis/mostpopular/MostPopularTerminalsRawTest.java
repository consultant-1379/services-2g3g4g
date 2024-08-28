/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.integritytests.terminalgroupanalysis.mostpopular;

import com.ericsson.eniq.events.server.integritytests.terminalgroupanalysis.mostpopulareventsummary.RawTablesPopulatorForTerminalGroupAnalysis;
import com.ericsson.eniq.events.server.resources.TerminalAndGroupAnalysisResource;
import com.ericsson.eniq.events.server.resources.TestsWithTemporaryTablesBaseTestCase;
import com.ericsson.eniq.events.server.test.queryresults.MostPopularTerminalsResult;
import com.ericsson.eniq.events.server.test.stubs.DummyUriInfoImpl;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;
import com.sun.jersey.core.util.MultivaluedMapImpl;
import org.junit.Test;

import javax.ws.rs.core.MultivaluedMap;
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
public class MostPopularTerminalsRawTest extends TestsWithTemporaryTablesBaseTestCase<MostPopularTerminalsResult> {

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
    public void testGetTerminalMostPopularEvents_FiveMinutes() throws Exception {
        final RawTablesPopulatorForTerminalGroupAnalysis populator = new RawTablesPopulatorForTerminalGroupAnalysis(
                connection);
        populator.createTemporaryTables();
        populator.populateTemporaryTables(DateTimeUtilities.getDateTimeMinus2Minutes());
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(TIME_QUERY_PARAM, FIVE_MINUTES);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        DummyUriInfoImpl.setUriInfo(map, terminalAndGroupAnalysisResource, SAMPLE_BASE_URI, TERMINAL_ANALYSIS + "/"
                + MOST_POPULAR);
        final String result = terminalAndGroupAnalysisResource.getMostPopularData();
        System.out.println(result);
        validateResult(result);

    }

    private void validateResult(final String result) throws Exception {
        final List<MostPopularTerminalsResult> results = getTranslator().translateResult(result,
                MostPopularTerminalsResult.class);
        assertThat(results.size(), is(3));
        final MostPopularTerminalsResult mostPopularTac = results.get(0);
        assertThat(mostPopularTac.getTac(), is(MOST_POPULAR_TAC));
        assertThat(mostPopularTac.getManufacturer(), is(MANUFACTURER_FOR_MOST_POPULAR_TAC));
        assertThat(mostPopularTac.getMarketingName(), is(MARKETING_NAME_FOR_MOST_POPULAR_TAC));
        assertThat(mostPopularTac.getNoEvents(), is(4));

        final MostPopularTerminalsResult secondMostPopularTacGroup = results.get(1);
        assertThat(secondMostPopularTacGroup.getTac(), is(SECOND_MOST_POPULAR_TAC));
        assertThat(secondMostPopularTacGroup.getManufacturer(), is(MANUFACTURER_FOR_SECOND_MOST_POPULAR_TAC));
        assertThat(secondMostPopularTacGroup.getMarketingName(), is(MARKETING_NAME_FOR_SECOND_MOST_POPULAR_TAC));
        assertThat(secondMostPopularTacGroup.getNoEvents(), is(3));

        final MostPopularTerminalsResult thirdMostPopularTacGroup = results.get(2);
        assertThat(thirdMostPopularTacGroup.getTac(), is(THIRD_MOST_POPULAR_TAC));
        assertThat(thirdMostPopularTacGroup.getManufacturer(), is(String.valueOf(THIRD_MOST_POPULAR_TAC)));
        assertThat(thirdMostPopularTacGroup.getMarketingName(), is(String.valueOf(THIRD_MOST_POPULAR_TAC)));
        assertThat(thirdMostPopularTacGroup.getNoEvents(), is(2));

    }
}
