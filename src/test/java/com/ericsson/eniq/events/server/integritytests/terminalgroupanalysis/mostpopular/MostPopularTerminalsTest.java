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
import com.ericsson.eniq.events.server.test.queryresults.MostPopularTerminalsResult;
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

public class MostPopularTerminalsTest extends TestsWithTemporaryTablesBaseTestCase<MostPopularTerminalsResult> {

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

    }

    @Test
    public void testGetTerminalMostPopularEvents_OneWeek() throws Exception {
        final TablesPopulatorForOneWeekQuery populator = new TablesPopulatorForOneWeekQuery(connection);
        populator.createTemporaryTables();
        populator.populateTemporaryTables();
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(TIME_QUERY_PARAM, ONE_WEEK);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        DummyUriInfoImpl.setUriInfo(map, terminalAndGroupAnalysisResource, SAMPLE_BASE_URI, TERMINAL_ANALYSIS + "/" + MOST_POPULAR);
        final String result = terminalAndGroupAnalysisResource.getMostPopularData();
        System.out.println(result);
        validateResult(result);

    }

    private void validateResult(final String result) throws Exception {
        final List<MostPopularTerminalsResult> results = getTranslator().translateResult(result, MostPopularTerminalsResult.class);
        assertThat(results.size(), is(3));
        final MostPopularTerminalsResult mostPopularTac = results.get(0);
        assertThat(mostPopularTac.getTac(), is(MOST_POPULAR_TAC));
        assertThat(mostPopularTac.getManufacturer(), is(MANUFACTURER_FOR_MOST_POPULAR_TAC));
        assertThat(mostPopularTac.getMarketingName(), is(MARKETING_NAME_FOR_MOST_POPULAR_TAC));
        assertThat(mostPopularTac.getNoEvents(), is(noErrorsForMostPopularTacInSgehDayTable + noErrorsForMostPopularTacInLTEDayTable
                + noSuccessesForMostPopularTacInLTEDayTable + noSuccessesForMostPopularTacInSgehDayTable));

        final MostPopularTerminalsResult secondMostPopularTacGroup = results.get(2);
        assertThat(secondMostPopularTacGroup.getTac(), is(SECOND_MOST_POPULAR_TAC));
        assertThat(secondMostPopularTacGroup.getManufacturer(), is(MANUFACTURER_FOR_SECOND_MOST_POPULAR_TAC));
        assertThat(secondMostPopularTacGroup.getMarketingName(), is(MARKETING_NAME_FOR_SECOND_MOST_POPULAR_TAC));
        assertThat(secondMostPopularTacGroup.getNoEvents(), is(noSuccessesForSecondMostPopularTacInLTEDayTable));

        final MostPopularTerminalsResult thirdMostPopularTacGroup = results.get(1);
        assertThat(thirdMostPopularTacGroup.getTac(), is(THIRD_MOST_POPULAR_TAC));
        assertThat(thirdMostPopularTacGroup.getManufacturer(), is(String.valueOf(THIRD_MOST_POPULAR_TAC)));
        assertThat(thirdMostPopularTacGroup.getMarketingName(), is(String.valueOf(THIRD_MOST_POPULAR_TAC)));
        assertThat(thirdMostPopularTacGroup.getNoEvents(), is(noErrorsForThirdMostPopularTacInLTEDayTable));

    }

}
