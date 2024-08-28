/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.integritytests.ranking.accessarea;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.*;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.util.List;

import javax.ws.rs.core.MultivaluedMap;

import org.junit.Before;
import org.junit.Test;

import com.ericsson.eniq.events.server.resources.BaseDataIntegrityTest;
import com.ericsson.eniq.events.server.serviceprovider.impl.ranking.MultipleRankingService;
import com.ericsson.eniq.events.server.test.queryresults.MultipleAccessAreaRankingResult;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;
import com.sun.jersey.core.util.MultivaluedMapImpl;

/**
 * @author edivkir
 * @since 2011
 * 
 */
public class MultipleRankingResourceWithPreparedTablesECellTest extends BaseDataIntegrityTest<MultipleAccessAreaRankingResult> {

    private final MultipleRankingService multipleRankingService = new MultipleRankingService();

    @Before
    public void onSetUp() {
        attachDependencies(multipleRankingService);
    }

    @Test
    public void testGetRankingData_ECELL_5Minutes() throws Exception {
        final boolean isSuccessOnlyDataPopulated = false;
        new RawTablesPopulatorForMultipleCellRanking().createAndPopulateTemporaryTables(connection, DateTimeUtilities.getDateTimeMinus2Minutes(),
                isSuccessOnlyDataPopulated);

        final List<MultipleAccessAreaRankingResult> rankingResult = getECellRanking(FIVE_MINUTES);
        assertThat(rankingResult.size(), is(2));

        final MultipleAccessAreaRankingResult worstRankingCell = rankingResult.get(0);
        assertThat(worstRankingCell.getRATDesc(), is(LTE));
        assertThat(worstRankingCell.getController(), is(ERBS1));
        assertThat(worstRankingCell.getCell(), is(LTECELL1));
        assertThat(worstRankingCell.getNoErrors(), is(3));
        assertThat(worstRankingCell.getNoSuccesses(), is("1"));

        final MultipleAccessAreaRankingResult secondWorstCell = rankingResult.get(1);
        assertThat(secondWorstCell.getRATDesc(), is(LTE));
        assertThat(secondWorstCell.getController(), is(ERBS2));
        assertThat(secondWorstCell.getCell(), is(LTECELL2));
        assertThat(secondWorstCell.getNoErrors(), is(1));
        assertThat(secondWorstCell.getNoSuccesses(), is("1"));
    }

    @Test
    public void testGetRankingDataWithDataTiering_ECELL_30Minutes() throws Exception {
        final boolean isSuccessOnlyDataPopulated = false;
        jndiProperties.setUpDataTieringJNDIProperty();

        new RawTablesPopulatorForMultipleCellRanking().createAndPopulateTemporaryTables(connection, DateTimeUtilities.getDateTimeMinus25Minutes(),
                isSuccessOnlyDataPopulated);
        new AggregationTablesPopulatorForMultipleCellRanking().createAndPopulateAggTables(connection, isSuccessOnlyDataPopulated);

        final List<MultipleAccessAreaRankingResult> rankingResult = getECellRanking(THIRTY_MINUTES);
        assertThat(rankingResult.size(), is(2));

        final MultipleAccessAreaRankingResult worstRankingCell = rankingResult.get(0);
        assertThat(worstRankingCell.getRATDesc(), is(LTE));
        assertThat(worstRankingCell.getController(), is(ERBS1));
        assertThat(worstRankingCell.getCell(), is(LTECELL1));
        assertThat(worstRankingCell.getNoErrors(), is(3));
        assertThat(worstRankingCell.getNoSuccesses(), is("17"));

        final MultipleAccessAreaRankingResult secondWorstCell = rankingResult.get(1);
        assertThat(secondWorstCell.getRATDesc(), is(LTE));
        assertThat(secondWorstCell.getController(), is(ERBS2));
        assertThat(secondWorstCell.getCell(), is(LTECELL2));
        assertThat(secondWorstCell.getNoErrors(), is(1));
        assertThat(secondWorstCell.getNoSuccesses(), is("0"));

        jndiProperties.setUpJNDIPropertiesForTest();
    }

    @Test
    public void testGetRankingData_ECELL_TwoWeeks() throws Exception {
        final boolean isSuccessOnlyDataPopulated = false;
        new AggregationTablesPopulatorForMultipleCellRanking().createAndPopulateAggTables(connection, isSuccessOnlyDataPopulated);

        final List<MultipleAccessAreaRankingResult> rankingResult = getECellRanking(TWO_WEEKS);
        assertThat(rankingResult.size(), is(2));

        final MultipleAccessAreaRankingResult worstRankingCell = rankingResult.get(0);
        assertThat(worstRankingCell.getRATDesc(), is(LTE));
        assertThat(worstRankingCell.getController(), is(ERBS1));
        assertThat(worstRankingCell.getCell(), is(LTECELL1));
        assertThat(worstRankingCell.getNoErrors(), is(AggregationTablesPopulatorForMultipleCellRanking.noErrorsForSecondWorstCell_LTECELL1));

        final MultipleAccessAreaRankingResult secondWorstCell = rankingResult.get(1);
        assertThat(secondWorstCell.getRATDesc(), is(LTE));
        assertThat(secondWorstCell.getController(), is(ERBS1));
        assertThat(secondWorstCell.getCell(), is(LTECELL2));
        assertThat(secondWorstCell.getNoErrors(), is(4));

    }

    @Test
    public void testGetRankingData_ECELL_5MinutesWithSuccessOnlyData_ReturnsEmptyResult() throws Exception {
        final boolean isSuccessOnlyDataPopulated = true;
        new RawTablesPopulatorForMultipleCellRanking().createAndPopulateTemporaryTables(connection, DateTimeUtilities.getDateTimeMinus2Minutes(),
                isSuccessOnlyDataPopulated);

        final List<MultipleAccessAreaRankingResult> rankingResult = getECellRanking(FIVE_MINUTES);
        assertThat(rankingResult.size(), is(0));
    }

    @Test
    public void testGetRankingDataWithDataTiering_ECELL_30MinutesWithSuccessOnlyData_ReturnsEmptyResult() throws Exception {
        final boolean isSuccessOnlyDataPopulated = true;
        jndiProperties.setUpDataTieringJNDIProperty();

        new RawTablesPopulatorForMultipleCellRanking().createAndPopulateTemporaryTables(connection, DateTimeUtilities.getDateTimeMinus25Minutes(),
                isSuccessOnlyDataPopulated);
        new AggregationTablesPopulatorForMultipleCellRanking().createAndPopulateAggTables(connection, isSuccessOnlyDataPopulated);

        final List<MultipleAccessAreaRankingResult> rankingResult = getECellRanking(THIRTY_MINUTES);
        assertThat(rankingResult.size(), is(0));

        jndiProperties.setUpJNDIPropertiesForTest();
    }

    @Test
    public void testGetRankingData_ECELL_TwoWeeksWithSuccessOnlyData_ReturnsEmptyResult() throws Exception {
        final boolean isSuccessOnlyDataPopulated = true;
        new AggregationTablesPopulatorForMultipleCellRanking().createAndPopulateAggTables(connection, isSuccessOnlyDataPopulated);

        final List<MultipleAccessAreaRankingResult> rankingResult = getECellRanking(TWO_WEEKS);
        assertThat(rankingResult.size(), is(0));
    }

    private List<MultipleAccessAreaRankingResult> getECellRanking(final String time) throws Exception {
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(DISPLAY_PARAM, "grid");
        map.putSingle(TYPE_PARAM, TYPE_ECELL);
        map.putSingle(TIME_QUERY_PARAM, time);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, "10");
        final String json = getData(multipleRankingService, map);
        System.out.println(json);
        final List<MultipleAccessAreaRankingResult> rankingResult = getTranslator().translateResult(json, MultipleAccessAreaRankingResult.class);
        return rankingResult;
    }
}
