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
package com.ericsson.eniq.events.server.integritytests.ranking.controller;

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
import com.ericsson.eniq.events.server.test.queryresults.MultipleControllerRankingResult;
import com.sun.jersey.core.util.MultivaluedMapImpl;

public class MultipleRankingResourceWithPreparedTablesRNCTest extends BaseDataIntegrityTest<MultipleControllerRankingResult> {

    private final MultipleRankingService multipleRankingResource = new MultipleRankingService();

    @Before
    public void onSetUp() {
        attachDependencies(multipleRankingResource);
    }

    @Test
    public void getRNCRankingWithDataTieringOn30Min() throws Exception {
        final boolean isSuccessOnlyDataPopulated = false;
        jndiProperties.setUpDataTieringJNDIProperty();
        new RawTablesPopulatorForMultipleControllerRanking().createAndPopulateRawTables(connection, isSuccessOnlyDataPopulated);
        new AggTablesPopulatorForMultipleControllerRanking().createAndPopulateAggTables(connection, isSuccessOnlyDataPopulated);
        final List<MultipleControllerRankingResult> rankingResult = getRNCRanking(THIRTY_MINUTES);

        final MultipleControllerRankingResult worstRankingRNC = rankingResult.get(0);
        assertThat(worstRankingRNC.getRATDesc(), is(_3G));
        assertThat(worstRankingRNC.getController(), is(RNC1));
        assertThat(worstRankingRNC.getNoErrors(), is(1));
        assertThat(worstRankingRNC.getNoSuccesses(), is(AggTablesPopulatorForMultipleControllerRanking.noSuccessesInSgehTable));
        jndiProperties.setUpJNDIPropertiesForTest();
    }

    @Test
    public void getRNCRankingWithDataTieringOn30MinWithSuccessOnlyData_ReturnsEmptyResult() throws Exception {
        final boolean isSuccessOnlyDataPopulated = true;
        jndiProperties.setUpDataTieringJNDIProperty();
        new RawTablesPopulatorForMultipleControllerRanking().createAndPopulateRawTables(connection, isSuccessOnlyDataPopulated);
        new AggTablesPopulatorForMultipleControllerRanking().createAndPopulateAggTables(connection, isSuccessOnlyDataPopulated);
        final List<MultipleControllerRankingResult> rankingResult = getRNCRanking(THIRTY_MINUTES);

        assertThat(rankingResult.size(), is(0));
        jndiProperties.setUpJNDIPropertiesForTest();
    }

    @Test
    public void testGetRankingData_RNC_TwoWeeks() throws Exception {
        final boolean isSuccessOnlyDataPopulated = false;

        new AggTablesPopulatorForMultipleControllerRanking().createAndPopulateAggTables(connection, isSuccessOnlyDataPopulated);

        final List<MultipleControllerRankingResult> rankingResult = getRNCRanking(TWO_WEEKS);
        assertThat(rankingResult.size(), is(2));

        final MultipleControllerRankingResult worstRankingBSC = rankingResult.get(0);
        assertThat(worstRankingBSC.getRATDesc(), is(_3G));
        assertThat(worstRankingBSC.getController(), is(RNC1));
        assertThat(worstRankingBSC.getNoErrors(), is(AggTablesPopulatorForMultipleControllerRanking.noErrorsForWorstRNC_RNC1));

        final MultipleControllerRankingResult secondWorstBSC = rankingResult.get(1);
        assertThat(secondWorstBSC.getRATDesc(), is(_3G));
        assertThat(secondWorstBSC.getController(), is(RNC2));
        assertThat(secondWorstBSC.getNoErrors(), is(AggTablesPopulatorForMultipleControllerRanking.noErrorsForSecondWorstRNC_RNC2));

    }

    @Test
    public void testGetRankingData_RNC_TwoWeeksWithSuccessOnlyData_ReturnsEmptyResult() throws Exception {
        final boolean isSuccessOnlyDataPopulated = true;

        new AggTablesPopulatorForMultipleControllerRanking().createAndPopulateAggTables(connection, isSuccessOnlyDataPopulated);

        final List<MultipleControllerRankingResult> rankingResult = getRNCRanking(TWO_WEEKS);
        assertThat(rankingResult.size(), is(0));
    }

    private List<MultipleControllerRankingResult> getRNCRanking(final String time) throws Exception {
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(TYPE_PARAM, TYPE_RNC);
        map.putSingle(TIME_QUERY_PARAM, time);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, "10");
        final String json = getData(multipleRankingResource, map);
        System.out.println(json);
        final List<MultipleControllerRankingResult> rankingResult = getTranslator().translateResult(json, MultipleControllerRankingResult.class);
        return rankingResult;
    }

}
