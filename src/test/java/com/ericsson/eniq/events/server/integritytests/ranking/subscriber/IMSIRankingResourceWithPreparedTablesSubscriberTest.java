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
package com.ericsson.eniq.events.server.integritytests.ranking.subscriber;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.*;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.*;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import javax.ws.rs.core.MultivaluedMap;

import org.jmock.Mockery;
import org.jmock.integration.junit4.JUnit4Mockery;
import org.jmock.lib.legacy.ClassImposteriser;
import org.junit.Before;
import org.junit.Test;

import com.ericsson.eniq.events.server.resources.BaseDataIntegrityTest;
import com.ericsson.eniq.events.server.serviceprovider.impl.ranking.IMSIRankingService;
import com.ericsson.eniq.events.server.test.queryresults.MultipleSubscriberRankingResult;
import com.ericsson.eniq.events.server.test.sql.SQLExecutor;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;
import com.sun.jersey.core.util.MultivaluedMapImpl;

public class IMSIRankingResourceWithPreparedTablesSubscriberTest extends BaseDataIntegrityTest<MultipleSubscriberRankingResult> {

    private final static List<String> tempIMSIRankingTables = new ArrayList<String>();

    private final int noErrorsForWorstImsiInLTENetwork = 11;

    private final String worstIMSI = "123456";

    private final String secondWorstIMSI = "12121212";

    private final int noErrorsForSecondWorstImsi = 7;

    private final int noErrorsForWorstImsiIn2G3GNetworks = 4;

    private final String thirdWorstIMSI = "654654";

    private final int noErrorsForThirdWorstImsi = 6;

    private final String fourthWorstIMSI = "99999";

    private final int noErrorsForFourthWorstImsiIn2G3GNetworks = 2;

    private final int noErrorsForFourthWorstImsiInLTENetworks = 1;

    private final String IMSIZero = "0";

    private final int noErrorsForImsiZeroIn2G3GNetworks = 20;

    private final int noErrorsForImsiZeroInLTENetworks = 12;

    private IMSIRankingService imsiRankingService;

    private final static List<String> tempSucRawTables = new ArrayList<String>();

    private final static List<String> imsiRawTables = new ArrayList<String>();

    private static final int SOME_TAC = 123456789;

    private static final String LTE_IMSI_RANK = "#EVENT_E_LTE_IMSI_RANK_DAY";

    private static final String SGEH_IMSI_RANK = "#EVENT_E_SGEH_IMSI_RANK_DAY";

    private final int noOfSuccessesForWorstImsiIn2G3GNetworks = 11;
    private final int noOfSuccessesForSecondWorstImsiIn2G3GNetworks = 22;
    private final int noOfSuccessesForThirdWorstImsiIn2G3GNetworks = 15;
    private final int noOfSuccessesForFourthWorstImsiIn2G3GNetworks = 10;
    private final int noOfSuccessesForImsiZeroIn2G3GNetworks = 26;

    private final int noOfSuccessesForWorstImsiInLTENetworks = 15;
    private final int noOfSuccessesForSecondWorstImsiInLTENetworks = 20;
    private final int noOfSuccessesForThirdWorstImsiInLTENetworks = 25;
    private final int noOfSuccessesForFourthWorstImsiInLTENetworks = 14;
    private final int noOfSuccessesForImsiZeroInLTENetworks = 22;

    static {
        tempIMSIRankingTables.add(TEMP_EVENT_E_SGEH_IMSI_RANK);
        tempIMSIRankingTables.add(TEMP_EVENT_E_LTE_IMSI_RANK);
        tempIMSIRankingTables.add(LTE_IMSI_RANK);
        tempIMSIRankingTables.add(SGEH_IMSI_RANK);

        tempSucRawTables.add(TEMP_EVENT_E_SGEH_SUC_RAW);
        tempSucRawTables.add(TEMP_EVENT_E_LTE_SUC_RAW);
        imsiRawTables.add(TEMP_EVENT_E_SGEH_IMSI_SUC_RAW);
        imsiRawTables.add(TEMP_EVENT_E_LTE_IMSI_SUC_RAW);
    }

    protected Mockery mockery = new JUnit4Mockery();

    {
        mockery.setImposteriser(ClassImposteriser.INSTANCE);
    }

    @Before
    public void onSetUp() {
        imsiRankingService = new IMSIRankingService();
        attachDependencies(imsiRankingService);
    }

    @Test
    public void testGetRankingData_Subscriber_TwoWeeks_NoSucRawJNDIPropertySet() throws Exception {

        createTemporaryIMSIRankingTables(tempIMSIRankingTables);
        createTemporaryRawSucTables(tempSucRawTables);
        createTemporaryIMSIRawSucTables(imsiRawTables);
        populateTemporaryTables();

        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(TIME_QUERY_PARAM, TWO_WEEKS);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, TEN_ROWS);
        final String json = getData(imsiRankingService, map);
        System.out.println(json);
        validateIMSIRankingResultForSucRaw(json);
    }

    @Test
    public void testGetRankingData_Subscriber_TwoWeeks_SucRawEnabled() throws Exception {
        jndiProperties.useSucRawJNDIProperty();

        createTemporaryIMSIRankingTables(tempIMSIRankingTables);
        createTemporaryRawSucTables(tempSucRawTables);
        createTemporaryIMSIRawSucTables(imsiRawTables);
        populateTemporaryTables();

        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(TIME_QUERY_PARAM, TWO_WEEKS);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, TEN_ROWS);
        final String json = getData(imsiRankingService, map);
        System.out.println(json);
        validateIMSIRankingResultForSucRaw(json);
        jndiProperties.setUpJNDIPropertiesForTest();
    }

    @Test
    public void testGetRankingData_Subscriber_TwoWeeks_SucRawDisabled() throws Exception {
        jndiProperties.disableSucRawJNDIProperty();

        createTemporaryIMSIRankingTables(tempIMSIRankingTables);
        createTemporaryRawSucTables(tempSucRawTables);
        createTemporaryIMSIRawSucTables(imsiRawTables);
        populateTemporaryTables();

        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(TIME_QUERY_PARAM, TWO_WEEKS);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, TEN_ROWS);
        final String json = getData(imsiRankingService, map);
        System.out.println(json);
        validateIMSIRankingResultForIMSISucRaw(json);
        jndiProperties.setUpJNDIPropertiesForTest();
    }

    private void createTemporaryIMSIRankingTables(List<String> tables) throws SQLException {
        for (final String table : tables) {
            createTemporaryIMSIRankingTable(table);
        }
    }

    private void createTemporaryIMSIRawSucTables(List<String> tables) throws SQLException {
        for (final String table : tables) {
            createTemporaryIMSISucRawTable(table);
        }
    }

    private void createTemporaryRawSucTables(List<String> tables) throws SQLException {
        for (final String table : tables) {
            createTemporaryRawSucTable(table);
        }
    }

    private void createTemporaryIMSIRankingTable(final String tempTableName) throws SQLException {
        SQLExecutor sqlExecutor = null;
        try {
            sqlExecutor = new SQLExecutor(connection);
            sqlExecutor.executeUpdate("create local temporary table " + tempTableName
                    + "(IMSI unsigned bigint, NO_OF_ERRORS int, DATETIME_ID timestamp)");

        } finally {
            closeSQLExector(sqlExecutor);
        }
    }

    private void createTemporaryIMSISucRawTable(final String tempTableName) throws SQLException {
        SQLExecutor sqlExecutor = null;
        try {
            sqlExecutor = new SQLExecutor(connection);
            sqlExecutor.executeUpdate("create local temporary table " + tempTableName
                    + "(IMSI unsigned bigint,TAC unsigned int, NO_OF_SUCCESSES int, DATETIME_ID timestamp)");

        } finally {
            closeSQLExector(sqlExecutor);
        }
    }

    private void createTemporaryRawSucTable(final String tempRawSucTable) throws SQLException {
        SQLExecutor sqlExecutor = null;
        try {
            sqlExecutor = new SQLExecutor(connection);
            sqlExecutor.executeUpdate("create local temporary table " + tempRawSucTable
                    + "(IMSI unsigned bigint, TAC int, DATETIME_ID timestamp, LOCAL_DATE_ID date)");

        } finally {
            closeSQLExector(sqlExecutor);
        }
    }

    private void populateTemporaryTables() throws SQLException {
        SQLExecutor sqlExecutor = null;
        try {
            sqlExecutor = new SQLExecutor(connection);
            final String dateTime = DateTimeUtilities.getDateTimeMinus48Hours();

            populateIMSIRankTables(sqlExecutor, dateTime);
            populateIMSISucRawTables(sqlExecutor, dateTime);
            populateSucRawTables(sqlExecutor, dateTime);

        } finally {
            closeSQLExector(sqlExecutor);
        }

    }

    private void populateIMSIRankTables(final SQLExecutor sqlExecutor, final String dateTime) throws SQLException {
        insertIntoAgg(SGEH_IMSI_RANK, worstIMSI, noErrorsForWorstImsiIn2G3GNetworks, sqlExecutor, dateTime);
        insertIntoAgg(SGEH_IMSI_RANK, secondWorstIMSI, noErrorsForSecondWorstImsi, sqlExecutor, dateTime);
        insertIntoAgg(SGEH_IMSI_RANK, thirdWorstIMSI, noErrorsForThirdWorstImsi, sqlExecutor, dateTime);
        insertIntoAgg(SGEH_IMSI_RANK, fourthWorstIMSI, noErrorsForFourthWorstImsiIn2G3GNetworks, sqlExecutor, dateTime);
        insertIntoAgg(SGEH_IMSI_RANK, IMSIZero, noErrorsForImsiZeroIn2G3GNetworks, sqlExecutor, dateTime);

        insertIntoAgg(LTE_IMSI_RANK, worstIMSI, noErrorsForWorstImsiInLTENetwork, sqlExecutor, dateTime);
        insertIntoAgg(LTE_IMSI_RANK, fourthWorstIMSI, noErrorsForFourthWorstImsiInLTENetworks, sqlExecutor, dateTime);
        insertIntoAgg(LTE_IMSI_RANK, IMSIZero, noErrorsForImsiZeroInLTENetworks, sqlExecutor, dateTime);
    }

    private void populateIMSISucRawTables(final SQLExecutor sqlExecutor, final String dateTime) throws SQLException {
        insertIntoImsiSucRawAgg(TEMP_EVENT_E_SGEH_IMSI_SUC_RAW, worstIMSI, SOME_TAC, noOfSuccessesForWorstImsiIn2G3GNetworks, sqlExecutor, dateTime);
        insertIntoImsiSucRawAgg(TEMP_EVENT_E_SGEH_IMSI_SUC_RAW, secondWorstIMSI, SOME_TAC, noOfSuccessesForSecondWorstImsiIn2G3GNetworks,
                sqlExecutor, dateTime);
        insertIntoImsiSucRawAgg(TEMP_EVENT_E_SGEH_IMSI_SUC_RAW, thirdWorstIMSI, SOME_TAC, noOfSuccessesForThirdWorstImsiIn2G3GNetworks, sqlExecutor,
                dateTime);
        insertIntoImsiSucRawAgg(TEMP_EVENT_E_SGEH_IMSI_SUC_RAW, fourthWorstIMSI, SOME_TAC, noOfSuccessesForFourthWorstImsiIn2G3GNetworks,
                sqlExecutor, dateTime);
        insertIntoImsiSucRawAgg(TEMP_EVENT_E_SGEH_IMSI_SUC_RAW, IMSIZero, SOME_TAC, noOfSuccessesForImsiZeroIn2G3GNetworks, sqlExecutor, dateTime);

        insertIntoImsiSucRawAgg(TEMP_EVENT_E_LTE_IMSI_SUC_RAW, worstIMSI, SOME_TAC, noOfSuccessesForWorstImsiInLTENetworks, sqlExecutor, dateTime);
        insertIntoImsiSucRawAgg(TEMP_EVENT_E_LTE_IMSI_SUC_RAW, secondWorstIMSI, SOME_TAC, noOfSuccessesForSecondWorstImsiInLTENetworks, sqlExecutor,
                dateTime);
        insertIntoImsiSucRawAgg(TEMP_EVENT_E_LTE_IMSI_SUC_RAW, thirdWorstIMSI, SOME_TAC, noOfSuccessesForThirdWorstImsiInLTENetworks, sqlExecutor,
                dateTime);
        insertIntoImsiSucRawAgg(TEMP_EVENT_E_LTE_IMSI_SUC_RAW, fourthWorstIMSI, SOME_TAC, noOfSuccessesForFourthWorstImsiInLTENetworks, sqlExecutor,
                dateTime);
        insertIntoImsiSucRawAgg(TEMP_EVENT_E_LTE_IMSI_SUC_RAW, IMSIZero, SOME_TAC, noOfSuccessesForImsiZeroInLTENetworks, sqlExecutor, dateTime);
    }

    private void populateSucRawTables(final SQLExecutor sqlExecutor, final String dateTime) throws SQLException {
        insertIntoRaw(TEMP_EVENT_E_SGEH_SUC_RAW, worstIMSI, sqlExecutor, dateTime);
        insertIntoRaw(TEMP_EVENT_E_SGEH_SUC_RAW, worstIMSI, sqlExecutor, dateTime);
        insertIntoRaw(TEMP_EVENT_E_SGEH_SUC_RAW, secondWorstIMSI, sqlExecutor, dateTime);
        insertIntoRaw(TEMP_EVENT_E_SGEH_SUC_RAW, secondWorstIMSI, sqlExecutor, dateTime);
        insertIntoRaw(TEMP_EVENT_E_SGEH_SUC_RAW, secondWorstIMSI, sqlExecutor, dateTime);
        insertIntoRaw(TEMP_EVENT_E_SGEH_SUC_RAW, thirdWorstIMSI, sqlExecutor, dateTime);
        insertIntoRaw(TEMP_EVENT_E_SGEH_SUC_RAW, fourthWorstIMSI, sqlExecutor, dateTime);
        insertIntoRaw(TEMP_EVENT_E_SGEH_SUC_RAW, fourthWorstIMSI, sqlExecutor, dateTime);
        insertIntoRaw(TEMP_EVENT_E_SGEH_SUC_RAW, fourthWorstIMSI, sqlExecutor, dateTime);
        insertIntoRaw(TEMP_EVENT_E_SGEH_SUC_RAW, IMSIZero, sqlExecutor, dateTime);

        insertIntoRaw(TEMP_EVENT_E_LTE_SUC_RAW, worstIMSI, sqlExecutor, dateTime);
        insertIntoRaw(TEMP_EVENT_E_LTE_SUC_RAW, secondWorstIMSI, sqlExecutor, dateTime);
        insertIntoRaw(TEMP_EVENT_E_LTE_SUC_RAW, secondWorstIMSI, sqlExecutor, dateTime);
        insertIntoRaw(TEMP_EVENT_E_LTE_SUC_RAW, secondWorstIMSI, sqlExecutor, dateTime);
        insertIntoRaw(TEMP_EVENT_E_LTE_SUC_RAW, thirdWorstIMSI, sqlExecutor, dateTime);
        insertIntoRaw(TEMP_EVENT_E_LTE_SUC_RAW, thirdWorstIMSI, sqlExecutor, dateTime);
        insertIntoRaw(TEMP_EVENT_E_LTE_SUC_RAW, thirdWorstIMSI, sqlExecutor, dateTime);
        insertIntoRaw(TEMP_EVENT_E_LTE_SUC_RAW, thirdWorstIMSI, sqlExecutor, dateTime);
        insertIntoRaw(TEMP_EVENT_E_LTE_SUC_RAW, fourthWorstIMSI, sqlExecutor, dateTime);
        insertIntoRaw(TEMP_EVENT_E_LTE_SUC_RAW, IMSIZero, sqlExecutor, dateTime);
    }

    private void insertIntoRaw(final String table, final String imsi, final SQLExecutor sqlExecutor, final String dateTime) throws SQLException {
        sqlExecutor.executeUpdate("insert into " + table + " values(" + imsi + "," + SOME_TAC + ",'" + dateTime + "','" + dateTime.substring(0, 10)
                + "')");
    }

    private void insertIntoAgg(final String table, final String imsi, final int count, final SQLExecutor sqlExecutor, final String dateTime)
            throws SQLException {
        sqlExecutor.executeUpdate("insert into " + table + " values(" + imsi + "," + count + ",'" + dateTime + "')");
    }

    private void insertIntoImsiSucRawAgg(String table, String imsi, int tac, int count, SQLExecutor sqlExecutor, String dateTime) throws SQLException {
        sqlExecutor.executeUpdate("insert into " + table + " values(" + imsi + "," + tac + "," + count + ",'" + dateTime + "')");
    }

    private void validateIMSIRankingResultForSucRaw(final String json) throws Exception {
        final List<MultipleSubscriberRankingResult> rankingResults = getTranslator().translateResult(json, MultipleSubscriberRankingResult.class);

        assertThat(rankingResults.size(), is(4));

        final MultipleSubscriberRankingResult worstIMSIInRanking = rankingResults.get(0);
        assertThat(worstIMSIInRanking.getIMSI(), is(worstIMSI));
        assertThat(worstIMSIInRanking.getNoErrors(), is(noErrorsForWorstImsiIn2G3GNetworks + noErrorsForWorstImsiInLTENetwork));
        assertThat(worstIMSIInRanking.getNoSuccesses(), is("3"));

        final MultipleSubscriberRankingResult secondWorstIMSIInRanking = rankingResults.get(1);
        assertThat(secondWorstIMSIInRanking.getIMSI(), is(secondWorstIMSI));
        assertThat(secondWorstIMSIInRanking.getNoErrors(), is(noErrorsForSecondWorstImsi));
        assertThat(secondWorstIMSIInRanking.getNoSuccesses(), is("6"));

        final MultipleSubscriberRankingResult thirdWorstIMSIInRanking = rankingResults.get(2);
        assertThat(thirdWorstIMSIInRanking.getIMSI(), is(thirdWorstIMSI));
        assertThat(thirdWorstIMSIInRanking.getNoErrors(), is(noErrorsForThirdWorstImsi));
        assertThat(thirdWorstIMSIInRanking.getNoSuccesses(), is("5"));

        final MultipleSubscriberRankingResult fourthWorstIMSIInRanking = rankingResults.get(3);
        assertThat(fourthWorstIMSIInRanking.getIMSI(), is(fourthWorstIMSI));
        assertThat(fourthWorstIMSIInRanking.getNoErrors(), is(noErrorsForFourthWorstImsiIn2G3GNetworks + noErrorsForFourthWorstImsiInLTENetworks));
        assertThat(fourthWorstIMSIInRanking.getNoSuccesses(), is("4"));

    }

    private void validateIMSIRankingResultForIMSISucRaw(final String json) throws Exception {
        final List<MultipleSubscriberRankingResult> rankingResults = getTranslator().translateResult(json, MultipleSubscriberRankingResult.class);

        assertThat(rankingResults.size(), is(4));

        final MultipleSubscriberRankingResult worstIMSIInRanking = rankingResults.get(0);
        assertThat(worstIMSIInRanking.getIMSI(), is(worstIMSI));
        assertThat(worstIMSIInRanking.getNoErrors(), is(noErrorsForWorstImsiIn2G3GNetworks + noErrorsForWorstImsiInLTENetwork));
        assertThat(worstIMSIInRanking.getNoSuccesses(),
                is(Integer.toString(noOfSuccessesForWorstImsiIn2G3GNetworks + noOfSuccessesForWorstImsiInLTENetworks)));

        final MultipleSubscriberRankingResult secondWorstIMSIInRanking = rankingResults.get(1);
        assertThat(secondWorstIMSIInRanking.getIMSI(), is(secondWorstIMSI));
        assertThat(secondWorstIMSIInRanking.getNoErrors(), is(noErrorsForSecondWorstImsi));
        assertThat(secondWorstIMSIInRanking.getNoSuccesses(),
                is(Integer.toString(noOfSuccessesForSecondWorstImsiIn2G3GNetworks + noOfSuccessesForSecondWorstImsiInLTENetworks)));

        final MultipleSubscriberRankingResult thirdWorstIMSIInRanking = rankingResults.get(2);
        assertThat(thirdWorstIMSIInRanking.getIMSI(), is(thirdWorstIMSI));
        assertThat(thirdWorstIMSIInRanking.getNoErrors(), is(noErrorsForThirdWorstImsi));
        assertThat(thirdWorstIMSIInRanking.getNoSuccesses(),
                is(Integer.toString(noOfSuccessesForThirdWorstImsiIn2G3GNetworks + noOfSuccessesForThirdWorstImsiInLTENetworks)));

        final MultipleSubscriberRankingResult fourthWorstIMSIInRanking = rankingResults.get(3);
        assertThat(fourthWorstIMSIInRanking.getIMSI(), is(fourthWorstIMSI));
        assertThat(fourthWorstIMSIInRanking.getNoErrors(), is(noErrorsForFourthWorstImsiIn2G3GNetworks + noErrorsForFourthWorstImsiInLTENetworks));
        assertThat(fourthWorstIMSIInRanking.getNoSuccesses(),
                is(Integer.toString(noOfSuccessesForFourthWorstImsiIn2G3GNetworks + noOfSuccessesForFourthWorstImsiInLTENetworks)));

    }

}