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
package com.ericsson.eniq.events.server.integritytests.ranking.apn;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.*;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.*;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.sql.SQLException;
import java.util.*;

import javax.ws.rs.core.MultivaluedMap;

import org.junit.Before;
import org.junit.Test;

import com.ericsson.eniq.events.server.resources.BaseDataIntegrityTest;
import com.ericsson.eniq.events.server.serviceprovider.impl.ranking.MultipleRankingService;
import com.ericsson.eniq.events.server.test.queryresults.MultipleAPNRankingResult;
import com.ericsson.eniq.events.server.test.schema.Nullable;
import com.ericsson.eniq.events.server.test.sql.SQLExecutor;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;
import com.sun.jersey.core.util.MultivaluedMapImpl;

public class MultipleRankingResourceWithPreparedRawTablesAPNTest extends BaseDataIntegrityTest<MultipleAPNRankingResult> {

    private final MultipleRankingService multipleRankingService = new MultipleRankingService();

    private static final int SOME_TAC = 123456;

    private final String worstAPN = "the worst apn";

    private final String secondWorstAPN = "the second worst apn";

    private final String thirdWorstAPN = "third worst apn";

    private final String SGEH_LATENCY = "0";

    @Before
    public void onSetUp() {
        attachDependencies(multipleRankingService);
    }

    @Test
    public void testGetRankingData_APN_30Minutes() throws Exception {
        final boolean isSuccessOnlyDataPopulated = false;

        final String dateTimeToUseForPopulatingTempTables = DateTimeUtilities.getDateTime(Calendar.MINUTE, -25);
        final String result = testGetRankingData_APN(THIRTY_MINUTES, dateTimeToUseForPopulatingTempTables, isSuccessOnlyDataPopulated);

        //EXPECTED RESULT( total count of each apn on err SGEH + LTE and suc SGEH +LTE ):
        //"the worst apn" 4 , 2
        //"the second worst apn" 2, 2
        //"third worst apn" 1, 1
        final List<MultipleAPNRankingResult> rankingResult = getTranslator().translateResult(result, MultipleAPNRankingResult.class);
        assertThat(rankingResult.size(), is(3));

        final MultipleAPNRankingResult worstApn = rankingResult.get(0);
        assertThat(worstApn.getManufacturer(), is(worstAPN));
        assertThat(worstApn.getNoErrors(), is(Integer.toString(4)));
        assertThat(worstApn.getNoSuccesses(), is(Integer.toString(2)));

        final MultipleAPNRankingResult nextWorstApn = rankingResult.get(1);
        assertThat(nextWorstApn.getManufacturer(), is(secondWorstAPN));
        assertThat(nextWorstApn.getNoErrors(), is(Integer.toString(2)));
        assertThat(nextWorstApn.getNoSuccesses(), is(Integer.toString(2)));

        final MultipleAPNRankingResult thirdWorstApn = rankingResult.get(2);
        assertThat(thirdWorstApn.getManufacturer(), is(thirdWorstAPN));
        assertThat(thirdWorstApn.getNoErrors(), is("1"));
        assertThat(thirdWorstApn.getNoSuccesses(), is(Integer.toString(1)));
    }

    @Test
    public void testGetAPNRankingWithDataTieringOn30Min() throws Exception {
        final boolean isSuccessOnlyDataPopulated = false;

        jndiProperties.setUpDataTieringJNDIProperty();

        final String dateTimeToUseForPopulatingTempTables = DateTimeUtilities.getDateTime(Calendar.MINUTE, -25);
        final String result = testGetRankingData_APN(THIRTY_MINUTES, dateTimeToUseForPopulatingTempTables, isSuccessOnlyDataPopulated);

        //EXPECTED RESULT( total count of each apn on err SGEH + LTE and suc SGEH +LTE ):
        //"the worst apn" 4 , 4
        //"the second worst apn" 2, 0
        //"third worst apn" 1, 0

        final List<MultipleAPNRankingResult> rankingResult = getTranslator().translateResult(result, MultipleAPNRankingResult.class);
        assertThat(rankingResult.size(), is(3));

        final MultipleAPNRankingResult worstApn = rankingResult.get(0);
        assertThat(worstApn.getManufacturer(), is(worstAPN));
        assertThat(worstApn.getNoErrors(), is(Integer.toString(4)));
        assertThat(worstApn.getNoSuccesses(), is(Integer.toString(4)));

        final MultipleAPNRankingResult nextWorstApn = rankingResult.get(1);
        assertThat(nextWorstApn.getManufacturer(), is(secondWorstAPN));
        assertThat(nextWorstApn.getNoErrors(), is(Integer.toString(2)));
        assertThat(nextWorstApn.getNoSuccesses(), is(Integer.toString(0)));

        final MultipleAPNRankingResult thirdWorstApn = rankingResult.get(2);
        assertThat(thirdWorstApn.getManufacturer(), is(thirdWorstAPN));
        assertThat(thirdWorstApn.getNoErrors(), is("1"));
        assertThat(thirdWorstApn.getNoSuccesses(), is(Integer.toString(0)));

        jndiProperties.setUpJNDIPropertiesForTest();
    }

    @Test
    public void testGetRankingData_APN_30MinutesWithSuccessOnlyData_ReturnsEmptyResult() throws Exception {
        final boolean isSuccessOnlyDataPopulated = true;

        final String dateTimeToUseForPopulatingTempTables = DateTimeUtilities.getDateTime(Calendar.MINUTE, -25);
        final String result = testGetRankingData_APN(THIRTY_MINUTES, dateTimeToUseForPopulatingTempTables, isSuccessOnlyDataPopulated);

        final List<MultipleAPNRankingResult> rankingResult = getTranslator().translateResult(result, MultipleAPNRankingResult.class);
        assertThat(rankingResult.size(), is(0));
    }

    @Test
    public void testGetAPNRankingWithDataTieringOn30MinWithSuccessOnlyData_ReturnsEmptyResult() throws Exception {
        final boolean isSuccessOnlyDataPopulated = true;

        jndiProperties.setUpDataTieringJNDIProperty();

        final String dateTimeToUseForPopulatingTempTables = DateTimeUtilities.getDateTime(Calendar.MINUTE, -25);
        final String result = testGetRankingData_APN(THIRTY_MINUTES, dateTimeToUseForPopulatingTempTables, isSuccessOnlyDataPopulated);

        final List<MultipleAPNRankingResult> rankingResult = getTranslator().translateResult(result, MultipleAPNRankingResult.class);
        assertThat(rankingResult.size(), is(0));

        jndiProperties.setUpJNDIPropertiesForTest();
    }

    private String testGetRankingData_APN(final String time, final String dateTimeToUseInTempTables, final boolean isSuccessOnlyDataPopulated)
            throws Exception {
        final Map<String, Nullable> columns = new HashMap<String, Nullable>();
        columns.put(TAC, Nullable.CAN_BE_NULL);
        columns.put(APN, Nullable.CAN_BE_NULL);
        columns.put(DATETIME_ID, Nullable.CANNOT_BE_NULL);
        createTemporaryTable(TEMP_EVENT_E_SGEH_ERR_RAW, columns);
        createTemporaryTable(TEMP_EVENT_E_SGEH_SUC_RAW, columns);
        createTemporaryTable(TEMP_EVENT_E_LTE_ERR_RAW, columns);
        createTemporaryTable(TEMP_EVENT_E_LTE_SUC_RAW, columns);

        columns.put(NO_OF_SUCCESSES, Nullable.CAN_BE_NULL);
        createTemporaryTable(TEMP_EVENT_E_LTE_APN_SUC_15MIN, columns);
        createTemporaryTable(TEMP_EVENT_E_SGEH_APN_SUC_15MIN, columns);

        populateRawEVENTTables(dateTimeToUseInTempTables, isSuccessOnlyDataPopulated);
        populateAggEVENTTables(dateTimeToUseInTempTables);
        createAndPopulateTemporaryDIMTable();

        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(TYPE_PARAM, TYPE_APN);
        map.putSingle(TIME_QUERY_PARAM, time);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, "10");
        final String json = getData(multipleRankingService, map);
        System.out.println(json);
        return json;
    }

    /**
     * This is how the data is setup: TEMP_EVENT_E_SGEH_ERR_RAW "the worst apn" "the worst apn" "the second worst apn"
     * 
     * TEMP_EVENT_E_SGEH_SUC_RAW "the worst apn" "the second worst apn"
     * 
     * TEMP_EVENT_E_LTE_ERR_RAW "the worst apn" "the worst apn" "the second worst apn" "third worst apn"
     * 
     * TEMP_EVENT_E_LTE_SUC_RAW "the worst apn" "the second worst apn" "third worst apn"
     * 
     */
    private void populateRawEVENTTables(final String dateTime, final boolean isSuccessOnlyDataPopulated) throws SQLException {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(APN, worstAPN);
        values.put(TAC, SOME_TAC);
        values.put(DATETIME_ID, dateTime);

        if (!isSuccessOnlyDataPopulated) {
            insertRow(TEMP_EVENT_E_SGEH_ERR_RAW, values);
            insertRow(TEMP_EVENT_E_SGEH_ERR_RAW, values);
        }
        insertRow(TEMP_EVENT_E_SGEH_SUC_RAW, values);

        if (!isSuccessOnlyDataPopulated) {
            insertRow(TEMP_EVENT_E_LTE_ERR_RAW, values);
            insertRow(TEMP_EVENT_E_LTE_ERR_RAW, values);
        }
        insertRow(TEMP_EVENT_E_LTE_SUC_RAW, values);

        values.put(APN, secondWorstAPN);
        if (!isSuccessOnlyDataPopulated) {
            insertRow(TEMP_EVENT_E_SGEH_ERR_RAW, values);
        }
        insertRow(TEMP_EVENT_E_SGEH_SUC_RAW, values);
        if (!isSuccessOnlyDataPopulated) {
            insertRow(TEMP_EVENT_E_LTE_ERR_RAW, values);
        }
        insertRow(TEMP_EVENT_E_LTE_SUC_RAW, values);

        values.put(APN, thirdWorstAPN);
        if (!isSuccessOnlyDataPopulated) {
            insertRow(TEMP_EVENT_E_LTE_ERR_RAW, values);
        }
        insertRow(TEMP_EVENT_E_LTE_SUC_RAW, values);
    }

    /*
     * This is how the data is setup: TEMP_EVENT_E_SGEH_APN_SUC_15MIN "the worst apn" "2"
     * 
     * TEMP_EVENT_E_LTE_APN_SUC_15MIN "the worst apn" "2"
     */
    private void populateAggEVENTTables(final String dateTime) throws Exception {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(APN, worstAPN);
        values.put(NO_OF_SUCCESSES, 2);
        values.put(DATETIME_ID, dateTime);
        insertRow(TEMP_EVENT_E_SGEH_APN_SUC_15MIN, values);
        insertRow(TEMP_EVENT_E_LTE_APN_SUC_15MIN, values);

    }

    private void createAndPopulateTemporaryDIMTable() throws SQLException {
        SQLExecutor sqlExecutor = null;
        try {
            sqlExecutor = new SQLExecutor(connection);
            sqlExecutor.executeUpdate("create local temporary table " + TEMP_DIM_E_SGEH_APN + "(APN varchar(127), LAST_SEEN timestamp)");
            sqlExecutor.executeUpdate("create local temporary table " + TEMP_DIM_E_LTE_APN + "(APN varchar(127), LAST_SEEN timestamp)");

            final String dateTime = DateTimeUtilities.getDateTimeMinus3Minutes();

            insertRawIntoDIMTable(TEMP_DIM_E_SGEH_APN, worstAPN, sqlExecutor, dateTime);
            insertRawIntoDIMTable(TEMP_DIM_E_SGEH_APN, secondWorstAPN, sqlExecutor, dateTime);
            insertRawIntoDIMTable(TEMP_DIM_E_SGEH_APN, thirdWorstAPN, sqlExecutor, dateTime);
            insertRawIntoDIMTable(TEMP_DIM_E_LTE_APN, worstAPN, sqlExecutor, dateTime);
            insertRawIntoDIMTable(TEMP_DIM_E_LTE_APN, secondWorstAPN, sqlExecutor, dateTime);
            insertRawIntoDIMTable(TEMP_DIM_E_LTE_APN, thirdWorstAPN, sqlExecutor, dateTime);

        } finally {
            if (sqlExecutor != null) {
                sqlExecutor.close();
            }
        }

    }

    private void insertRawIntoDIMTable(final String table, final String apn, final SQLExecutor sqlExecutor, final String dateTime)
            throws SQLException {
        sqlExecutor.executeUpdate("insert into " + table + " values('" + apn + "','" + dateTime + "')");
    }

}
