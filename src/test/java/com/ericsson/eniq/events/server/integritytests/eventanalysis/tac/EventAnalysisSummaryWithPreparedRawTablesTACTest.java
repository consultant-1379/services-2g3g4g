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
package com.ericsson.eniq.events.server.integritytests.eventanalysis.tac;

import com.ericsson.eniq.events.server.resources.BaseDataIntegrityTest;
import com.ericsson.eniq.events.server.serviceprovider.impl.eventanalysis.EventAnalysisService;
import com.ericsson.eniq.events.server.test.queryresults.TACEventAnalysisSummaryResult;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;
import com.sun.jersey.core.util.MultivaluedMapImpl;
import org.junit.Before;
import org.junit.Test;

import javax.ws.rs.core.MultivaluedMap;
import java.sql.SQLException;
import java.util.*;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.common.EventIDConstants.*;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.*;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.*;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class EventAnalysisSummaryWithPreparedRawTablesTACTest extends BaseDataIntegrityTest<TACEventAnalysisSummaryResult> {

    private final EventAnalysisService eventAnalysisService = new EventAnalysisService();

    private static Collection<String> columnsForRawTable = new ArrayList<String>();

    private final static List<String> rawTables = new ArrayList<String>();

    private static Collection<String> columnsForManufTacEventIdSuc15MinTable = new ArrayList<String>();

    private static final int UNKNOWN_TAC = 65535;
    private final int NO_OF_SUCCESS_COUNT = 999780; 
    private final int SUCCESS_RATIO_RESULT_COUNT = 1;
    private final double SUCCESS_RATIO_VALUE = 99.99;

    static {

        rawTables.add(TEMP_EVENT_E_SGEH_ERR_RAW);
        rawTables.add(TEMP_EVENT_E_SGEH_SUC_RAW);
        rawTables.add(TEMP_EVENT_E_LTE_ERR_RAW);
        rawTables.add(TEMP_EVENT_E_LTE_SUC_RAW);

        columnsForRawTable.add(EVENT_ID);
        columnsForRawTable.add(TAC);
        columnsForRawTable.add(IMSI);
        columnsForRawTable.add(DEACTIVATION_TRIGGER);
        columnsForRawTable.add(DATETIME_ID);

        columnsForManufTacEventIdSuc15MinTable.add(EVENT_ID);
        columnsForManufTacEventIdSuc15MinTable.add(TAC);
        columnsForManufTacEventIdSuc15MinTable.add(DATETIME_ID);
        columnsForManufTacEventIdSuc15MinTable.add(NO_OF_SUCCESSES);

    }

    @Before
    public void onSetUp() throws Exception {

        attachDependencies(eventAnalysisService);

        for (final String rawTable : rawTables) {
            createTemporaryTable(rawTable, columnsForRawTable);
        }

        createTemporaryTable(TEMP_EVENT_E_LTE_MANUF_TAC_EVENTID_SUC_15MIN, columnsForManufTacEventIdSuc15MinTable);
        createTemporaryTable(TEMP_EVENT_E_SGEH_MANUF_TAC_EVENTID_SUC_15MIN, columnsForManufTacEventIdSuc15MinTable);

        jndiProperties.setUpJNDIPropertiesForTest();
    }

    @Test
    public void testGetSummaryDataWhereJustSuccessEventsExist_TAC_30Minutes() throws Exception {
        populateTemporaryTablesWithOnlySuccessEvents();

        final String json = getData(SONY_ERICSSON_TAC);
        validateSuccessEventsAreReturned(json);
    }

    private void validateSuccessEventsAreReturned(final String json) throws Exception {
        final List<TACEventAnalysisSummaryResult> summaryResult = getTranslator().translateResult(json, TACEventAnalysisSummaryResult.class);
        assertThat(summaryResult.size(), is(1));

        final TACEventAnalysisSummaryResult firstResult = summaryResult.get(0);
        assertThat(firstResult.getTAC(), is(SONY_ERICSSON_TAC));
        assertThat(firstResult.getEventId(), is((ATTACH_IN_4G)));
        assertThat(firstResult.getEventIdDesc(), is(L_ATTACH));
        assertThat(firstResult.getErrorCount(), is((0)));
        assertThat(firstResult.getSuccessCount(), is((2)));
        assertThat(firstResult.getOccurrences(), is((2)));
        assertThat(firstResult.getSuccessRatio(), is(firstResult.getExpectedSuccessRatio()));
        assertThat(firstResult.getErrorSubscriberCount(), is((0)));

    }

    private String getData(final int tac) throws Exception {
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(TIME_QUERY_PARAM, THIRTY_MINUTES);
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(TAC_PARAM, String.valueOf(tac));
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, "500");

        final String json = getData(eventAnalysisService, map);
        System.out.println(json);
        return json;
    }

    private void populateTemporaryTablesWithOnlySuccessEvents() throws SQLException {
        final String dateTime = DateTimeUtilities.getDateTime(Calendar.MINUTE, -30);
        insertRowIntoRawTable(TEMP_EVENT_E_LTE_SUC_RAW, ATTACH_IN_4G, SONY_ERICSSON_TAC, 12345677, dateTime);
        insertRowIntoRawTable(TEMP_EVENT_E_LTE_SUC_RAW, ATTACH_IN_4G, SONY_ERICSSON_TAC, 12345678, dateTime);
    }

    @Test
    public void testGetSummaryDataForFullDataSet_TAC_30Minutes() throws Exception {
        jndiProperties.setUpDataTieringJNDIProperty();
        populateTemporaryTablesWithFullDataSet(SONY_ERICSSON_TAC);
        final String json = getData(SONY_ERICSSON_TAC);
        validateResultForFullDataSet(json);
    }

    @Test
    public void testGetSummaryDataForFullDataSet_TAC_30Minutes_NoModel() throws Exception {
        jndiProperties.setUpDataTieringJNDIProperty();
        populateTemporaryTablesWithFullDataSet(UNKNOWN_TAC);
        final String json = getData(UNKNOWN_TAC);
        validateResultForFullDataSet_NoModel(json);
    }

    @Test
    public void testGetSummaryDataForFullDataSet_ExcTAC_30Minutes() throws Exception {
        jndiProperties.setUpDataTieringJNDIProperty();
        populateTemporaryTablesWithFullDataSet(SONY_ERICSSON_TAC);
        populateGroupTableWithExTACGroup(SONY_ERICSSON_TAC);
        final String json = getData(SONY_ERICSSON_TAC);
        validateResultForFullDataSet(json);
    }

    @Test
    public void testGetSummaryDataForFullDataSet_ExcTAC_30Minutes_NoModel() throws Exception {
        jndiProperties.setUpDataTieringJNDIProperty();
        populateTemporaryTablesWithFullDataSet(UNKNOWN_TAC);
        populateGroupTableWithExTACGroup(UNKNOWN_TAC);
        final String json = getData(UNKNOWN_TAC);
        validateResultForFullDataSet_NoModel(json);
    }

    @Test
    public void testGetSummaryDataForSuccessRatio_TAC_30Minutes() throws Exception {
        jndiProperties.setUpDataTieringJNDIProperty();
        populateTemporaryTablesForRowandAgg();
        final String json = getData(SONY_ERICSSON_TAC);
        final List<TACEventAnalysisSummaryResult> summaryResult = getTranslator().translateResult(json, TACEventAnalysisSummaryResult.class);
        assertThat(summaryResult.size(), is(SUCCESS_RATIO_RESULT_COUNT));
        final TACEventAnalysisSummaryResult result = summaryResult.get(0);
        assertThat(result.getSuccessRatio(), is(SUCCESS_RATIO_VALUE));
    }

    private void validateResultForFullDataSet(final String json) throws Exception {
        final List<TACEventAnalysisSummaryResult> summaryResult = getTranslator().translateResult(json, TACEventAnalysisSummaryResult.class);
        assertThat(summaryResult.size(), is(4));

        //sort the result set by eventID (see implementation of compareTo method in BaseEventAnalysisSummaryResultSortableByEventID
        Collections.sort(summaryResult);

        final TACEventAnalysisSummaryResult firstResult = summaryResult.get(0);
        assertThat(firstResult.getTAC(), is(SONY_ERICSSON_TAC));
        assertThat(firstResult.getEventId(), is((ACTIVATE_IN_2G_AND_3G)));
        assertThat(firstResult.getEventIdDesc(), is(ACTIVATE));
        assertThat(firstResult.getErrorCount(), is((1)));
        assertThat(firstResult.getSuccessCount(), is((1)));
        assertThat(firstResult.getOccurrences(), is((2)));
        assertThat(firstResult.getSuccessRatio(), is(firstResult.getExpectedSuccessRatio()));
        assertThat(firstResult.getErrorSubscriberCount(), is((1)));

        final TACEventAnalysisSummaryResult secondResult = summaryResult.get(1);
        assertThat(secondResult.getTAC(), is(SONY_ERICSSON_TAC));
        assertThat(secondResult.getEventId(), is((ATTACH_IN_4G)));
        assertThat(secondResult.getEventIdDesc(), is(L_ATTACH));
        assertThat(secondResult.getErrorCount(), is((1)));
        assertThat(secondResult.getSuccessCount(), is((2)));
        assertThat(secondResult.getOccurrences(), is((3)));
        assertThat(secondResult.getSuccessRatio(), is(secondResult.getExpectedSuccessRatio()));
        assertThat(secondResult.getErrorSubscriberCount(), is((1)));

        final TACEventAnalysisSummaryResult thirdResult = summaryResult.get(2);
        assertThat(thirdResult.getTAC(), is(SONY_ERICSSON_TAC));
        assertThat(thirdResult.getEventId(), is((SERVICE_REQUEST_IN_4G)));
        assertThat(thirdResult.getEventIdDesc(), is(L_SERVICE_REQUEST));
        assertThat(thirdResult.getErrorCount(), is((1)));
        assertThat(thirdResult.getSuccessCount(), is((0)));
        assertThat(thirdResult.getOccurrences(), is((1)));
        assertThat(thirdResult.getSuccessRatio(), is(thirdResult.getExpectedSuccessRatio()));
        assertThat(thirdResult.getErrorSubscriberCount(), is((1)));

        final TACEventAnalysisSummaryResult fourthResult = summaryResult.get(3);
        assertThat(fourthResult.getTAC(), is(SONY_ERICSSON_TAC));
        assertThat(fourthResult.getEventId(), is((SERVICE_REQUEST_IN_2G_AND_3G)));
        assertThat(fourthResult.getEventIdDesc(), is(SERVICE_REQUEST));
        assertThat(fourthResult.getErrorCount(), is((1)));
        assertThat(fourthResult.getSuccessCount(), is((1)));
        assertThat(fourthResult.getOccurrences(), is((2)));
        assertThat(fourthResult.getSuccessRatio(), is(fourthResult.getExpectedSuccessRatio()));
        assertThat(fourthResult.getErrorSubscriberCount(), is((1)));

    }

    private void validateResultForFullDataSet_NoModel(final String json) throws Exception {
        final List<TACEventAnalysisSummaryResult> summaryResult = getTranslator().translateResult(json, TACEventAnalysisSummaryResult.class);
        assertThat(summaryResult.size(), is(4));

        //sort the result set by eventID (see implementation of compareTo method in BaseEventAnalysisSummaryResultSortableByEventID
        Collections.sort(summaryResult);

        final TACEventAnalysisSummaryResult firstResult = summaryResult.get(0);
        assertThat(firstResult.getTAC(), is(UNKNOWN_TAC));
        assertThat(firstResult.getManufacturer(), is(String.valueOf(UNKNOWN_TAC)));
        assertThat(firstResult.getModel(), is(String.valueOf(UNKNOWN_TAC)));
        assertThat(firstResult.getEventId(), is((ACTIVATE_IN_2G_AND_3G)));
        assertThat(firstResult.getEventIdDesc(), is(ACTIVATE));
        assertThat(firstResult.getErrorCount(), is((1)));
        assertThat(firstResult.getSuccessCount(), is((1)));
        assertThat(firstResult.getOccurrences(), is((2)));
        assertThat(firstResult.getSuccessRatio(), is(firstResult.getExpectedSuccessRatio()));
        assertThat(firstResult.getErrorSubscriberCount(), is((1)));

    }

    private void populateTemporaryTablesWithFullDataSet(final int tac) throws SQLException {
        final String dateTime = DateTimeUtilities.getDateTime(Calendar.MINUTE, -30);

        insertRowIntoRawTable(TEMP_EVENT_E_SGEH_ERR_RAW, ACTIVATE_IN_2G_AND_3G, tac, 12345671, dateTime);
        insertRowIntoRawTable(TEMP_EVENT_E_SGEH_ERR_RAW, SERVICE_REQUEST_IN_2G_AND_3G, tac, 12345672, dateTime);

        insertRowIntoAggTable(TEMP_EVENT_E_SGEH_MANUF_TAC_EVENTID_SUC_15MIN, ACTIVATE_IN_2G_AND_3G, tac, dateTime, 1);
        insertRowIntoAggTable(TEMP_EVENT_E_SGEH_MANUF_TAC_EVENTID_SUC_15MIN, SERVICE_REQUEST_IN_2G_AND_3G, tac, dateTime, 1);

        insertRowIntoRawTable(TEMP_EVENT_E_LTE_ERR_RAW, ATTACH_IN_4G, tac, 12345675, dateTime);
        insertRowIntoRawTable(TEMP_EVENT_E_LTE_ERR_RAW, SERVICE_REQUEST_IN_4G, tac, 12345676, dateTime);

        insertRowIntoAggTable(TEMP_EVENT_E_LTE_MANUF_TAC_EVENTID_SUC_15MIN, ATTACH_IN_4G, tac, dateTime, 2);

    }

    private void populateTemporaryTablesForRowandAgg() throws SQLException {
        final String dateTime = DateTimeUtilities.getDateTime(Calendar.MINUTE, -30);
        insertRowIntoRawTable(TEMP_EVENT_E_LTE_ERR_RAW, ATTACH_IN_4G, SONY_ERICSSON_TAC, 12345675, dateTime);
        insertRowIntoAggTable(TEMP_EVENT_E_LTE_MANUF_TAC_EVENTID_SUC_15MIN, ATTACH_IN_4G, SONY_ERICSSON_TAC, dateTime, NO_OF_SUCCESS_COUNT);
    }

    private void populateGroupTableWithExTACGroup(final int tac) throws SQLException {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(GROUP_NAME, EXCLUSIVE_TAC_GROUP);
        values.put(TAC, tac);
        insertRow(TEMP_GROUP_TYPE_E_TAC, values);
    }

    private void insertRowIntoRawTable(final String table, final int eventId, final int tac, final int imsi, final String dateTime)
            throws SQLException {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(EVENT_ID, eventId);
        values.put(TAC, tac);
        values.put(IMSI, imsi);
        values.put(DATETIME_ID, dateTime);
        values.put(DEACTIVATION_TRIGGER, 1);
        insertRow(table, values);
    }

    private void insertRowIntoAggTable(final String table, final int eventId, final int tac, final String dateTime, final int numberOfSuccesses)
            throws SQLException {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(EVENT_ID, eventId);
        values.put(TAC, tac);
        values.put(DATETIME_ID, dateTime);
        values.put(NO_OF_SUCCESSES, numberOfSuccesses);
        insertRow(table, values);
    }

}
