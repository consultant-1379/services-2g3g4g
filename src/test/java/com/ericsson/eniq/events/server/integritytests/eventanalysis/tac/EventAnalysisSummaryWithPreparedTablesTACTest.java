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
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.common.EventIDConstants.*;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.*;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.*;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class EventAnalysisSummaryWithPreparedTablesTACTest extends BaseDataIntegrityTest<TACEventAnalysisSummaryResult> {

    private final EventAnalysisService eventAnalysisService = new EventAnalysisService();

    private static Collection<String> columnsForRawTable = new ArrayList<String>();

    private static Collection<String> columnsForAggregationTable = new ArrayList<String>();

    private final static List<String> aggregationTables = new ArrayList<String>();

    private final static List<String> rawTables = new ArrayList<String>();

    private final SimpleDateFormat dateOnlyFormatter = new SimpleDateFormat(DATE_FORMAT);

    private static final int UNKNOWN_TAC = 65535;
    private final int IMSI_VALUE1 =12345675;
    private final int IMSI_VALUE2 =12345677;
    private final int NO_OF_SUCCESS_COUNT = 999780; 
    private final int NO_OF_SUCCESS_COUNT_1 = 1999;
    private final int NO_OF_ERROR_COUNT = 863;
    private final int SUCCESS_RATIO_RESULT_COUNT = 2;
    private final double SUCCESS_RATIO_VALUE = 99.99;
    private final double SUCCESS_RATIO_VALUE_1 = 69.85;

    static {
        aggregationTables.add(TEMP_EVENT_E_SGEH_MANUF_TAC_EVENTID_ERR_DAY);
        aggregationTables.add(TEMP_EVENT_E_SGEH_MANUF_TAC_EVENTID_SUC_DAY);
        aggregationTables.add(TEMP_EVENT_E_LTE_MANUF_TAC_EVENTID_ERR_DAY);
        aggregationTables.add(TEMP_EVENT_E_LTE_MANUF_TAC_EVENTID_SUC_DAY);
        rawTables.add(TEMP_EVENT_E_SGEH_ERR_RAW);
        rawTables.add(TEMP_EVENT_E_SGEH_SUC_RAW);
        rawTables.add(TEMP_EVENT_E_LTE_ERR_RAW);
        rawTables.add(TEMP_EVENT_E_LTE_SUC_RAW);
        columnsForAggregationTable.add(MANUFACTURER);
        columnsForAggregationTable.add(EVENT_ID);
        columnsForAggregationTable.add(NO_OF_SUCCESSES);
        columnsForAggregationTable.add(NO_OF_ERRORS);
        columnsForAggregationTable.add(TAC);
        columnsForAggregationTable.add(IMSI);
        columnsForAggregationTable.add(NO_OF_NET_INIT_DEACTIVATES);
        columnsForAggregationTable.add(DATETIME_ID);

        columnsForRawTable.add(EVENT_ID);
        columnsForRawTable.add(TAC);
        columnsForRawTable.add(IMSI);
        columnsForRawTable.add(DEACTIVATION_TRIGGER);
        columnsForRawTable.add(DATETIME_ID);
        columnsForRawTable.add(LOCAL_DATE_ID);
    }

    @Before
    public void onSetUp() throws Exception {

        attachDependencies(eventAnalysisService);
        for (final String tempTable : aggregationTables) {
            createTemporaryTable(tempTable, columnsForAggregationTable);
        }

        for (final String ratTable : rawTables) {
            createTemporaryTable(ratTable, columnsForRawTable);
        }

    }

    @Test
    public void testGetSummaryDataWithJustSuccessEventsReturnsThose() throws Exception {

        populateTemporaryTablesWithOnlySuccessEventsForOneEventType();
        final String json = getData(SONY_ERICSSON_TAC);
        validateResultContainsOneEvent(json);
    }

    private void validateResultContainsOneEvent(final String json) throws Exception {
        final List<TACEventAnalysisSummaryResult> summaryResult = getTranslator().translateResult(json, TACEventAnalysisSummaryResult.class);
        assertThat(summaryResult.size(), is(1));

        final TACEventAnalysisSummaryResult firstResult = summaryResult.get(0);
        assertThat(firstResult.getTAC(), is(SONY_ERICSSON_TAC));
        assertThat(firstResult.getEventId(), is((ACTIVATE_IN_2G_AND_3G)));
        assertThat(firstResult.getEventIdDesc(), is(ACTIVATE));
        assertThat(firstResult.getErrorCount(), is((0)));
        assertThat(firstResult.getSuccessCount(), is((5)));
        assertThat(firstResult.getOccurrences(), is((5)));
        assertThat(firstResult.getSuccessRatio(), is(firstResult.getExpectedSuccessRatio()));
        assertThat(firstResult.getErrorSubscriberCount(), is((0)));

    }

    private void populateTemporaryTablesWithOnlySuccessEventsForOneEventType() throws SQLException, ParseException {
        final String dateTime = DateTimeUtilities.getDateTime(Calendar.MINUTE, -4500);
        final String localDateId = dateOnlyFormatter.format(dateOnlyFormatter.parse(dateTime));
        insertRowIntoRawTable(TEMP_EVENT_E_SGEH_SUC_RAW, ACTIVATE_IN_2G_AND_3G, SONY_ERICSSON_TAC, 12345673, dateTime, localDateId);
        insertRowIntoAggregationTable(TEMP_EVENT_E_SGEH_MANUF_TAC_EVENTID_SUC_DAY, SONY_ERICSSON, ACTIVATE_IN_2G_AND_3G, 5, 0, SONY_ERICSSON_TAC,
                12345673, dateTime);
    }

    @Test
    public void testGetSummaryData_TAC_1Week() throws Exception {

        populateTemporaryTablesWithFullDataSet(SONY_ERICSSON_TAC);
        final String json = getData(SONY_ERICSSON_TAC);
        validateResultForFullDataSet(json);
    }

    @Test
    public void testGetSummaryData_TAC_1WeekForSuccessRatioTest() throws Exception {
        populateTemporaryTablesForSuccessratioTest();
        final String json = getData(SONY_ERICSSON_TAC);

        final List<TACEventAnalysisSummaryResult> summaryResult = getTranslator().translateResult(json, TACEventAnalysisSummaryResult.class);
        assertThat(summaryResult.size(), is(SUCCESS_RATIO_RESULT_COUNT));

        final TACEventAnalysisSummaryResult resultSet1 = summaryResult.get(0);
        assertThat(resultSet1.getSuccessRatio(), is(SUCCESS_RATIO_VALUE));
        
        final TACEventAnalysisSummaryResult resultSet2 = summaryResult.get(1);
        assertThat(resultSet2.getSuccessRatio(), is(SUCCESS_RATIO_VALUE_1));
    }

    @Test
    public void testGetSummaryData_TAC_1Week_NoModel() throws Exception {

        populateTemporaryTablesWithFullDataSet(UNKNOWN_TAC);
        final String json = getData(UNKNOWN_TAC);
        validateResultForFullDataSet_NoModel(json);
    }

    private String getData(final int tac) throws Exception {
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(TIME_QUERY_PARAM, TWO_WEEKS);
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

    private void validateResultForFullDataSet(final String json) throws Exception {
        final List<TACEventAnalysisSummaryResult> summaryResult = getTranslator().translateResult(json, TACEventAnalysisSummaryResult.class);
        assertThat(summaryResult.size(), is(4));

        final TACEventAnalysisSummaryResult firstResult = summaryResult.get(0);
        assertThat(firstResult.getTAC(), is(SONY_ERICSSON_TAC));
        assertThat(firstResult.getEventId(), is((SERVICE_REQUEST_IN_2G_AND_3G)));
        assertThat(firstResult.getEventIdDesc(), is(SERVICE_REQUEST));
        assertThat(firstResult.getErrorCount(), is((8)));
        assertThat(firstResult.getSuccessCount(), is((6)));
        assertThat(firstResult.getOccurrences(), is((14)));
        assertThat(firstResult.getSuccessRatio(), is(42.86));
        assertThat(firstResult.getErrorSubscriberCount(), is((1)));

        final TACEventAnalysisSummaryResult fourthResult = summaryResult.get(1);
        assertThat(fourthResult.getTAC(), is(SONY_ERICSSON_TAC));
        assertThat(fourthResult.getEventId(), is((ACTIVATE_IN_2G_AND_3G)));
        assertThat(fourthResult.getEventIdDesc(), is(ACTIVATE));
        assertThat(fourthResult.getErrorCount(), is((7)));
        assertThat(fourthResult.getSuccessCount(), is((5)));
        assertThat(fourthResult.getOccurrences(), is((12)));
        assertThat(fourthResult.getSuccessRatio(), is(41.67));
        assertThat(fourthResult.getErrorSubscriberCount(), is((1)));

        final TACEventAnalysisSummaryResult thirdResult = summaryResult.get(2);
        assertThat(thirdResult.getTAC(), is(SONY_ERICSSON_TAC));
        assertThat(thirdResult.getEventId(), is((ATTACH_IN_4G)));
        assertThat(thirdResult.getEventIdDesc(), is(L_ATTACH));
        assertThat(thirdResult.getErrorCount(), is((1)));
        assertThat(thirdResult.getSuccessCount(), is((7)));
        assertThat(thirdResult.getOccurrences(), is((8)));
        assertThat(thirdResult.getSuccessRatio(), is(87.50));
        assertThat(thirdResult.getErrorSubscriberCount(), is((1)));

        final TACEventAnalysisSummaryResult secondResult = summaryResult.get(3);
        assertThat(secondResult.getTAC(), is(SONY_ERICSSON_TAC));
        assertThat(secondResult.getEventId(), is((SERVICE_REQUEST_IN_4G)));
        assertThat(secondResult.getEventIdDesc(), is(L_SERVICE_REQUEST));
        assertThat(secondResult.getErrorCount(), is((2)));
        assertThat(secondResult.getSuccessCount(), is((0)));
        assertThat(secondResult.getOccurrences(), is((2)));
        assertThat(secondResult.getSuccessRatio(), is(0.00));
        assertThat(secondResult.getErrorSubscriberCount(), is((1)));

    }

    private void validateResultForFullDataSet_NoModel(final String json) throws Exception {
        final List<TACEventAnalysisSummaryResult> summaryResult = getTranslator().translateResult(json, TACEventAnalysisSummaryResult.class);
        assertThat(summaryResult.size(), is(4));

        final TACEventAnalysisSummaryResult firstResult = summaryResult.get(0);
        assertThat(firstResult.getTAC(), is(UNKNOWN_TAC));
        assertThat(firstResult.getManufacturer(), is(String.valueOf(UNKNOWN_TAC)));
        assertThat(firstResult.getModel(), is(String.valueOf(UNKNOWN_TAC)));
        assertThat(firstResult.getEventId(), is((SERVICE_REQUEST_IN_2G_AND_3G)));
        assertThat(firstResult.getEventIdDesc(), is(SERVICE_REQUEST));
        assertThat(firstResult.getErrorCount(), is((8)));
        assertThat(firstResult.getSuccessCount(), is((6)));
        assertThat(firstResult.getOccurrences(), is((14)));
        assertThat(firstResult.getSuccessRatio(), is(42.86));
        assertThat(firstResult.getErrorSubscriberCount(), is((1)));
    }

    private void populateTemporaryTablesForSuccessratioTest() throws SQLException {
        final String dateTime = DateTimeUtilities.getDateTime(Calendar.MINUTE, -4500);
        insertRowIntoAggregationTable(TEMP_EVENT_E_LTE_MANUF_TAC_EVENTID_ERR_DAY, SONY_ERICSSON, ATTACH_IN_4G, 0, 1, SONY_ERICSSON_TAC, IMSI_VALUE1,
                dateTime);
        insertRowIntoAggregationTable(TEMP_EVENT_E_LTE_MANUF_TAC_EVENTID_SUC_DAY, SONY_ERICSSON, ATTACH_IN_4G, NO_OF_SUCCESS_COUNT, 0, SONY_ERICSSON_TAC, IMSI_VALUE2,
                dateTime);
        insertRowIntoAggregationTable(TEMP_EVENT_E_LTE_MANUF_TAC_EVENTID_ERR_DAY, SONY_ERICSSON, HANDOVER_IN_4G, 0, NO_OF_ERROR_COUNT, SONY_ERICSSON_TAC, IMSI_VALUE1,
                dateTime);
        insertRowIntoAggregationTable(TEMP_EVENT_E_LTE_MANUF_TAC_EVENTID_SUC_DAY, SONY_ERICSSON, HANDOVER_IN_4G, NO_OF_SUCCESS_COUNT_1, 0, SONY_ERICSSON_TAC, IMSI_VALUE2,
                dateTime);
    }

    private void populateTemporaryTablesWithFullDataSet(final int tac) throws SQLException, ParseException {
        final String dateTime = DateTimeUtilities.getDateTime(Calendar.MINUTE, -4500);
        final String localDateId = dateOnlyFormatter.format(dateOnlyFormatter.parse(dateTime));

        insertRowIntoRawTable(TEMP_EVENT_E_SGEH_ERR_RAW, ACTIVATE_IN_2G_AND_3G, tac, 12345671, dateTime, localDateId);
        insertRowIntoRawTable(TEMP_EVENT_E_SGEH_ERR_RAW, SERVICE_REQUEST_IN_2G_AND_3G, tac, 12345672, dateTime, localDateId);

        insertRowIntoRawTable(TEMP_EVENT_E_SGEH_SUC_RAW, ACTIVATE_IN_2G_AND_3G, tac, 12345673, dateTime, localDateId);
        insertRowIntoRawTable(TEMP_EVENT_E_SGEH_SUC_RAW, SERVICE_REQUEST_IN_2G_AND_3G, tac, 12345674, dateTime, localDateId);

        insertRowIntoRawTable(TEMP_EVENT_E_LTE_ERR_RAW, ATTACH_IN_4G, tac, 12345675, dateTime, localDateId);
        insertRowIntoRawTable(TEMP_EVENT_E_LTE_ERR_RAW, SERVICE_REQUEST_IN_4G, tac, 12345676, dateTime, localDateId);

        insertRowIntoRawTable(TEMP_EVENT_E_LTE_SUC_RAW, ATTACH_IN_4G, tac, 12345677, dateTime, localDateId);
        insertRowIntoRawTable(TEMP_EVENT_E_LTE_SUC_RAW, ATTACH_IN_4G, tac, 12345678, dateTime, localDateId);

        insertRowIntoAggregationTable(TEMP_EVENT_E_SGEH_MANUF_TAC_EVENTID_SUC_DAY, SONY_ERICSSON, ACTIVATE_IN_2G_AND_3G, 5, 0, tac, 12345671,
                dateTime);
        insertRowIntoAggregationTable(TEMP_EVENT_E_SGEH_MANUF_TAC_EVENTID_SUC_DAY, SONY_ERICSSON, SERVICE_REQUEST_IN_2G_AND_3G, 6, 0, tac, 12345672,
                dateTime);

        insertRowIntoAggregationTable(TEMP_EVENT_E_SGEH_MANUF_TAC_EVENTID_ERR_DAY, SONY_ERICSSON, ACTIVATE_IN_2G_AND_3G, 0, 7, tac, 12345673,
                dateTime);
        insertRowIntoAggregationTable(TEMP_EVENT_E_SGEH_MANUF_TAC_EVENTID_ERR_DAY, SONY_ERICSSON, SERVICE_REQUEST_IN_2G_AND_3G, 0, 8, tac, 12345674,
                dateTime);

        insertRowIntoAggregationTable(TEMP_EVENT_E_LTE_MANUF_TAC_EVENTID_ERR_DAY, SONY_ERICSSON, ATTACH_IN_4G, 0, 1, tac, 12345675, dateTime);
        insertRowIntoAggregationTable(TEMP_EVENT_E_LTE_MANUF_TAC_EVENTID_ERR_DAY, SONY_ERICSSON, SERVICE_REQUEST_IN_4G, 0, 2, tac, 12345676, dateTime);

        insertRowIntoAggregationTable(TEMP_EVENT_E_LTE_MANUF_TAC_EVENTID_SUC_DAY, SONY_ERICSSON, ATTACH_IN_4G, 3, 0, tac, 12345677, dateTime);
        insertRowIntoAggregationTable(TEMP_EVENT_E_LTE_MANUF_TAC_EVENTID_SUC_DAY, SONY_ERICSSON, ATTACH_IN_4G, 4, 0, tac, 12345678, dateTime);

    }

    private void insertRowIntoAggregationTable(final String table, final String manufacturer, final int eventId, final int successCount,
                                               final int errorCount, final int tac, final int imsi, final String dateTime) throws SQLException {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(MANUFACTURER, manufacturer);
        values.put(EVENT_ID, eventId);
        values.put(NO_OF_SUCCESSES, successCount);
        values.put(NO_OF_ERRORS, errorCount);
        values.put(TAC, tac);
        values.put(IMSI, imsi);
        values.put(NO_OF_NET_INIT_DEACTIVATES, 0);
        values.put(DATETIME_ID, dateTime);
        insertRow(table, values);
    }

    private void insertRowIntoRawTable(final String table, final int eventId, final int tac, final int imsi, final String dateTime, final String localDateId)
            throws SQLException {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(EVENT_ID, eventId);
        values.put(TAC, tac);
        values.put(IMSI, imsi);
        values.put(DATETIME_ID, dateTime);
        values.put(LOCAL_DATE_ID, localDateId);
        values.put(DEACTIVATION_TRIGGER, 1);
        insertRow(table, values);

    }

    private void insertRowIntoDTAggregationTable(final String table, final int tac, final int imsi, final String dateTime) throws SQLException {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(TAC, tac);
        values.put(IMSI, imsi);
        values.put(DATETIME_ID, dateTime);
        values.put(DATAVOL_DL, 500000);
        values.put(DATAVOL_UL, 500000);
        insertRow(table, values);
    }

    private void insertRowIntoDTPDPAggregationTable(final String table, final int tac, final int imsi, final String dateTime) throws SQLException {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(TAC, tac);
        values.put(IMSI, imsi);
        values.put(DATETIME_ID, dateTime);
        values.put(DATAVOL_DL, 500000);
        values.put(DATAVOL_UL, 500000);
        values.put(NO_OF_TOTAL, 30000);
        insertRow(table, values);
    }

}
