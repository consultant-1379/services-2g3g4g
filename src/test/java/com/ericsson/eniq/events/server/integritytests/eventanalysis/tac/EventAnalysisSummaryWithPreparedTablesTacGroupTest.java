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

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.common.EventIDConstants.*;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.*;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.*;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import javax.ws.rs.core.MultivaluedMap;

import org.junit.Before;
import org.junit.Test;

import com.ericsson.eniq.events.server.resources.BaseDataIntegrityTest;
import com.ericsson.eniq.events.server.serviceprovider.impl.eventanalysis.EventAnalysisService;
import com.ericsson.eniq.events.server.test.queryresults.TACGroupEventAnalysisSummaryResult;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;
import com.sun.jersey.core.util.MultivaluedMapImpl;

public class EventAnalysisSummaryWithPreparedTablesTacGroupTest extends BaseDataIntegrityTest<TACGroupEventAnalysisSummaryResult> {

    private final EventAnalysisService eventAnalysisService = new EventAnalysisService();

    private static Collection<String> columnsForRawTable = new ArrayList<String>();

    private static Collection<String> columnsForAggregationTable = new ArrayList<String>();

    private final static List<String> aggregationTables = new ArrayList<String>();

    private final SimpleDateFormat dateOnlyFormatter = new SimpleDateFormat(DATE_FORMAT);

    private final static List<String> rawTables = new ArrayList<String>();
    private final int NO_OF_SUCCESS_COUNT = 999780; 
    private final int NO_OF_ERROR_COUNT = 863;
    private final int NO_OF_SUCCESS_COUNT_1 = 1999;
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

        populateGroupTable();
    }

    private void populateGroupTable() throws SQLException {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(GROUP_NAME, SONY_ERICSSON_TAC_GROUP);
        values.put(TAC, SONY_ERICSSON_TAC);
        insertRow(TEMP_GROUP_TYPE_E_TAC, values);
    }

    @Test
    public void testGetSummaryDataWithJustSuccessEventsReturnsThose() throws Exception {
        populateTemporaryTablesWithOnlySuccessEventsForOneEventType();
        final String json = getData();
        validateResultContainsOneEvent(json);
    }

    private void validateResultContainsOneEvent(final String json) throws Exception {
        final List<TACGroupEventAnalysisSummaryResult> summaryResult = getTranslator()
                .translateResult(json, TACGroupEventAnalysisSummaryResult.class);
        assertThat(summaryResult.size(), is(1));

        final TACGroupEventAnalysisSummaryResult firstResult = summaryResult.get(0);
        assertThat(firstResult.getTACGroup(), is(SONY_ERICSSON_TAC_GROUP));
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

        populateTemporaryTablesWithFullDataSet();
        final String json = getData();
        validateResultForFullDataSet(json);
    }

    @Test
    public void testGetSummaryData_TAC_1WeekForSuccessRatioTest() throws Exception {

        populateTemporaryTablesForSuccessRatioTest();
        final String json = getData();
        final List<TACGroupEventAnalysisSummaryResult> summaryResult = getTranslator()
                .translateResult(json, TACGroupEventAnalysisSummaryResult.class);
        assertThat(summaryResult.size(), is(SUCCESS_RATIO_RESULT_COUNT));
        final TACGroupEventAnalysisSummaryResult resultSet1 = summaryResult.get(0);
        assertThat(resultSet1.getSuccessRatio(), is(SUCCESS_RATIO_VALUE));
        
        final TACGroupEventAnalysisSummaryResult resultSet2 = summaryResult.get(1);
        assertThat(resultSet2.getSuccessRatio(), is(SUCCESS_RATIO_VALUE_1));
        
    }

    private String getData() throws Exception {
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(TIME_QUERY_PARAM, String.valueOf(MINUTES_IN_2_WEEKS));
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(GROUP_NAME_PARAM, SONY_ERICSSON_TAC_GROUP);
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, "500");

        final String json = getData(eventAnalysisService, map);
        System.out.println(json);
        return json;
    }

    private void validateResultForFullDataSet(final String json) throws Exception {
        final List<TACGroupEventAnalysisSummaryResult> summaryResults = getTranslator().translateResult(json,
                TACGroupEventAnalysisSummaryResult.class);
        assertThat(summaryResults.size(), is(4));

        for (TACGroupEventAnalysisSummaryResult summaryResult : summaryResults) {
            if (summaryResult.getEventId() == ATTACH_IN_4G) {
                verifyAttach4GRow(summaryResult);
            } else if (summaryResult.getEventId() == SERVICE_REQUEST_IN_4G) {
                verifyServiceRequest4GRow(summaryResult);
            } else if (summaryResult.getEventId() == SERVICE_REQUEST_IN_2G_AND_3G) {
                verifyServiceRequest2G3GRow(summaryResult);
            } else {
                verifyActivate2G3GRow(summaryResult);
            }
        }
    }

    private void verifyActivate2G3GRow(final TACGroupEventAnalysisSummaryResult result) {
        assertThat(result.getTACGroup(), is(SONY_ERICSSON_TAC_GROUP));
        assertThat(result.getEventId(), is((ACTIVATE_IN_2G_AND_3G)));
        assertThat(result.getEventIdDesc(), is(ACTIVATE));
        assertThat(result.getErrorCount(), is((7)));
        assertThat(result.getSuccessCount(), is((5)));
        assertThat(result.getOccurrences(), is((12)));
        assertThat(result.getSuccessRatio(), is(41.67));
        assertThat(result.getErrorSubscriberCount(), is((1)));
    }

    private void verifyAttach4GRow(final TACGroupEventAnalysisSummaryResult result) {
        assertThat(result.getTACGroup(), is(SONY_ERICSSON_TAC_GROUP));
        assertThat(result.getEventId(), is((ATTACH_IN_4G)));
        assertThat(result.getEventIdDesc(), is(L_ATTACH));
        assertThat(result.getErrorCount(), is((1)));
        assertThat(result.getSuccessCount(), is((7)));
        assertThat(result.getOccurrences(), is((8)));
        assertThat(result.getSuccessRatio(), is(87.50));
        assertThat(result.getErrorSubscriberCount(), is((1)));
    }

    private void verifyServiceRequest2G3GRow(final TACGroupEventAnalysisSummaryResult result) {
        assertThat(result.getTACGroup(), is(SONY_ERICSSON_TAC_GROUP));
        assertThat(result.getEventId(), is((SERVICE_REQUEST_IN_2G_AND_3G)));
        assertThat(result.getEventIdDesc(), is(SERVICE_REQUEST));
        assertThat(result.getErrorCount(), is((8)));
        assertThat(result.getSuccessCount(), is((6)));
        assertThat(result.getOccurrences(), is((14)));
        assertThat(result.getSuccessRatio(), is(42.86));
        assertThat(result.getErrorSubscriberCount(), is((1)));
    }

    private void verifyServiceRequest4GRow(final TACGroupEventAnalysisSummaryResult result) {
        assertThat(result.getTACGroup(), is(SONY_ERICSSON_TAC_GROUP));
        assertThat(result.getEventId(), is((SERVICE_REQUEST_IN_4G)));
        assertThat(result.getEventIdDesc(), is(L_SERVICE_REQUEST));
        assertThat(result.getErrorCount(), is((2)));
        assertThat(result.getSuccessCount(), is((0)));
        assertThat(result.getOccurrences(), is((2)));
        assertThat(result.getSuccessRatio(), is(0.00));
        assertThat(result.getErrorSubscriberCount(), is((1)));
    }

    private void populateTemporaryTablesForSuccessRatioTest() throws SQLException {
        final String dateTime = DateTimeUtilities.getDateTime(Calendar.MINUTE, -4500);
        insertRowIntoAggregationTable(TEMP_EVENT_E_LTE_MANUF_TAC_EVENTID_ERR_DAY, SONY_ERICSSON, ATTACH_IN_4G, 0, 1, SONY_ERICSSON_TAC, 12345675,
                dateTime);
        insertRowIntoAggregationTable(TEMP_EVENT_E_LTE_MANUF_TAC_EVENTID_SUC_DAY, SONY_ERICSSON, ATTACH_IN_4G, NO_OF_SUCCESS_COUNT, 0, SONY_ERICSSON_TAC, 12345677,
                dateTime);
        insertRowIntoAggregationTable(TEMP_EVENT_E_LTE_MANUF_TAC_EVENTID_ERR_DAY, SONY_ERICSSON, HANDOVER_IN_4G, 0, NO_OF_ERROR_COUNT, SONY_ERICSSON_TAC, 12345675,
                dateTime);
        insertRowIntoAggregationTable(TEMP_EVENT_E_LTE_MANUF_TAC_EVENTID_SUC_DAY, SONY_ERICSSON, HANDOVER_IN_4G, NO_OF_SUCCESS_COUNT_1, 0, SONY_ERICSSON_TAC, 12345677,
                dateTime);
    }

    private void populateTemporaryTablesWithFullDataSet() throws SQLException, ParseException {
        final String dateTime = DateTimeUtilities.getDateTime(Calendar.MINUTE, -4500);
        final String localDateId = dateOnlyFormatter.format(dateOnlyFormatter.parse(dateTime));

        insertRowIntoRawTable(TEMP_EVENT_E_SGEH_ERR_RAW, ACTIVATE_IN_2G_AND_3G, SONY_ERICSSON_TAC, 12345671, dateTime, localDateId);
        insertRowIntoRawTable(TEMP_EVENT_E_SGEH_ERR_RAW, SERVICE_REQUEST_IN_2G_AND_3G, SONY_ERICSSON_TAC, 12345672, dateTime, localDateId);

        insertRowIntoRawTable(TEMP_EVENT_E_SGEH_SUC_RAW, ACTIVATE_IN_2G_AND_3G, SONY_ERICSSON_TAC, 12345673, dateTime, localDateId);
        insertRowIntoRawTable(TEMP_EVENT_E_SGEH_SUC_RAW, SERVICE_REQUEST_IN_2G_AND_3G, SONY_ERICSSON_TAC, 12345674, dateTime, localDateId);

        insertRowIntoRawTable(TEMP_EVENT_E_LTE_ERR_RAW, ATTACH_IN_4G, SONY_ERICSSON_TAC, 12345675, dateTime, localDateId);
        insertRowIntoRawTable(TEMP_EVENT_E_LTE_ERR_RAW, SERVICE_REQUEST_IN_4G, SONY_ERICSSON_TAC, 12345676, dateTime, localDateId);

        insertRowIntoRawTable(TEMP_EVENT_E_LTE_SUC_RAW, ATTACH_IN_4G, SONY_ERICSSON_TAC, 12345677, dateTime, localDateId);
        insertRowIntoRawTable(TEMP_EVENT_E_LTE_SUC_RAW, ATTACH_IN_4G, SONY_ERICSSON_TAC, 12345678, dateTime, localDateId);

        insertRowIntoAggregationTable(TEMP_EVENT_E_SGEH_MANUF_TAC_EVENTID_SUC_DAY, SONY_ERICSSON, ACTIVATE_IN_2G_AND_3G, 5, 0, SONY_ERICSSON_TAC,
                12345671, dateTime);
        insertRowIntoAggregationTable(TEMP_EVENT_E_SGEH_MANUF_TAC_EVENTID_SUC_DAY, SONY_ERICSSON, SERVICE_REQUEST_IN_2G_AND_3G, 6, 0,
                SONY_ERICSSON_TAC, 12345672, dateTime);

        insertRowIntoAggregationTable(TEMP_EVENT_E_SGEH_MANUF_TAC_EVENTID_ERR_DAY, SONY_ERICSSON, ACTIVATE_IN_2G_AND_3G, 0, 7, SONY_ERICSSON_TAC,
                12345673, dateTime);
        insertRowIntoAggregationTable(TEMP_EVENT_E_SGEH_MANUF_TAC_EVENTID_ERR_DAY, SONY_ERICSSON, SERVICE_REQUEST_IN_2G_AND_3G, 0, 8,
                SONY_ERICSSON_TAC, 12345674, dateTime);

        insertRowIntoAggregationTable(TEMP_EVENT_E_LTE_MANUF_TAC_EVENTID_ERR_DAY, SONY_ERICSSON, ATTACH_IN_4G, 0, 1, SONY_ERICSSON_TAC, 12345675,
                dateTime);
        insertRowIntoAggregationTable(TEMP_EVENT_E_LTE_MANUF_TAC_EVENTID_ERR_DAY, SONY_ERICSSON, SERVICE_REQUEST_IN_4G, 0, 2, SONY_ERICSSON_TAC,
                12345676, dateTime);

        insertRowIntoAggregationTable(TEMP_EVENT_E_LTE_MANUF_TAC_EVENTID_SUC_DAY, SONY_ERICSSON, ATTACH_IN_4G, 3, 0, SONY_ERICSSON_TAC, 12345677,
                dateTime);
        insertRowIntoAggregationTable(TEMP_EVENT_E_LTE_MANUF_TAC_EVENTID_SUC_DAY, SONY_ERICSSON, ATTACH_IN_4G, 4, 0, SONY_ERICSSON_TAC, 12345678,
                dateTime);

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

}
