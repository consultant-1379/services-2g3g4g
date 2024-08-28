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
package com.ericsson.eniq.events.server.integritytests.eventanalysis.sgsnmme;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.common.EventIDConstants.*;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.*;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.*;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.net.URISyntaxException;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import javax.ws.rs.core.MultivaluedMap;

import org.junit.Before;
import org.junit.Test;

import com.ericsson.eniq.events.server.resources.BaseDataIntegrityTest;
import com.ericsson.eniq.events.server.serviceprovider.impl.eventanalysis.EventAnalysisService;
import com.ericsson.eniq.events.server.test.queryresults.EventAnalysisSGSNMMESummaryResult;
import com.ericsson.eniq.events.server.test.stubs.DummyUriInfoImpl;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;
import com.sun.jersey.core.util.MultivaluedMapImpl;

public class EventAnalysisSummaryWithPreparedTablesSGSNMMETest extends BaseDataIntegrityTest<EventAnalysisSGSNMMESummaryResult> {

    private EventAnalysisService eventAnalysisService;

    private final int numErrorsForLAttach = 2;

    private final int numSuccessesForLAttach = 3;

    private final int numErrorsForPDNConnectEvent = 1;
    private final String SAMPLE_MME1 = "SampleMME1";
    private final String SAMPLE_MME_GROUP1 = "sampleMMEGroup1";
    private final int NO_OF_SUCCESS_COUNT = 999780;
    private final int NO_OF_ERROR_COUNT = 863;
    private final int NO_OF_SUCCESS_COUNT_1 = 1999;
    private final int SUCCESS_RATIO_RESULT_COUNT = 2;
    private final double SUCCESS_RATIO_VALUE = 99.99;
    private final double SUCCESS_RATIO_VALUE_1 = 69.85;
    private final SimpleDateFormat dateOnlyFormatter = new SimpleDateFormat(DATE_FORMAT);

    @Before
    public void onSetUp() throws Exception {
        eventAnalysisService = new EventAnalysisService();
        attachDependencies(eventAnalysisService);
        createTemporaryTables();
        populateTemporaryTables();
    }

    @Test
    public void testGetEventAnalysisSummaryDataBySGSN_4GDataOnly() throws Exception {
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(SGSN_PARAM, SAMPLE_MME);
        final String result = runQuery(map);
        validateResult(result);
    }

    @Test
    public void testGetEventAnalysisSummaryDataBySGSN_4GDataForSuccessRatioOnly() throws Exception {
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(SGSN_PARAM, SAMPLE_MME1);
        final String result = runQuery(map);
        final List<EventAnalysisSGSNMMESummaryResult> queryResults = getTranslator().translateResult(result, EventAnalysisSGSNMMESummaryResult.class);
        assertThat(queryResults.size(), is((SUCCESS_RATIO_RESULT_COUNT)));
        final EventAnalysisSGSNMMESummaryResult resultSet1 = queryResults.get(0);
        assertThat(resultSet1.getSuccessRatio(), is(SUCCESS_RATIO_VALUE));
        final EventAnalysisSGSNMMESummaryResult resultSet2 = queryResults.get(1);
        assertThat(resultSet2.getSuccessRatio(), is(SUCCESS_RATIO_VALUE_1));
    }

    @Test
    public void testGetEventAnalysisSummaryDataBySGSNGroup_4GDataForSuccessRatioOnly() throws Exception {
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(GROUP_NAME_PARAM, SAMPLE_MME_GROUP1);
        final String result = runQuery(map);
        final List<EventAnalysisSGSNMMESummaryResult> queryResults = getTranslator().translateResult(result, EventAnalysisSGSNMMESummaryResult.class);
        assertThat(queryResults.size(), is((SUCCESS_RATIO_RESULT_COUNT)));
        for (EventAnalysisSGSNMMESummaryResult resultSet : queryResults) {
            if (resultSet.getSuccessRatio() == SUCCESS_RATIO_VALUE_1) {
                assertThat(resultSet.getSuccessRatio(), is(SUCCESS_RATIO_VALUE_1));
            } else if (resultSet.getSuccessRatio() == SUCCESS_RATIO_VALUE) {
                assertThat(resultSet.getSuccessRatio(), is(SUCCESS_RATIO_VALUE));
            } else {
                fail("unknown success ratio");
            }
        }
    }

    @Test
    public void testGetEventAnalysisSummaryDataBySGSNGroup_4GDataOnly() throws Exception {
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(GROUP_NAME_PARAM, SAMPLE_MME_GROUP);
        final String result = runQuery(map);
        validateResult(result);
    }

    private String runQuery(final MultivaluedMap<String, String> map) throws URISyntaxException, Exception {
        map.putSingle(TYPE_PARAM, TYPE_SGSN);
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(TIME_QUERY_PARAM, TWO_WEEKS);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, "500");
        DummyUriInfoImpl.setUriInfo(map, eventAnalysisService);

        final String result = getData(eventAnalysisService, map);
        System.out.println(result);
        return result;
    }

    private void createTemporaryTables() throws Exception {
        createSgehAndLteTables();

        createGroupTable();
    }

    private void createSgehAndLteTables() throws Exception {
        final Collection<String> columnsInAggTable = new ArrayList<String>();
        columnsInAggTable.add(EVENT_SOURCE_NAME);
        columnsInAggTable.add(EVENT_ID);
        columnsInAggTable.add(NO_OF_ERRORS);
        columnsInAggTable.add(NO_OF_SUCCESSES);
        columnsInAggTable.add(NO_OF_NET_INIT_DEACTIVATES);
        columnsInAggTable.add(DATETIME_ID);
        createTemporaryTable(TEMP_EVENT_E_LTE_EVNTSRC_EVENTID_ERR_DAY, columnsInAggTable);
        createTemporaryTable(TEMP_EVENT_E_SGEH_EVNTSRC_EVENTID_ERR_DAY, columnsInAggTable);
        createTemporaryTable(TEMP_EVENT_E_LTE_EVNTSRC_EVENTID_SUC_DAY, columnsInAggTable);
        createTemporaryTable(TEMP_EVENT_E_SGEH_EVNTSRC_EVENTID_SUC_DAY, columnsInAggTable);
        final Collection<String> columnsInRawTable = new ArrayList<String>();
        columnsInRawTable.add(EVENT_SOURCE_NAME);
        columnsInRawTable.add(EVENT_ID);
        columnsInRawTable.add(IMSI);
        columnsInRawTable.add(TAC);
        columnsInRawTable.add(DATETIME_ID);
        columnsInRawTable.add(LOCAL_DATE_ID);
        createTemporaryTable(TEMP_EVENT_E_LTE_ERR_RAW, columnsInRawTable);
        createTemporaryTable(TEMP_EVENT_E_SGEH_ERR_RAW, columnsInRawTable);
    }

    private void createGroupTable() throws Exception {
        final Collection<String> columns = new ArrayList<String>();
        columns.add(GROUP_NAME);
        columns.add(EVENT_SOURCE_NAME);
        createTemporaryTable(TEMP_GROUP_TYPE_E_EVNTSRC, columns);

    }

    private void populateTemporaryTables() throws SQLException, ParseException {
        final String dateTimeId = DateTimeUtilities.getDateTimeMinus48Hours();
        final String localDateId = dateOnlyFormatter.format(dateOnlyFormatter.parse(dateTimeId));
        insertRowIntoAggregationTable(TEMP_EVENT_E_LTE_EVNTSRC_EVENTID_ERR_DAY, SAMPLE_MME, ATTACH_IN_4G, numErrorsForLAttach, 0, dateTimeId);
        insertRowIntoAggregationTable(TEMP_EVENT_E_LTE_EVNTSRC_EVENTID_ERR_DAY, SAMPLE_MME1, ATTACH_IN_4G, 1, 0, dateTimeId);
        insertRowIntoAggregationTable(TEMP_EVENT_E_LTE_EVNTSRC_EVENTID_ERR_DAY, SAMPLE_MME1, HANDOVER_IN_4G, NO_OF_ERROR_COUNT, 0, dateTimeId);
        insertRowIntoAggregationTable(TEMP_EVENT_E_LTE_EVNTSRC_EVENTID_SUC_DAY, SAMPLE_MME, ATTACH_IN_4G, 0, numSuccessesForLAttach, dateTimeId);
        insertRowIntoAggregationTable(TEMP_EVENT_E_LTE_EVNTSRC_EVENTID_SUC_DAY, SAMPLE_MME1, ATTACH_IN_4G, 0, NO_OF_SUCCESS_COUNT, dateTimeId);
        insertRowIntoAggregationTable(TEMP_EVENT_E_LTE_EVNTSRC_EVENTID_SUC_DAY, SAMPLE_MME1, HANDOVER_IN_4G, 0, NO_OF_SUCCESS_COUNT_1, dateTimeId);
        insertRowIntoRawTable(TEMP_EVENT_E_LTE_ERR_RAW, SAMPLE_MME, ATTACH_IN_4G, SAMPLE_IMSI, dateTimeId, localDateId);
        insertRowIntoRawTable(TEMP_EVENT_E_LTE_ERR_RAW, SAMPLE_MME, ATTACH_IN_4G, 1354987987, dateTimeId, localDateId);

        insertRowIntoAggregationTable(TEMP_EVENT_E_LTE_EVNTSRC_EVENTID_ERR_DAY, SAMPLE_MME, PDN_CONNECT_IN_4G, numErrorsForPDNConnectEvent, 0,
                dateTimeId);
        insertRowIntoRawTable(TEMP_EVENT_E_LTE_ERR_RAW, SAMPLE_MME, PDN_CONNECT_IN_4G, SAMPLE_IMSI, dateTimeId, localDateId);

        insertRowIntoGroupTable(TEMP_GROUP_TYPE_E_EVNTSRC, SAMPLE_MME_GROUP, SAMPLE_MME);
        insertRowIntoGroupTable(TEMP_GROUP_TYPE_E_EVNTSRC, SAMPLE_MME_GROUP1, SAMPLE_MME1);

    }

    private void insertRowIntoGroupTable(final String table, final String groupName, final String mme) throws SQLException {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(GROUP_NAME, groupName);
        values.put(EVENT_SOURCE_NAME, mme);
        insertRow(table, values);
    }

    private void insertRowIntoRawTable(final String table, final String mme, final int eventId, final long imsi, final String dateTimeId,
                                       final String localDateId) throws SQLException {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(EVENT_SOURCE_NAME, mme);
        values.put(EVENT_ID, eventId);
        values.put(IMSI, imsi);
        values.put(TAC, SAMPLE_TAC);
        values.put(DATETIME_ID, dateTimeId);
        values.put(LOCAL_DATE_ID, localDateId);
        insertRow(table, values);
    }

    private void insertRowIntoAggregationTable(final String table, final String mme, final int eventId, final int numErrors, final int numSuccesses,
                                               final String dateTimeId) throws SQLException {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(EVENT_SOURCE_NAME, mme);
        values.put(EVENT_ID, eventId);
        values.put(NO_OF_ERRORS, numErrors);
        values.put(NO_OF_SUCCESSES, numSuccesses);
        values.put(NO_OF_NET_INIT_DEACTIVATES, 0);
        values.put(DATETIME_ID, dateTimeId);
        insertRow(table, values);
    }

    private void validateResult(final String result) throws Exception {
        final List<EventAnalysisSGSNMMESummaryResult> queryResults = getTranslator().translateResult(result, EventAnalysisSGSNMMESummaryResult.class);
        assertThat(queryResults.size(), is((2)));

        final EventAnalysisSGSNMMESummaryResult resultForLAttachEvent = queryResults.get(0);
        assertThat(resultForLAttachEvent.getEventId(), is(ATTACH_IN_4G));
        assertThat(resultForLAttachEvent.getEventIdDesc(), is(L_ATTACH));
        assertThat(resultForLAttachEvent.getErrorCount(), is(numErrorsForLAttach));
        assertThat(resultForLAttachEvent.getSuccessCount(), is(numSuccessesForLAttach));
        assertThat(resultForLAttachEvent.getOccurrences(), is(numErrorsForLAttach + numSuccessesForLAttach));
        assertThat(resultForLAttachEvent.getSuccessRatio(), is(resultForLAttachEvent.calculateExpectedSuccessRatio()));
        assertThat(resultForLAttachEvent.getErrorSubscriberCount(), is(2));

        final EventAnalysisSGSNMMESummaryResult resultForPDNConnectEvent = queryResults.get(1);
        assertThat(resultForPDNConnectEvent.getEventId(), is(PDN_CONNECT_IN_4G));
        assertThat(resultForPDNConnectEvent.getEventIdDesc(), is(L_PDN_CONNECT));
        assertThat(resultForPDNConnectEvent.getErrorCount(), is(numErrorsForPDNConnectEvent));
        assertThat(resultForPDNConnectEvent.getSuccessCount(), is(0));
        assertThat(resultForPDNConnectEvent.getOccurrences(), is(numErrorsForPDNConnectEvent));
        assertThat(resultForPDNConnectEvent.getSuccessRatio(), is(resultForPDNConnectEvent.calculateExpectedSuccessRatio()));
        assertThat(resultForPDNConnectEvent.getErrorSubscriberCount(), is(1));
    }
}
