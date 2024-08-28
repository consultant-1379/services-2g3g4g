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
package com.ericsson.eniq.events.server.integritytests.eventanalysis.accessarea;

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
import com.ericsson.eniq.events.server.test.queryresults.ManufacturerEventAnalysisSummaryResult;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;
import com.sun.jersey.core.util.MultivaluedMapImpl;

public class EventAnalysisAccessAreaGroupTest extends BaseDataIntegrityTest<ManufacturerEventAnalysisSummaryResult> {

    private final EventAnalysisService eventAnalysisService = new EventAnalysisService();
    private final List<String> tempTables = new ArrayList<String>();
    private final List<String> columnsInRawTables = new ArrayList<String>();
    private final List<String> columnsInAggTable = new ArrayList<String>();
    private final List<String> columnsInGroupTable = new ArrayList<String>();
    private final List<String> columnsInEventTypeTable = new ArrayList<String>();

    private final String HIERARCHY_1_VALUE = "1228";
    private final String HIERARCHY_3_VALUE = "ONRM_RootMo_R:LTE01ERBS00002";
    private final String HIERARCHY_3_VALUE2 = "ONRM_RootMo_R:LTE01ERBS00003";
    private final int DEACTIVATION_TRIGGER_VALUE = 0;
    private final long IMSI_VALUE = 12345678912345L;
    private final String GROUPNAME_VALUE = "group1";
    private final String GROUPNAME_VALUE2 = "group2";
    private final int NO_OF_SUCCESS_COUNT = 999780;
    private final int NO_OF_SUCCESS_COUNT_1 = 1999;
    private final int NO_OF_FAILED_COUNT = 863;
    private final int SUCCESS_RATIO_RESULT_COUNT = 2;
    private final double SUCCESS_RATIO_VALUE = 99.99;
    private final double SUCCESS_RATIO_VALUE_1 = 69.85;
    private final int TIME_OFFSET_1WEEK = -4320;
    private final int TIME_OFFSET_1DAY = -720;
    private final int TIME_OFFSET_6HOURS = -180;
    private final int NO_OF_NET_INIT_DEACTIVATES_VALUE = 0;

    private final int noOfSuccessesForLTE15MIN = 10;
    private final int noOfErrorsForLTE15MIN = 5;

    private final int noOfSuccessesForSGEH15MIN = 15;
    private final int noOfErrorsForSGEH15MIN = 7;

    private final int noOfSuccessesForLTEDay = 150;
    private final int noOfErrorsForLTEDay = 66;

    private final int noOfSuccessesForSGEHDay = 3;
    private final int noOfErrorsForSGEHDay = 2;
    private final SimpleDateFormat dateOnlyFormatter = new SimpleDateFormat(DATE_FORMAT);

    @Before
    public void onSetUp() throws Exception {

        attachDependencies(eventAnalysisService);

        createRawTables();
        createAggTables();
        createDimTables();
    }

    private void createRawTables() throws Exception {
        tempTables.add(TEMP_EVENT_E_SGEH_ERR_RAW);
        tempTables.add(TEMP_EVENT_E_SGEH_SUC_RAW);
        tempTables.add(TEMP_EVENT_E_LTE_ERR_RAW);
        tempTables.add(TEMP_EVENT_E_LTE_SUC_RAW);

        columnsInRawTables.add(EVENT_ID);
        columnsInRawTables.add(TAC);
        columnsInRawTables.add(IMSI);
        columnsInRawTables.add(DATETIME_ID);
        columnsInRawTables.add(LOCAL_DATE_ID);
        columnsInRawTables.add(RAT);
        columnsInRawTables.add(VENDOR);
        columnsInRawTables.add(HIERARCHY_1);
        columnsInRawTables.add(HIERARCHY_3);
        columnsInRawTables.add(DEACTIVATION_TRIGGER);

        for (final String tempTable : tempTables) {
            createTemporaryTable(tempTable, columnsInRawTables);
        }
    }

    private void createAggTables() throws Exception {
        columnsInAggTable.add(NO_OF_SUCCESSES);
        columnsInAggTable.add(NO_OF_ERRORS);
        columnsInAggTable.add(NO_OF_NET_INIT_DEACTIVATES);
        columnsInAggTable.add(DATETIME_ID);
        columnsInAggTable.add(EVENT_ID);
        columnsInAggTable.add(HIERARCHY_1);
        columnsInAggTable.add(HIERARCHY_3);

        createTemporaryTable(TEMP_EVENT_E_LTE_VEND_HIER321_EVENTID_SUC_15MIN, columnsInAggTable);
        createTemporaryTable(TEMP_EVENT_E_SGEH_VEND_HIER321_EVENTID_SUC_15MIN, columnsInAggTable);
        createTemporaryTable(TEMP_EVENT_E_LTE_VEND_HIER321_EVENTID_ERR_15MIN, columnsInAggTable);
        createTemporaryTable(TEMP_EVENT_E_SGEH_VEND_HIER321_EVENTID_ERR_15MIN, columnsInAggTable);

        createTemporaryTable(TEMP_EVENT_E_LTE_VEND_HIER321_EVENTID_SUC_DAY, columnsInAggTable);
        createTemporaryTable(TEMP_EVENT_E_SGEH_VEND_HIER321_EVENTID_SUC_DAY, columnsInAggTable);
        createTemporaryTable(TEMP_EVENT_E_LTE_VEND_HIER321_EVENTID_ERR_DAY, columnsInAggTable);
        createTemporaryTable(TEMP_EVENT_E_SGEH_VEND_HIER321_EVENTID_ERR_DAY, columnsInAggTable);
    }

    private void createDimTables() throws Exception {
        columnsInGroupTable.add(GROUP_NAME);
        columnsInGroupTable.add(HIERARCHY_1);
        columnsInGroupTable.add(HIERARCHY_3);

        createTemporaryTable(TEMP_GROUP_TYPE_E_RAT_VEND_HIER321, columnsInGroupTable);

        columnsInEventTypeTable.add(EVENT_ID);

        createTemporaryTable(TEMP_DIM_E_LTE_EVENTTYPE, columnsInEventTypeTable);
        createTemporaryTable(TEMP_DIM_E_SGEH_EVENTTYPE, columnsInEventTypeTable);
    }

    @Test
    public void testAccessAreaGroup_OneHour() throws Exception {
        jndiProperties.setUpDataTieringJNDIProperty();
        populateTemporaryTables(-45);
        final String json = getData(ONE_HOUR, GROUPNAME_VALUE);
        validateResultForOneHour(json);
        jndiProperties.setUpJNDIPropertiesForTest();
    }

    @Test
    public void testAccessAreaGroup_SixHours() throws Exception {
        populateTemporaryTables(-180);
        final String json = getData(SIX_HOURS, GROUPNAME_VALUE);
        validateResultForSixHours(json);
    }

    @Test
    public void testAccessAreaGroup_OneWeek() throws Exception {
        populateTemporaryTables(-4320);
        final String json = getData(ONE_WEEK, GROUPNAME_VALUE);
        validateResultForOneWeek(json);
    }

    @Test
    public void testAccessAreaGroup_OneWeekForSuccessRatioTest() throws Exception {
        populateTemporaryTablesForSuccessRatioTest(TIME_OFFSET_1WEEK);
        final String json = getData(ONE_WEEK, GROUPNAME_VALUE2);

        final List<ManufacturerEventAnalysisSummaryResult> summaryResult = getTranslator().translateResult(json,
                ManufacturerEventAnalysisSummaryResult.class);
        assertThat(summaryResult.size(), is(SUCCESS_RATIO_RESULT_COUNT));

        final ManufacturerEventAnalysisSummaryResult firstResult = summaryResult.get(0);
        assertThat(firstResult.getSuccessRatio(), is(SUCCESS_RATIO_VALUE));

        final ManufacturerEventAnalysisSummaryResult secondResult = summaryResult.get(1);
        assertThat(secondResult.getSuccessRatio(), is(SUCCESS_RATIO_VALUE_1));
    }

    @Test
    public void testAccessAreaGroup_OneDayForSuccessRatioTest() throws Exception {
        populateTemporaryTablesForOneDaySuccessRatioTest(TIME_OFFSET_1DAY);
        final String json = getData(ONE_DAY, GROUPNAME_VALUE2);

        final List<ManufacturerEventAnalysisSummaryResult> summaryResult = getTranslator().translateResult(json,
                ManufacturerEventAnalysisSummaryResult.class);
        assertThat(summaryResult.size(), is(SUCCESS_RATIO_RESULT_COUNT));

        final ManufacturerEventAnalysisSummaryResult firstResult = summaryResult.get(0);
        assertThat(firstResult.getSuccessRatio(), is(SUCCESS_RATIO_VALUE));

        final ManufacturerEventAnalysisSummaryResult secondResult = summaryResult.get(1);
        assertThat(secondResult.getSuccessRatio(), is(SUCCESS_RATIO_VALUE_1));
    }

    @Test
    public void testAccessAreaGroup_SixHoursForSuccessRatioTest() throws Exception {
        populateTemporaryTablesForSixHoursSuccessRatioTest(TIME_OFFSET_6HOURS);
        final String json = getData(SIX_HOURS, GROUPNAME_VALUE2);

        final List<ManufacturerEventAnalysisSummaryResult> summaryResult = getTranslator().translateResult(json,
                ManufacturerEventAnalysisSummaryResult.class);
        assertThat(summaryResult.size(), is(SUCCESS_RATIO_RESULT_COUNT));

        final ManufacturerEventAnalysisSummaryResult firstResult = summaryResult.get(0);
        assertThat(firstResult.getSuccessRatio(), is(SUCCESS_RATIO_VALUE));

        final ManufacturerEventAnalysisSummaryResult secondResult = summaryResult.get(1);
        assertThat(secondResult.getSuccessRatio(), is(SUCCESS_RATIO_VALUE_1));
    }

    private String getData(String time, final String groupNameValue) throws Exception {
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(TIME_QUERY_PARAM, time);
        map.putSingle(TYPE_PARAM, TYPE_CELL);
        map.putSingle(GROUP_NAME_PARAM, groupNameValue);
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, "500");

        final String json = getData(eventAnalysisService, map);
        System.out.println(json);
        return json;
    }

    private void populateTemporaryTablesForSuccessRatioTest(int time) throws SQLException {
        final String dateTime = DateTimeUtilities.getDateTime(Calendar.MINUTE, time);
        insertRowIntoAggregation(TEMP_EVENT_E_LTE_VEND_HIER321_EVENTID_SUC_DAY, NO_OF_SUCCESS_COUNT, 0, NO_OF_NET_INIT_DEACTIVATES_VALUE, dateTime,
                HIERARCHY_1_VALUE, HIERARCHY_3_VALUE2, ATTACH_IN_4G);
        insertRowIntoAggregation(TEMP_EVENT_E_LTE_VEND_HIER321_EVENTID_ERR_DAY, 0, 1, NO_OF_NET_INIT_DEACTIVATES_VALUE, dateTime, HIERARCHY_1_VALUE,
                HIERARCHY_3_VALUE2, ATTACH_IN_4G);
        insertRowIntoAggregation(TEMP_EVENT_E_LTE_VEND_HIER321_EVENTID_SUC_DAY, NO_OF_SUCCESS_COUNT_1, 0, NO_OF_NET_INIT_DEACTIVATES_VALUE, dateTime,
                HIERARCHY_1_VALUE, HIERARCHY_3_VALUE2, HANDOVER_IN_4G);
        insertRowIntoAggregation(TEMP_EVENT_E_LTE_VEND_HIER321_EVENTID_ERR_DAY, 0, NO_OF_FAILED_COUNT, NO_OF_NET_INIT_DEACTIVATES_VALUE, dateTime,
                HIERARCHY_1_VALUE, HIERARCHY_3_VALUE2, HANDOVER_IN_4G);
        populateDimTablesForSuccessRatioTest();
    }

    private void populateTemporaryTablesForOneDaySuccessRatioTest(int time) throws SQLException {
        final String dateTime = DateTimeUtilities.getDateTime(Calendar.MINUTE, time);
        insertRowIntoAggregation(TEMP_EVENT_E_LTE_VEND_HIER321_EVENTID_SUC_15MIN, NO_OF_SUCCESS_COUNT, 0, NO_OF_NET_INIT_DEACTIVATES_VALUE, dateTime,
                HIERARCHY_1_VALUE, HIERARCHY_3_VALUE2, ATTACH_IN_4G);
        insertRowIntoAggregation(TEMP_EVENT_E_LTE_VEND_HIER321_EVENTID_ERR_15MIN, 0, 1, NO_OF_NET_INIT_DEACTIVATES_VALUE, dateTime,
                HIERARCHY_1_VALUE, HIERARCHY_3_VALUE2, ATTACH_IN_4G);

        insertRowIntoAggregation(TEMP_EVENT_E_LTE_VEND_HIER321_EVENTID_SUC_15MIN, NO_OF_SUCCESS_COUNT_1, 0, NO_OF_NET_INIT_DEACTIVATES_VALUE,
                dateTime, HIERARCHY_1_VALUE, HIERARCHY_3_VALUE2, HANDOVER_IN_4G);
        insertRowIntoAggregation(TEMP_EVENT_E_LTE_VEND_HIER321_EVENTID_ERR_15MIN, 0, NO_OF_FAILED_COUNT, NO_OF_NET_INIT_DEACTIVATES_VALUE, dateTime,
                HIERARCHY_1_VALUE, HIERARCHY_3_VALUE2, HANDOVER_IN_4G);
        populateDimTablesForSuccessRatioTest();
    }

    private void populateTemporaryTablesForSixHoursSuccessRatioTest(int time) throws SQLException {
        final String dateTime = DateTimeUtilities.getDateTime(Calendar.MINUTE, time);

        insertRowIntoAggregation(TEMP_EVENT_E_LTE_VEND_HIER321_EVENTID_SUC_15MIN, NO_OF_SUCCESS_COUNT, 0, NO_OF_NET_INIT_DEACTIVATES_VALUE, dateTime,
                HIERARCHY_1_VALUE, HIERARCHY_3_VALUE2, ATTACH_IN_4G);
        insertRowIntoAggregation(TEMP_EVENT_E_LTE_VEND_HIER321_EVENTID_ERR_15MIN, 0, 1, NO_OF_NET_INIT_DEACTIVATES_VALUE, dateTime,
                HIERARCHY_1_VALUE, HIERARCHY_3_VALUE2, ATTACH_IN_4G);

        insertRowIntoAggregation(TEMP_EVENT_E_LTE_VEND_HIER321_EVENTID_SUC_15MIN, NO_OF_SUCCESS_COUNT_1, 0, NO_OF_NET_INIT_DEACTIVATES_VALUE,
                dateTime, HIERARCHY_1_VALUE, HIERARCHY_3_VALUE2, HANDOVER_IN_4G);
        insertRowIntoAggregation(TEMP_EVENT_E_LTE_VEND_HIER321_EVENTID_ERR_15MIN, 0, NO_OF_FAILED_COUNT, NO_OF_NET_INIT_DEACTIVATES_VALUE, dateTime,
                HIERARCHY_1_VALUE, HIERARCHY_3_VALUE2, HANDOVER_IN_4G);
        populateDimTablesForSuccessRatioTest();
    }

    private void populateDimTablesForSuccessRatioTest() throws SQLException {
        Map<String, Object> valuesForSuccessRatioTest = new HashMap<String, Object>();
        valuesForSuccessRatioTest.put(GROUP_NAME, GROUPNAME_VALUE2);
        valuesForSuccessRatioTest.put(HIERARCHY_1, HIERARCHY_1_VALUE);
        valuesForSuccessRatioTest.put(HIERARCHY_3, HIERARCHY_3_VALUE2);
        insertRow(TEMP_GROUP_TYPE_E_RAT_VEND_HIER321, valuesForSuccessRatioTest);

        valuesForSuccessRatioTest = new HashMap<String, Object>();
        valuesForSuccessRatioTest.put(EVENT_ID, ATTACH_IN_4G);
        insertRow(TEMP_DIM_E_LTE_EVENTTYPE, valuesForSuccessRatioTest);

        valuesForSuccessRatioTest = new HashMap<String, Object>();
        valuesForSuccessRatioTest.put(EVENT_ID, HANDOVER_IN_4G);
        insertRow(TEMP_DIM_E_LTE_EVENTTYPE, valuesForSuccessRatioTest);

    }

    private void populateTemporaryTables(int time) throws SQLException, ParseException {
        final String dateTime = DateTimeUtilities.getDateTime(Calendar.MINUTE, time);
        final String localDateId = dateOnlyFormatter.format(dateOnlyFormatter.parse(dateTime));
        populateRawTables(dateTime, localDateId);
        populateAggTables(dateTime);
        populateDimTables();
    }

    private void populateRawTables(final String dateTime, final String localDateId) throws SQLException {
        insertRow(TEMP_EVENT_E_SGEH_ERR_RAW, SERVICE_REQUEST_IN_2G_AND_3G, SONY_ERICSSON_TAC, IMSI_VALUE, RAT_FOR_GSM, dateTime, ERICSSON,
                HIERARCHY_1_VALUE, HIERARCHY_3_VALUE, DEACTIVATION_TRIGGER_VALUE, localDateId);
        insertRow(TEMP_EVENT_E_SGEH_ERR_RAW, SERVICE_REQUEST_IN_2G_AND_3G, SONY_ERICSSON_TAC, IMSI_VALUE, RAT_FOR_GSM, dateTime, ERICSSON,
                HIERARCHY_1_VALUE, HIERARCHY_3_VALUE, DEACTIVATION_TRIGGER_VALUE, localDateId);

        insertRow(TEMP_EVENT_E_SGEH_SUC_RAW, ACTIVATE_IN_2G_AND_3G, SONY_ERICSSON_TAC, IMSI_VALUE, RAT_FOR_GSM, dateTime, ERICSSON,
                HIERARCHY_1_VALUE, HIERARCHY_3_VALUE, DEACTIVATION_TRIGGER_VALUE, localDateId);
        insertRow(TEMP_EVENT_E_SGEH_SUC_RAW, SERVICE_REQUEST_IN_2G_AND_3G, SONY_ERICSSON_TAC, IMSI_VALUE, RAT_FOR_GSM, dateTime, ERICSSON,
                HIERARCHY_1_VALUE, HIERARCHY_3_VALUE, DEACTIVATION_TRIGGER_VALUE, localDateId);
        insertRow(TEMP_EVENT_E_SGEH_SUC_RAW, SERVICE_REQUEST_IN_2G_AND_3G, SONY_ERICSSON_TAC, IMSI_VALUE, RAT_FOR_GSM, dateTime, ERICSSON,
                HIERARCHY_1_VALUE, HIERARCHY_3_VALUE, DEACTIVATION_TRIGGER_VALUE, localDateId);

        insertRow(TEMP_EVENT_E_LTE_ERR_RAW, ATTACH_IN_4G, SONY_ERICSSON_TAC, IMSI_VALUE, RAT_FOR_LTE, dateTime, ERICSSON, HIERARCHY_1_VALUE,
                HIERARCHY_3_VALUE, DEACTIVATION_TRIGGER_VALUE, localDateId);
        insertRow(TEMP_EVENT_E_LTE_ERR_RAW, SERVICE_REQUEST_IN_4G, SONY_ERICSSON_TAC, IMSI_VALUE, RAT_FOR_LTE, dateTime, ERICSSON, HIERARCHY_1_VALUE,
                HIERARCHY_3_VALUE, DEACTIVATION_TRIGGER_VALUE, localDateId);

        insertRow(TEMP_EVENT_E_LTE_SUC_RAW, ATTACH_IN_4G, SONY_ERICSSON_TAC, IMSI_VALUE, RAT_FOR_LTE, dateTime, ERICSSON, HIERARCHY_1_VALUE,
                HIERARCHY_3_VALUE, DEACTIVATION_TRIGGER_VALUE, localDateId);
        insertRow(TEMP_EVENT_E_LTE_SUC_RAW, ATTACH_IN_4G, SONY_ERICSSON_TAC, IMSI_VALUE, RAT_FOR_LTE, dateTime, ERICSSON, HIERARCHY_1_VALUE,
                HIERARCHY_3_VALUE, DEACTIVATION_TRIGGER_VALUE, localDateId);
        insertRow(TEMP_EVENT_E_LTE_SUC_RAW, SERVICE_REQUEST_IN_4G, SONY_ERICSSON_TAC, IMSI_VALUE, RAT_FOR_LTE, dateTime, ERICSSON, HIERARCHY_1_VALUE,
                HIERARCHY_3_VALUE, DEACTIVATION_TRIGGER_VALUE, localDateId);
    }

    private void populateAggTables(final String dateTime) throws SQLException {
        insertRowIntoAggregation(TEMP_EVENT_E_LTE_VEND_HIER321_EVENTID_SUC_15MIN, noOfSuccessesForLTE15MIN, 0, 0, dateTime, HIERARCHY_1_VALUE,
                HIERARCHY_3_VALUE, ATTACH_IN_4G);
        insertRowIntoAggregation(TEMP_EVENT_E_SGEH_VEND_HIER321_EVENTID_SUC_15MIN, noOfSuccessesForSGEH15MIN, 0, 0, dateTime, HIERARCHY_1_VALUE,
                HIERARCHY_3_VALUE, ACTIVATE_IN_2G_AND_3G);
        insertRowIntoAggregation(TEMP_EVENT_E_LTE_VEND_HIER321_EVENTID_ERR_15MIN, 0, noOfErrorsForLTE15MIN, 0, dateTime, HIERARCHY_1_VALUE,
                HIERARCHY_3_VALUE, SERVICE_REQUEST_IN_4G);
        insertRowIntoAggregation(TEMP_EVENT_E_SGEH_VEND_HIER321_EVENTID_ERR_15MIN, 0, noOfErrorsForSGEH15MIN, 0, dateTime, HIERARCHY_1_VALUE,
                HIERARCHY_3_VALUE, SERVICE_REQUEST_IN_2G_AND_3G);

        insertRowIntoAggregation(TEMP_EVENT_E_LTE_VEND_HIER321_EVENTID_SUC_DAY, noOfSuccessesForLTEDay, 0, 0, dateTime, HIERARCHY_1_VALUE,
                HIERARCHY_3_VALUE, ATTACH_IN_4G);
        insertRowIntoAggregation(TEMP_EVENT_E_SGEH_VEND_HIER321_EVENTID_SUC_DAY, noOfSuccessesForSGEHDay, 0, 0, dateTime, HIERARCHY_1_VALUE,
                HIERARCHY_3_VALUE, ACTIVATE_IN_2G_AND_3G);
        insertRowIntoAggregation(TEMP_EVENT_E_LTE_VEND_HIER321_EVENTID_ERR_DAY, 0, noOfErrorsForLTEDay, 0, dateTime, HIERARCHY_1_VALUE,
                HIERARCHY_3_VALUE, SERVICE_REQUEST_IN_4G);
        insertRowIntoAggregation(TEMP_EVENT_E_SGEH_VEND_HIER321_EVENTID_ERR_DAY, 0, noOfErrorsForSGEHDay, 0, dateTime, HIERARCHY_1_VALUE,
                HIERARCHY_3_VALUE, SERVICE_REQUEST_IN_2G_AND_3G);
    }

    private void populateDimTables() throws SQLException {
        Map<String, Object> values = new HashMap<String, Object>();
        values.put(GROUP_NAME, GROUPNAME_VALUE);
        values.put(HIERARCHY_1, HIERARCHY_1_VALUE);
        values.put(HIERARCHY_3, HIERARCHY_3_VALUE);
        insertRow(TEMP_GROUP_TYPE_E_RAT_VEND_HIER321, values);

        values = new HashMap<String, Object>();
        values.put(EVENT_ID, SERVICE_REQUEST_IN_2G_AND_3G);
        insertRow(TEMP_DIM_E_SGEH_EVENTTYPE, values);

        values = new HashMap<String, Object>();
        values.put(EVENT_ID, ACTIVATE_IN_2G_AND_3G);
        insertRow(TEMP_DIM_E_SGEH_EVENTTYPE, values);

        values = new HashMap<String, Object>();
        values.put(EVENT_ID, SERVICE_REQUEST_IN_4G);
        insertRow(TEMP_DIM_E_LTE_EVENTTYPE, values);

        values = new HashMap<String, Object>();
        values.put(EVENT_ID, ATTACH_IN_4G);
        insertRow(TEMP_DIM_E_LTE_EVENTTYPE, values);
    }

    private void insertRow(final String table, final int eventId, final int tac, final long imsi, final int rat, final String dateTime,
                           final String vendor, final String hierarchy1, final String hierarchy3, final int deactivationTrigger,
                           final String localDateId) throws SQLException {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(EVENT_ID, eventId);
        values.put(TAC, tac);
        values.put(IMSI, imsi);
        values.put(RAT, rat);
        values.put(DATETIME_ID, dateTime);
        values.put(LOCAL_DATE_ID, localDateId);
        values.put(VENDOR, vendor);
        values.put(HIERARCHY_1, hierarchy1);
        values.put(HIERARCHY_3, hierarchy3);
        values.put(DEACTIVATION_TRIGGER, DEACTIVATION_TRIGGER_VALUE);
        insertRow(table, values);
    }

    private void insertRowIntoAggregation(final String table, final int noOfSuccesses, final int noOfErrors, final int noOfNetInitDeactivates,
                                          final String dateTime, final String hierarchy1, final String hierarchy3, final int eventId)
            throws SQLException {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(NO_OF_SUCCESSES, noOfSuccesses);
        values.put(NO_OF_ERRORS, noOfErrors);
        values.put(NO_OF_NET_INIT_DEACTIVATES, noOfNetInitDeactivates);
        values.put(DATETIME_ID, dateTime);
        values.put(HIERARCHY_1, hierarchy1);
        values.put(HIERARCHY_3, hierarchy3);
        values.put(EVENT_ID, eventId);
        insertRow(table, values);
    }

    private void validateResultForOneHour(final String json) throws Exception {
        final List<ManufacturerEventAnalysisSummaryResult> summaryResult = getTranslator().translateResult(json,
                ManufacturerEventAnalysisSummaryResult.class);
        assertThat(summaryResult.size(), is(4));

        for (ManufacturerEventAnalysisSummaryResult result : summaryResult) {
            if (result.getEventId() == ACTIVATE_IN_2G_AND_3G) {
                assertThat(result.getEventId(), is((ACTIVATE_IN_2G_AND_3G)));
                assertThat(result.getEventIdDesc(), is(ACTIVATE));
                assertThat(result.getErrorCount(), is((0)));
                assertThat(result.getSuccessCount(), is((noOfSuccessesForSGEH15MIN)));
                assertThat(result.getOccurrences(), is((noOfSuccessesForSGEH15MIN)));
                assertThat(result.getSuccessRatio(), is(result.getExpectedSuccessRatio()));
                assertThat(result.getErrorSubscriberCount(), is((0)));
            } else if (result.getEventId() == SERVICE_REQUEST_IN_2G_AND_3G) {
                assertThat(result.getEventId(), is((SERVICE_REQUEST_IN_2G_AND_3G)));
                assertThat(result.getEventIdDesc(), is(SERVICE_REQUEST));
                assertThat(result.getErrorCount(), is((2)));
                assertThat(result.getSuccessCount(), is((0)));
                assertThat(result.getOccurrences(), is((2)));
                assertThat(result.getSuccessRatio(), is(result.getExpectedSuccessRatio()));
                assertThat(result.getErrorSubscriberCount(), is((1)));
            } else if (result.getEventId() == ATTACH_IN_4G) {
                assertThat(result.getEventId(), is((ATTACH_IN_4G)));
                assertThat(result.getEventIdDesc(), is(L_ATTACH));
                assertThat(result.getErrorCount(), is((1)));
                assertThat(result.getSuccessCount(), is((noOfSuccessesForLTE15MIN)));
                assertThat(result.getOccurrences(), is((noOfSuccessesForLTE15MIN + 1)));
                assertThat(result.getSuccessRatio(), is(result.getExpectedSuccessRatio()));
                assertThat(result.getErrorSubscriberCount(), is((1)));
            } else if (result.getEventId() == SERVICE_REQUEST_IN_4G) {
                assertThat(result.getEventId(), is((SERVICE_REQUEST_IN_4G)));
                assertThat(result.getEventIdDesc(), is(L_SERVICE_REQUEST));
                assertThat(result.getErrorCount(), is((1)));
                assertThat(result.getSuccessCount(), is((0)));
                assertThat(result.getOccurrences(), is((1)));
                assertThat(result.getSuccessRatio(), is(result.getExpectedSuccessRatio()));
                assertThat(result.getErrorSubscriberCount(), is((1)));
            } else {
                fail("Event type should not be in results");
            }
        }
    }

    private void validateResultForSixHours(final String json) throws Exception {
        final List<ManufacturerEventAnalysisSummaryResult> summaryResult = getTranslator().translateResult(json,
                ManufacturerEventAnalysisSummaryResult.class);
        assertThat(summaryResult.size(), is(4));

        for (ManufacturerEventAnalysisSummaryResult result : summaryResult) {
            if (result.getEventId() == ACTIVATE_IN_2G_AND_3G) {
                assertThat(result.getEventId(), is((ACTIVATE_IN_2G_AND_3G)));
                assertThat(result.getEventIdDesc(), is(ACTIVATE));
                assertThat(result.getErrorCount(), is((0)));
                assertThat(result.getSuccessCount(), is((noOfSuccessesForSGEH15MIN)));
                assertThat(result.getOccurrences(), is((noOfSuccessesForSGEH15MIN)));
                assertThat(result.getSuccessRatio(), is(result.getExpectedSuccessRatio()));
                assertThat(result.getErrorSubscriberCount(), is((0)));
            } else if (result.getEventId() == ATTACH_IN_4G) {
                assertThat(result.getEventId(), is((ATTACH_IN_4G)));
                assertThat(result.getEventIdDesc(), is(L_ATTACH));
                assertThat(result.getErrorCount(), is((0)));
                assertThat(result.getSuccessCount(), is((noOfSuccessesForLTE15MIN)));
                assertThat(result.getOccurrences(), is((noOfSuccessesForLTE15MIN)));
                assertThat(result.getSuccessRatio(), is(result.getExpectedSuccessRatio()));
                assertThat(result.getErrorSubscriberCount(), is((1)));
            } else if (result.getEventId() == SERVICE_REQUEST_IN_4G) {
                assertThat(result.getEventId(), is((SERVICE_REQUEST_IN_4G)));
                assertThat(result.getEventIdDesc(), is(L_SERVICE_REQUEST));
                assertThat(result.getErrorCount(), is((noOfErrorsForLTE15MIN)));
                assertThat(result.getSuccessCount(), is((0)));
                assertThat(result.getOccurrences(), is((noOfErrorsForLTE15MIN)));
                assertThat(result.getSuccessRatio(), is(result.getExpectedSuccessRatio()));
                assertThat(result.getErrorSubscriberCount(), is(1));
            } else if (result.getEventId() == SERVICE_REQUEST_IN_2G_AND_3G) {
                assertThat(result.getEventId(), is((SERVICE_REQUEST_IN_2G_AND_3G)));
                assertThat(result.getEventIdDesc(), is(SERVICE_REQUEST));
                assertThat(result.getErrorCount(), is((noOfErrorsForSGEH15MIN)));
                assertThat(result.getSuccessCount(), is((0)));
                assertThat(result.getOccurrences(), is((noOfErrorsForSGEH15MIN)));
                assertThat(result.getSuccessRatio(), is(result.getExpectedSuccessRatio()));
                assertThat(result.getErrorSubscriberCount(), is((1)));
            } else {
                fail("Unknown event id");
            }
        }
    }

    private void validateResultForOneWeek(final String json) throws Exception {
        final List<ManufacturerEventAnalysisSummaryResult> summaryResult = getTranslator().translateResult(json,
                ManufacturerEventAnalysisSummaryResult.class);
        assertThat(summaryResult.size(), is(4));

        for (ManufacturerEventAnalysisSummaryResult result : summaryResult) {
            if (result.getEventId() == ACTIVATE_IN_2G_AND_3G) {
                assertThat(result.getEventId(), is((ACTIVATE_IN_2G_AND_3G)));
                assertThat(result.getEventIdDesc(), is(ACTIVATE));
                assertThat(result.getErrorCount(), is((0)));
                assertThat(result.getSuccessCount(), is((noOfSuccessesForSGEHDay)));
                assertThat(result.getOccurrences(), is((noOfSuccessesForSGEHDay)));
                assertThat(result.getSuccessRatio(), is(result.getExpectedSuccessRatio()));
                assertThat(result.getErrorSubscriberCount(), is((0)));
            } else if (result.getEventId() == ATTACH_IN_4G) {
                assertThat(result.getEventId(), is((ATTACH_IN_4G)));
                assertThat(result.getEventIdDesc(), is(L_ATTACH));
                assertThat(result.getErrorCount(), is((0)));
                assertThat(result.getSuccessCount(), is((noOfSuccessesForLTEDay)));
                assertThat(result.getOccurrences(), is((noOfSuccessesForLTEDay)));
                assertThat(result.getSuccessRatio(), is(result.getExpectedSuccessRatio()));
                assertThat(result.getErrorSubscriberCount(), is((1)));
            } else if (result.getEventId() == SERVICE_REQUEST_IN_4G) {
                assertThat(result.getEventId(), is((SERVICE_REQUEST_IN_4G)));
                assertThat(result.getEventIdDesc(), is(L_SERVICE_REQUEST));
                assertThat(result.getErrorCount(), is((noOfErrorsForLTEDay)));
                assertThat(result.getSuccessCount(), is((0)));
                assertThat(result.getOccurrences(), is((noOfErrorsForLTEDay)));
                assertThat(result.getSuccessRatio(), is(result.getExpectedSuccessRatio()));
                assertThat(result.getErrorSubscriberCount(), is(1));
            } else if (result.getEventId() == SERVICE_REQUEST_IN_2G_AND_3G) {
                assertThat(result.getEventId(), is((SERVICE_REQUEST_IN_2G_AND_3G)));
                assertThat(result.getEventIdDesc(), is(SERVICE_REQUEST));
                assertThat(result.getErrorCount(), is((noOfErrorsForSGEHDay)));
                assertThat(result.getSuccessCount(), is((0)));
                assertThat(result.getOccurrences(), is((noOfErrorsForSGEHDay)));
                assertThat(result.getSuccessRatio(), is(result.getExpectedSuccessRatio()));
                assertThat(result.getErrorSubscriberCount(), is((1)));
            } else {
                fail("Unknown event id");
            }
        }

    }
}
