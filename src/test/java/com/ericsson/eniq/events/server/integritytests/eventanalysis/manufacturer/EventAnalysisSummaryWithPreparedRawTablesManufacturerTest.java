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
package com.ericsson.eniq.events.server.integritytests.eventanalysis.manufacturer;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.common.EventIDConstants.*;
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
import com.ericsson.eniq.events.server.serviceprovider.impl.eventanalysis.EventAnalysisService;
import com.ericsson.eniq.events.server.test.queryresults.ManufacturerEventAnalysisSummaryResult;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;
import com.sun.jersey.core.util.MultivaluedMapImpl;

public class EventAnalysisSummaryWithPreparedRawTablesManufacturerTest extends BaseDataIntegrityTest<ManufacturerEventAnalysisSummaryResult> {

    private final EventAnalysisService eventAnalysisService = new EventAnalysisService();
    private final List<String> tempTables = new ArrayList<String>();
    private final List<String> columnsInRawTables = new ArrayList<String>();
    private final List<String> columnsInAggregationTables = new ArrayList<String>();
    private final static String UNKNOWN_TAC = "999999";

    @Before
    public void onSetUp() throws Exception {

        attachDependencies(eventAnalysisService);

        tempTables.add(TEMP_EVENT_E_SGEH_ERR_RAW);
        tempTables.add(TEMP_EVENT_E_SGEH_SUC_RAW);
        tempTables.add(TEMP_EVENT_E_LTE_ERR_RAW);
        tempTables.add(TEMP_EVENT_E_LTE_SUC_RAW);

        columnsInRawTables.add(EVENT_ID);
        columnsInRawTables.add(TAC);
        columnsInRawTables.add(IMSI);
        columnsInRawTables.add(DATETIME_ID);
        columnsInRawTables.add(RAT);
        columnsInRawTables.add(LOCAL_DATE_ID);

        for (final String tempTable : tempTables) {
            createTemporaryTable(tempTable, columnsInRawTables);
        }

        columnsInAggregationTables.add(MANUFACTURER);
        columnsInAggregationTables.add(EVENT_ID);
        columnsInAggregationTables.add(NO_OF_SUCCESSES);
        columnsInAggregationTables.add(NO_OF_ERRORS);
        columnsInAggregationTables.add(DATETIME_ID);
        columnsInAggregationTables.add(TAC);

        createTemporaryTable(TEMP_EVENT_E_LTE_MANUF_TAC_EVENTID_SUC_15MIN, columnsInAggregationTables);
        createTemporaryTable(TEMP_EVENT_E_SGEH_MANUF_TAC_EVENTID_SUC_15MIN, columnsInAggregationTables);
        createTemporaryTable(TEMP_EVENT_E_LTE_MANUF_TAC_EVENTID_ERR_15MIN, columnsInAggregationTables);
        createTemporaryTable(TEMP_EVENT_E_SGEH_MANUF_TAC_EVENTID_ERR_15MIN, columnsInAggregationTables);

        createTemporaryTable(TEMP_EVENT_E_LTE_MANUF_TAC_EVENTID_SUC_DAY, columnsInAggregationTables);
        createTemporaryTable(TEMP_EVENT_E_SGEH_MANUF_TAC_EVENTID_SUC_DAY, columnsInAggregationTables);
        createTemporaryTable(TEMP_EVENT_E_LTE_MANUF_TAC_EVENTID_ERR_DAY, columnsInAggregationTables);
        createTemporaryTable(TEMP_EVENT_E_SGEH_MANUF_TAC_EVENTID_ERR_DAY, columnsInAggregationTables);
        jndiProperties.setUpJNDIPropertiesForTest();
    }

    @Test
    public void testGetSummaryDataWhenOnlyOneSuccessEventExists() throws Exception {
        populateTemporaryTablesWithJustOneSuccessEvent();
        final String json = getData(ONE_HOUR, SONY_ERICSSON);
        validateOneEventReturned(json);
    }

    private void validateOneEventReturned(final String json) throws Exception {
        final List<ManufacturerEventAnalysisSummaryResult> summaryResult = getTranslator().translateResult(json,
                ManufacturerEventAnalysisSummaryResult.class);
        assertThat(summaryResult.size(), is(1));

        final ManufacturerEventAnalysisSummaryResult firstResult = summaryResult.get(0);
        assertThat(firstResult.getManufacturer(), is(SONY_ERICSSON));
        assertThat(firstResult.getEventId(), is((ATTACH_IN_4G)));
        assertThat(firstResult.getEventIdDesc(), is(L_ATTACH));
        assertThat(firstResult.getErrorCount(), is((0)));
        assertThat(firstResult.getSuccessCount(), is((1)));
        assertThat(firstResult.getOccurrences(), is((1)));
        assertThat(firstResult.getSuccessRatio(), is(firstResult.getExpectedSuccessRatio()));
        assertThat(firstResult.getErrorSubscriberCount(), is((0)));

    }

    private void populateTemporaryTablesWithJustOneSuccessEvent() throws SQLException {
        final String dateTime = DateTimeUtilities.getDateTime(Calendar.MINUTE, -45);
        insertRow(TEMP_EVENT_E_LTE_SUC_RAW, ATTACH_IN_4G, SONY_ERICSSON_TAC, 12345677, RAT_FOR_LTE, dateTime);

    }

    @Test
    public void testGetSummaryData_Manufacturer_OneHour() throws Exception {
        final int amountOfTime = -45;
        jndiProperties.setUpDataTieringJNDIProperty();
        populateTemporaryTablesWithFullDataSet(amountOfTime);
        final String json = getData(ONE_HOUR, SONY_ERICSSON);
        validateResultForFullDataSetForRaw(json);
        jndiProperties.setUpJNDIPropertiesForTest();
    }

    @Test
    public void testGetSummaryData_Manufacturer_SixHour() throws Exception {
        final int amountOfTime = -345;
        jndiProperties.setUpDataTieringJNDIProperty();
        populateTemporaryTablesWithFullDataSet(amountOfTime);
        final String json = getData(SIX_HOURS, SONY_ERICSSON);
        validateResultForFullDataSetFor15Min(json);
        jndiProperties.setUpJNDIPropertiesForTest();
    }

    @Test
    public void testGetSummaryData_Manufacturer_OneWeek() throws Exception {
        final int amountOfTime = -10062;
        jndiProperties.setUpDataTieringJNDIProperty();
        populateTemporaryTablesWithFullDataSet(amountOfTime);
        final String json = getData(ONE_WEEK, SONY_ERICSSON);
        validateResultForFullDataSetForDay(json);
        jndiProperties.setUpJNDIPropertiesForTest();
    }

    @Test
    public void testGetSummaryData_Manufacturer_TACExclusion() throws Exception {
        final int amountOfTime = -45;
        populateTemporaryTablesWithFullDataSet(amountOfTime);
        populateGroupTableWithExTACGroup();
        final String json = getData(ONE_HOUR, SONY_ERICSSON);
        validateResultIsEmpty(json);
    }

    @Test
    public void testGetSummaryData_Manufacturer_OneHour_ForUnknownTac() throws Exception {
        final int amountOfTime = -45;
        jndiProperties.setUpDataTieringJNDIProperty();
        populateTemporaryTablesWithFullDataSetForUnknownTac(amountOfTime);
        final String json = getData(ONE_HOUR, UNKNOWN_TAC);
        validateResultForFullDataSetForRawForUnknownTac(json);
        jndiProperties.setUpJNDIPropertiesForTest();
    }

    @Test
    public void testGetSummaryData_Manufacturer_SixHour_ForUnknownTac() throws Exception {
        final int amountOfTime = -345;
        jndiProperties.setUpDataTieringJNDIProperty();
        populateTemporaryTablesWithFullDataSetForUnknownTac(amountOfTime);
        final String json = getData(SIX_HOURS, UNKNOWN_TAC);
        validateResultForFullDataSetFor15MinForUnknownTac(json);
        jndiProperties.setUpJNDIPropertiesForTest();
    }

    @Test
    public void testGetSummaryData_Manufacturer_OneWeek_ForUnknownTac() throws Exception {
        final int amountOfTime = -10062;
        jndiProperties.setUpDataTieringJNDIProperty();
        populateTemporaryTablesWithFullDataSetForUnknownTac(amountOfTime);
        final String json = getData(ONE_WEEK, UNKNOWN_TAC);
        validateResultForFullDataSetForDayForUnknownTac(json);
        jndiProperties.setUpJNDIPropertiesForTest();
    }

    private void validateResultIsEmpty(final String json) throws Exception {
        final List<ManufacturerEventAnalysisSummaryResult> summaryResult = getTranslator().translateResult(json,
                ManufacturerEventAnalysisSummaryResult.class);
        assertThat(summaryResult.size(), is(0));
    }

    private void populateGroupTableWithExTACGroup() throws SQLException {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(GROUP_NAME, EXCLUSIVE_TAC_GROUP);
        values.put(TAC, SONY_ERICSSON_TAC);
        insertRow(TEMP_GROUP_TYPE_E_TAC, values);
    }

    private String getData(final String timeParameter, final String manufacturer) throws Exception {
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(TIME_QUERY_PARAM, timeParameter);
        map.putSingle(MAN_PARAM, manufacturer);
        map.putSingle(TYPE_PARAM, TYPE_MAN);
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, "500");

        final String json = getData(eventAnalysisService, map);
        System.out.println(json);
        return json;
    }

    private void validateResultForFullDataSetForRaw(final String json) throws Exception {
        final List<ManufacturerEventAnalysisSummaryResult> summaryResult = getTranslator().translateResult(json,
                ManufacturerEventAnalysisSummaryResult.class);
        assertThat(summaryResult.size(), is(3));

        final ManufacturerEventAnalysisSummaryResult firstResult = summaryResult.get(0);
        assertThat(firstResult.getManufacturer(), is(SONY_ERICSSON));
        assertThat(firstResult.getEventId(), is((SERVICE_REQUEST_IN_2G_AND_3G)));
        assertThat(firstResult.getEventIdDesc(), is(SERVICE_REQUEST));
        assertThat(firstResult.getErrorCount(), is((2)));
        assertThat(firstResult.getSuccessCount(), is((15)));
        assertThat(firstResult.getOccurrences(), is((17)));
        assertThat(firstResult.getSuccessRatio(), is(firstResult.getExpectedSuccessRatio()));
        assertThat(firstResult.getErrorSubscriberCount(), is((2)));

        final ManufacturerEventAnalysisSummaryResult thirdResult = summaryResult.get(1);
        assertThat(thirdResult.getManufacturer(), is(SONY_ERICSSON));
        assertThat(thirdResult.getEventId(), is((ATTACH_IN_4G)));
        assertThat(thirdResult.getEventIdDesc(), is(L_ATTACH));
        assertThat(thirdResult.getErrorCount(), is((1)));
        assertThat(thirdResult.getSuccessCount(), is((10)));
        assertThat(thirdResult.getOccurrences(), is((11)));
        assertThat(thirdResult.getSuccessRatio(), is(thirdResult.getExpectedSuccessRatio()));
        assertThat(thirdResult.getErrorSubscriberCount(), is((1)));

        final ManufacturerEventAnalysisSummaryResult secondResult = summaryResult.get(2);
        assertThat(secondResult.getManufacturer(), is(SONY_ERICSSON));
        assertThat(secondResult.getEventId(), is((SERVICE_REQUEST_IN_4G)));
        assertThat(secondResult.getEventIdDesc(), is(L_SERVICE_REQUEST));
        assertThat(secondResult.getErrorCount(), is((1)));
        assertThat(secondResult.getSuccessCount(), is((0)));
        assertThat(secondResult.getOccurrences(), is((1)));
        assertThat(secondResult.getSuccessRatio(), is(secondResult.getExpectedSuccessRatio()));
        assertThat(secondResult.getErrorSubscriberCount(), is((1)));
    }

    private void validateResultForFullDataSetForRawForUnknownTac(final String json) throws Exception {
        final List<ManufacturerEventAnalysisSummaryResult> summaryResult = getTranslator().translateResult(json,
                ManufacturerEventAnalysisSummaryResult.class);
        assertThat(summaryResult.size(), is(3));

        final ManufacturerEventAnalysisSummaryResult firstResult = summaryResult.get(0);
        assertThat(firstResult.getManufacturer(), is(UNKNOWN_TAC));
        assertThat(firstResult.getEventId(), is((SERVICE_REQUEST_IN_2G_AND_3G)));
        assertThat(firstResult.getEventIdDesc(), is(SERVICE_REQUEST));
        assertThat(firstResult.getErrorCount(), is((2)));
        assertThat(firstResult.getSuccessCount(), is((15)));
        assertThat(firstResult.getOccurrences(), is((17)));
        assertThat(firstResult.getSuccessRatio(), is(firstResult.getExpectedSuccessRatio()));
        assertThat(firstResult.getErrorSubscriberCount(), is((2)));

        final ManufacturerEventAnalysisSummaryResult thirdResult = summaryResult.get(1);
        assertThat(thirdResult.getManufacturer(), is(UNKNOWN_TAC));
        assertThat(thirdResult.getEventId(), is((ATTACH_IN_4G)));
        assertThat(thirdResult.getEventIdDesc(), is(L_ATTACH));
        assertThat(thirdResult.getErrorCount(), is((1)));
        assertThat(thirdResult.getSuccessCount(), is((10)));
        assertThat(thirdResult.getOccurrences(), is((11)));
        assertThat(thirdResult.getSuccessRatio(), is(thirdResult.getExpectedSuccessRatio()));
        assertThat(thirdResult.getErrorSubscriberCount(), is((1)));

        final ManufacturerEventAnalysisSummaryResult secondResult = summaryResult.get(2);
        assertThat(secondResult.getManufacturer(), is(UNKNOWN_TAC));
        assertThat(secondResult.getEventId(), is((SERVICE_REQUEST_IN_4G)));
        assertThat(secondResult.getEventIdDesc(), is(L_SERVICE_REQUEST));
        assertThat(secondResult.getErrorCount(), is((1)));
        assertThat(secondResult.getSuccessCount(), is((0)));
        assertThat(secondResult.getOccurrences(), is((1)));
        assertThat(secondResult.getSuccessRatio(), is(secondResult.getExpectedSuccessRatio()));
        assertThat(secondResult.getErrorSubscriberCount(), is((1)));
    }

    private void validateResultForFullDataSetFor15Min(final String json) throws Exception {
        final List<ManufacturerEventAnalysisSummaryResult> summaryResult = getTranslator().translateResult(json,
                ManufacturerEventAnalysisSummaryResult.class);
        assertThat(summaryResult.size(), is(3));

        final ManufacturerEventAnalysisSummaryResult firstResult = summaryResult.get(0);
        assertThat(firstResult.getManufacturer(), is(SONY_ERICSSON));
        assertThat(firstResult.getEventId(), is((SERVICE_REQUEST_IN_2G_AND_3G)));
        assertThat(firstResult.getEventIdDesc(), is(SERVICE_REQUEST));
        assertThat(firstResult.getErrorCount(), is((7)));
        assertThat(firstResult.getSuccessCount(), is((15)));
        assertThat(firstResult.getOccurrences(), is((22)));
        assertThat(firstResult.getSuccessRatio(), is(firstResult.getExpectedSuccessRatio()));
        assertThat(firstResult.getErrorSubscriberCount(), is((2)));

        final ManufacturerEventAnalysisSummaryResult thirdResult = summaryResult.get(1);
        assertThat(thirdResult.getManufacturer(), is(SONY_ERICSSON));
        assertThat(thirdResult.getEventId(), is((ATTACH_IN_4G)));
        assertThat(thirdResult.getEventIdDesc(), is(L_ATTACH));
        assertThat(thirdResult.getErrorCount(), is((6)));
        assertThat(thirdResult.getSuccessCount(), is((10)));
        assertThat(thirdResult.getOccurrences(), is((16)));
        assertThat(thirdResult.getSuccessRatio(), is(thirdResult.getExpectedSuccessRatio()));
        assertThat(thirdResult.getErrorSubscriberCount(), is((1)));

        final ManufacturerEventAnalysisSummaryResult secondResult = summaryResult.get(2);
        assertThat(secondResult.getManufacturer(), is(SONY_ERICSSON));
        assertThat(secondResult.getEventId(), is((SERVICE_REQUEST_IN_4G)));
        assertThat(secondResult.getEventIdDesc(), is(L_SERVICE_REQUEST));
        assertThat(secondResult.getErrorCount(), is((2)));
        assertThat(secondResult.getSuccessCount(), is((0)));
        assertThat(secondResult.getOccurrences(), is((2)));
        assertThat(secondResult.getSuccessRatio(), is(secondResult.getExpectedSuccessRatio()));
        assertThat(secondResult.getErrorSubscriberCount(), is((1)));
    }

    private void validateResultForFullDataSetFor15MinForUnknownTac(final String json) throws Exception {
        final List<ManufacturerEventAnalysisSummaryResult> summaryResult = getTranslator().translateResult(json,
                ManufacturerEventAnalysisSummaryResult.class);
        assertThat(summaryResult.size(), is(3));

        final ManufacturerEventAnalysisSummaryResult firstResult = summaryResult.get(0);
        assertThat(firstResult.getManufacturer(), is(UNKNOWN_TAC));
        assertThat(firstResult.getEventId(), is((SERVICE_REQUEST_IN_2G_AND_3G)));
        assertThat(firstResult.getEventIdDesc(), is(SERVICE_REQUEST));
        assertThat(firstResult.getErrorCount(), is((7)));
        assertThat(firstResult.getSuccessCount(), is((15)));
        assertThat(firstResult.getOccurrences(), is((22)));
        assertThat(firstResult.getSuccessRatio(), is(firstResult.getExpectedSuccessRatio()));
        assertThat(firstResult.getErrorSubscriberCount(), is((2)));

        final ManufacturerEventAnalysisSummaryResult thirdResult = summaryResult.get(1);
        assertThat(thirdResult.getManufacturer(), is(UNKNOWN_TAC));
        assertThat(thirdResult.getEventId(), is((ATTACH_IN_4G)));
        assertThat(thirdResult.getEventIdDesc(), is(L_ATTACH));
        assertThat(thirdResult.getErrorCount(), is((6)));
        assertThat(thirdResult.getSuccessCount(), is((10)));
        assertThat(thirdResult.getOccurrences(), is((16)));
        assertThat(thirdResult.getSuccessRatio(), is(thirdResult.getExpectedSuccessRatio()));
        assertThat(thirdResult.getErrorSubscriberCount(), is((1)));

        final ManufacturerEventAnalysisSummaryResult secondResult = summaryResult.get(2);
        assertThat(secondResult.getManufacturer(), is(UNKNOWN_TAC));
        assertThat(secondResult.getEventId(), is((SERVICE_REQUEST_IN_4G)));
        assertThat(secondResult.getEventIdDesc(), is(L_SERVICE_REQUEST));
        assertThat(secondResult.getErrorCount(), is((2)));
        assertThat(secondResult.getSuccessCount(), is((0)));
        assertThat(secondResult.getOccurrences(), is((2)));
        assertThat(secondResult.getSuccessRatio(), is(secondResult.getExpectedSuccessRatio()));
        assertThat(secondResult.getErrorSubscriberCount(), is((1)));
    }

    private void validateResultForFullDataSetForDay(final String json) throws Exception {
        final List<ManufacturerEventAnalysisSummaryResult> summaryResult = getTranslator().translateResult(json,
                ManufacturerEventAnalysisSummaryResult.class);
        assertThat(summaryResult.size(), is(3));

        final ManufacturerEventAnalysisSummaryResult firstResult = summaryResult.get(0);
        assertThat(firstResult.getManufacturer(), is(SONY_ERICSSON));
        assertThat(firstResult.getEventId(), is((SERVICE_REQUEST_IN_2G_AND_3G)));
        assertThat(firstResult.getEventIdDesc(), is(SERVICE_REQUEST));
        assertThat(firstResult.getErrorCount(), is((8)));
        assertThat(firstResult.getSuccessCount(), is((16)));
        assertThat(firstResult.getOccurrences(), is((24)));
        assertThat(firstResult.getSuccessRatio(), is(firstResult.getExpectedSuccessRatio()));
        assertThat(firstResult.getErrorSubscriberCount(), is((2)));

        final ManufacturerEventAnalysisSummaryResult thirdResult = summaryResult.get(1);
        assertThat(thirdResult.getManufacturer(), is(SONY_ERICSSON));
        assertThat(thirdResult.getEventId(), is((ATTACH_IN_4G)));
        assertThat(thirdResult.getEventIdDesc(), is(L_ATTACH));
        assertThat(thirdResult.getErrorCount(), is((7)));
        assertThat(thirdResult.getSuccessCount(), is((11)));
        assertThat(thirdResult.getOccurrences(), is((18)));
        assertThat(thirdResult.getSuccessRatio(), is(thirdResult.getExpectedSuccessRatio()));
        assertThat(thirdResult.getErrorSubscriberCount(), is((1)));

        final ManufacturerEventAnalysisSummaryResult secondResult = summaryResult.get(2);
        assertThat(secondResult.getManufacturer(), is(SONY_ERICSSON));
        assertThat(secondResult.getEventId(), is((SERVICE_REQUEST_IN_4G)));
        assertThat(secondResult.getEventIdDesc(), is(L_SERVICE_REQUEST));
        assertThat(secondResult.getErrorCount(), is((3)));
        assertThat(secondResult.getSuccessCount(), is((0)));
        assertThat(secondResult.getOccurrences(), is((3)));
        assertThat(secondResult.getSuccessRatio(), is(secondResult.getExpectedSuccessRatio()));
        assertThat(secondResult.getErrorSubscriberCount(), is((1)));
    }

    private void validateResultForFullDataSetForDayForUnknownTac(final String json) throws Exception {
        final List<ManufacturerEventAnalysisSummaryResult> summaryResult = getTranslator().translateResult(json,
                ManufacturerEventAnalysisSummaryResult.class);
        assertThat(summaryResult.size(), is(3));

        final ManufacturerEventAnalysisSummaryResult firstResult = summaryResult.get(0);
        assertThat(firstResult.getManufacturer(), is(UNKNOWN_TAC));
        assertThat(firstResult.getEventId(), is((SERVICE_REQUEST_IN_2G_AND_3G)));
        assertThat(firstResult.getEventIdDesc(), is(SERVICE_REQUEST));
        assertThat(firstResult.getErrorCount(), is((8)));
        assertThat(firstResult.getSuccessCount(), is((16)));
        assertThat(firstResult.getOccurrences(), is((24)));
        assertThat(firstResult.getSuccessRatio(), is(firstResult.getExpectedSuccessRatio()));
        assertThat(firstResult.getErrorSubscriberCount(), is((2)));

        final ManufacturerEventAnalysisSummaryResult thirdResult = summaryResult.get(1);
        assertThat(thirdResult.getManufacturer(), is(UNKNOWN_TAC));
        assertThat(thirdResult.getEventId(), is((ATTACH_IN_4G)));
        assertThat(thirdResult.getEventIdDesc(), is(L_ATTACH));
        assertThat(thirdResult.getErrorCount(), is((7)));
        assertThat(thirdResult.getSuccessCount(), is((11)));
        assertThat(thirdResult.getOccurrences(), is((18)));
        assertThat(thirdResult.getSuccessRatio(), is(thirdResult.getExpectedSuccessRatio()));
        assertThat(thirdResult.getErrorSubscriberCount(), is((1)));

        final ManufacturerEventAnalysisSummaryResult secondResult = summaryResult.get(2);
        assertThat(secondResult.getManufacturer(), is(UNKNOWN_TAC));
        assertThat(secondResult.getEventId(), is((SERVICE_REQUEST_IN_4G)));
        assertThat(secondResult.getEventIdDesc(), is(L_SERVICE_REQUEST));
        assertThat(secondResult.getErrorCount(), is((3)));
        assertThat(secondResult.getSuccessCount(), is((0)));
        assertThat(secondResult.getOccurrences(), is((3)));
        assertThat(secondResult.getSuccessRatio(), is(secondResult.getExpectedSuccessRatio()));
        assertThat(secondResult.getErrorSubscriberCount(), is((1)));
    }

    private void populateTemporaryTablesWithFullDataSet(final int amountOfTime) throws SQLException {
        final String dateTime = DateTimeUtilities.getDateTime(Calendar.MINUTE, amountOfTime);

        insertRow(TEMP_EVENT_E_SGEH_ERR_RAW, SERVICE_REQUEST_IN_2G_AND_3G, SONY_ERICSSON_TAC, 12345671, RAT_FOR_GSM, dateTime);
        insertRow(TEMP_EVENT_E_SGEH_ERR_RAW, SERVICE_REQUEST_IN_2G_AND_3G, SONY_ERICSSON_TAC, 12345672, RAT_FOR_GSM, dateTime);

        insertRow(TEMP_EVENT_E_SGEH_SUC_RAW, ACTIVATE_IN_2G_AND_3G, SONY_ERICSSON_TAC, 12345673, RAT_FOR_GSM, dateTime);
        insertRow(TEMP_EVENT_E_SGEH_SUC_RAW, SERVICE_REQUEST_IN_2G_AND_3G, SONY_ERICSSON_TAC, 12345674, RAT_FOR_GSM, dateTime);
        insertRow(TEMP_EVENT_E_SGEH_SUC_RAW, SERVICE_REQUEST_IN_2G_AND_3G, SONY_ERICSSON_TAC, 12345674, RAT_FOR_GSM, dateTime);

        insertRow(TEMP_EVENT_E_LTE_ERR_RAW, ATTACH_IN_4G, SONY_ERICSSON_TAC, 12345675, RAT_FOR_LTE, dateTime);
        insertRow(TEMP_EVENT_E_LTE_ERR_RAW, SERVICE_REQUEST_IN_4G, SONY_ERICSSON_TAC, 12345676, RAT_FOR_LTE, dateTime);

        insertRow(TEMP_EVENT_E_LTE_SUC_RAW, ATTACH_IN_4G, SONY_ERICSSON_TAC, 12345677, RAT_FOR_LTE, dateTime);
        insertRow(TEMP_EVENT_E_LTE_SUC_RAW, ATTACH_IN_4G, SONY_ERICSSON_TAC, 12345678, RAT_FOR_LTE, dateTime);
        insertRow(TEMP_EVENT_E_LTE_SUC_RAW, SERVICE_REQUEST_IN_4G, SONY_ERICSSON_TAC, 12345676, RAT_FOR_LTE, dateTime);

        //15minute tables
        insertRowIntoAggregation(TEMP_EVENT_E_LTE_MANUF_TAC_EVENTID_SUC_15MIN, SONY_ERICSSON, ATTACH_IN_4G, 10, 0, dateTime, SONY_ERICSSON_TAC);
        insertRowIntoAggregation(TEMP_EVENT_E_SGEH_MANUF_TAC_EVENTID_SUC_15MIN, SONY_ERICSSON, SERVICE_REQUEST_IN_2G_AND_3G, 15, 0, dateTime,
                SONY_ERICSSON_TAC);
        insertRowIntoAggregation(TEMP_EVENT_E_LTE_MANUF_TAC_EVENTID_ERR_15MIN, SONY_ERICSSON, ATTACH_IN_4G, 0, 6, dateTime, SONY_ERICSSON_TAC);
        insertRowIntoAggregation(TEMP_EVENT_E_SGEH_MANUF_TAC_EVENTID_ERR_15MIN, SONY_ERICSSON, SERVICE_REQUEST_IN_2G_AND_3G, 0, 7, dateTime,
                SONY_ERICSSON_TAC);
        insertRowIntoAggregation(TEMP_EVENT_E_LTE_MANUF_TAC_EVENTID_ERR_15MIN, SONY_ERICSSON, SERVICE_REQUEST_IN_4G, 0, 2, dateTime,
                SONY_ERICSSON_TAC);

        //EXCLUSIVE_TAC
        insertRowIntoAggregation(TEMP_EVENT_E_SGEH_MANUF_TAC_EVENTID_SUC_15MIN, SONY_ERICSSON, SERVICE_REQUEST_IN_2G_AND_3G, 15, 0, dateTime,
                SAMPLE_EXCLUSIVE_TAC);

        //DAY tables
        insertRowIntoAggregation(TEMP_EVENT_E_LTE_MANUF_TAC_EVENTID_SUC_DAY, SONY_ERICSSON, ATTACH_IN_4G, 11, 0, dateTime, SONY_ERICSSON_TAC);
        insertRowIntoAggregation(TEMP_EVENT_E_SGEH_MANUF_TAC_EVENTID_SUC_DAY, SONY_ERICSSON, SERVICE_REQUEST_IN_2G_AND_3G, 16, 0, dateTime,
                SONY_ERICSSON_TAC);
        insertRowIntoAggregation(TEMP_EVENT_E_LTE_MANUF_TAC_EVENTID_ERR_DAY, SONY_ERICSSON, ATTACH_IN_4G, 0, 7, dateTime, SONY_ERICSSON_TAC);
        insertRowIntoAggregation(TEMP_EVENT_E_SGEH_MANUF_TAC_EVENTID_ERR_DAY, SONY_ERICSSON, SERVICE_REQUEST_IN_2G_AND_3G, 0, 8, dateTime,
                SONY_ERICSSON_TAC);
        insertRowIntoAggregation(TEMP_EVENT_E_LTE_MANUF_TAC_EVENTID_ERR_DAY, SONY_ERICSSON, SERVICE_REQUEST_IN_4G, 0, 3, dateTime, SONY_ERICSSON_TAC);
    }

    private void populateTemporaryTablesWithFullDataSetForUnknownTac(final int amountOfTime) throws SQLException {
        final String dateTime = DateTimeUtilities.getDateTime(Calendar.MINUTE, amountOfTime);

        insertRow(TEMP_EVENT_E_SGEH_ERR_RAW, SERVICE_REQUEST_IN_2G_AND_3G, Integer.parseInt(UNKNOWN_TAC), 12345671, RAT_FOR_GSM, dateTime);
        insertRow(TEMP_EVENT_E_SGEH_ERR_RAW, SERVICE_REQUEST_IN_2G_AND_3G, Integer.parseInt(UNKNOWN_TAC), 12345672, RAT_FOR_GSM, dateTime);

        insertRow(TEMP_EVENT_E_SGEH_SUC_RAW, ACTIVATE_IN_2G_AND_3G, Integer.parseInt(UNKNOWN_TAC), 12345673, RAT_FOR_GSM, dateTime);
        insertRow(TEMP_EVENT_E_SGEH_SUC_RAW, SERVICE_REQUEST_IN_2G_AND_3G, Integer.parseInt(UNKNOWN_TAC), 12345674, RAT_FOR_GSM, dateTime);
        insertRow(TEMP_EVENT_E_SGEH_SUC_RAW, SERVICE_REQUEST_IN_2G_AND_3G, Integer.parseInt(UNKNOWN_TAC), 12345674, RAT_FOR_GSM, dateTime);

        insertRow(TEMP_EVENT_E_LTE_ERR_RAW, ATTACH_IN_4G, Integer.parseInt(UNKNOWN_TAC), 12345675, RAT_FOR_LTE, dateTime);
        insertRow(TEMP_EVENT_E_LTE_ERR_RAW, SERVICE_REQUEST_IN_4G, Integer.parseInt(UNKNOWN_TAC), 12345676, RAT_FOR_LTE, dateTime);

        insertRow(TEMP_EVENT_E_LTE_SUC_RAW, ATTACH_IN_4G, Integer.parseInt(UNKNOWN_TAC), 12345677, RAT_FOR_LTE, dateTime);
        insertRow(TEMP_EVENT_E_LTE_SUC_RAW, ATTACH_IN_4G, Integer.parseInt(UNKNOWN_TAC), 12345678, RAT_FOR_LTE, dateTime);
        insertRow(TEMP_EVENT_E_LTE_SUC_RAW, SERVICE_REQUEST_IN_4G, Integer.parseInt(UNKNOWN_TAC), 12345676, RAT_FOR_LTE, dateTime);

        //15minute tables
        insertRowIntoAggregation(TEMP_EVENT_E_LTE_MANUF_TAC_EVENTID_SUC_15MIN, SONY_ERICSSON, ATTACH_IN_4G, 10, 0, dateTime,
                Integer.parseInt(UNKNOWN_TAC));
        insertRowIntoAggregation(TEMP_EVENT_E_SGEH_MANUF_TAC_EVENTID_SUC_15MIN, SONY_ERICSSON, SERVICE_REQUEST_IN_2G_AND_3G, 15, 0, dateTime,
                Integer.parseInt(UNKNOWN_TAC));
        insertRowIntoAggregation(TEMP_EVENT_E_LTE_MANUF_TAC_EVENTID_ERR_15MIN, SONY_ERICSSON, ATTACH_IN_4G, 0, 6, dateTime,
                Integer.parseInt(UNKNOWN_TAC));
        insertRowIntoAggregation(TEMP_EVENT_E_SGEH_MANUF_TAC_EVENTID_ERR_15MIN, SONY_ERICSSON, SERVICE_REQUEST_IN_2G_AND_3G, 0, 7, dateTime,
                Integer.parseInt(UNKNOWN_TAC));
        insertRowIntoAggregation(TEMP_EVENT_E_LTE_MANUF_TAC_EVENTID_ERR_15MIN, SONY_ERICSSON, SERVICE_REQUEST_IN_4G, 0, 2, dateTime,
                Integer.parseInt(UNKNOWN_TAC));

        //DAY tables
        insertRowIntoAggregation(TEMP_EVENT_E_LTE_MANUF_TAC_EVENTID_SUC_DAY, SONY_ERICSSON, ATTACH_IN_4G, 11, 0, dateTime,
                Integer.parseInt(UNKNOWN_TAC));
        insertRowIntoAggregation(TEMP_EVENT_E_SGEH_MANUF_TAC_EVENTID_SUC_DAY, SONY_ERICSSON, SERVICE_REQUEST_IN_2G_AND_3G, 16, 0, dateTime,
                Integer.parseInt(UNKNOWN_TAC));
        insertRowIntoAggregation(TEMP_EVENT_E_LTE_MANUF_TAC_EVENTID_ERR_DAY, SONY_ERICSSON, ATTACH_IN_4G, 0, 7, dateTime,
                Integer.parseInt(UNKNOWN_TAC));
        insertRowIntoAggregation(TEMP_EVENT_E_SGEH_MANUF_TAC_EVENTID_ERR_DAY, SONY_ERICSSON, SERVICE_REQUEST_IN_2G_AND_3G, 0, 8, dateTime,
                Integer.parseInt(UNKNOWN_TAC));
        insertRowIntoAggregation(TEMP_EVENT_E_LTE_MANUF_TAC_EVENTID_ERR_DAY, SONY_ERICSSON, SERVICE_REQUEST_IN_4G, 0, 3, dateTime,
                Integer.parseInt(UNKNOWN_TAC));
    }

    private void insertRow(final String table, final int eventId, final int tac, final int imsi, final int rat, final String dateTime)
            throws SQLException {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(EVENT_ID, eventId);
        values.put(TAC, tac);
        values.put(IMSI, imsi);
        values.put(RAT, rat);
        values.put(DATETIME_ID, dateTime);
        values.put(LOCAL_DATE_ID, dateTime.substring(0, 10));
        insertRow(table, values);
    }

    private void insertRowIntoAggregation(final String table, final String manufacturer, final int eventId, final int noOfSuccesses,
                                          final int noOfErrors, final String dateTime, final int tac) throws SQLException {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(MANUFACTURER, manufacturer);
        values.put(EVENT_ID, eventId);
        values.put(NO_OF_SUCCESSES, noOfSuccesses);
        values.put(NO_OF_ERRORS, noOfErrors);
        values.put(DATETIME_ID, dateTime);
        values.put(TAC, tac);
        insertRow(table, values);
    }

}
