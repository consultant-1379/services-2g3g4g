/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.integritytests.eventvolume;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.*;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.*;

import java.sql.SQLException;
import java.util.*;

import org.junit.Test;

/**
 * @author ejoegaf
 * @since 2011
 *
 */
public class EventVolumeBSCWithPreparedTablesTest extends BaseEventVolumeWithPreparedTablesTest {

    @Test
    public void testGetEventVolumeData_5Minutes() throws Exception {
        populateTemporaryTablesWithFiveMinDataSet();
        final String result = getBSCData(FIVE_MINUTES);
        validateResults(result);
    }

    @Test
    public void testGetEventVolumeData_5Minutes_WithTACExclusion() throws Exception {
        populateTemporaryTablesWithFiveMinDataSet();
        populateGroupTableWithExclusiveTAC();
        final String result = getBSCData(FIVE_MINUTES);
        validateEmptyResult(result);
    }

    @Test
    public void testGetEventVolumeData_OneWeek_WithTACExclusion() throws Exception {
        populateTemporaryTablesWithOneWeekDataSet();
        populateGroupTableWithExclusiveTAC();
        final String result = getBSCData(ONE_WEEK, POSITIVE_TIMEZONE_0800_SPACE_PREFIX);
        validateEmptyResult(result);
    }

    @Test
    public void testGetEventVolumeData_30Minutes() throws Exception {
        populateTemporaryTablesWithThirtyMinDataSet();
        final String result = getBSCData(THIRTY_MINUTES);
        validateResults(result);
    }

    @Test
    public void testGetEventVolumnDataWithDataTiering_30Minutes() throws Exception {
        jndiProperties.setUpDataTieringJNDIProperty();
        populateTemporaryTablesWithThirtyMinDataSet();
        final String result = getBSCData(THIRTY_MINUTES);
        System.err.println(result);
        validateResults(result);
        jndiProperties.setUpJNDIPropertiesForTest();
    }

    @Test
    public void test_getEventVolumeData_1Week_PositiveTimezone1130() throws Exception {
        populateTemporaryTables_OneDayAgg();
        final String result = getBSCData(ONE_WEEK, POSITIVE_TIMEZONE_1130_SPACE_PREFIX);
        validateTime(result);
    }

    @Test
    public void test_getEventVolumeData_1Week_PositiveTimezone0800() throws Exception {
        populateTemporaryTables_OneDayAgg();
        final String result = getBSCData(ONE_WEEK, POSITIVE_TIMEZONE_0800_SPACE_PREFIX);
        validateTime(result);
    }

    @Test
    public void test_getEventVolumeData_1Week_NeutralTimezone() throws Exception {
        populateTemporaryTables_OneDayAgg();
        final String result = getBSCData(ONE_WEEK, NEUTRAL_TIMEZONE_0000_SPACE_PREFIX);
        validateTime(result);
    }

    @Test(expected = Exception.class)
    public void test_getEventVolumeData_1Week_InvalidSign() throws Exception {
        getBSCData(ONE_WEEK, TIMEZONE_INVALID_SIGN);
    }

    private String getBSCData(final String time) {
        return getData(time, TYPE_BSC, "BSC1,Ericsson,GSM");
    }

    private String getBSCData(final String time, final String tzOffset) {
        return getData(time, TYPE_BSC, "BSC1,Ericsson,GSM", tzOffset);
    }

    @Test
    public void test_getGroupEventVolumeData_1Week_PositiveTimezone1130() throws Exception {
        populateTemporaryTables_OneDayAgg();
        createAndPopulateGroupTable(TEMP_GROUP_TYPE_E_RAT_VEND_HIER3, SAMPLE_HIERARCHY_3);
        final String result = getBSCGroupData(ONE_WEEK, POSITIVE_TIMEZONE_1130_SPACE_PREFIX);
        validateTime(result);
    }

    @Test
    public void test_getGroupEventVolumeData_1Week_PositiveTimezone0800() throws Exception {
        populateTemporaryTables_OneDayAgg();
        createAndPopulateGroupTable(TEMP_GROUP_TYPE_E_RAT_VEND_HIER3, SAMPLE_HIERARCHY_3);
        final String result = getBSCGroupData(ONE_WEEK, POSITIVE_TIMEZONE_0800_SPACE_PREFIX);
        validateTime(result);
    }

    @Test
    public void test_getGroupEventVolumeData_1Week_NeutralTimezone() throws Exception {
        populateTemporaryTables_OneDayAgg();
        createAndPopulateGroupTable(TEMP_GROUP_TYPE_E_RAT_VEND_HIER3, SAMPLE_HIERARCHY_3);
        final String result = getBSCGroupData(ONE_WEEK, NEUTRAL_TIMEZONE_0000_SPACE_PREFIX);
        validateTime(result);
    }

    private String getBSCGroupData(final String time, final String tzOffset) {
        return getGroupData(time, TYPE_BSC, SAMPLE_BSC_GROUP, tzOffset);
    }

    @Override
    protected Collection<String> getColumnsForAggTables() {
        final Collection<String> columnsForAggTables1 = new ArrayList<String>();
        columnsForAggTables1.add(EVENT_ID);
        columnsForAggTables1.add(NO_OF_ERRORS);
        columnsForAggTables1.add(NO_OF_SUCCESSES);
        columnsForAggTables1.add(DATETIME_ID);
        columnsForAggTables1.add(RAT);
        columnsForAggTables1.add(VENDOR);
        columnsForAggTables1.add(HIERARCHY_3);

        return columnsForAggTables1;

    }

    @Override
    protected List<String> getAggTables_15min() {

        final List<String> aggTables1 = new ArrayList<String>();
        aggTables1.add(TEMP_EVENT_E_SGEH_VEND_HIER3_EVENTID_ERR_15MIN);
        aggTables1.add(TEMP_EVENT_E_SGEH_VEND_HIER3_EVENTID_SUC_15MIN);
        aggTables1.add(TEMP_EVENT_E_LTE_VEND_HIER3_EVENTID_ERR_15MIN);
        aggTables1.add(TEMP_EVENT_E_LTE_VEND_HIER3_EVENTID_SUC_15MIN);
        return aggTables1;
    }

    @Override
    protected List<String> getAggTables_Day() {

        final List<String> aggTables1 = new ArrayList<String>();
        aggTables1.add(TEMP_EVENT_E_LTE_VEND_HIER3_EVENTID_SUC_DAY);
        aggTables1.add(TEMP_EVENT_E_SGEH_VEND_HIER3_EVENTID_SUC_DAY);
        aggTables1.add(TEMP_EVENT_E_LTE_VEND_HIER3_EVENTID_ERR_DAY);
        aggTables1.add(TEMP_EVENT_E_SGEH_VEND_HIER3_EVENTID_ERR_DAY);
        return aggTables1;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected void insertRowIntoAggTable(final String table, final int eventId, final String dateTime) throws SQLException {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(EVENT_ID, eventId);
        values.put(DATETIME_ID, dateTime);
        values.put(NO_OF_SUCCESSES, ONE_SUCCESS_EVENT);
        values.put(NO_OF_ERRORS, ONE_ERROR_EVENT);
        values.put(RAT, RAT_FOR_GSM);
        values.put(VENDOR, ERICSSON);
        values.put(HIERARCHY_3, "BSC1");
        insertRow(table, values);
    }
}
