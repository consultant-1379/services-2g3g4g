/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.integritytests.eventvolume;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.common.EventIDConstants.*;
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
public class EventVolumeSGSNWithPreparedTablesTest extends BaseEventVolumeWithPreparedTablesTest {

    protected final static int[] sampleEventIDs = { ACTIVATE_IN_2G_AND_3G, DEACTIVATE_IN_2G_AND_3G };

    @Test
    public void testGetEventVolumeData_5Minutes() throws Exception {
        populateTemporaryTablesWithFiveMinDataSet();
        final String result = getSGSNData(FIVE_MINUTES);
        validateResults(result);
    }

    @Test
    public void testGetEventVolumeData_5Minutes_WithTACExclusion() throws Exception {
        populateTemporaryTablesWithFiveMinDataSet();
        populateGroupTableWithExclusiveTAC();
        final String result = getSGSNData(FIVE_MINUTES);
        validateEmptyResult(result);
    }

    @Test
    public void testGetEventVolumeData_30Minutes() throws Exception {
        populateTemporaryTablesWithThirtyMinDataSet();
        final String result = getSGSNData(THIRTY_MINUTES);
        validateResults(result);
    }

    @Test
    public void test_getEventVolumeData_1Week_PositiveTimezone1130() throws Exception {
        populateTemporaryTables_OneDayAgg();
        final String result = getSGSNData(ONE_WEEK, POSITIVE_TIMEZONE_1130_SPACE_PREFIX);
        validateTime(result);
    }

    @Test
    public void test_getEventVolumeData_1Week_PositiveTimezone0800() throws Exception {
        populateTemporaryTables_OneDayAgg();
        final String result = getSGSNData(ONE_WEEK, POSITIVE_TIMEZONE_0800_SPACE_PREFIX);
        validateTime(result);
    }

    @Test
    public void test_getEventVolumeData_1Week_NeutralTimezone() throws Exception {
        populateTemporaryTables_OneDayAgg();
        final String result = getSGSNData(ONE_WEEK, NEUTRAL_TIMEZONE_0000_SPACE_PREFIX);
        validateTime(result);
    }

    @Test(expected = Exception.class)
    public void test_getEventVolumeData_1Week_InvalidSign() throws Exception {
        getSGSNData(ONE_WEEK, TIMEZONE_INVALID_SIGN);
    }

    private String getSGSNData(final String time) {
        return getData(time, TYPE_SGSN, SAMPLE_SGSN);
    }

    private String getSGSNData(final String time, final String tzOffset) {
        return getData(time, TYPE_SGSN, SAMPLE_SGSN, tzOffset);
    }

    @Test
    public void test_getGroupEventVolumeData_1Week_PositiveTimezone1130() throws Exception {
        populateTemporaryTables_OneDayAgg();
        createAndPopulateGroupTable(TEMP_GROUP_TYPE_E_EVNTSRC, SAMPLE_SGSN_GROUP);
        final String result = getSGSNGroupData(ONE_WEEK, POSITIVE_TIMEZONE_1130_SPACE_PREFIX);
        validateTime(result);
    }

    @Test
    public void test_getGroupEventVolumeData_1Week_PositiveTimezone0800() throws Exception {
        populateTemporaryTables_OneDayAgg();
        createAndPopulateGroupTable(TEMP_GROUP_TYPE_E_EVNTSRC, SAMPLE_SGSN_GROUP);
        final String result = getSGSNGroupData(ONE_WEEK, POSITIVE_TIMEZONE_0800_SPACE_PREFIX);
        validateTime(result);
    }

    @Test
    public void test_getGroupEventVolumeData_1Week_NeutralTimezone() throws Exception {
        populateTemporaryTables_OneDayAgg();
        createAndPopulateGroupTable(TEMP_GROUP_TYPE_E_EVNTSRC, SAMPLE_SGSN_GROUP);
        final String result = getSGSNGroupData(ONE_WEEK, NEUTRAL_TIMEZONE_0000_SPACE_PREFIX);
        validateTime(result);
    }

    private String getSGSNGroupData(final String time, final String tzOffset) {
        return getGroupData(time, TYPE_SGSN, SAMPLE_SGSN_GROUP, tzOffset);
    }

    @Override
    protected Collection<String> getColumnsForAggTables() {
        final Collection<String> columnsForAggTables1 = new ArrayList<String>();
        columnsForAggTables1.add(EVENT_ID);
        columnsForAggTables1.add(NO_OF_ERRORS);
        columnsForAggTables1.add(NO_OF_SUCCESSES);
        columnsForAggTables1.add(EVENT_SOURCE_NAME);
        columnsForAggTables1.add(DATETIME_ID);
        return columnsForAggTables1;

    }

    @Override
    protected List<String> getAggTables_15min() {

        final List<String> aggTables1 = new ArrayList<String>();
        aggTables1.add(TEMP_EVENT_E_SGEH_EVNTSRC_EVENTID_ERR_1MIN);
        aggTables1.add(TEMP_EVENT_E_SGEH_EVNTSRC_EVENTID_SUC_1MIN);
        aggTables1.add(TEMP_EVENT_E_LTE_EVNTSRC_EVENTID_ERR_1MIN);
        aggTables1.add(TEMP_EVENT_E_LTE_EVNTSRC_EVENTID_SUC_1MIN);
        return aggTables1;
    }

    @Override
    protected List<String> getAggTables_Day() {

        final List<String> aggTables1 = new ArrayList<String>();
        aggTables1.add(TEMP_EVENT_E_SGEH_EVNTSRC_EVENTID_ERR_DAY);
        aggTables1.add(TEMP_EVENT_E_SGEH_EVNTSRC_EVENTID_SUC_DAY);
        aggTables1.add(TEMP_EVENT_E_LTE_EVNTSRC_EVENTID_ERR_DAY);
        aggTables1.add(TEMP_EVENT_E_LTE_EVNTSRC_EVENTID_SUC_DAY);
        return aggTables1;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected void insertRowIntoAggTable(final String table, final int eventId, final String dateTime) throws SQLException {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(EVENT_ID, eventId);
        values.put(EVENT_SOURCE_NAME, SAMPLE_SGSN);
        values.put(DATETIME_ID, dateTime);
        values.put(NO_OF_SUCCESSES, ONE_SUCCESS_EVENT);
        values.put(NO_OF_ERRORS, ONE_ERROR_EVENT);
        insertRow(table, values);
    }
}
