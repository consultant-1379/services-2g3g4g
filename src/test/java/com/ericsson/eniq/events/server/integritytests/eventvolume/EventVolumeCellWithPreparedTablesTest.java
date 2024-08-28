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
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

import org.junit.Test;

/**
 * @author ejoegaf
 * @since 2011
 *
 */
public class EventVolumeCellWithPreparedTablesTest extends BaseEventVolumeWithPreparedTablesTest {

    private final static int FIFTY_FIVE_MINS_AGO = 55;

    private final static int TWENTY_FOUR_TIME_SLOTS = 24;//for this test the data sampling is every 12 minutes

    private final static String DATE_TIME_FORMAT = "yyyy-MM-dd HH:mm:ss";

    private final SimpleDateFormat dateOnlyFormatter = new SimpleDateFormat(DATE_FORMAT);

    @Test
    public void testGetEventVolumeData_5Minutes() throws Exception {
        populateTemporaryTablesWithFiveMinDataSet();
        final String result = getCellData(FIVE_MINUTES);
        validateResults(result);
    }

    @Test
    public void testGetEventVolumeData_5Minutes_WithTACExclusion() throws Exception {
        populateTemporaryTablesWithFiveMinDataSet();
        populateGroupTableWithExclusiveTAC();
        final String result = getCellData(FIVE_MINUTES);
        validateEmptyResult(result);
    }

    @Test
    public void testGetEventVolumeData_OneWeek_WithTACExclusion() throws Exception {
        populateTemporaryTablesWithOneWeekDataSet();
        populateGroupTableWithExclusiveTAC();
        final String result = getCellData(ONE_WEEK, POSITIVE_TIMEZONE_0800_SPACE_PREFIX);
        validateEmptyResult(result);
    }

    @Test
    public void testGetEventVolumeData_6Hours() throws Exception {
        populateTemporaryTables_15minAgg();
        populateTemporaryTablesWith6hourDataSet();
        final String result = getCellData("120");
        validateResults_6Hours(result);
    }

    @Test
    public void test_getEventVolumeData_1Week_PositiveTimezone1130() throws Exception {
        populateTemporaryTables_OneDayAgg();
        final String result = getCellData(ONE_WEEK, POSITIVE_TIMEZONE_1130_SPACE_PREFIX);
        validateTime(result);
    }

    @Test
    public void test_getEventVolumeData_1Week_PositiveTimezone0800() throws Exception {
        populateTemporaryTables_OneDayAgg();
        final String result = getCellData(ONE_WEEK, POSITIVE_TIMEZONE_0800_SPACE_PREFIX);
        validateTime(result);
    }

    @Test
    public void test_getEventVolumeData_1Week_NeutralTimezone() throws Exception {
        populateTemporaryTables_OneDayAgg();
        final String result = getCellData(ONE_WEEK, NEUTRAL_TIMEZONE_0000_SPACE_PREFIX);
        validateTime(result);
    }

    @Test(expected = Exception.class)
    public void test_getEventVolumeData_1Week_InvalidSign() throws Exception {
        getCellData(ONE_WEEK, TIMEZONE_INVALID_SIGN);
    }


    private String getCellData(final String time) {
        return getData(time, TYPE_CELL, "00,,BSC1,Ericsson,GSM");
    }

    private String getCellData(final String time, final String tzOffset) {
        return getData(time, TYPE_CELL, "00,,BSC1,Ericsson,GSM", tzOffset);
    }

    @Test
    public void test_getGroupEventVolumeData_1Week_PositiveTimezone1130() throws Exception {
        populateTemporaryTables_OneDayAgg();
        createAndPopulateGroupTable(TEMP_GROUP_TYPE_E_RAT_VEND_HIER321, SAMPLE_CELL_GROUP);
        final String result = getCellGroupData(ONE_WEEK, POSITIVE_TIMEZONE_1130_SPACE_PREFIX);
        validateTime(result);
    }

    @Test
    public void test_getGroupEventVolumeData_1Week_PositiveTimezone0800() throws Exception {
        populateTemporaryTables_OneDayAgg();
        createAndPopulateGroupTable(TEMP_GROUP_TYPE_E_RAT_VEND_HIER321, SAMPLE_CELL_GROUP);
        final String result = getCellGroupData(ONE_WEEK, POSITIVE_TIMEZONE_0800_SPACE_PREFIX);
        validateTime(result);
    }

    @Test
    public void test_getGroupEventVolumeData_1Week_NeutralTimezone() throws Exception {
        populateTemporaryTables_OneDayAgg();
        createAndPopulateGroupTable(TEMP_GROUP_TYPE_E_RAT_VEND_HIER321, SAMPLE_CELL_GROUP);
        final String result = getCellGroupData(ONE_WEEK, NEUTRAL_TIMEZONE_0000_SPACE_PREFIX);
        validateTime(result);
    }

    private String getCellGroupData(final String time, final String tzOffset) {
        return getGroupData(time, TYPE_CELL, SAMPLE_CELL_GROUP, tzOffset);
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
        columnsForAggTables1.add(HIERARCHY_1);
        columnsForAggTables1.add(HIERARCHY_3);

        return columnsForAggTables1;

    }

    @Override
    protected List<String> getAggTables_Day() {

        final List<String> aggTables1 = new ArrayList<String>();
        aggTables1.add(TEMP_EVENT_E_LTE_VEND_HIER321_EVENTID_SUC_DAY);
        aggTables1.add(TEMP_EVENT_E_SGEH_VEND_HIER321_EVENTID_SUC_DAY);
        aggTables1.add(TEMP_EVENT_E_LTE_VEND_HIER321_EVENTID_ERR_DAY);
        aggTables1.add(TEMP_EVENT_E_SGEH_VEND_HIER321_EVENTID_ERR_DAY);
        return aggTables1;
    }

    @Override
    protected List<String> getAggTables_15min() {

        final List<String> aggTables1 = new ArrayList<String>();
        aggTables1.add(TEMP_EVENT_E_SGEH_VEND_HIER321_EVENTID_ERR_15MIN);
        aggTables1.add(TEMP_EVENT_E_SGEH_VEND_HIER321_EVENTID_SUC_15MIN);
        aggTables1.add(TEMP_EVENT_E_LTE_VEND_HIER321_EVENTID_ERR_15MIN);
        aggTables1.add(TEMP_EVENT_E_LTE_VEND_HIER321_EVENTID_SUC_15MIN);
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
        values.put(HIERARCHY_1, "00");
        values.put(HIERARCHY_3, "BSC1");
        insertRow(table, values);
    }

    /**
     * due to data sampling it is very difficult to predict which time slots will be chosen. Therefore the easiest solution is to place events for
     * each minute between the times of interest.
     * 
     * @throws Exception
     */
    protected void populateTemporaryTablesWith6hourDataSet() throws Exception {
        final Map<Integer, String> dateTimeMap = getDateTimeMap(FIFTY_FIVE_MINS_AGO, TWENTY_FOUR_TIME_SLOTS);
        final String localDateId = dateOnlyFormatter.format(dateOnlyFormatter.parse(dateTimeMap.get(FIFTY_FIVE_MINS_AGO)));
        for (int noMinsAgo = FIFTY_FIVE_MINS_AGO; noMinsAgo < (FIFTY_FIVE_MINS_AGO + TWENTY_FOUR_TIME_SLOTS); noMinsAgo++) {
            for (final String rawTable : rawTables) {
                for (final int eventID : eventIDs) {
                    insertRowIntoRawTable_6hourTest(rawTable, eventID, dateTimeMap.get(noMinsAgo), localDateId);
                }
            }
            for (final String aggTable : aggTables_15min) {
                for (final int eventID : eventIDs) {
                    insertRowIntoAggTable(aggTable, eventID, dateTimeMap.get(noMinsAgo));
                }
            }
        }
    }

    /**
     * Create a map of date time strings based on the number of minutes ago. Can't use the existing methods in DateTimeUtilities as they get a new
     * Date object each time which may result in time having moved to the next minute.
     * 
     * @param startMinsAgo
     * @param noTimeSlots
     * @return
     */
    private Map<Integer, String> getDateTimeMap(final int startMinsAgo, final int noTimeSlots) {
        final DateFormat dateFormat = new SimpleDateFormat(DATE_TIME_FORMAT);
        final Date date = new Date();

        // Make sure we're always in GMT so our tests dont fail when timezone changes!
        final Calendar calendar = Calendar.getInstance(new SimpleTimeZone(0, "GMT"));
        dateFormat.setCalendar(calendar);

        final Map<Integer, String> dateTimeMap = new HashMap<Integer, String>();

        for (int minsAgo = startMinsAgo; minsAgo < (startMinsAgo + noTimeSlots); minsAgo++) {
            calendar.setTime(date);
            calendar.add(Calendar.MINUTE, -minsAgo);
            dateTimeMap.put(minsAgo, dateFormat.format(calendar.getTime()));
        }

        return dateTimeMap;
    }

    @SuppressWarnings("unchecked")
    protected void insertRowIntoRawTable_6hourTest(final String table, final int eventId, final String dateTime, final String localDateId)
            throws SQLException {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(EVENT_ID, eventId);
        values.put(TAC, SONY_ERICSSON_TAC);
        values.put(EVENT_SOURCE_NAME, SAMPLE_SGSN);
        values.put(IMSI, SAMPLE_IMSI);
        values.put(DATETIME_ID, dateTime);
        values.put(LOCAL_DATE_ID, localDateId);
        values.put(RAT, RAT_FOR_GSM);
        values.put(VENDOR, ERICSSON);
        values.put(HIERARCHY_1, "00");
        values.put(HIERARCHY_3, "BSC1");
        values.put(DEACTIVATION_TRIGGER, 1);
        values.put(APN, SAMPLE_APN);
        insertRow(table, values);
    }

}
