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

package com.ericsson.eniq.events.server.integritytests.eventvolume;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.*;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.*;

import java.sql.SQLException;
import java.util.*;

import org.junit.Test;

public class NetworkEventVolumeWithPreparedTablesTest extends BaseEventVolumeWithPreparedTablesTest {

    @Override
    public void onSetUp() throws Exception {
        super.onSetUp();
        populateTemporaryTables_OneDayAgg();

    }

    @Test
    public void test_getEventVolumeData_1Week_PositiveTimezone1130() throws Exception {
        final String result = getNetworkData(ONE_WEEK, POSITIVE_TIMEZONE_1130_SPACE_PREFIX);
        validateTime(result);
    }

    @Test
    public void test_getEventVolumeData_1Week_PositiveTimezone0800() throws Exception {
        final String result = getNetworkData(ONE_WEEK, POSITIVE_TIMEZONE_0800_SPACE_PREFIX);
        validateTime(result);
    }

    @Test
    public void test_getEventVolumeData_1Week_NeutralTimezone() throws Exception {
        final String result = getNetworkData(ONE_WEEK, NEUTRAL_TIMEZONE_0000_SPACE_PREFIX);
        validateTime(result);
    }

    @Test(expected = Exception.class)
    public void test_getEventVolumeData_1Week_InvalidSign() throws Exception {
        getNetworkData(ONE_WEEK, TIMEZONE_INVALID_SIGN);
    }

    private String getNetworkData(final String time, final String tzOffset) {
        return getData(time, tzOffset);
    }

    @Override
    protected Collection<String> getColumnsForAggTables() {
        final Collection<String> columnsForAggTables1 = new ArrayList<String>();
        columnsForAggTables1.add(EVENT_ID);
        columnsForAggTables1.add(NO_OF_ERRORS);
        columnsForAggTables1.add(NO_OF_SUCCESSES);
        columnsForAggTables1.add(DATETIME_ID);
        return columnsForAggTables1;

    }

    @Override
    protected List<String> getAggTables_Day() {
        final List<String> aggTables1 = new ArrayList<String>();
        aggTables1.add(TEMP_EVENT_E_LTE_EVNTSRC_EVENTID_ERR_DAY);
        aggTables1.add(TEMP_EVENT_E_LTE_EVNTSRC_EVENTID_SUC_DAY);
        aggTables1.add(TEMP_EVENT_E_SGEH_EVNTSRC_EVENTID_ERR_DAY);
        aggTables1.add(TEMP_EVENT_E_SGEH_EVNTSRC_EVENTID_SUC_DAY);
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
        insertRow(table, values);

    }

    @Override
    protected List<String> getAggTables_15min() {
        final List<String> aggTables1 = new ArrayList<String>();
        aggTables1.add(TEMP_EVENT_E_LTE_EVNTSRC_EVENTID_ERR_15MIN);
        aggTables1.add(TEMP_EVENT_E_LTE_EVNTSRC_EVENTID_SUC_15MIN);
        aggTables1.add(TEMP_EVENT_E_SGEH_EVNTSRC_EVENTID_ERR_15MIN);
        aggTables1.add(TEMP_EVENT_E_SGEH_EVNTSRC_EVENTID_SUC_15MIN);

        return aggTables1;
    }

}
