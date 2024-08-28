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
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import javax.ws.rs.core.MultivaluedMap;

import com.ericsson.eniq.events.server.resources.EventVolumeResource;
import com.ericsson.eniq.events.server.resources.TestsWithTemporaryTablesBaseTestCase;
import com.ericsson.eniq.events.server.test.queryresults.EventVolumeResult;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;
import com.sun.jersey.core.util.MultivaluedMapImpl;

/**
 * @author ejoegaf
 * @since 2011
 *
 */
public abstract class BaseEventVolumeWithPreparedTablesTest extends TestsWithTemporaryTablesBaseTestCase<EventVolumeResult> {

    protected final EventVolumeResource eventVolumeResource = new EventVolumeResource();

    protected static Collection<String> columnsForRawTables = new ArrayList<String>();

    protected final Collection<String> columnsForAggTables = getColumnsForAggTables();

    protected static Collection<String> groupColumns = new ArrayList<String>();

    protected final static List<String> rawTables = new ArrayList<String>();

    protected final List<String> aggTables_Day = getAggTables_Day();

    protected final List<String> aggTables_15min = getAggTables_15min();

    private final SimpleDateFormat dateOnlyFormatter = new SimpleDateFormat(DATE_FORMAT);

    protected final static int[] eventIDs = { ATTACH_IN_2G_AND_3G, ACTIVATE_IN_2G_AND_3G, RAU_IN_2G_AND_3G, ISRAU_IN_2G_AND_3G,
            DEACTIVATE_IN_2G_AND_3G, ATTACH_IN_4G, DETACH_IN_4G, HANDOVER_IN_4G, TAU_IN_4G, DEDICATED_BEARER_ACTIVATE_IN_4G,
            DEDICATED_BEARER_DEACTIVATE_IN_4G, PDN_CONNECT_IN_4G, PDN_DISCONNECT_IN_4G, SERVICE_REQUEST_IN_4G, DETACH_IN_2G_AND_3G,
            SERVICE_REQUEST_IN_2G_AND_3G };

    protected final static int[] sampleEventIDs = { ACTIVATE_IN_2G_AND_3G, DEACTIVATE_IN_2G_AND_3G };

    private final static int EXPECTED_TOTAL_RESULTS_PER_EVENT_ID = 4;

    private final static int EXPECTED_ERROR_RESULTS_PER_EVENT_ID = 2;

    protected final static int ONE_SUCCESS_EVENT = 1;

    protected final static int ONE_ERROR_EVENT = 1;

    static {

        rawTables.add(TEMP_EVENT_E_SGEH_ERR_RAW);
        rawTables.add(TEMP_EVENT_E_SGEH_SUC_RAW);
        rawTables.add(TEMP_EVENT_E_LTE_ERR_RAW);
        rawTables.add(TEMP_EVENT_E_LTE_SUC_RAW);

        columnsForRawTables.add(EVENT_ID);
        columnsForRawTables.add(TAC);
        columnsForRawTables.add(EVENT_SOURCE_NAME);
        columnsForRawTables.add(IMSI);
        columnsForRawTables.add(DEACTIVATION_TRIGGER);
        columnsForRawTables.add(DATETIME_ID);
        columnsForRawTables.add(RAT);
        columnsForRawTables.add(VENDOR);
        columnsForRawTables.add(HIERARCHY_1);
        columnsForRawTables.add(HIERARCHY_3);
        columnsForRawTables.add(LOCAL_DATE_ID);
        columnsForRawTables.add(APN);

        groupColumns.add(GROUP_NAME);
        groupColumns.add(TAC);
        groupColumns.add(APN);
        groupColumns.add(HIERARCHY_3);
        groupColumns.add(HIERARCHY_1);
        groupColumns.add(EVENT_SOURCE_NAME);

    }

    @Override
    public void onSetUp() throws Exception {
        super.onSetUp();
        attachDependencies(eventVolumeResource);
        createTemporaryTables_Raw();
    }

    protected abstract Collection<String> getColumnsForAggTables();

    protected abstract List<String> getAggTables_Day();

    protected abstract List<String> getAggTables_15min();

    protected abstract void insertRowIntoAggTable(final String table, final int eventId, final String dateTime) throws SQLException;

    protected String getData(final String time, final String type, final String node) {
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();

        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(TIME_QUERY_PARAM, time);
        map.putSingle(TYPE_PARAM, type);
        map.putSingle(NODE_PARAM, node);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, "500");

        return eventVolumeResource.getData("requestID", map);
    }

    protected String getData(final String time, final String type, final String node, final String tzOffset) {
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();

        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(TIME_QUERY_PARAM, time);
        map.putSingle(TYPE_PARAM, type);
        map.putSingle(NODE_PARAM, node);
        map.putSingle(TZ_OFFSET, tzOffset);
        map.putSingle(MAX_ROWS, "500");

        return eventVolumeResource.getData("requestID", map);
    }

    protected String getData(final String time, final String tzOffset) {
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();

        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(TIME_QUERY_PARAM, time);
        map.putSingle(TZ_OFFSET, tzOffset);
        map.putSingle(MAX_ROWS, "500");

        return eventVolumeResource.getData("requestID", map);
    }

    protected String getGroupData(final String time, final String type, final String group) {
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();

        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(TIME_QUERY_PARAM, time);
        map.putSingle(TYPE_PARAM, type);
        map.putSingle(GROUP_NAME_PARAM, group);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, "500");

        return eventVolumeResource.getData("requestID", map);

    }

    protected String getGroupData(final String time, final String nodeType, final String groupName, final String tzOffset) {
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();

        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(TIME_QUERY_PARAM, time);
        map.putSingle(TYPE_PARAM, nodeType);
        map.putSingle(GROUP_NAME_PARAM, groupName);
        map.putSingle(TZ_OFFSET, tzOffset);
        map.putSingle(MAX_ROWS, "500");

        return eventVolumeResource.getData("requestID", map);

    }

    protected void validateResults(final String jsonResult) throws Exception {
        jsonAssertUtils.assertJSONSucceeds(jsonResult);

        final List<EventVolumeResult> eventVolumeResults = getTranslator().translateResult(jsonResult, EventVolumeResult.class);
        assertThat(eventVolumeResults.size(), is(2)); //2  time intervals

        for (final EventVolumeResult eventVolumeResult : eventVolumeResults) {
            validateSingleResult(eventVolumeResult);
        }
    }

    protected void validateResults_6Hours(final String jsonResult) throws Exception {
        jsonAssertUtils.assertJSONSucceeds(jsonResult);

        final List<EventVolumeResult> eventVolumeResults = getTranslator().translateResult(jsonResult, EventVolumeResult.class);
        assertThat(eventVolumeResults.size(), is(24)); //24  points = 6* (1 hour divided by 15min)

        for (final EventVolumeResult eventVolumeResult : eventVolumeResults) {
            validateSingleResult(eventVolumeResult);
        }
    }

    protected void validateResultsForDayAggs(final String jsonResult) throws Exception {
        jsonAssertUtils.assertJSONSucceeds(jsonResult);

        final List<EventVolumeResult> eventVolumeResults = getTranslator().translateResult(jsonResult, EventVolumeResult.class);
        assertThat(eventVolumeResults.size(), is(2)); //2  time intervals

        for (final EventVolumeResult eventVolumeResult : eventVolumeResults) {
            validateSingleResult(eventVolumeResult);
            validateTime_SingleResult(eventVolumeResult);

        }

    }

    protected void validateTime(final String jsonResult) throws Exception {
        final List<EventVolumeResult> eventVolumeResults = getTranslator().translateResult(jsonResult, EventVolumeResult.class);
        for (final EventVolumeResult eventVolumeResult : eventVolumeResults) {
            validateTime_SingleResult(eventVolumeResult);

        }
    }

    private void validateTime_SingleResult(final EventVolumeResult eventVolumeResult) {
        final String dateTime = eventVolumeResult.getTime();
        final String time = dateTime.split(" ")[1];
        assertEquals("Time should be in the format of 00:00:00.0", "00:00:00.0", time);
    }

    protected void validateEmptyResult(final String jsonResult) throws Exception {
        jsonAssertUtils.assertJSONSucceeds(jsonResult);

        final List<EventVolumeResult> eventVolumeResults = getTranslator().translateResult(jsonResult, EventVolumeResult.class);
        assertThat(eventVolumeResults.size(), is(0)); //2  time intervals
    }

    protected void validateSingleResult(final EventVolumeResult eventVolumeResult) {

        assertThat(eventVolumeResult.getAttachEventCount(), is(EXPECTED_TOTAL_RESULTS_PER_EVENT_ID));
        assertThat(eventVolumeResult.getAttachEventFailCount(), is(EXPECTED_ERROR_RESULTS_PER_EVENT_ID));
        assertThat(eventVolumeResult.getActivateEventCount(), is(EXPECTED_TOTAL_RESULTS_PER_EVENT_ID));
        assertThat(eventVolumeResult.getActivateEventFailCount(), is(EXPECTED_ERROR_RESULTS_PER_EVENT_ID));
        assertThat(eventVolumeResult.getRauEventCount(), is(EXPECTED_TOTAL_RESULTS_PER_EVENT_ID));
        assertThat(eventVolumeResult.getRauEventFailCount(), is(EXPECTED_ERROR_RESULTS_PER_EVENT_ID));
        assertThat(eventVolumeResult.getIsrauEventCount(), is(EXPECTED_TOTAL_RESULTS_PER_EVENT_ID));
        assertThat(eventVolumeResult.getIsrauEventFailCount(), is(EXPECTED_ERROR_RESULTS_PER_EVENT_ID));
        assertThat(eventVolumeResult.getDeactivateEventCount(), is(EXPECTED_TOTAL_RESULTS_PER_EVENT_ID));
        assertThat(eventVolumeResult.getDeactivateEventFailCount(), is(EXPECTED_ERROR_RESULTS_PER_EVENT_ID));
        assertThat(eventVolumeResult.getDetachEventCount(), is(EXPECTED_TOTAL_RESULTS_PER_EVENT_ID));
        assertThat(eventVolumeResult.getDetachEventFailCount(), is(EXPECTED_ERROR_RESULTS_PER_EVENT_ID));
        assertThat(eventVolumeResult.getServiceRequestEventCount(), is(EXPECTED_TOTAL_RESULTS_PER_EVENT_ID));
        assertThat(eventVolumeResult.getServiceRequestEventFailCount(), is(EXPECTED_ERROR_RESULTS_PER_EVENT_ID));
        assertThat(eventVolumeResult.getLAttachEventCount(), is(EXPECTED_TOTAL_RESULTS_PER_EVENT_ID));
        assertThat(eventVolumeResult.getLAttachEventFailCount(), is(EXPECTED_ERROR_RESULTS_PER_EVENT_ID));
        assertThat(eventVolumeResult.getLDetachEventCount(), is(EXPECTED_TOTAL_RESULTS_PER_EVENT_ID));
        assertThat(eventVolumeResult.getLDetachEventFailCount(), is(EXPECTED_ERROR_RESULTS_PER_EVENT_ID));
        assertThat(eventVolumeResult.getLHandoverEventCount(), is(EXPECTED_TOTAL_RESULTS_PER_EVENT_ID));
        assertThat(eventVolumeResult.getLHandoverFailEventCount(), is(EXPECTED_ERROR_RESULTS_PER_EVENT_ID));
        assertThat(eventVolumeResult.getLTauEventCount(), is(EXPECTED_TOTAL_RESULTS_PER_EVENT_ID));
        assertThat(eventVolumeResult.getLTauFailEventCount(), is(EXPECTED_ERROR_RESULTS_PER_EVENT_ID));
        assertThat(eventVolumeResult.getLDedicatedBearerActivateEventCount(), is(EXPECTED_TOTAL_RESULTS_PER_EVENT_ID));
        assertThat(eventVolumeResult.getLDedicatedBearerActivateFailEventCount(), is(EXPECTED_ERROR_RESULTS_PER_EVENT_ID));
        assertThat(eventVolumeResult.getLDedicatedBearerDeactivateEventCount(), is(EXPECTED_TOTAL_RESULTS_PER_EVENT_ID));
        assertThat(eventVolumeResult.getLDedicatedBearerDeactivateFailEventCount(), is(EXPECTED_ERROR_RESULTS_PER_EVENT_ID));
        assertThat(eventVolumeResult.getLPdnConnectEventCount(), is(EXPECTED_TOTAL_RESULTS_PER_EVENT_ID));
        assertThat(eventVolumeResult.getLPdnConnectFailEventCount(), is(EXPECTED_ERROR_RESULTS_PER_EVENT_ID));
        assertThat(eventVolumeResult.getLPdnDisconnectEventCount(), is(EXPECTED_TOTAL_RESULTS_PER_EVENT_ID));
        assertThat(eventVolumeResult.getLPdnDisconnectFailEventCount(), is(EXPECTED_ERROR_RESULTS_PER_EVENT_ID));
        assertThat(eventVolumeResult.getLServiceRequestEventCount(), is(EXPECTED_TOTAL_RESULTS_PER_EVENT_ID));
        assertThat(eventVolumeResult.getLServiceRequestFailEventCount(), is(EXPECTED_ERROR_RESULTS_PER_EVENT_ID));

    }

    protected void populateTemporaryTablesWithFiveMinDataSet() throws SQLException, ParseException {
        final String time1 = DateTimeUtilities.getDateTimeMinus2Minutes();
        final String time2 = DateTimeUtilities.getDateTimeMinus3Minutes();
        final String localDateId = dateOnlyFormatter.format(dateOnlyFormatter.parse(time1));

        //here I add 2 events - 1 for each of the 2 minutes in question.
        //I do this for each event type, and to each RAW table.
        //This should result in 2 successes and 2 failures per minute. 
        populateRawTables(time1, time2, localDateId);
    }

    protected void populateTemporaryTablesWithOneWeekDataSet() throws Exception {
        final String time1 = DateTimeUtilities.getDateTimeMinus2Minutes();
        final String time2 = DateTimeUtilities.getDateTimeMinus3Minutes();
        final String localDateId = dateOnlyFormatter.format(dateOnlyFormatter.parse(time1));

        populateRawTables(time1, time2, localDateId);
        createAndPopulateDayAggTables(time1, time2);
    }

    protected void populateTemporaryTablesWithThirtyMinDataSet() throws Exception {
        final String time1 = DateTimeUtilities.getDateTimeMinus25Minutes();
        final String time2 = DateTimeUtilities.getDateTimeMinus30Minutes();
        final String localDateId = dateOnlyFormatter.format(dateOnlyFormatter.parse(time1));

        populateRawTables(time1, time2, localDateId);
        createAndPopulate15minAggTables(time1, time2);

    }

    @SuppressWarnings("unchecked")
    protected void insertRowIntoRawTable(final String table, final int eventId, final String dateTime, final String localDateId) throws SQLException {
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

    @SuppressWarnings("unchecked")
    protected void populateGroupTableWithExclusiveTAC() throws SQLException {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(GROUP_NAME, EXCLUSIVE_TAC_GROUP);
        values.put(TAC, SONY_ERICSSON_TAC);
        insertRow(TEMP_GROUP_TYPE_E_TAC, values);
    }

    @SuppressWarnings("unchecked")
    protected void populateTemporaryTables_OneDayAgg() throws Exception {
        final String time1 = DateTimeUtilities.getDateMinus24Hours();
        final String time2 = DateTimeUtilities.getDateTimeMinus72Hours();
        for (final String aggTable : aggTables_Day) {
            createTemporaryTable(aggTable, columnsForAggTables);
            for (final int eventID : sampleEventIDs) {
                insertRowIntoAggTable(aggTable, eventID, time1);
                insertRowIntoAggTable(aggTable, eventID, time2);
            }
        }
    }

    @SuppressWarnings("unchecked")
    protected void createTemporaryTables_Raw() throws Exception {
        for (final String rawTable : rawTables) {
            createTemporaryTable(rawTable, columnsForRawTables);
        }
    }

    /**
     * @param time1
     * @param time2
     * @param localDateId
     * @throws SQLException
     */
    private void populateRawTables(final String time1, final String time2, final String localDateId) throws SQLException {
        for (final String rawTable : rawTables) {
            for (final int eventID : eventIDs) {
                insertRowIntoRawTable(rawTable, eventID, time1, localDateId);
                insertRowIntoRawTable(rawTable, eventID, time2, localDateId);
            }
        }
    }

    /**
     * @param time1
     * @param time2
     * @throws Exception
     * @throws SQLException
     */
    @SuppressWarnings("unchecked")
    private void createAndPopulate15minAggTables(final String time1, final String time2) throws Exception, SQLException {
        for (final String aggTable : aggTables_15min) {
            createTemporaryTable(aggTable, columnsForAggTables);
            for (final int eventID : eventIDs) {
                insertRowIntoAggTable(aggTable, eventID, time1);
                insertRowIntoAggTable(aggTable, eventID, time2);
            }
        }
    }

    /**
     * @param time1
     * @param time2
     * @throws Exception
     * @throws SQLException
     */
    @SuppressWarnings("unchecked")
    private void createAndPopulateDayAggTables(final String time1, final String time2) throws Exception, SQLException {
        for (final String aggTable : aggTables_Day) {
            createTemporaryTable(aggTable, columnsForAggTables);
            for (final int eventID : eventIDs) {
                insertRowIntoAggTable(aggTable, eventID, time1);
                insertRowIntoAggTable(aggTable, eventID, time2);
            }
        }
    }

    @SuppressWarnings("unchecked")
    protected void populateTemporaryTables_15minAgg() throws Exception {
        for (final String aggTable : aggTables_15min) {
            createTemporaryTable(aggTable, columnsForAggTables);
        }
    }

    @SuppressWarnings("unchecked")
    protected void createAndPopulateGroupTable(final String table, final String groupName) throws Exception, SQLException {
        createTemporaryTable(table, groupColumns);
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(GROUP_NAME, groupName);
        values.put(TAC, SAMPLE_TAC);
        values.put(APN, SAMPLE_APN);
        values.put(HIERARCHY_3, SAMPLE_HIERARCHY_3);
        values.put(HIERARCHY_1, SAMPLE_HIER);
        values.put(EVENT_SOURCE_NAME, SAMPLE_SGSN);
        insertRow(table, values);
    }

}
