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

package com.ericsson.eniq.events.server.integritytests.kpi;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.common.EventIDConstants.*;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.*;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.*;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.SQLException;
import java.text.*;
import java.util.*;

import com.ericsson.eniq.events.server.integritytests.stubs.ReplaceTablesWithTempTablesTemplateUtils;
import com.ericsson.eniq.events.server.kpi.*;
import com.ericsson.eniq.events.server.resources.KPIResource;
import com.ericsson.eniq.events.server.resources.TestsWithTemporaryTablesBaseTestCase;
import com.ericsson.eniq.events.server.test.queryresults.KPIAnalysisResult;

public abstract class BaseKPIAnalysisTest extends TestsWithTemporaryTablesBaseTestCase<KPIAnalysisResult> {

    protected final KPIResource kpiResource = new KPIResource();

    private Connection anotherConnection;

    private final static List<String> tempRawTables = new ArrayList<String>();

    private final static Collection<String> columnsInRawTables = new ArrayList<String>();

    private final static Collection<String> columnsInAggTables = new ArrayList<String>();

    private final static Collection<String> columnsInSGSNGroupTable = new ArrayList<String>();

    private final static Collection<String> columnsInTacGroupTable = new ArrayList<String>();

    private final static Collection<String> columnsInAPNGroupTable = new ArrayList<String>();

    private final static Collection<String> columnsInControllerGroupTable = new ArrayList<String>();

    private final static Collection<String> columnsInAccessAreaGroupTable = new ArrayList<String>();

    private final static Collection<String> columnsInTacDimTable = new ArrayList<String>();

    protected final static List<String> tempAPNAggTables = new ArrayList<String>();

    protected final static List<String> tempControllerAggTables = new ArrayList<String>();

    protected final static List<String> tempAccessAreaAggTables = new ArrayList<String>();

    protected final static List<String> tempTACAggTables = new ArrayList<String>();

    protected final static List<String> tempSGSNAggTables = new ArrayList<String>();

    protected static final String THREE_HOURS = "180";
    protected static final String MINUS_ONE_HOUR = "-0100";
    protected static final String MINUS_EIGHT_HOURS = "-0800";
    protected static final String MINUS_FIVE_AND_A_HALF_HOURS = "-0530";
    protected static final String PLUS_THREE_HOURS = "+0300";
    protected static final String PLUS_NINE_AND_A_HALF_HOURS = "+0930";

    private static final double SIXTY_SIX_POINT_SIX_SEVEN = 66.67;

    private static final double ZERO_POINT_ZERO_ZERO = 0.00;

    private static final String ZERO = "0";
    private static final String FOUR = "4";

    private static final int FIFTEEN_MINUTES = 15;
    private static final int THIRTY_MINUTES = 30;
    private static final int ONE_HOUR = 60;
    private static final int ONE_DAY = 1440;
    private static final int TWO_DAYS = 2880;

    static {

        tempRawTables.add(TEMP_EVENT_E_LTE_ERR_RAW);
        tempRawTables.add(TEMP_EVENT_E_LTE_SUC_RAW);
        tempRawTables.add(TEMP_EVENT_E_SGEH_ERR_RAW);
        tempRawTables.add(TEMP_EVENT_E_SGEH_SUC_RAW);

        columnsInRawTables.add(EVENT_ID);
        columnsInRawTables.add(IMSI);
        columnsInRawTables.add(EVENT_SOURCE_NAME);
        columnsInRawTables.add(PAGING_ATTEMPTS);
        columnsInRawTables.add(DEACTIVATION_TRIGGER);
        columnsInRawTables.add(EVENT_SUBTYPE_ID);
        columnsInRawTables.add(TAC);
        columnsInRawTables.add(DATETIME_ID);
        columnsInRawTables.add(LOCAL_DATE_ID);
        columnsInRawTables.add(APN);
        columnsInRawTables.add(HIERARCHY_3);
        columnsInRawTables.add(VENDOR);
        columnsInRawTables.add(HIERARCHY_1);

        //TAC Aggregate Tables
        tempTACAggTables.add(TEMP_EVENT_E_SGEH_MANUF_TAC_EVENTID_ERR_15MIN);
        tempTACAggTables.add(TEMP_EVENT_E_SGEH_MANUF_TAC_EVENTID_SUC_15MIN);
        tempTACAggTables.add(TEMP_EVENT_E_LTE_MANUF_TAC_EVENTID_SUC_15MIN);
        tempTACAggTables.add(TEMP_EVENT_E_LTE_MANUF_TAC_EVENTID_ERR_15MIN);
        tempTACAggTables.add(TEMP_EVENT_E_SGEH_MANUF_TAC_EVENTID_ERR_DAY);
        tempTACAggTables.add(TEMP_EVENT_E_SGEH_MANUF_TAC_EVENTID_SUC_DAY);
        tempTACAggTables.add(TEMP_EVENT_E_LTE_MANUF_TAC_EVENTID_SUC_DAY);
        tempTACAggTables.add(TEMP_EVENT_E_LTE_MANUF_TAC_EVENTID_ERR_DAY);

        //Controller Aggregate Tables
        tempControllerAggTables.add(TEMP_EVENT_E_SGEH_VEND_HIER3_EVENTID_ERR_15MIN);
        tempControllerAggTables.add(TEMP_EVENT_E_SGEH_VEND_HIER3_EVENTID_SUC_15MIN);
        tempControllerAggTables.add(TEMP_EVENT_E_LTE_VEND_HIER3_EVENTID_SUC_15MIN);
        tempControllerAggTables.add(TEMP_EVENT_E_LTE_VEND_HIER3_EVENTID_ERR_15MIN);
        tempControllerAggTables.add(TEMP_EVENT_E_SGEH_VEND_HIER3_EVENTID_ERR_DAY);
        tempControllerAggTables.add(TEMP_EVENT_E_SGEH_VEND_HIER3_EVENTID_SUC_DAY);
        tempControllerAggTables.add(TEMP_EVENT_E_LTE_VEND_HIER3_EVENTID_SUC_DAY);
        tempControllerAggTables.add(TEMP_EVENT_E_LTE_VEND_HIER3_EVENTID_ERR_DAY);

        //SGSN Aggregate Tables
        tempSGSNAggTables.add(TEMP_EVENT_E_SGEH_EVNTSRC_EVENTID_ERR_15MIN);
        tempSGSNAggTables.add(TEMP_EVENT_E_SGEH_EVNTSRC_EVENTID_SUC_15MIN);
        tempSGSNAggTables.add(TEMP_EVENT_E_LTE_EVNTSRC_EVENTID_ERR_15MIN);
        tempSGSNAggTables.add(TEMP_EVENT_E_LTE_EVNTSRC_EVENTID_SUC_15MIN);
        tempSGSNAggTables.add(TEMP_EVENT_E_SGEH_EVNTSRC_EVENTID_SUC_DAY);
        tempSGSNAggTables.add(TEMP_EVENT_E_SGEH_EVNTSRC_EVENTID_ERR_DAY);
        tempSGSNAggTables.add(TEMP_EVENT_E_LTE_EVNTSRC_EVENTID_SUC_DAY);
        tempSGSNAggTables.add(TEMP_EVENT_E_LTE_EVNTSRC_EVENTID_ERR_DAY);

        columnsInAggTables.add(EVENT_ID);
        columnsInAggTables.add(EVENT_SOURCE_NAME);
        columnsInAggTables.add(NO_OF_PAGING_ATTEMPTS);
        columnsInAggTables.add(NO_OF_ERRORS);
        columnsInAggTables.add(NO_OF_SUCCESSES);
        columnsInAggTables.add(EVENT_SUBTYPE_ID);
        columnsInAggTables.add(DATETIME_ID);
        columnsInAggTables.add(NO_OF_NET_INIT_DEACTIVATES);
        columnsInAggTables.add(NO_OF_TOTAL_ERR_SUBSCRIBERS);
        columnsInAggTables.add(MANUFACTURER);
        columnsInAggTables.add(TAC);
        columnsInAggTables.add(APN);
        columnsInAggTables.add(HIERARCHY_3);
        columnsInAggTables.add(VENDOR);
        columnsInAggTables.add(HIERARCHY_1);

        columnsInSGSNGroupTable.add(GROUP_NAME);
        columnsInSGSNGroupTable.add(EVENT_SOURCE_NAME);

        columnsInTacGroupTable.add(GROUP_NAME);
        columnsInTacGroupTable.add(TAC);

        columnsInTacDimTable.add(TAC);
        columnsInTacDimTable.add(MANUFACTURER);

        columnsInAPNGroupTable.add(GROUP_NAME);
        columnsInAPNGroupTable.add(APN);

        columnsInControllerGroupTable.add(GROUP_NAME);
        columnsInControllerGroupTable.add(HIERARCHY_3);

        columnsInAccessAreaGroupTable.add(GROUP_NAME);
        columnsInAccessAreaGroupTable.add(VENDOR);
        columnsInAccessAreaGroupTable.add(HIERARCHY_1);
        columnsInAccessAreaGroupTable.add(HIERARCHY_3);

    }

    @Override
    public void onSetUp() throws Exception {
        super.onSetUp();
        ReplaceTablesWithTempTablesTemplateUtils.useTemporaryTableFor("DIM_E_SGEH_TAC");
        final KPIQueryfactory queryFactory = new KPIQueryfactory();
        final KpiUtilities kpiUtilities = new KpiUtilities();
        kpiUtilities.setTemplateUtils(templateUtils);
        kpiUtilities.applicationStartup();
        queryFactory.setKpiUtilities(kpiUtilities);
        final KpiFactory kpiFactory = new KpiFactory();
        kpiFactory.setKpiUtilities(kpiUtilities);
        queryFactory.setKpiFactory(kpiFactory);
        kpiResource.setLteQueryBuilder(queryFactory);
        attachDependencies(kpiResource);

    }

    /**
     * This method will create another db connection and add it to the interceptedDbConnectionManager. This is necessary if the code will require a
     * second db request because DataServiceBean will close the connection after each query, e.g. the code will verify if a TAC is in the
     * EXCLUSIVE_TAC group before sending an event analysis.
     * 
     * @throws Exception
     */
    protected void addConnectionToInterceptedDbConnectionManager(final List<String> tempAggTables) throws Exception {
        anotherConnection = getDWHDataSourceConnection();
        createTemporaryTablesOnSpecificConnection(anotherConnection, tempAggTables);
        createGroupTablesOnSpecificConnection(anotherConnection);
        interceptedDbConnectionManager.addConnection(anotherConnection);
    }

    @SuppressWarnings("unchecked")
    protected void createTemporaryTables(final List<String> tempAggTables) throws Exception {
        for (final String tempRawTable : tempRawTables) {
            createTemporaryTable(tempRawTable, columnsInRawTables);
        }
        for (final String tempAggTable : tempAggTables) {
            createTemporaryTable(tempAggTable, columnsInAggTables);
        }
        createGroupTables();
    }

    private void createTemporaryTablesOnSpecificConnection(final Connection conn, final List<String> tempAggTables) throws Exception {
        for (final String tempRawTable : tempRawTables) {
            createTemporaryTableOnSpecificConnection(conn, tempRawTable, columnsInRawTables);
        }
        for (final String tempAggTable : tempAggTables) {
            createTemporaryTableOnSpecificConnection(conn, tempAggTable, columnsInAggTables);
        }
    }

    @SuppressWarnings("unchecked")
    protected void addSGSNtoSGSNGroup() throws Exception {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(GROUP_NAME, SAMPLE_MME_GROUP);
        values.put(EVENT_SOURCE_NAME, SAMPLE_MME);
        insertRow(TEMP_GROUP_TYPE_E_EVNTSRC, values);
    }

    @SuppressWarnings("unchecked")
    protected void addTACtoTACGroup() throws Exception {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(GROUP_NAME, SAMPLE_TAC_GROUP);
        values.put(TAC, SAMPLE_TAC);
        insertRow(TEMP_GROUP_TYPE_E_TAC, values);
    }

    @SuppressWarnings("unchecked")
    protected void addControllerToControllerGroup() throws Exception {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(GROUP_NAME, SAMPLE_BSC_GROUP);
        values.put(HIERARCHY_3, TEST_VALUE_ENODEB_NODE);
        insertRow(TEMP_GROUP_TYPE_E_RAT_VEND_HIER3, values);
    }

    @SuppressWarnings("unchecked")
    protected void addManufacturertoDimTable() throws Exception {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(TAC, SAMPLE_TAC);
        values.put(MANUFACTURER, TEST_VALUE_MANUFACTURER);
        insertRow(TEMP_DIM_E_SGEH_TAC, values);
    }

    @SuppressWarnings("unchecked")
    private void createGroupTables() throws Exception {
        createTemporaryTable(TEMP_GROUP_TYPE_E_EVNTSRC, columnsInSGSNGroupTable);
        createTemporaryTable(TEMP_GROUP_TYPE_E_APN, columnsInAPNGroupTable);
        createTemporaryTable(TEMP_GROUP_TYPE_E_RAT_VEND_HIER3, columnsInControllerGroupTable);
        createTemporaryTable(TEMP_DIM_E_SGEH_TAC, columnsInTacDimTable);
    }

    private void createGroupTablesOnSpecificConnection(final Connection conn) throws Exception {
        createTemporaryTableOnSpecificConnection(conn, TEMP_GROUP_TYPE_E_EVNTSRC, columnsInSGSNGroupTable);
        createTemporaryTableOnSpecificConnection(conn, TEMP_GROUP_TYPE_E_TAC, columnsInTacGroupTable);
        createTemporaryTableOnSpecificConnection(conn, TEMP_GROUP_TYPE_E_APN, columnsInAPNGroupTable);
        createTemporaryTableOnSpecificConnection(conn, TEMP_GROUP_TYPE_E_RAT_VEND_HIER3, columnsInControllerGroupTable);
        createTemporaryTableOnSpecificConnection(conn, TEMP_GROUP_TYPE_E_RAT_VEND_HIER321, columnsInAccessAreaGroupTable);
        createTemporaryTableOnSpecificConnection(conn, TEMP_DIM_E_SGEH_TAC, columnsInTacDimTable);
    }

    @SuppressWarnings("unchecked")
    private void insertRowIntoAggTable(final String table, final int eventId, final String eventSrcName, final int pagingAttempts,
                                       final int noErrors, final int noSuccesses, final Integer eventSubType, final String dateTime,
                                       final int netInitDeactivates, final int totalErrSubs, final String manufacturer, final String tac,
                                       final String apn, final String hierarchy_3, final String hierarchy_1, final String vendor) throws SQLException {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(EVENT_ID, eventId);
        values.put(EVENT_SOURCE_NAME, eventSrcName);
        values.put(NO_OF_PAGING_ATTEMPTS, pagingAttempts);
        values.put(NO_OF_ERRORS, noErrors);
        values.put(NO_OF_SUCCESSES, noSuccesses);
        values.put(EVENT_SUBTYPE_ID, eventSubType);
        values.put(DATETIME_ID, dateTime);
        values.put(NO_OF_NET_INIT_DEACTIVATES, netInitDeactivates);
        values.put(NO_OF_TOTAL_ERR_SUBSCRIBERS, totalErrSubs);
        values.put(MANUFACTURER, manufacturer);
        values.put(TAC, tac);
        values.put(APN, apn);
        values.put(HIERARCHY_3, hierarchy_3);
        values.put(HIERARCHY_1, hierarchy_1);
        values.put(VENDOR, vendor);
        insertRow(table, values);
    }

    @SuppressWarnings("unchecked")
    protected void insertRowIntoRawTable(final String table, final int eventId, final long imsi, final String eventSrcName, final int pagingAttempts,
                                         final Integer deactivationTrigger, final Integer eventSubType, final int tac, final String dateTime,
                                         final String localdateId, final String apn, final String hierarchy_3, final String hierarchy_1,
                                         final String vendor) throws SQLException {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(EVENT_ID, eventId);
        values.put(IMSI, imsi);
        values.put(EVENT_SOURCE_NAME, eventSrcName);
        values.put(PAGING_ATTEMPTS, pagingAttempts);
        values.put(DEACTIVATION_TRIGGER, deactivationTrigger);
        values.put(EVENT_SUBTYPE_ID, eventSubType);
        values.put(TAC, tac);
        values.put(DATETIME_ID, dateTime);
        values.put(LOCAL_DATE_ID, localdateId);
        values.put(APN, apn);
        values.put(HIERARCHY_3, hierarchy_3);
        values.put(HIERARCHY_1, hierarchy_1);
        values.put(VENDOR, vendor);
        insertRow(table, values);
    }

    /**
     * Method that inserts data into raw and relevant agg tables.
     * 
     * @param minutes
     *            , determines how far back the data in populated until.
     * @param type
     *            , determines the type of agg tables that are populated.
     * @throws SQLException
     * @throws ParseException
     */
    protected void populateTemporaryTablesWithEvents(final String minutes, final String type) throws SQLException, ParseException {
        final int numberOfMinutes = Integer.valueOf(minutes) + ONE_HOUR;
        final int numberOfAggEntries = numberOfMinutes / FIFTEEN_MINUTES;
        final ArrayList<String> rawDateTimeList = new ArrayList<String>();
        final DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:00");
        Date date = new Date();
        final Calendar calendar = Calendar.getInstance(new SimpleTimeZone(0, "GMT"));

        dateFormat.setCalendar(calendar);
        calendar.setTime(date);
        calendar.add(Calendar.MINUTE, THIRTY_MINUTES);

        for (int i = numberOfMinutes; i > 0; i = i - FIFTEEN_MINUTES) {
            calendar.add(Calendar.MINUTE, -i);
            final String dateTime = dateFormat.format(calendar.getTime());
            rawDateTimeList.add(dateTime);
            insertMultipleEventsIntoRawTables(dateTime);
            calendar.setTime(date);
        }

        final String firstEventTime = rawDateTimeList.get(0);
        final String firstRopTime = alignToPrevious15MinRop(firstEventTime);

        date = dateFormat.parse(firstRopTime);
        calendar.setTime(date);

        for (int i = 0; i < numberOfAggEntries; i++) {
            final String dateTime = dateFormat.format(calendar.getTime());
            insertMultipleEventsInto15MinAggTables(dateTime, type);
            calendar.add(Calendar.MINUTE, FIFTEEN_MINUTES);
        }
    }

    protected void populateTemporaryTablesWithEvents_OneWeek(final String type) throws SQLException, ParseException {
        final int numberOfMinutes = Integer.valueOf(ONE_WEEK) + TWO_DAYS;
        final int numberOfAggEntries = numberOfMinutes / ONE_DAY;

        final ArrayList<String> rawDateTimeList = new ArrayList<String>();
        final DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:00");
        Date date = new Date();
        final Calendar calendar = Calendar.getInstance(new SimpleTimeZone(0, "GMT"));
        dateFormat.setCalendar(calendar);
        calendar.setTime(date);
        calendar.add(Calendar.MINUTE, ONE_DAY);

        for (int i = numberOfMinutes; i > 0; i = i - ONE_DAY) {
            calendar.add(Calendar.MINUTE, -i);
            final String dateTime = dateFormat.format(calendar.getTime());
            rawDateTimeList.add(dateTime);
            insertMultipleEventsIntoRawTables(dateTime);
            calendar.setTime(date);
        }

        final String firstEventTime = rawDateTimeList.get(0);
        final String firstRopTime = alignToPreviousDayRop(firstEventTime);

        date = dateFormat.parse(firstRopTime);
        calendar.setTime(date);

        for (int i = 0; i < numberOfAggEntries; i++) {
            final String dateTime = dateFormat.format(calendar.getTime());
            insertMultipleEventsIntoDayAggTables(dateTime, type);
            calendar.add(Calendar.MINUTE, ONE_DAY);
        }
    }

    /**
     * @param dateTime
     * @param type
     * @throws SQLException
     */
    private void insertMultipleEventsInto15MinAggTables(final String dateTime, final String type) throws SQLException {
        if (type.equals(SGSN)) {
            insertMultipleEventsIntoNamedTables(dateTime, TEMP_EVENT_E_LTE_EVNTSRC_EVENTID_SUC_15MIN, TEMP_EVENT_E_LTE_EVNTSRC_EVENTID_ERR_15MIN);
        } else if (type.equals(TAC)) {
            insertMultipleEventsIntoNamedTables(dateTime, TEMP_EVENT_E_LTE_MANUF_TAC_EVENTID_SUC_15MIN, TEMP_EVENT_E_LTE_MANUF_TAC_EVENTID_ERR_15MIN);
        } else if (type.equals(HIERARCHY_3)) {
            insertMultipleEventsIntoNamedTables(dateTime, TEMP_EVENT_E_LTE_VEND_HIER3_EVENTID_SUC_15MIN,
                    TEMP_EVENT_E_LTE_VEND_HIER3_EVENTID_ERR_15MIN);
        }
    }

    /**
     * @param dateTime
     * @param type
     * @throws SQLException
     */
    private void insertMultipleEventsIntoDayAggTables(final String dateTime, final String type) throws SQLException {
        if (type.equals(SGSN)) {
            insertMultipleEventsIntoNamedTables(dateTime, TEMP_EVENT_E_LTE_EVNTSRC_EVENTID_SUC_DAY, TEMP_EVENT_E_LTE_EVNTSRC_EVENTID_ERR_DAY);
        } else if (type.equals(TAC)) {
            insertMultipleEventsIntoNamedTables(dateTime, TEMP_EVENT_E_LTE_MANUF_TAC_EVENTID_SUC_DAY, TEMP_EVENT_E_LTE_MANUF_TAC_EVENTID_ERR_DAY);
        } else if (type.equals(HIERARCHY_3)) {
            insertMultipleEventsIntoNamedTables(dateTime, TEMP_EVENT_E_LTE_VEND_HIER3_EVENTID_SUC_DAY, TEMP_EVENT_E_LTE_VEND_HIER3_EVENTID_ERR_DAY);
        }
    }

    /**
     * @param dateTime
     * @throws SQLException
     */
    private void insertMultipleEventsIntoNamedTables(final String dateTime, final String successTable, final String errorTable) throws SQLException {
        insertRowIntoAggTable(successTable, SERVICE_REQUEST_IN_4G, SAMPLE_MME, 5, 0, 1, 0, dateTime, 0, 0, TEST_VALUE_MANUFACTURER,
                SAMPLE_TAC_TO_STRING, SAMPLE_APN, TEST_VALUE_ENODEB_NODE, TEST_VALUE_ECELL_NODE, TEST_VALUE_VENDOR);
        insertRowIntoAggTable(errorTable, SERVICE_REQUEST_IN_4G, SAMPLE_MME, 5, 2, 0, 0, dateTime, 0, 2, TEST_VALUE_MANUFACTURER,
                SAMPLE_TAC_TO_STRING, SAMPLE_APN, TEST_VALUE_ENODEB_NODE, TEST_VALUE_ECELL_NODE, TEST_VALUE_VENDOR);

        insertRowIntoAggTable(successTable, SERVICE_REQUEST_IN_4G, SAMPLE_MME, 0, 0, 1, 0, dateTime, 0, 0, TEST_VALUE_MANUFACTURER,
                SAMPLE_TAC_TO_STRING, SAMPLE_APN, TEST_VALUE_ENODEB_NODE, TEST_VALUE_ECELL_NODE, TEST_VALUE_VENDOR);
        insertRowIntoAggTable(errorTable, SERVICE_REQUEST_IN_4G, SAMPLE_MME, 0, 2, 0, 0, dateTime, 0, 2, TEST_VALUE_MANUFACTURER,
                SAMPLE_TAC_TO_STRING, SAMPLE_APN, TEST_VALUE_ENODEB_NODE, TEST_VALUE_ECELL_NODE, TEST_VALUE_VENDOR);

        insertRowIntoAggTable(successTable, DEDICATED_BEARER_ACTIVATE_IN_4G, SAMPLE_MME, 5, 0, 4, 0, dateTime, 0, 2, TEST_VALUE_MANUFACTURER,
                SAMPLE_TAC_TO_STRING, SAMPLE_APN, TEST_VALUE_ENODEB_NODE, TEST_VALUE_ECELL_NODE, TEST_VALUE_VENDOR);
        insertRowIntoAggTable(errorTable, DEDICATED_BEARER_ACTIVATE_IN_4G, SAMPLE_MME, 5, 2, 0, 0, dateTime, 0, 2, TEST_VALUE_MANUFACTURER,
                SAMPLE_TAC_TO_STRING, SAMPLE_APN, TEST_VALUE_ENODEB_NODE, TEST_VALUE_ECELL_NODE, TEST_VALUE_VENDOR);

        insertRowIntoAggTable(successTable, ATTACH_IN_4G, SAMPLE_MME, 0, 0, 4, 0, dateTime, 0, 0, TEST_VALUE_MANUFACTURER, SAMPLE_TAC_TO_STRING,
                SAMPLE_APN, TEST_VALUE_ENODEB_NODE, TEST_VALUE_ECELL_NODE, TEST_VALUE_VENDOR);
        insertRowIntoAggTable(errorTable, ATTACH_IN_4G, SAMPLE_MME, 0, 2, 0, 0, dateTime, 0, 2, TEST_VALUE_MANUFACTURER, SAMPLE_TAC_TO_STRING,
                SAMPLE_APN, TEST_VALUE_ENODEB_NODE, TEST_VALUE_ECELL_NODE, TEST_VALUE_VENDOR);

        insertRowIntoAggTable(successTable, DETACH_IN_4G, SAMPLE_MME, 0, 0, 4, 0, dateTime, 0, 0, TEST_VALUE_MANUFACTURER, SAMPLE_TAC_TO_STRING,
                SAMPLE_APN, TEST_VALUE_ENODEB_NODE, TEST_VALUE_ECELL_NODE, TEST_VALUE_VENDOR);
        insertRowIntoAggTable(errorTable, DETACH_IN_4G, SAMPLE_MME, 0, 2, 0, 0, dateTime, 0, 2, TEST_VALUE_MANUFACTURER, SAMPLE_TAC_TO_STRING,
                SAMPLE_APN, TEST_VALUE_ENODEB_NODE, TEST_VALUE_ECELL_NODE, TEST_VALUE_VENDOR);

        insertRowIntoAggTable(successTable, HANDOVER_IN_4G, SAMPLE_MME, 0, 0, 2, 2, dateTime, 0, 0, TEST_VALUE_MANUFACTURER, SAMPLE_TAC_TO_STRING,
                SAMPLE_APN, TEST_VALUE_ENODEB_NODE, TEST_VALUE_ECELL_NODE, TEST_VALUE_VENDOR);
        insertRowIntoAggTable(errorTable, HANDOVER_IN_4G, SAMPLE_MME, 0, 1, 0, 2, dateTime, 0, 1, TEST_VALUE_MANUFACTURER, SAMPLE_TAC_TO_STRING,
                SAMPLE_APN, TEST_VALUE_ENODEB_NODE, TEST_VALUE_ECELL_NODE, TEST_VALUE_VENDOR);

        insertRowIntoAggTable(successTable, HANDOVER_IN_4G, SAMPLE_MME, 0, 0, 2, 7, dateTime, 0, 0, TEST_VALUE_MANUFACTURER, SAMPLE_TAC_TO_STRING,
                SAMPLE_APN, TEST_VALUE_ENODEB_NODE, TEST_VALUE_ECELL_NODE, TEST_VALUE_VENDOR);
        insertRowIntoAggTable(errorTable, HANDOVER_IN_4G, SAMPLE_MME, 0, 1, 0, 7, dateTime, 0, 1, TEST_VALUE_MANUFACTURER, SAMPLE_TAC_TO_STRING,
                SAMPLE_APN, TEST_VALUE_ENODEB_NODE, TEST_VALUE_ECELL_NODE, TEST_VALUE_VENDOR);

        insertRowIntoAggTable(successTable, TAU_IN_4G, SAMPLE_MME, 0, 0, 2, 0, dateTime, 0, 0, TEST_VALUE_MANUFACTURER, SAMPLE_TAC_TO_STRING,
                SAMPLE_APN, TEST_VALUE_ENODEB_NODE, TEST_VALUE_ECELL_NODE, TEST_VALUE_VENDOR);
        insertRowIntoAggTable(errorTable, TAU_IN_4G, SAMPLE_MME, 0, 1, 0, 0, dateTime, 0, 1, TEST_VALUE_MANUFACTURER, SAMPLE_TAC_TO_STRING,
                SAMPLE_APN, TEST_VALUE_ENODEB_NODE, TEST_VALUE_ECELL_NODE, TEST_VALUE_VENDOR);

        insertRowIntoAggTable(successTable, TAU_IN_4G, SAMPLE_MME, 0, 0, 2, 1, dateTime, 0, 0, TEST_VALUE_MANUFACTURER, SAMPLE_TAC_TO_STRING,
                SAMPLE_APN, TEST_VALUE_ENODEB_NODE, TEST_VALUE_ECELL_NODE, TEST_VALUE_VENDOR);
        insertRowIntoAggTable(errorTable, TAU_IN_4G, SAMPLE_MME, 0, 1, 0, 1, dateTime, 0, 1, TEST_VALUE_MANUFACTURER, SAMPLE_TAC_TO_STRING,
                SAMPLE_APN, TEST_VALUE_ENODEB_NODE, TEST_VALUE_ECELL_NODE, TEST_VALUE_VENDOR);

        insertRowIntoAggTable(successTable, PDN_CONNECT_IN_4G, SAMPLE_MME, 0, 0, 2, 0, dateTime, 0, 0, TEST_VALUE_MANUFACTURER, SAMPLE_TAC_TO_STRING,
                SAMPLE_APN, TEST_VALUE_ENODEB_NODE, TEST_VALUE_ECELL_NODE, TEST_VALUE_VENDOR);
        insertRowIntoAggTable(errorTable, PDN_CONNECT_IN_4G, SAMPLE_MME, 0, 1, 0, 0, dateTime, 0, 1, TEST_VALUE_MANUFACTURER, SAMPLE_TAC_TO_STRING,
                SAMPLE_APN, TEST_VALUE_ENODEB_NODE, TEST_VALUE_ECELL_NODE, TEST_VALUE_VENDOR);
    }

    /**
     * @param dateTime
     * @throws SQLException
     */
    private void insertMultipleEventsIntoRawTables(final String dateTime) throws SQLException {
        final String localDateId = dateTime.substring(0, 10);
        insertRowIntoRawTable(TEMP_EVENT_E_LTE_ERR_RAW, SERVICE_REQUEST_IN_4G, SAMPLE_IMSI, SAMPLE_MME, 5, 0, 0, SAMPLE_TAC, dateTime, localDateId,
                SAMPLE_APN, TEST_VALUE_ENODEB_NODE, TEST_VALUE_ECELL_NODE, TEST_VALUE_VENDOR);
        insertRowIntoRawTable(TEMP_EVENT_E_LTE_ERR_RAW, SERVICE_REQUEST_IN_4G, SAMPLE_IMSI_3, SAMPLE_MME, 5, 0, 0, SAMPLE_TAC, dateTime, localDateId,
                SAMPLE_APN, TEST_VALUE_ENODEB_NODE, TEST_VALUE_ECELL_NODE, TEST_VALUE_VENDOR);

        insertRowIntoRawTable(TEMP_EVENT_E_LTE_ERR_RAW, SERVICE_REQUEST_IN_4G, SAMPLE_IMSI_2, SAMPLE_MME, 0, 0, 0, SAMPLE_TAC, dateTime, localDateId,
                SAMPLE_APN, TEST_VALUE_ENODEB_NODE, TEST_VALUE_ECELL_NODE, TEST_VALUE_VENDOR);
        insertRowIntoRawTable(TEMP_EVENT_E_LTE_ERR_RAW, SERVICE_REQUEST_IN_4G, SAMPLE_IMSI_5, SAMPLE_MME, 0, 0, 0, SAMPLE_TAC, dateTime, localDateId,
                SAMPLE_APN, TEST_VALUE_ENODEB_NODE, TEST_VALUE_ECELL_NODE, TEST_VALUE_VENDOR);

        insertRowIntoRawTable(TEMP_EVENT_E_LTE_ERR_RAW, DEDICATED_BEARER_ACTIVATE_IN_4G, SAMPLE_IMSI, SAMPLE_MME, 0, 0, 0, SAMPLE_TAC, dateTime,
                localDateId, SAMPLE_APN, TEST_VALUE_ENODEB_NODE, TEST_VALUE_ECELL_NODE, TEST_VALUE_VENDOR);
        insertRowIntoRawTable(TEMP_EVENT_E_LTE_ERR_RAW, DEDICATED_BEARER_ACTIVATE_IN_4G, SAMPLE_IMSI_3, SAMPLE_MME, 0, 0, 0, SAMPLE_TAC, dateTime,
                localDateId, SAMPLE_APN, TEST_VALUE_ENODEB_NODE, TEST_VALUE_ECELL_NODE, TEST_VALUE_VENDOR);

        insertRowIntoRawTable(TEMP_EVENT_E_LTE_ERR_RAW, ATTACH_IN_4G, SAMPLE_IMSI_2, SAMPLE_MME, 0, 0, 0, SAMPLE_TAC, dateTime, localDateId,
                SAMPLE_APN, TEST_VALUE_ENODEB_NODE, TEST_VALUE_ECELL_NODE, TEST_VALUE_VENDOR);
        insertRowIntoRawTable(TEMP_EVENT_E_LTE_ERR_RAW, ATTACH_IN_4G, SAMPLE_IMSI_5, SAMPLE_MME, 0, 0, 0, SAMPLE_TAC, dateTime, localDateId,
                SAMPLE_APN, TEST_VALUE_ENODEB_NODE, TEST_VALUE_ECELL_NODE, TEST_VALUE_VENDOR);

        insertRowIntoRawTable(TEMP_EVENT_E_LTE_ERR_RAW, DETACH_IN_4G, SAMPLE_IMSI_2, SAMPLE_MME, 0, 0, 0, SAMPLE_TAC, dateTime, localDateId,
                SAMPLE_APN, TEST_VALUE_ENODEB_NODE, TEST_VALUE_ECELL_NODE, TEST_VALUE_VENDOR);
        insertRowIntoRawTable(TEMP_EVENT_E_LTE_ERR_RAW, DETACH_IN_4G, SAMPLE_IMSI_5, SAMPLE_MME, 0, 0, 0, SAMPLE_TAC, dateTime, localDateId,
                SAMPLE_APN, TEST_VALUE_ENODEB_NODE, TEST_VALUE_ECELL_NODE, TEST_VALUE_VENDOR);

        insertRowIntoRawTable(TEMP_EVENT_E_LTE_ERR_RAW, HANDOVER_IN_4G, SAMPLE_IMSI, SAMPLE_MME, 0, 0, 2, SAMPLE_TAC, dateTime, localDateId,
                SAMPLE_APN, TEST_VALUE_ENODEB_NODE, TEST_VALUE_ECELL_NODE, TEST_VALUE_VENDOR);

        insertRowIntoRawTable(TEMP_EVENT_E_LTE_ERR_RAW, HANDOVER_IN_4G, SAMPLE_IMSI_2, SAMPLE_MME, 0, 0, 7, SAMPLE_TAC, dateTime, localDateId,
                SAMPLE_APN, TEST_VALUE_ENODEB_NODE, TEST_VALUE_ECELL_NODE, TEST_VALUE_VENDOR);

        insertRowIntoRawTable(TEMP_EVENT_E_LTE_ERR_RAW, TAU_IN_4G, SAMPLE_IMSI_3, SAMPLE_MME, 0, 0, 0, SAMPLE_TAC, dateTime, localDateId, SAMPLE_APN,
                TEST_VALUE_ENODEB_NODE, TEST_VALUE_ECELL_NODE, TEST_VALUE_VENDOR);

        insertRowIntoRawTable(TEMP_EVENT_E_LTE_ERR_RAW, TAU_IN_4G, SAMPLE_IMSI_5, SAMPLE_MME, 0, 0, 1, SAMPLE_TAC, dateTime, localDateId, SAMPLE_APN,
                TEST_VALUE_ENODEB_NODE, TEST_VALUE_ECELL_NODE, TEST_VALUE_VENDOR);

        insertRowIntoRawTable(TEMP_EVENT_E_LTE_ERR_RAW, PDN_CONNECT_IN_4G, SAMPLE_IMSI, SAMPLE_MME, 0, 0, 0, SAMPLE_TAC, dateTime, localDateId,
                SAMPLE_APN, TEST_VALUE_ENODEB_NODE, TEST_VALUE_ECELL_NODE, TEST_VALUE_VENDOR);

    }

    public String alignToPrevious15MinRop(final String dateTime) {
        final Calendar c = Calendar.getInstance();

        Date d = null;
        SimpleDateFormat formatter = null;
        formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        try {
            d = formatter.parse(dateTime);
        } catch (final ParseException e) {
            e.printStackTrace();
        }
        c.setTime(d);
        final int currentMinute = c.get(Calendar.MINUTE);
        final int minutesToLastRop = currentMinute % FIFTEEN_MINUTES;
        c.add(Calendar.MINUTE, -minutesToLastRop);
        c.set(Calendar.SECOND, 0);
        c.set(Calendar.MILLISECOND, 0);

        return formatter.format(c.getTime());
    }

    public String alignToPreviousDayRop(final String dateTime) {
        final Calendar c = Calendar.getInstance();

        Date d = null;
        SimpleDateFormat formatter = null;
        formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        try {
            d = formatter.parse(dateTime);
        } catch (final ParseException e) {
            e.printStackTrace();
        }
        c.setTime(d);
        final int currentMinute = c.get(Calendar.MINUTE);
        final int hoursToLastDayRop = c.get(Calendar.HOUR_OF_DAY);

        c.set(Calendar.SECOND, 0);
        c.set(Calendar.MILLISECOND, 0);
        c.add(Calendar.MINUTE, -currentMinute);
        c.add(Calendar.HOUR_OF_DAY, -(hoursToLastDayRop));

        return formatter.format(c.getTime());
    }

    /**
     * @param timePeriod
     * @return
     * @throws NumberFormatException
     */
    protected int getExpectedSize(final String timePeriod) throws NumberFormatException {
        final int minutes = Integer.valueOf(timePeriod);
        int expectedSize = 0;
        if (minutes >= Integer.valueOf(ONE_WEEK)) {
            expectedSize = minutes / ONE_DAY;
        } else {
            expectedSize = minutes / FIFTEEN_MINUTES;
        }
        return expectedSize;
    }

    /**
     * @param json
     * @param expectedSize
     * @throws Exception
     */
    protected void validateResults(final String json, final String queryTimePeriod) throws Exception {
        final List<KPIAnalysisResult> kpiResults = getTranslator().translateResult(json, KPIAnalysisResult.class);
        assertThat(kpiResults.size(), is(getExpectedSize(queryTimePeriod)));
        for (final KPIAnalysisResult kpiResult : kpiResults) {
            assertNotNull(kpiResult);
            if (queryTimePeriod.equals(ONE_WEEK)) {
                assertThat(kpiResult.getDateTimeStr().substring(11), is("00:00:00.0"));
            }
            validateSingleResult(kpiResult);
        }
    }

    protected void validateSingleResult(final KPIAnalysisResult kpiResult) throws Exception {
        assertThat(kpiResult.getAttachSuccessRate(), is(ZERO_POINT_ZERO_ZERO));
        assertThat(kpiResult.getPdpContextActSR(), is(ZERO_POINT_ZERO_ZERO));
        assertThat(kpiResult.getRaUpdateSuccessRate(), is(ZERO_POINT_ZERO_ZERO));
        assertThat(kpiResult.getInterSGSN_MMERAUpdateSR(), is(ZERO_POINT_ZERO_ZERO));
        assertThat(kpiResult.getPdpContextCutoffRatio(), is(ZERO_POINT_ZERO_ZERO));
        assertThat(kpiResult.getDetachSuccessRate(), is(ZERO_POINT_ZERO_ZERO));
        assertThat(kpiResult.getServiceRequestFailureRatio(), is(ZERO_POINT_ZERO_ZERO));
        assertThat(kpiResult.getPagingFailureRatio(), is(ZERO_POINT_ZERO_ZERO));
        assertThat(kpiResult.getPagingattemptsperSubs(), is(ZERO_POINT_ZERO_ZERO));
        assertThat(kpiResult.getImpactedSubscribers(), is(ZERO));
        assertThat(kpiResult.getAttachSuccessRateLTE(), is(SIXTY_SIX_POINT_SIX_SEVEN));
        assertThat(kpiResult.getPdnConnectionSuccessRate(), is(SIXTY_SIX_POINT_SIX_SEVEN));
        assertThat(kpiResult.getImpactedSubscribersLTE(), is(FOUR));
        assertThat(kpiResult.getBearerActivationSuccessRate(), is(SIXTY_SIX_POINT_SIX_SEVEN));
        assertThat(kpiResult.getUeInitiatedServiceRequestFailureRatioLTE(), is(SIXTY_SIX_POINT_SIX_SEVEN));
        assertThat(kpiResult.getPagingFailureRatioLTE(), is(SIXTY_SIX_POINT_SIX_SEVEN));
        assertThat(kpiResult.getTrackingAreaUpdateSuccessRate(), is(SIXTY_SIX_POINT_SIX_SEVEN));
        assertThat(kpiResult.getInterMMETrackingAreaUpdateSuccessRate(), is(SIXTY_SIX_POINT_SIX_SEVEN));
        assertThat(kpiResult.getX2basedhandover(), is(SIXTY_SIX_POINT_SIX_SEVEN));
        assertThat(kpiResult.getX2basedHOwithoutSGWrelocation(), is(ZERO_POINT_ZERO_ZERO));
        assertThat(kpiResult.getX2basedHOwithSGWrelocation(), is(ZERO_POINT_ZERO_ZERO));
        assertThat(kpiResult.getS1basedhandover(), is(SIXTY_SIX_POINT_SIX_SEVEN));
        assertThat(kpiResult.getS1basedHOwithoutSGWandwithMMErelocation(), is(ZERO_POINT_ZERO_ZERO));
        assertThat(kpiResult.getS1basedHOwithoutSGWandMMErelocation(), is(ZERO_POINT_ZERO_ZERO));
        assertThat(kpiResult.getS1basedHOwithSGWandwithoutMMErelocation(), is(ZERO_POINT_ZERO_ZERO));
        assertThat(kpiResult.getS1basedHOwithSGWandMMErelocation(), is(ZERO_POINT_ZERO_ZERO));

    }

}