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

package com.ericsson.eniq.events.server.integritytests.subsessionbi;

import com.ericsson.eniq.events.server.resources.SubsessionBIResource;
import com.ericsson.eniq.events.server.resources.TestsWithTemporaryTablesBaseTestCase;
import com.ericsson.eniq.events.server.test.queryresults.SubBIResult;
import com.ericsson.eniq.events.server.test.sql.SQLExecutor;
import com.ericsson.eniq.events.server.test.stubs.DummyUriInfoImpl;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;
import com.sun.jersey.core.util.MultivaluedMapImpl;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import javax.ws.rs.core.MultivaluedMap;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.*;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.*;

public class SubBIFailedEventsWithPreparedTablesTest extends
        TestsWithTemporaryTablesBaseTestCase<SubBIResult> {

    private SubsessionBIResource subsessionBIResource;

    private final static List<String> tempRawDataTables = new ArrayList<String>();

    private final static List<String> tempImsiRawDataTables = new ArrayList<String>();

    private final static long testIMSI = 312030410000004L;

    private final static String testPTMSI = "0908";

    private final static long testMSISDN = 11001100L;

    private final static Collection<String> groupColumns = new ArrayList<String>();

    private final static Collection<String> msisdnColumns = new ArrayList<String>();

    static {
        tempRawDataTables.add(TEMP_EVENT_E_SGEH_ERR_RAW);
        tempRawDataTables.add(TEMP_EVENT_E_SGEH_SUC_RAW);
        tempRawDataTables.add(TEMP_EVENT_E_LTE_ERR_RAW);
        tempRawDataTables.add(TEMP_EVENT_E_LTE_SUC_RAW);

        tempImsiRawDataTables.add(TEMP_EVENT_E_SGEH_IMSI_SUC_RAW);
        tempImsiRawDataTables.add(TEMP_EVENT_E_LTE_IMSI_SUC_RAW);

        msisdnColumns.add(IMSI);
        msisdnColumns.add(MSISDN);

        groupColumns.add(IMSI);
        groupColumns.add(GROUP_NAME);
    }

    @Override
    public void onSetUp() throws Exception {
        super.onSetUp();
        subsessionBIResource = new SubsessionBIResource();
        createTemporaryRawTableWithColumnTypes();
        createTemporaryImsiRawTableWithColumnTypes();
        createTemporaryTable(TEMP_GROUP_TYPE_E_IMSI, groupColumns);
        createTemporaryTable(TEMP_DIM_E_IMSI_MSISDN, msisdnColumns);
        attachDependencies(subsessionBIResource);
    }

    /**
     * @throws Exception
     */
    private void createTemporaryRawTableWithColumnTypes() throws Exception {
        final Map<String, String> rawTableColumns = new HashMap<String, String>();
        rawTableColumns.put("DATE_ID", "date");
        rawTableColumns.put("EVENT_ID", "tinyint");
        rawTableColumns.put("IMSI", "unsigned bigint");
        rawTableColumns.put("PTMSI", "unsigned int");
        rawTableColumns.put("MSISDN", "unsigned bigint");
        rawTableColumns.put("DATETIME_ID", "timestamp");
        rawTableColumns.put("LOCAL_DATE_ID", "date");

        for (final String tempTable : tempRawDataTables) {
            createTemporaryTableWithColumnTypes(tempTable, rawTableColumns);
        }
    }

    private void createTemporaryImsiRawTableWithColumnTypes() throws Exception {
        final Map<String, String> rawTableColumns = new HashMap<String, String>();
        rawTableColumns.put("DATE_ID", "date");
        rawTableColumns.put("EVENT_ID", "tinyint");
        rawTableColumns.put("IMSI", "unsigned bigint");
        rawTableColumns.put("PTMSI", "unsigned int");
        rawTableColumns.put("MSISDN", "unsigned bigint");
        rawTableColumns.put("DATETIME_ID", "timestamp");
        rawTableColumns.put("NO_OF_SUCCESSES", "unsigned bigint");

        for (final String tempTable : tempImsiRawDataTables) {
            createTemporaryTableWithColumnTypes(tempTable, rawTableColumns);
        }
    }

    @Test
    public void testSubBIFailedEventsIMSI_30Minutes() throws Exception {
        jndiProperties.disableSucRawJNDIProperty();
        populateRawTemporaryTables(DateTimeUtilities
                .getDateTimeMinus30Minutes());
        populateImsiRawTemporaryTables(DateTimeUtilities
                .getDateTimeMinus30Minutes());
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(DISPLAY_PARAM, CHART_PARAM);
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TYPE_PARAM, "IMSI");
        map.putSingle(IMSI_PARAM, Long.toString(testIMSI));
        map.putSingle(TZ_OFFSET, "+0000");
        map.putSingle(MAX_ROWS, "20");
        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String json = subsessionBIResource.getSubBIFailureData();

        final List<SubBIResult> results = getTranslator().translateResult(json,
                SubBIResult.class);

        validateResultsContainSGEHAndLTE(results);
    }

    @Test
    public void testSubBIFailedEventsIMSI_OneWeek() throws Exception {
        jndiProperties.disableSucRawJNDIProperty();
        populateRawTemporaryTables(DateTimeUtilities.getDateTimeMinus48Hours());
        populateImsiRawTemporaryTables(DateTimeUtilities
                .getDateTimeMinus48Hours());
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(DISPLAY_PARAM, CHART_PARAM);
        map.putSingle(TIME_QUERY_PARAM, "10080");
        map.putSingle(TYPE_PARAM, "IMSI");
        map.putSingle(IMSI_PARAM, Long.toString(testIMSI));
        map.putSingle(TZ_OFFSET, "+0000");
        map.putSingle(MAX_ROWS, "20");
        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String json = subsessionBIResource.getSubBIFailureData();

        final List<SubBIResult> results = getTranslator().translateResult(json,
                SubBIResult.class);

        validateResultsContainSGEHAndLTE(results);
    }

    @Test
    public void testSubBIFailedEventsPTMSI_30Minutes() throws Exception {
        jndiProperties.disableSucRawJNDIProperty();
        populateRawTemporaryTables(DateTimeUtilities
                .getDateTimeMinus30Minutes());
        populateImsiRawTemporaryTables(DateTimeUtilities
                .getDateTimeMinus30Minutes());
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(DISPLAY_PARAM, CHART_PARAM);
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TYPE_PARAM, "PTMSI");
        map.putSingle(PTMSI_PARAM, testPTMSI);
        map.putSingle(TZ_OFFSET, "+0000");
        map.putSingle(MAX_ROWS, "20");
        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String json = subsessionBIResource.getSubBIFailureData();

        final List<SubBIResult> results = getTranslator().translateResult(json,
                SubBIResult.class);

        validateResultsContainsOnlySGEH(results);
    }

    @Test
    public void testSubBIFailedEventsPTMSI_OneWeek() throws Exception {
        jndiProperties.disableSucRawJNDIProperty();
        populateRawTemporaryTables(DateTimeUtilities.getDateTimeMinus48Hours());
        populateImsiRawTemporaryTables(DateTimeUtilities
                .getDateTimeMinus48Hours());
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(DISPLAY_PARAM, CHART_PARAM);
        map.putSingle(TIME_QUERY_PARAM, "10080");
        map.putSingle(TYPE_PARAM, "PTMSI");
        map.putSingle(PTMSI_PARAM, testPTMSI);
        map.putSingle(TZ_OFFSET, "+0000");
        map.putSingle(MAX_ROWS, "20");
        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String json = subsessionBIResource.getSubBIFailureData();

        final List<SubBIResult> results = getTranslator().translateResult(json,
                SubBIResult.class);

        validateResultsContainsOnlySGEH(results);
    }

    @Test
    public void testSubBIFailedEventsMSISDN_30Minutes() throws Exception {
        jndiProperties.disableSucRawJNDIProperty();
        populateRawTemporaryTables(DateTimeUtilities
                .getDateTimeMinus30Minutes());
        populateImsiRawTemporaryTables(DateTimeUtilities
                .getDateTimeMinus30Minutes());
        populate_IMSI_MSISDN_TopologyTables();
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(DISPLAY_PARAM, CHART_PARAM);
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TYPE_PARAM, TYPE_MSISDN);
        map.putSingle(MSISDN_PARAM, Long.toString(testMSISDN));
        map.putSingle(TZ_OFFSET, "+0000");
        map.putSingle(MAX_ROWS, "20");
        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String json = subsessionBIResource.getSubBIFailureData();

        final List<SubBIResult> results = getTranslator().translateResult(json,
                SubBIResult.class);

        validateResultsContainSGEHAndLTE(results);
    }

    @Test
    public void testSubBIFailedEventsMSISDN_OneWeek() throws Exception {
        jndiProperties.disableSucRawJNDIProperty();
        populateRawTemporaryTables(DateTimeUtilities.getDateTimeMinus48Hours());
        populateImsiRawTemporaryTables(DateTimeUtilities
                .getDateTimeMinus48Hours());
        populate_IMSI_MSISDN_TopologyTables();
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(DISPLAY_PARAM, CHART_PARAM);
        map.putSingle(TIME_QUERY_PARAM, "10080");
        map.putSingle(TYPE_PARAM, TYPE_MSISDN);
        map.putSingle(MSISDN_PARAM, Long.toString(testMSISDN));
        map.putSingle(TZ_OFFSET, "+0000");
        map.putSingle(MAX_ROWS, "20");
        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String json = subsessionBIResource.getSubBIFailureData();

        final List<SubBIResult> results = getTranslator().translateResult(json,
                SubBIResult.class);

        validateResultsContainSGEHAndLTE(results);
    }

    @Test
    public void testSubBIFailedEventsIMSIGroup_30Minutes() throws Exception {
        jndiProperties.disableSucRawJNDIProperty();
        populateRawTemporaryTables(DateTimeUtilities
                .getDateTimeMinus30Minutes());
        populateImsiRawTemporaryTables(DateTimeUtilities
                .getDateTimeMinus30Minutes());
        populate_Group_Tables();
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(DISPLAY_PARAM, CHART_PARAM);
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(GROUP_NAME_PARAM, SAMPLE_IMSI_GROUP);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        map.putSingle(TZ_OFFSET, TZ_OFFSET_OF_ZERO);
        map.putSingle(MAX_ROWS, "20");
        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String json = subsessionBIResource.getSubBIFailureData();

        final List<SubBIResult> results = getTranslator().translateResult(json,
                SubBIResult.class);

        validateResultsContainSGEHAndLTE(results);
    }

    @Test
    public void testSubBIFailedEventsIMSIGroup_OneWeek() throws Exception {
        jndiProperties.disableSucRawJNDIProperty();
        populateRawTemporaryTables(DateTimeUtilities.getDateTimeMinus48Hours());
        populateImsiRawTemporaryTables(DateTimeUtilities
                .getDateTimeMinus48Hours());
        populate_Group_Tables();
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(DISPLAY_PARAM, CHART_PARAM);
        map.putSingle(TIME_QUERY_PARAM, "10080");
        map.putSingle(GROUP_NAME_PARAM, SAMPLE_IMSI_GROUP);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        map.putSingle(TZ_OFFSET, TZ_OFFSET_OF_ZERO);
        map.putSingle(MAX_ROWS, "20");
        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String json = subsessionBIResource.getSubBIFailureData();

        final List<SubBIResult> results = getTranslator().translateResult(json,
                SubBIResult.class);

        validateResultsContainSGEHAndLTE(results);
    }

    /**
     * Validates that only SGEH events are present
     * 
     * @param results
     */
    private void validateResultsContainsOnlySGEH(final List<SubBIResult> results) {
        final SubBIResult activateResult = results.get(0);
        assertEquals("1", activateResult.getSuccessCount());
        assertEquals("1", activateResult.getFailureCount());
        assertEquals("ACTIVATE,1", activateResult.getXAxisLabel());

        final SubBIResult israuResult = results.get(1);
        assertEquals("1", israuResult.getSuccessCount());
        assertEquals("1", israuResult.getFailureCount());
        assertEquals("ISRAU,3", israuResult.getXAxisLabel());

        final SubBIResult rauResult = results.get(2);
        assertEquals("1", rauResult.getSuccessCount());
        assertEquals("1", rauResult.getFailureCount());
        assertEquals("RAU,2", rauResult.getXAxisLabel());
    }

    /**
     * Validates that all events are present
     * 
     * @param results
     */
    private void validateResultsContainSGEHAndLTE(
            final List<SubBIResult> results) {

        final SubBIResult activateResult = results.get(0);
        assertEquals("1", activateResult.getSuccessCount());
        assertEquals("1", activateResult.getFailureCount());
        assertEquals("ACTIVATE,1", activateResult.getXAxisLabel());

        final SubBIResult israuResult = results.get(1);
        assertEquals("1", israuResult.getSuccessCount());
        assertEquals("1", israuResult.getFailureCount());
        assertEquals("ISRAU,3", israuResult.getXAxisLabel());

        final SubBIResult lAttachResult = results.get(2);
        assertEquals("1", lAttachResult.getSuccessCount());
        assertEquals("1", lAttachResult.getFailureCount());
        assertEquals("L_ATTACH,5", lAttachResult.getXAxisLabel());

        final SubBIResult lDetachResult = results.get(3);
        assertEquals("1", lDetachResult.getSuccessCount());
        assertEquals("1", lDetachResult.getFailureCount());
        assertEquals("L_DETACH,6", lDetachResult.getXAxisLabel());

        final SubBIResult lHandoverResult = results.get(4);
        assertEquals("1", lHandoverResult.getSuccessCount());
        assertEquals("1", lHandoverResult.getFailureCount());
        assertEquals("L_HANDOVER,7", lHandoverResult.getXAxisLabel());

        final SubBIResult rauResult = results.get(5);
        assertEquals("1", rauResult.getSuccessCount());
        assertEquals("1", rauResult.getFailureCount());
        assertEquals("RAU,2", rauResult.getXAxisLabel());
    }

    private void insertRow(final SQLExecutor sqlExecutor, final String table,
            final int eventID, final String datetimeID, final long imsi,
            final String ptmsi, final String date, final long msisdn)
            throws SQLException {
        final String localDateID = datetimeID.substring(0, 10);
        sqlExecutor.executeUpdate("insert into " + table + " values('" + date
                + "'," + eventID + "," + imsi + ",'" + localDateID + "', "
                + msisdn + ",'" + datetimeID + "'," + ptmsi + ")");
    }

    private void insertImsiRow(final SQLExecutor sqlExecutor,
            final String table, final int eventID, final String datetimeID,
            final long imsi, final String ptmsi, final String date,
            final int noOfSuccesses, final long msisdn) throws SQLException {
        sqlExecutor.executeUpdate("insert into " + table + " values('" + date
                + "'," + eventID + "," + imsi + ",                "
                + noOfSuccesses + ", " + msisdn + ", '" + datetimeID + "',"
                + ptmsi + ")");
    }

    private void populateRawTemporaryTables(final String dateTime)
            throws SQLException {
        SQLExecutor sqlExecutor = null;
        try {
            final String date = dateTime;
            sqlExecutor = new SQLExecutor(connection);

            insertRow(sqlExecutor, TEMP_EVENT_E_SGEH_ERR_RAW, 1, dateTime,
                    testIMSI, testPTMSI, date, testMSISDN);
            insertRow(sqlExecutor, TEMP_EVENT_E_SGEH_ERR_RAW, 2, dateTime,
                    testIMSI, testPTMSI, date, testMSISDN);
            insertRow(sqlExecutor, TEMP_EVENT_E_SGEH_ERR_RAW, 3, dateTime,
                    testIMSI, testPTMSI, date, testMSISDN);

            insertRow(sqlExecutor, TEMP_EVENT_E_SGEH_SUC_RAW, 1, dateTime,
                    testIMSI, testPTMSI, date, testMSISDN);
            insertRow(sqlExecutor, TEMP_EVENT_E_SGEH_SUC_RAW, 2, dateTime,
                    testIMSI, testPTMSI, date, testMSISDN);
            insertRow(sqlExecutor, TEMP_EVENT_E_SGEH_SUC_RAW, 3, dateTime,
                    testIMSI, testPTMSI, date, testMSISDN);

            insertRow(sqlExecutor, TEMP_EVENT_E_LTE_ERR_RAW, 5, dateTime,
                    testIMSI, testPTMSI, date, testMSISDN);
            insertRow(sqlExecutor, TEMP_EVENT_E_LTE_ERR_RAW, 6, dateTime,
                    testIMSI, testPTMSI, date, testMSISDN);
            insertRow(sqlExecutor, TEMP_EVENT_E_LTE_ERR_RAW, 7, dateTime,
                    testIMSI, testPTMSI, date, testMSISDN);

            insertRow(sqlExecutor, TEMP_EVENT_E_LTE_SUC_RAW, 5, dateTime,
                    testIMSI, testPTMSI, date, testMSISDN);
            insertRow(sqlExecutor, TEMP_EVENT_E_LTE_SUC_RAW, 6, dateTime,
                    testIMSI, testPTMSI, date, testMSISDN);
            insertRow(sqlExecutor, TEMP_EVENT_E_LTE_SUC_RAW, 7, dateTime,
                    testIMSI, testPTMSI, date, testMSISDN);
        } finally {
            if (sqlExecutor != null) {
                sqlExecutor.close();
            }
        }
    }

    private void populate_IMSI_MSISDN_TopologyTables() {
        Map<String, Object> values = new HashMap<String, Object>();
        values.put("IMSI", testIMSI);
        values.put("MSISDN", testMSISDN);
        try {
            insertRow(TEMP_DIM_E_IMSI_MSISDN, values);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private void populate_Group_Tables() {
        Map<String, Object> values = new HashMap<String, Object>();
        values = new HashMap<String, Object>();
        values.put("GROUP_NAME", SAMPLE_IMSI_GROUP);
        values.put("IMSI", testIMSI);
        try {
            insertRow(TEMP_GROUP_TYPE_E_IMSI, values);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private void populateImsiRawTemporaryTables(final String dateTime)
            throws SQLException {
        SQLExecutor sqlExecutor = null;
        try {
            sqlExecutor = new SQLExecutor(connection);
            final String date = dateTime;
            final int noOfSuccesses = 1;
            insertImsiRow(sqlExecutor, TEMP_EVENT_E_SGEH_IMSI_SUC_RAW, 1,
                    dateTime, testIMSI, testPTMSI, date, noOfSuccesses,
                    testMSISDN);
            insertImsiRow(sqlExecutor, TEMP_EVENT_E_SGEH_IMSI_SUC_RAW, 2,
                    dateTime, testIMSI, testPTMSI, date, noOfSuccesses,
                    testMSISDN);
            insertImsiRow(sqlExecutor, TEMP_EVENT_E_SGEH_IMSI_SUC_RAW, 3,
                    dateTime, testIMSI, testPTMSI, date, noOfSuccesses,
                    testMSISDN);

            insertImsiRow(sqlExecutor, TEMP_EVENT_E_LTE_IMSI_SUC_RAW, 5,
                    dateTime, testIMSI, testPTMSI, date, noOfSuccesses,
                    testMSISDN);
            insertImsiRow(sqlExecutor, TEMP_EVENT_E_LTE_IMSI_SUC_RAW, 6,
                    dateTime, testIMSI, testPTMSI, date, noOfSuccesses,
                    testMSISDN);
            insertImsiRow(sqlExecutor, TEMP_EVENT_E_LTE_IMSI_SUC_RAW, 7,
                    dateTime, testIMSI, testPTMSI, date, noOfSuccesses,
                    testMSISDN);
        } finally {
            if (sqlExecutor != null) {
                sqlExecutor.close();
            }
        }
    }

}
