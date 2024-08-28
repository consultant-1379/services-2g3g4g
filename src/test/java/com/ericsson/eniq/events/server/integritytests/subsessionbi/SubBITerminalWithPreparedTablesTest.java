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

import com.ericsson.eniq.events.server.integritytests.stubs.ReplaceTablesWithTempTablesTemplateUtils;
import com.ericsson.eniq.events.server.resources.SubsessionBIResource;
import com.ericsson.eniq.events.server.resources.TestsWithTemporaryTablesBaseTestCase;
import com.ericsson.eniq.events.server.test.populator.LookupTechPackPopulator;
import com.ericsson.eniq.events.server.test.queryresults.SubBITerminalResult;
import com.ericsson.eniq.events.server.test.sql.SQLExecutor;
import com.ericsson.eniq.events.server.test.stubs.DummyUriInfoImpl;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;
import com.sun.jersey.core.util.MultivaluedMapImpl;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

import javax.ws.rs.core.MultivaluedMap;
import java.sql.SQLException;
import java.util.*;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.*;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.*;

/**
 * @since 2011
 * 
 */
public class SubBITerminalWithPreparedTablesTest extends TestsWithTemporaryTablesBaseTestCase<SubBITerminalResult> {

    private SubsessionBIResource subsessionBIResource;

    private static final Map<String, String> groupColumnsForIMSIGroupTable = new HashMap<String, String>();
    private static final Map<String, Object> baseValues = new HashMap<String, Object>();

    private final static List<String> tempRawDataTables = new ArrayList<String>();
    private final static List<String> tempImsiRawDataTables = new ArrayList<String>();

    private final static Collection<String> rawDataColumns = new ArrayList<String>();
    private final static Collection<String> imsiRawDataColumns = new ArrayList<String>();

    private static final String TEST_TIME = THIRTY_MINUTES;
    private static final String TEST_TIME_WEEK = ONE_WEEK;
    private static final String TEST_TIME_SIX_HOURS =SIX_HOURS;

    private static final String TEST_GROUP_NAME = "VIP";
    private final static long TEST_IMSI = 312030410000004L;
    private final static long TEST_IMEISV = 1234567982131400L;
    private final static long TEST_IMSI_LTE = 312030419990004L;
    private static String TEST_DATETIME = DateTimeUtilities.getDateTimeMinus30Minutes();
    private static String TEST_LOCAL_DATETIME = DateTimeUtilities.getDateTimeMinus30Minutes().substring(0, 10);
    private static String TEST_DATETIME_WEEK = DateTimeUtilities.getDateTimeMinus36Hours();
    private static String TEST_LOCAL_DATETIME_WEEK = DateTimeUtilities.getDateTimeMinus36Hours().substring(0, 10);
    private static Boolean OneWeek = true;

    static {
        tempRawDataTables.add(TEMP_EVENT_E_SGEH_ERR_RAW);
        tempRawDataTables.add(TEMP_EVENT_E_SGEH_SUC_RAW);
        tempRawDataTables.add(TEMP_EVENT_E_LTE_ERR_RAW);
        tempRawDataTables.add(TEMP_EVENT_E_LTE_SUC_RAW);

        tempImsiRawDataTables.add(TEMP_EVENT_E_SGEH_IMSI_SUC_RAW);
        tempImsiRawDataTables.add(TEMP_EVENT_E_LTE_IMSI_SUC_RAW);

        rawDataColumns.add(TAC);
        rawDataColumns.add(DATETIME_ID);
        rawDataColumns.add(IMSI);
        rawDataColumns.add(IMEISV);
        rawDataColumns.add(LOCAL_DATE_ID);


        imsiRawDataColumns.add(TAC);
        imsiRawDataColumns.add(DATETIME_ID);
        imsiRawDataColumns.add(IMSI);
        imsiRawDataColumns.add(IMEISV);
        imsiRawDataColumns.add(NO_OF_SUCCESSES);

        groupColumnsForIMSIGroupTable.put(IMSI, UNSIGNED_BIGINT);
        groupColumnsForIMSIGroupTable.put(GROUP_NAME, VARCHAR_64);

        baseValues.put(IMEISV, TEST_IMEISV);
    }

    @Override
    public void onSetUp() throws Exception {
        super.onSetUp();
        ReplaceTablesWithTempTablesTemplateUtils.useTemporaryTableFor(DIM_E_SGEH_TAC);
        new LookupTechPackPopulator().createAndPopulateLookupTable(connection, TEMP_DIM_E_SGEH_TAC);
        subsessionBIResource = new SubsessionBIResource();

        for(String tempTable : tempRawDataTables){
            createTemporaryTable(tempTable, rawDataColumns);
        }

        for(String tempTable : tempImsiRawDataTables){
            createTemporaryTable(tempTable, imsiRawDataColumns);
        }

        createTemporaryTableWithColumnTypes(TEMP_GROUP_TYPE_E_IMSI, groupColumnsForIMSIGroupTable);

        populateTemporaryTables();
        populateImsiRawTemporaryTables();
        populateTemporaryTablesForWeek();

        attachDependencies(subsessionBIResource);
    }

    private void insertImsiRow(final SQLExecutor sqlExecutor, final String table, final int tac, final String datetimeID, final long imsi,
                               final long imeisv, final int noOfSuccesses) throws SQLException {
        sqlExecutor.executeUpdate("insert into " + table + " values(" + imeisv + "," + tac + "," + imsi + "," +
                   noOfSuccesses + ",'" + datetimeID + "')");
    }

    private void populateTemporaryTables() throws SQLException {
        final SQLExecutor sqlExecutor = null;
        try {
            // Populate SGEH tables
            Map<String, Object> rowSpecificValues = new HashMap<String, Object>();
            rowSpecificValues.putAll(baseValues);

            rowSpecificValues.put(TAC, SAMPLE_TAC);
            rowSpecificValues.put(IMSI, TEST_IMSI);
            rowSpecificValues.put(IMEISV, TEST_IMEISV);

            rowSpecificValues.put(DATETIME_ID, TEST_DATETIME);
            rowSpecificValues.put(LOCAL_DATE_ID, TEST_LOCAL_DATETIME);
            insertRow(TEMP_EVENT_E_SGEH_ERR_RAW, rowSpecificValues);

            rowSpecificValues.put(DATETIME_ID, TEST_DATETIME);
            rowSpecificValues.put(LOCAL_DATE_ID, TEST_LOCAL_DATETIME);
            insertRow(TEMP_EVENT_E_SGEH_ERR_RAW, rowSpecificValues);

            rowSpecificValues.put(DATETIME_ID, TEST_DATETIME);
            rowSpecificValues.put(LOCAL_DATE_ID, TEST_LOCAL_DATETIME);
            insertRow(TEMP_EVENT_E_SGEH_SUC_RAW, rowSpecificValues);

            // Populate LTE tables
            rowSpecificValues.put(TAC, SAMPLE_TAC_2);
            rowSpecificValues.put(IMSI, TEST_IMSI_LTE);
            rowSpecificValues.put(IMEISV, TEST_IMEISV);

            rowSpecificValues.put(DATETIME_ID, TEST_DATETIME);
            rowSpecificValues.put(LOCAL_DATE_ID, TEST_LOCAL_DATETIME);
            insertRow(TEMP_EVENT_E_LTE_ERR_RAW, rowSpecificValues);

            rowSpecificValues.put(DATETIME_ID, TEST_DATETIME);
            rowSpecificValues.put(LOCAL_DATE_ID, TEST_LOCAL_DATETIME);
            insertRow(TEMP_EVENT_E_LTE_SUC_RAW, rowSpecificValues);

            rowSpecificValues.put(DATETIME_ID, TEST_DATETIME);
            rowSpecificValues.put(LOCAL_DATE_ID, TEST_LOCAL_DATETIME);
            insertRow(TEMP_EVENT_E_LTE_SUC_RAW, rowSpecificValues);

            // Populate imsi group table
            rowSpecificValues.clear();
            rowSpecificValues.put(GROUP_NAME, TEST_GROUP_NAME);
            rowSpecificValues.put(IMSI, TEST_IMSI);
            insertRow(TEMP_GROUP_TYPE_E_IMSI, rowSpecificValues);
            rowSpecificValues.put(IMSI, TEST_IMSI_LTE);
            insertRow(TEMP_GROUP_TYPE_E_IMSI, rowSpecificValues);

        } finally {
            if (sqlExecutor != null) {
                sqlExecutor.close();
            }
        }
    }

    private void populateTemporaryTablesForWeek() throws SQLException {
        final SQLExecutor sqlExecutor = null;
        try {
            // Populate SGEH tables
            Map<String, Object> rowSpecificValues = new HashMap<String, Object>();
            rowSpecificValues.putAll(baseValues);

            rowSpecificValues.put(TAC, SAMPLE_TAC);
            rowSpecificValues.put(IMSI, TEST_IMSI);
            rowSpecificValues.put(IMEISV, TEST_IMEISV);

            rowSpecificValues.put(DATETIME_ID, TEST_DATETIME_WEEK);
            rowSpecificValues.put(LOCAL_DATE_ID, TEST_LOCAL_DATETIME_WEEK);
            insertRow(TEMP_EVENT_E_SGEH_ERR_RAW, rowSpecificValues);

            rowSpecificValues.put(DATETIME_ID, TEST_DATETIME_WEEK);
            rowSpecificValues.put(LOCAL_DATE_ID, TEST_LOCAL_DATETIME_WEEK);
            insertRow(TEMP_EVENT_E_SGEH_ERR_RAW, rowSpecificValues);

            rowSpecificValues.put(DATETIME_ID, TEST_DATETIME_WEEK);
            rowSpecificValues.put(LOCAL_DATE_ID, TEST_LOCAL_DATETIME_WEEK);
            insertRow(TEMP_EVENT_E_SGEH_SUC_RAW, rowSpecificValues);

            // Populate LTE tables
            rowSpecificValues.put(TAC, SAMPLE_TAC_2);
            rowSpecificValues.put(IMSI, TEST_IMSI_LTE);
            rowSpecificValues.put(IMEISV, TEST_IMEISV);

            rowSpecificValues.put(DATETIME_ID, TEST_DATETIME_WEEK);
            rowSpecificValues.put(LOCAL_DATE_ID, TEST_LOCAL_DATETIME_WEEK);
            insertRow(TEMP_EVENT_E_LTE_ERR_RAW, rowSpecificValues);

            rowSpecificValues.put(DATETIME_ID, TEST_DATETIME_WEEK);
            rowSpecificValues.put(LOCAL_DATE_ID, TEST_LOCAL_DATETIME_WEEK);
            insertRow(TEMP_EVENT_E_LTE_SUC_RAW, rowSpecificValues);
            
            rowSpecificValues.put(DATETIME_ID, TEST_DATETIME_WEEK);
            rowSpecificValues.put(LOCAL_DATE_ID, TEST_LOCAL_DATETIME_WEEK);
            insertRow(TEMP_EVENT_E_LTE_SUC_RAW, rowSpecificValues);

        } finally {
            if (sqlExecutor != null) {
                sqlExecutor.close();
            }
        }
    }

    private void populateImsiRawTemporaryTables() throws SQLException {
        SQLExecutor sqlExecutor = null;
        try {
            sqlExecutor = new SQLExecutor(connection);

            final int noOfSuccesses = 1;
            insertImsiRow(sqlExecutor, TEMP_EVENT_E_SGEH_IMSI_SUC_RAW, SAMPLE_TAC, TEST_DATETIME, TEST_IMSI, TEST_IMEISV, noOfSuccesses);
            insertImsiRow(sqlExecutor, TEMP_EVENT_E_SGEH_IMSI_SUC_RAW, SAMPLE_TAC, TEST_DATETIME_WEEK, TEST_IMSI, TEST_IMEISV, noOfSuccesses);

            insertImsiRow(sqlExecutor, TEMP_EVENT_E_LTE_IMSI_SUC_RAW, SAMPLE_TAC_2, TEST_DATETIME, TEST_IMSI_LTE, TEST_IMEISV,  noOfSuccesses);
            insertImsiRow(sqlExecutor, TEMP_EVENT_E_LTE_IMSI_SUC_RAW, SAMPLE_TAC_2, TEST_DATETIME_WEEK, TEST_IMSI_LTE, TEST_IMEISV,  noOfSuccesses);

            insertImsiRow(sqlExecutor, TEMP_EVENT_E_LTE_IMSI_SUC_RAW, SAMPLE_TAC_2, TEST_DATETIME, TEST_IMSI_LTE, TEST_IMEISV,  noOfSuccesses);
            insertImsiRow(sqlExecutor, TEMP_EVENT_E_LTE_IMSI_SUC_RAW, SAMPLE_TAC_2, TEST_DATETIME_WEEK, TEST_IMSI_LTE, TEST_IMEISV,  noOfSuccesses);
        } finally {
            if (sqlExecutor != null) {
                sqlExecutor.close();
            }
        }
    }

    /**
     * Test method for {@link com.ericsson.eniq.events.server.resources.SubsessionBIResource#getSubBITerminalData()}.
     */
    @Test
    public void testGetSubBITerminalDataGroup() throws Exception {
        jndiProperties.disableSucRawJNDIProperty();
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(GROUP_NAME_PARAM, TEST_GROUP_NAME);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        map.putSingle(TZ_OFFSET, TZ_OFFSET_OF_ZERO);
        map.putSingle(MAX_ROWS, DEFAULT_MAX_ROWS);
        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        OneWeek= false;
        final String result = subsessionBIResource.getSubBITerminalData();
        System.out.println(result);

        final List<SubBITerminalResult> results = getTranslator().translateResult(result, SubBITerminalResult.class);

        validResults(results.get(1), OneWeek);
        validResults(results.get(0), OneWeek);
    }
    @Test
    public void testGetSubBITerminalDataGroupSixHours() throws Exception {
        jndiProperties.disableSucRawJNDIProperty();
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME_SIX_HOURS);
        map.putSingle(GROUP_NAME_PARAM, TEST_GROUP_NAME);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        map.putSingle(TZ_OFFSET, TZ_OFFSET_OF_ZERO);
        map.putSingle(MAX_ROWS, DEFAULT_MAX_ROWS);
        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        OneWeek= false;
        final String result = subsessionBIResource.getSubBITerminalData();
        System.out.println(result);

        final List<SubBITerminalResult> results = getTranslator().translateResult(result, SubBITerminalResult.class);

        validResults(results.get(1), OneWeek);
        validResults(results.get(0), OneWeek);
    }

    @Test
    public void testGetSubBITerminalDataGroupOneWeek() throws Exception {
        jndiProperties.disableSucRawJNDIProperty();
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME_WEEK);
        map.putSingle(GROUP_NAME_PARAM, TEST_GROUP_NAME);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        map.putSingle(TZ_OFFSET, TZ_OFFSET_OF_ZERO);
        map.putSingle(MAX_ROWS, DEFAULT_MAX_ROWS);
        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        OneWeek=true;
        final String result = subsessionBIResource.getSubBITerminalData();
        System.out.println(result);

        final List<SubBITerminalResult> results = getTranslator().translateResult(result, SubBITerminalResult.class);

        validResults(results.get(1), OneWeek);
        validResults(results.get(0), OneWeek);
    }

    @Test
    public void testGetSubBITerminalData() throws Exception {
        jndiProperties.disableSucRawJNDIProperty();
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        map.putSingle(IMSI_PARAM, Long.toString(TEST_IMSI_LTE));
        map.putSingle(TZ_OFFSET, TZ_OFFSET_OF_ZERO);
        map.putSingle(MAX_ROWS, DEFAULT_MAX_ROWS);
        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        OneWeek = false;
        final String result = subsessionBIResource.getSubBITerminalData();
        System.out.println(result);

        final List<SubBITerminalResult> results = getTranslator().translateResult(result, SubBITerminalResult.class);

        validResults(results.get(0), OneWeek);
    }

    @Test
    public void testGetSubBITerminalDataSixHours() throws Exception {
        jndiProperties.disableSucRawJNDIProperty();
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME_SIX_HOURS);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        map.putSingle(IMSI_PARAM, Long.toString(TEST_IMSI_LTE));
        map.putSingle(TZ_OFFSET, TZ_OFFSET_OF_ZERO);
        map.putSingle(MAX_ROWS, DEFAULT_MAX_ROWS);
        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        OneWeek = false;
        final String result = subsessionBIResource.getSubBITerminalData();
        System.out.println(result);

        final List<SubBITerminalResult> results = getTranslator().translateResult(result, SubBITerminalResult.class);

        validResults(results.get(0), OneWeek);
    }

    @Test
    public void testGetSubBITerminalDataOneWeek() throws Exception {
        jndiProperties.disableSucRawJNDIProperty();
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME_WEEK);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        map.putSingle(IMSI_PARAM, Long.toString(TEST_IMSI_LTE));
        map.putSingle(TZ_OFFSET, TZ_OFFSET_OF_ZERO);
        map.putSingle(MAX_ROWS, DEFAULT_MAX_ROWS);
        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        OneWeek = true;
        final String result = subsessionBIResource.getSubBITerminalData();
        System.out.println(result);

        final List<SubBITerminalResult> results = getTranslator().translateResult(result, SubBITerminalResult.class);

        validResults(results.get(0), OneWeek);
    }

    private void validResults(final SubBITerminalResult result, Boolean OneWeek) {
        if (result.toString().contains(Long.toString(TEST_IMSI))) {
            final SubBITerminalResult expected = new SubBITerminalResult();

            expected.setImsi(Long.toString(TEST_IMSI));
            expected.setImeisv(Long.toString(TEST_IMEISV));
            expected.setTotalEvents("3");
            expected.setManufacturer(MANUFACTURER_FOR_SAMPLE_TAC);
            expected.setMarketingName(MARKETING_NAME_FOR_SAMPLE_TAC);
            expected.setTac(SAMPLE_TAC);
            if (OneWeek){
                expected.setFirstSeen(TEST_DATETIME_WEEK + ".0");
                expected.setLastSeen(TEST_DATETIME_WEEK + ".0");
            } else {
                expected.setFirstSeen(TEST_DATETIME + ".0");
                expected.setLastSeen(TEST_DATETIME + ".0");
            }
            expected.setFailureCount("2");
            expected.setSuccessCount("1");
            expected.setCapability(SAMPLE_BAND);

            assertEquals(expected, result);
        } else {
            final SubBITerminalResult expected = new SubBITerminalResult();
            expected.setImsi(Long.toString(TEST_IMSI_LTE));
            expected.setManufacturer(MANUFACTURER_FOR_SAMPLE_TAC_2);
            expected.setMarketingName(MARKETING_NAME_FOR_SAMPLE_TAC_2);
            expected.setTac(SAMPLE_TAC_2);
            expected.setCapability(SAMPLE_BAND);
            expected.setImeisv(Long.toString(TEST_IMEISV));
            expected.setFailureCount("1");
            expected.setSuccessCount("2");
            expected.setTotalEvents("3");
            if (OneWeek){
                expected.setFirstSeen(TEST_DATETIME_WEEK + ".0");
                expected.setLastSeen(TEST_DATETIME_WEEK + ".0");
            } else {
                expected.setFirstSeen(TEST_DATETIME + ".0");
                expected.setLastSeen(TEST_DATETIME + ".0");
            }

            assertEquals(expected, result);
        }
    }
}
