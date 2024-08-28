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

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.*;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.*;
import static org.junit.Assert.*;

import java.sql.SQLException;
import java.util.*;

import javax.ws.rs.core.MultivaluedMap;

import org.junit.Test;

import com.ericsson.eniq.events.server.integritytests.stubs.ReplaceTablesWithTempTablesTemplateUtils;
import com.ericsson.eniq.events.server.resources.SubsessionBIResource;
import com.ericsson.eniq.events.server.resources.TestsWithTemporaryTablesBaseTestCase;
import com.ericsson.eniq.events.server.test.populator.LookupTechPackPopulator;
import com.ericsson.eniq.events.server.test.queryresults.SubBITerminalResult;
import com.ericsson.eniq.events.server.test.sql.SQLExecutor;
import com.ericsson.eniq.events.server.test.stubs.DummyUriInfoImpl;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;
import com.sun.jersey.core.util.MultivaluedMapImpl;

/**
 * @since 2011
 * 
 */
public class SubBITerminalSuccessRawWithPreparedTablesTest extends TestsWithTemporaryTablesBaseTestCase<SubBITerminalResult> {

    private static final Map<String, String> groupColumnsForIMSIGroupTable = new HashMap<String, String>();

    private static final Map<String, String> rawTableColumns = new HashMap<String, String>();

    private static final Map<String, Object> baseValues = new HashMap<String, Object>();

    private SubsessionBIResource subsessionBIResource;

    private final static List<String> tempDataTables = new ArrayList<String>();

    private static final String TEST_TIME = "30";

    private static final String TEST_TIME_WEEK = "10080";

    private static final String TEST_TIME_SIX_HOURS = "360";

    private static final String TEST_GROUP_NAME = "VIP";

    private static final String TEST_TZ_OFFSET = "+0000";

    private static final String TEST_MAX_ROWS = "50";

    private final static long TEST_IMSI = 312030410000004L;

    private final static long TEST_IMEISV = 1234567982131400L;

    private final static long TEST_IMSI_LTE = 312030419990004L;

    private static String TEST_DATETIME = DateTimeUtilities.getDateTimeMinus30Minutes();

    private static String TEST_LOCAL_DATETIME = DateTimeUtilities.getDateTimeMinus30Minutes().substring(0, 10);

    private static String TEST_DATETIME_WEEK = DateTimeUtilities.getDateTimeMinus36Hours();

    private static String TEST_LOCAL_DATETIME_WEEK = DateTimeUtilities.getDateTimeMinus36Hours().substring(0, 10);

    private static Boolean OneWeek = true;

    static {
        tempDataTables.add(TEMP_EVENT_E_SGEH_ERR_RAW);
        tempDataTables.add(TEMP_EVENT_E_SGEH_SUC_RAW);
        tempDataTables.add(TEMP_EVENT_E_LTE_ERR_RAW);
        tempDataTables.add(TEMP_EVENT_E_LTE_SUC_RAW);

        groupColumnsForIMSIGroupTable.put("IMSI", "unsigned bigint");
        groupColumnsForIMSIGroupTable.put("GROUP_NAME", "varchar(64)");

        rawTableColumns.put("TAC", "unsigned int");
        rawTableColumns.put("DATETIME_ID", "timestamp");
        rawTableColumns.put("IMSI", "unsigned bigint");
        rawTableColumns.put("IMEISV", "unsigned bigint");
        rawTableColumns.put("LOCAL_DATE_ID", "date");

        baseValues.put("IMEISV", TEST_IMEISV);
    }

    @Override
    public void onSetUp() throws Exception {
        super.onSetUp();
        ReplaceTablesWithTempTablesTemplateUtils.useTemporaryTableFor(DIM_E_SGEH_TAC);
        new LookupTechPackPopulator().createAndPopulateLookupTable(connection, TEMP_DIM_E_SGEH_TAC);
        subsessionBIResource = new SubsessionBIResource();

        for (final String tempTable : tempDataTables) {
            createTemporaryTableWithColumnTypes(tempTable, rawTableColumns);
        }

        createTemporaryTableWithColumnTypes(TEMP_GROUP_TYPE_E_IMSI, groupColumnsForIMSIGroupTable);

        populateTemporaryTables();

        populateTemporaryTablesForWeek();

        attachDependencies(subsessionBIResource);
    }

    private void populateTemporaryTables() throws SQLException {
        final SQLExecutor sqlExecutor = null;
        try {
            // Populate SGEH tables
            Map<String, Object> rowSpecificValues = new HashMap<String, Object>();
            rowSpecificValues.putAll(baseValues);

            rowSpecificValues.put("TAC", SAMPLE_TAC);
            rowSpecificValues.put("IMSI", TEST_IMSI);
            rowSpecificValues.put("DATETIME_ID", TEST_DATETIME);
            rowSpecificValues.put(LOCAL_DATE_ID, TEST_LOCAL_DATETIME);
            insertRow(TEMP_EVENT_E_SGEH_ERR_RAW, rowSpecificValues);

            rowSpecificValues.put("DATETIME_ID", TEST_DATETIME);
            rowSpecificValues.put(LOCAL_DATE_ID, TEST_LOCAL_DATETIME);
            insertRow(TEMP_EVENT_E_SGEH_ERR_RAW, rowSpecificValues);

            rowSpecificValues.put("DATETIME_ID", TEST_DATETIME);
            rowSpecificValues.put(LOCAL_DATE_ID, TEST_LOCAL_DATETIME);
            insertRow(TEMP_EVENT_E_SGEH_SUC_RAW, rowSpecificValues);

            // Populate LTE tables
            rowSpecificValues.put("TAC", SAMPLE_TAC_2);
            rowSpecificValues.put("IMSI", TEST_IMSI_LTE);

            rowSpecificValues.put("DATETIME_ID", TEST_DATETIME);
            rowSpecificValues.put(LOCAL_DATE_ID, TEST_LOCAL_DATETIME);
            insertRow(TEMP_EVENT_E_LTE_ERR_RAW, rowSpecificValues);

            rowSpecificValues.put("DATETIME_ID", TEST_DATETIME);
            rowSpecificValues.put(LOCAL_DATE_ID, TEST_LOCAL_DATETIME);
            insertRow(TEMP_EVENT_E_LTE_SUC_RAW, rowSpecificValues);

            rowSpecificValues.put("DATETIME_ID", TEST_DATETIME);
            rowSpecificValues.put(LOCAL_DATE_ID, TEST_LOCAL_DATETIME);
            insertRow(TEMP_EVENT_E_LTE_SUC_RAW, rowSpecificValues);

            // Populate imsi group table
            rowSpecificValues = new HashMap<String, Object>();
            rowSpecificValues.put("GROUP_NAME", TEST_GROUP_NAME);
            rowSpecificValues.put("IMSI", TEST_IMSI);
            insertRow(TEMP_GROUP_TYPE_E_IMSI, rowSpecificValues);
            rowSpecificValues.put("IMSI", TEST_IMSI_LTE);
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

            rowSpecificValues.put("TAC", SAMPLE_TAC);
            rowSpecificValues.put("IMSI", TEST_IMSI);
            rowSpecificValues.put("DATETIME_ID", TEST_DATETIME_WEEK);
            rowSpecificValues.put(LOCAL_DATE_ID, TEST_LOCAL_DATETIME_WEEK);
            insertRow(TEMP_EVENT_E_SGEH_ERR_RAW, rowSpecificValues);

            rowSpecificValues.put("DATETIME_ID", TEST_DATETIME_WEEK);
            rowSpecificValues.put(LOCAL_DATE_ID, TEST_LOCAL_DATETIME_WEEK);
            insertRow(TEMP_EVENT_E_SGEH_ERR_RAW, rowSpecificValues);

            rowSpecificValues.put("DATETIME_ID", TEST_DATETIME_WEEK);
            rowSpecificValues.put(LOCAL_DATE_ID, TEST_LOCAL_DATETIME_WEEK);
            insertRow(TEMP_EVENT_E_SGEH_SUC_RAW, rowSpecificValues);

            // Populate LTE tables
            rowSpecificValues.put("TAC", SAMPLE_TAC_2);
            rowSpecificValues.put("IMSI", TEST_IMSI_LTE);

            rowSpecificValues.put("DATETIME_ID", TEST_DATETIME_WEEK);
            rowSpecificValues.put(LOCAL_DATE_ID, TEST_LOCAL_DATETIME_WEEK);
            insertRow(TEMP_EVENT_E_LTE_ERR_RAW, rowSpecificValues);

            rowSpecificValues.put("DATETIME_ID", TEST_DATETIME_WEEK);
            rowSpecificValues.put(LOCAL_DATE_ID, TEST_LOCAL_DATETIME_WEEK);
            insertRow(TEMP_EVENT_E_LTE_SUC_RAW, rowSpecificValues);

            rowSpecificValues.put("DATETIME_ID", TEST_DATETIME_WEEK);
            rowSpecificValues.put(LOCAL_DATE_ID, TEST_LOCAL_DATETIME_WEEK);
            insertRow(TEMP_EVENT_E_LTE_SUC_RAW, rowSpecificValues);

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
        jndiProperties.useSucRawJNDIProperty();
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(GROUP_NAME_PARAM, TEST_GROUP_NAME);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        OneWeek = false;
        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String result = subsessionBIResource.getSubBITerminalData();
        System.out.println(result);

        final List<SubBITerminalResult> results = getTranslator().translateResult(result, SubBITerminalResult.class);

        validResults(results.get(1), OneWeek);
        validResults(results.get(0), OneWeek);
    }

    @Test
    public void testGetSubBITerminalDataGroupSixHours() throws Exception {
        jndiProperties.useSucRawJNDIProperty();
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME_SIX_HOURS);
        map.putSingle(GROUP_NAME_PARAM, TEST_GROUP_NAME);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        OneWeek = false;
        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String result = subsessionBIResource.getSubBITerminalData();
        System.out.println(result);

        final List<SubBITerminalResult> results = getTranslator().translateResult(result, SubBITerminalResult.class);

        validResults(results.get(1), OneWeek);
        validResults(results.get(0), OneWeek);
    }

    @Test
    public void testGetSubBITerminalDataGroupOneWeek() throws Exception {
        jndiProperties.useSucRawJNDIProperty();
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME_WEEK);
        map.putSingle(GROUP_NAME_PARAM, TEST_GROUP_NAME);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String result = subsessionBIResource.getSubBITerminalData();
        System.out.println(result);
        OneWeek = true;
        final List<SubBITerminalResult> results = getTranslator().translateResult(result, SubBITerminalResult.class);

        validResults(results.get(1), OneWeek);
        validResults(results.get(0), OneWeek);
    }

    @Test
    public void testGetSubBITerminalData() throws Exception {
        jndiProperties.useSucRawJNDIProperty();
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        map.putSingle(IMSI_PARAM, Long.toString(TEST_IMSI_LTE));
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        OneWeek = false;
        final String result = subsessionBIResource.getSubBITerminalData();
        System.out.println(result);

        final List<SubBITerminalResult> results = getTranslator().translateResult(result, SubBITerminalResult.class);

        validResults(results.get(0), OneWeek);
    }

    @Test
    public void testGetSubBITerminalDataSixHours() throws Exception {
        jndiProperties.useSucRawJNDIProperty();
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME_SIX_HOURS);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        map.putSingle(IMSI_PARAM, Long.toString(TEST_IMSI_LTE));
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        OneWeek = false;
        final String result = subsessionBIResource.getSubBITerminalData();
        System.out.println(result);

        final List<SubBITerminalResult> results = getTranslator().translateResult(result, SubBITerminalResult.class);

        validResults(results.get(0), OneWeek);
    }

    @Test
    public void testGetSubBITerminalDataOneWeek() throws Exception {
        jndiProperties.useSucRawJNDIProperty();
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME_WEEK);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        map.putSingle(IMSI_PARAM, Long.toString(TEST_IMSI_LTE));
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
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
            if (OneWeek) {
                expected.setFirstSeen(TEST_DATETIME_WEEK + ".000000");
                expected.setLastSeen(TEST_DATETIME_WEEK + ".000000");
            } else {
                expected.setFirstSeen(TEST_DATETIME + ".000000");
                expected.setLastSeen(TEST_DATETIME + ".000000");
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
            if (OneWeek) {
                expected.setFirstSeen(TEST_DATETIME_WEEK + ".000000");
                expected.setLastSeen(TEST_DATETIME_WEEK + ".000000");
            } else {
                expected.setFirstSeen(TEST_DATETIME + ".000000");
                expected.setLastSeen(TEST_DATETIME + ".000000");
            }

            assertEquals(expected, result);
        }
    }
}
