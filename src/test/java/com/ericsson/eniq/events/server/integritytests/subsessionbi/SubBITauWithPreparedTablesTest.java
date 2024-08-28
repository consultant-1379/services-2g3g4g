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
import com.ericsson.eniq.events.server.test.queryresults.SubBITauResult;
import com.ericsson.eniq.events.server.test.sql.SQLExecutor;
import com.ericsson.eniq.events.server.test.stubs.DummyUriInfoImpl;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;
import com.sun.jersey.core.util.MultivaluedMapImpl;
import org.junit.Before;
import org.junit.Test;

import javax.ws.rs.core.MultivaluedMap;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.*;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.*;
import static junit.framework.Assert.assertEquals;

public class SubBITauWithPreparedTablesTest extends TestsWithTemporaryTablesBaseTestCase<SubBITauResult> {

    private static final Map<String, String> groupColumnsForIMSIGroupTable = new HashMap<String, String>();

    private static final Map<String, String> imsiMsisdnColumns = new HashMap<String, String>();

    private static final Map<String, String> imsiRawTableColumns = new HashMap<String, String>();

    private static final Map<String, String> dimTableColumns = new HashMap<String, String>();

    private static final Map<String, String> rawTableColumns = new HashMap<String, String>();

    private SubsessionBIResource subsessionBIResource;

    private static final String TEST_GROUP_NAME = "VIP";

    private static final String TEST_MAX_ROWS = "50";

    private final static long TEST_IMSI_LTE = 312030419990004L;

    private static final long TEST_MSISDN = 312030410000004L;

    private final static int TEST_EVENT_ID = 8;

    private final static int TEST_EVENT_SUBTYPE_ID = 3;

    private static String TEST_DATETIME = DateTimeUtilities.getDateTimeMinus30Minutes();

    private static String TEST_LOCAL_DATETIME = DateTimeUtilities.getDateTimeMinus30Minutes().substring(0, 10);

    private static String TEST_DATETIME_WEEK = DateTimeUtilities.getDateTimeMinus36Hours();

    private static String TEST_LOCAL_DATETIME_WEEK = DateTimeUtilities.getDateTimeMinus36Hours().substring(0, 10);

    private final static int TEST_FAILURES = 1;

    private final static int TEST_SUCCESSES = 2;

    private final static String TEST_TAC_TAU = "1280600,From Gn SGSN TAU type";
    
    public Map<String, String> getMsisdnColumns(){
        imsiMsisdnColumns.put(IMSI, UNSIGNED_BIGINT);
        imsiMsisdnColumns.put(MSISDN, UNSIGNED_BIGINT);
        return imsiMsisdnColumns;
    }

    public Map<String, String> getGroupColumnsForImsiGroupTable(){
        groupColumnsForIMSIGroupTable.put(IMSI,UNSIGNED_BIGINT);
        groupColumnsForIMSIGroupTable.put(GROUP_NAME, VARCHAR_64);
        return groupColumnsForIMSIGroupTable;
    }

    public Map<String, String> getRawTableColumns(){
        rawTableColumns.put(TAC, UNSIGNED_INT);
        rawTableColumns.put(IMSI, UNSIGNED_BIGINT);
        rawTableColumns.put(EVENT_ID, TINYINT);
        rawTableColumns.put(EVENT_SUBTYPE_ID, TINYINT);
        rawTableColumns.put(DATETIME_ID, TIMESTAMP);
        rawTableColumns.put(LOCAL_DATE_ID, DATE);
        return rawTableColumns;
    }

    public Map<String, String> getImsiRawTableColumns(){
        imsiRawTableColumns.put(TAC, UNSIGNED_INT);
        imsiRawTableColumns.put(IMSI, UNSIGNED_BIGINT);
        imsiRawTableColumns.put(NO_OF_SUCCESSES, UNSIGNED_BIGINT);
        imsiRawTableColumns.put(EVENT_ID, TINYINT);
        imsiRawTableColumns.put(EVENT_SUBTYPE_ID, TINYINT);
        imsiRawTableColumns.put(DATETIME_ID, TIMESTAMP);
        return imsiRawTableColumns;
    }

    public Map<String, String> getDimRawTableColumns(){
        dimTableColumns.put(EVENT_ID, TINYINT);
        dimTableColumns.put(EVENT_SUBTYPE_ID, TINYINT);
        return dimTableColumns;
    }


    @Override
    @Before
    public void onSetUp() throws Exception {
        super.onSetUp();
        subsessionBIResource = new SubsessionBIResource();

        createTemporaryTableWithColumnTypes(TEMP_EVENT_E_LTE_ERR_RAW, getRawTableColumns());

        createTemporaryTableWithColumnTypes(TEMP_EVENT_E_LTE_IMSI_SUC_RAW, getImsiRawTableColumns());

        createTemporaryTableWithColumnTypes(TEMP_DIM_E_LTE_EVENT_SUBTYPE, getDimRawTableColumns());

        createTemporaryTableWithColumnTypes(TEMP_DIM_E_IMSI_MSISDN, getMsisdnColumns());

        createTemporaryTableWithColumnTypes(TEMP_GROUP_TYPE_E_IMSI, getGroupColumnsForImsiGroupTable());

        populateTemporaryTables();

        attachDependencies(subsessionBIResource);
    }

    private void populateTemporaryTables() throws SQLException {
        populateLteTables();
        populateImsiGroupTables();
        populateImsiRawTemporaryTables();
        populateDimMsisdnTables();
        populateDimSubtypeTables();
    }



    private void insertImsiRow(final SQLExecutor sqlExecutor, final String table, final int tac, final long imsi, final int no_of_successes,
                               final int event_id, final int event_subtype_id, final String datetimeID) throws SQLException {
        sqlExecutor.executeUpdate("insert into " + table + "(tac, imsi, no_of_successes,event_id, event_subtype_id, datetime_id) values(" + tac + "," + imsi + "," + no_of_successes + "," + event_id + "," + event_subtype_id + ",'" + datetimeID + "')");
    }

    private void populateImsiRawTemporaryTables() throws SQLException {
        SQLExecutor sqlExecutor = null;
        try {
            sqlExecutor = new SQLExecutor(connection);
            final int noOfSuccesses = 1;
            insertImsiRow(sqlExecutor, TEMP_EVENT_E_LTE_IMSI_SUC_RAW, SAMPLE_TAC_2, TEST_IMSI_LTE, noOfSuccesses,TEST_EVENT_ID, TEST_EVENT_SUBTYPE_ID, TEST_DATETIME);
            insertImsiRow(sqlExecutor, TEMP_EVENT_E_LTE_IMSI_SUC_RAW, SAMPLE_TAC_2, TEST_IMSI_LTE, noOfSuccesses,TEST_EVENT_ID, TEST_EVENT_SUBTYPE_ID, TEST_DATETIME);
            insertImsiRow(sqlExecutor, TEMP_EVENT_E_LTE_IMSI_SUC_RAW, SAMPLE_TAC_2, TEST_IMSI_LTE, noOfSuccesses,TEST_EVENT_ID, TEST_EVENT_SUBTYPE_ID, TEST_DATETIME_WEEK);
            insertImsiRow(sqlExecutor, TEMP_EVENT_E_LTE_IMSI_SUC_RAW, SAMPLE_TAC_2, TEST_IMSI_LTE, noOfSuccesses,TEST_EVENT_ID, TEST_EVENT_SUBTYPE_ID, TEST_DATETIME_WEEK);
        } finally {
            if (sqlExecutor != null) {
                sqlExecutor.close();
            }
        }
    }

    public void populateLteTables() throws SQLException {
        Map<String, Object> rowSpecificValues = new HashMap<String, Object>();

        rowSpecificValues.put(TAC, SAMPLE_TAC_2);
        rowSpecificValues.put(IMSI, TEST_IMSI_LTE);
        rowSpecificValues.put(EVENT_ID, TEST_EVENT_ID);
        rowSpecificValues.put(EVENT_SUBTYPE_ID, TEST_EVENT_SUBTYPE_ID);
        rowSpecificValues.put(DATETIME_ID, TEST_DATETIME);
        rowSpecificValues.put(LOCAL_DATE_ID, TEST_LOCAL_DATETIME);

        insertRow(TEMP_EVENT_E_LTE_ERR_RAW, rowSpecificValues);

        rowSpecificValues.put(DATETIME_ID, TEST_DATETIME_WEEK);
        rowSpecificValues.put(LOCAL_DATE_ID, TEST_LOCAL_DATETIME_WEEK);

        insertRow(TEMP_EVENT_E_LTE_ERR_RAW, rowSpecificValues);
    }

    public void populateImsiGroupTables() throws SQLException{
        Map<String, Object> rowSpecificValues = new HashMap<String, Object>();

        rowSpecificValues.put(GROUP_NAME, TEST_GROUP_NAME);

        rowSpecificValues.put(IMSI, TEST_IMSI_LTE);
        insertRow(TEMP_GROUP_TYPE_E_IMSI, rowSpecificValues);
    }

    private void populateDimSubtypeTables() throws SQLException {
        Map<String, Object> rowSpecificValues = new HashMap<String, Object>();

        rowSpecificValues.put(EVENT_ID, TEST_EVENT_ID);
        rowSpecificValues.put(EVENT_SUBTYPE_ID, TEST_EVENT_SUBTYPE_ID);
        insertRow(TEMP_DIM_E_LTE_EVENT_SUBTYPE, rowSpecificValues);
    }

    private void populateDimMsisdnTables() throws SQLException {
        Map<String, Object> rowSpecificValues = new HashMap<String, Object>();

        rowSpecificValues.put(MSISDN, TEST_MSISDN);
        rowSpecificValues.put(IMSI, TEST_IMSI_LTE);
        insertRow(TEMP_DIM_E_IMSI_MSISDN, rowSpecificValues);
    }

    public MultivaluedMap<String, String> getRequestParameters(){
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        return map;
    }

    @Test
    public void testGetSubBITauDataGroup() throws Exception {
        jndiProperties.disableSucRawJNDIProperty();
        final MultivaluedMap<String, String> map = getRequestParameters();
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        map.putSingle(GROUP_NAME_PARAM, TEST_GROUP_NAME);
        map.putSingle(TIME_QUERY_PARAM, THIRTY_MINUTES);
        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String result = subsessionBIResource.getSubBITAUData();
        System.out.println(result);

        final List<SubBITauResult> results = getTranslator().translateResult(result, SubBITauResult.class);
        validResults(results.get(0));
    }

    @Test
    public void testGetSubBITauDataGroupOneWeek() throws Exception {
        jndiProperties.disableSucRawJNDIProperty();
        final MultivaluedMap<String, String> map = getRequestParameters();
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        map.putSingle(GROUP_NAME_PARAM, TEST_GROUP_NAME);
        map.putSingle(TIME_QUERY_PARAM, ONE_WEEK);
        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String result = subsessionBIResource.getSubBITAUData();
        System.out.println(result);

        final List<SubBITauResult> results = getTranslator().translateResult(result, SubBITauResult.class);
        validResults(results.get(0));
    }


    @Test
    public void testGetSubBITauData() throws Exception {
        jndiProperties.disableSucRawJNDIProperty();
        final MultivaluedMap<String, String> map = getRequestParameters();
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        map.putSingle(IMSI_PARAM, Long.toString(TEST_IMSI_LTE));
        map.putSingle(TIME_QUERY_PARAM, THIRTY_MINUTES);
        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String result = subsessionBIResource.getSubBITAUData();
        System.out.println(result);


        final List<SubBITauResult> results = getTranslator().translateResult(result, SubBITauResult.class);
        validResults(results.get(0));
    }

    @Test
    public void testGetSubBITauDataOneWeek() throws Exception {
        jndiProperties.disableSucRawJNDIProperty();
        final MultivaluedMap<String, String> map = getRequestParameters();
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        map.putSingle(IMSI_PARAM, Long.toString(TEST_IMSI_LTE));
        map.putSingle(TIME_QUERY_PARAM, ONE_WEEK);
        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String result = subsessionBIResource.getSubBITAUData();
        System.out.println(result);


        final List<SubBITauResult> results = getTranslator().translateResult(result, SubBITauResult.class);
        validResults(results.get(0));
    }


    @Test
    public void testGetSubBITauDataMSISDN() throws Exception {
        jndiProperties.disableSucRawJNDIProperty();
        final MultivaluedMap<String, String> map = getRequestParameters();
        map.putSingle(TYPE_PARAM, TYPE_MSISDN);
        map.putSingle(MSISDN_PARAM,  Long.toString(TEST_MSISDN));
        map.putSingle(TIME_QUERY_PARAM, ONE_WEEK);
        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String result = subsessionBIResource.getSubBITAUData();
        System.out.println(result);

        final List<SubBITauResult> results = getTranslator().translateResult(result, SubBITauResult.class);
        validResults(results.get(0));
    }


    private void validResults(final SubBITauResult result) {
        final SubBITauResult expected = new SubBITauResult();

        expected.setFailures(TEST_FAILURES);
        expected.setSuccesses(TEST_SUCCESSES);
        expected.setTac_tau(TEST_TAC_TAU);
        System.out.println(expected);
        assertEquals(expected, result);
    }








}
