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
import com.ericsson.eniq.events.server.test.queryresults.SubBIHandoverResult;
import com.ericsson.eniq.events.server.test.stubs.DummyUriInfoImpl;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;
import com.sun.jersey.core.util.MultivaluedMapImpl;
import org.junit.Test;

import javax.ws.rs.core.MultivaluedMap;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.*;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.*;
import static junit.framework.Assert.assertEquals;


/**
 * @since 2014
 *
 */
public class SubBIHandoverSuccessRawWithPreparedTablesTest extends TestsWithTemporaryTablesBaseTestCase<SubBIHandoverResult> {

    private static final Map<String, String> groupColumnsForIMSIGroupTable = new HashMap<String, String>();

    private static final Map<String, String> rawTableColumns = new HashMap<String, String>();

    private static final Map<String, String> imsiMsisdnColumns = new HashMap<String, String>();

    private SubsessionBIResource subsessionBIResource;

    private final  List<String> tempDataTables = new ArrayList<String>();

    private static final String TEST_GROUP_NAME = "VIP";

    private static final String TEST_MAX_ROWS = "50";

    private final static long TEST_IMSI = 312030410000004L;

    private final static long TEST_IMSI_LTE = 312030419990004L;

    private static final long TEST_MSISDN = 312030410000004L;

    private final static long TEST_EVENT_ID = 7;

    private final static long TEST_EVENT_SUBTYPE_ID = 2;

    private final static String TEST_HIERARCHY = "0";

    private final static String TEST_VENDOR = "Unknown";

    private static String TEST_DATETIME = DateTimeUtilities.getDateTimeMinus30Minutes();

    private static String TEST_LOCAL_DATETIME = DateTimeUtilities.getDateTimeMinus30Minutes().substring(0, 10);

    private static String TEST_DATETIME_WEEK = DateTimeUtilities.getDateTimeMinus36Hours();

    private static String TEST_LOCAL_DATETIME_WEEK = DateTimeUtilities.getDateTimeMinus36Hours().substring(0, 10);

    private final static int TEST_FAILURES = 1;

    private final static int TEST_SUCCESSES = 1;

    private final static String TEST_ECELL = "0,Unknown";

    public List<String> getTempTables(){
        tempDataTables.add(TEMP_EVENT_E_LTE_ERR_RAW);
        tempDataTables.add(TEMP_EVENT_E_LTE_SUC_RAW);
        return tempDataTables;
    }

    public Map<String, String> getGroupColumnsForImsiGroupTable(){
        groupColumnsForIMSIGroupTable.put(IMSI,UNSIGNED_BIGINT);
        groupColumnsForIMSIGroupTable.put(GROUP_NAME, VARCHAR_64);
        return groupColumnsForIMSIGroupTable;
    }

    public Map<String, String> getMsisdnColumns(){
        imsiMsisdnColumns.put(IMSI, UNSIGNED_BIGINT);
        imsiMsisdnColumns.put(MSISDN, UNSIGNED_BIGINT);
        return imsiMsisdnColumns;
    }

    public Map<String, String> getRawTableColumns(){
        rawTableColumns.put(IMSI, UNSIGNED_BIGINT);
        rawTableColumns.put(EVENT_ID, TINYINT);
        rawTableColumns.put(EVENT_SUBTYPE_ID, TINYINT);
        rawTableColumns.put(HIERARCHY_1, VARCHAR_64);
        rawTableColumns.put(TAC, UNSIGNED_INT);
        rawTableColumns.put(VENDOR, VARCHAR_64);
        rawTableColumns.put(DATETIME_ID, TIMESTAMP);
        rawTableColumns.put(LOCAL_DATE_ID, DATE);
        return rawTableColumns;
    }


   @Override
    public void onSetUp() throws Exception {
        super.onSetUp();
        subsessionBIResource = new SubsessionBIResource();

        for (final String tempTable : getTempTables()) {
            createTemporaryTableWithColumnTypes(tempTable, getRawTableColumns());
        }

        createTemporaryTableWithColumnTypes(TEMP_GROUP_TYPE_E_IMSI, getGroupColumnsForImsiGroupTable());

        createTemporaryTableWithColumnTypes(TEMP_DIM_E_IMSI_MSISDN, getMsisdnColumns());

        populateTemporaryTables();

        attachDependencies(subsessionBIResource);
    }


    private void populateTemporaryTables() throws SQLException {
            populateLteTables();
            populateImsiGroupTables();
            populateDimTables();
    }

    public void populateLteTables() throws SQLException {
        Map<String, Object> rowSpecificValues = new HashMap<String, Object>();

        rowSpecificValues.put(IMSI, TEST_IMSI_LTE);
        rowSpecificValues.put(EVENT_ID, TEST_EVENT_ID);
        rowSpecificValues.put(EVENT_SUBTYPE_ID, TEST_EVENT_SUBTYPE_ID);
        rowSpecificValues.put(HIERARCHY_1, TEST_HIERARCHY);
        rowSpecificValues.put(TAC, SAMPLE_TAC_2);
        rowSpecificValues.put(VENDOR, TEST_VENDOR);

        rowSpecificValues.put(DATETIME_ID, TEST_DATETIME);
        rowSpecificValues.put(LOCAL_DATE_ID, TEST_LOCAL_DATETIME);
        insertRow(TEMP_EVENT_E_LTE_ERR_RAW, rowSpecificValues);

        rowSpecificValues.put(DATETIME_ID, TEST_DATETIME_WEEK);
        rowSpecificValues.put(LOCAL_DATE_ID, TEST_LOCAL_DATETIME_WEEK);
        insertRow(TEMP_EVENT_E_LTE_ERR_RAW, rowSpecificValues);

        rowSpecificValues.put(DATETIME_ID, TEST_DATETIME_WEEK);
        rowSpecificValues.put(LOCAL_DATE_ID, TEST_LOCAL_DATETIME_WEEK);
        insertRow(TEMP_EVENT_E_LTE_SUC_RAW, rowSpecificValues);

        rowSpecificValues.put(DATETIME_ID, TEST_DATETIME);
        rowSpecificValues.put(LOCAL_DATE_ID, TEST_LOCAL_DATETIME);
        insertRow(TEMP_EVENT_E_LTE_SUC_RAW, rowSpecificValues);
    }

    public void populateImsiGroupTables() throws SQLException{
        Map<String, Object> rowSpecificValues = new HashMap<String, Object>();

        rowSpecificValues.put(GROUP_NAME, TEST_GROUP_NAME);

        rowSpecificValues.put(IMSI, TEST_IMSI);
        insertRow(TEMP_GROUP_TYPE_E_IMSI, rowSpecificValues);

        rowSpecificValues.put(IMSI, TEST_IMSI_LTE);
        insertRow(TEMP_GROUP_TYPE_E_IMSI, rowSpecificValues);
    }

    private void populateDimTables() throws SQLException {
        Map<String, Object> rowSpecificValues = new HashMap<String, Object>();
        rowSpecificValues.put(MSISDN, TEST_MSISDN);
        rowSpecificValues.put(IMSI, TEST_IMSI_LTE);
        insertRow(TEMP_DIM_E_IMSI_MSISDN, rowSpecificValues);
    }

    public MultivaluedMap<String, String> getRequestParameters(){
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_FIVE_THIRTY);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        return map;
    }



    @Test
    public void testGetSubBIHandoverDataGroup() throws Exception {
        jndiProperties.useSucRawJNDIProperty();
        final MultivaluedMap<String, String> map = getRequestParameters();
        map.putSingle(GROUP_NAME_PARAM, TEST_GROUP_NAME);
        map.putSingle(TIME_QUERY_PARAM, THIRTY_MINUTES);
        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String result = subsessionBIResource.getSubBIHandoverData();
        System.out.println(result);

        final List<SubBIHandoverResult> results = getTranslator().translateResult(result, SubBIHandoverResult.class);

        validResults(results.get(0));
    }

    @Test
    public void testGetSubBIHandoverDataGroupOneWeek() throws Exception {
        jndiProperties.useSucRawJNDIProperty();
        final MultivaluedMap<String, String> map = getRequestParameters();
        map.putSingle(GROUP_NAME_PARAM, TEST_GROUP_NAME);
        map.putSingle(TIME_QUERY_PARAM, ONE_WEEK);
        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String result = subsessionBIResource.getSubBIHandoverData();
        System.out.println(result);

        final List<SubBIHandoverResult> results = getTranslator().translateResult(result, SubBIHandoverResult.class);

        validResults(results.get(0));
    }
    @Test
    public void testGetSubBIHandoverData() throws Exception {
        jndiProperties.useSucRawJNDIProperty();
        final MultivaluedMap<String, String> map = getRequestParameters();
        map.putSingle(IMSI_PARAM, Long.toString(TEST_IMSI_LTE));
        map.putSingle(TIME_QUERY_PARAM, THIRTY_MINUTES);
        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String result = subsessionBIResource.getSubBIHandoverData();
        System.out.println(result);

        final List<SubBIHandoverResult> results = getTranslator().translateResult(result, SubBIHandoverResult.class);

        validResults(results.get(0));
    }

    @Test
    public void testGetSubBIHandoverDataOneWeek() throws Exception {
        jndiProperties.useSucRawJNDIProperty();
        final MultivaluedMap<String, String> map = getRequestParameters();
        map.putSingle(IMSI_PARAM, Long.toString(TEST_IMSI_LTE));
        map.putSingle(TIME_QUERY_PARAM, ONE_WEEK);
        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String result = subsessionBIResource.getSubBIHandoverData();
        System.out.println(result);

        final List<SubBIHandoverResult> results = getTranslator().translateResult(result, SubBIHandoverResult.class);

        validResults(results.get(0));
    }

    @Test
    public void testGetSubBIHandoverDataMsisdn() throws Exception {
        jndiProperties.useSucRawJNDIProperty();
        final MultivaluedMap<String, String> map = getRequestParameters();
        map.putSingle(TYPE_PARAM, TYPE_MSISDN);
        map.putSingle(MSISDN_PARAM,  Long.toString(TEST_MSISDN));
        map.putSingle(TIME_QUERY_PARAM, THIRTY_MINUTES);
        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String result = subsessionBIResource.getSubBIHandoverData();
        System.out.println(result);

        final List<SubBIHandoverResult> results = getTranslator().translateResult(result, SubBIHandoverResult.class);

        validResults(results.get(0));
    }

    private void validResults(final SubBIHandoverResult result) {
        final SubBIHandoverResult expected = new SubBIHandoverResult();

        expected.setFailures(TEST_FAILURES);
        expected.setSuccesses(TEST_SUCCESSES);
        expected.setEcell(TEST_ECELL);

        assertEquals(expected, result);
    }


}
