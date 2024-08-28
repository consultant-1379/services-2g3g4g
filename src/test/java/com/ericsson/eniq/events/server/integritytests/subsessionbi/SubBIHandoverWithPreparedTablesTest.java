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
import com.ericsson.eniq.events.server.test.schema.Nullable;
import com.ericsson.eniq.events.server.test.stubs.DummyUriInfoImpl;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;
import com.sun.jersey.core.util.MultivaluedMapImpl;
import org.junit.Test;
import static org.junit.Assert.assertThat;

import javax.ws.rs.core.MultivaluedMap;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.*;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.*;
import static org.hamcrest.CoreMatchers.is;

public class SubBIHandoverWithPreparedTablesTest extends TestsWithTemporaryTablesBaseTestCase<SubBIHandoverResult> {

    private static final Map<String, String> groupColumnsForIMSIGroupTable = new HashMap<String, String>();

    private static final Map<String, String> rawTableColumns = new HashMap<String, String>();

    private static final Map<String, Nullable> dimNullableColumns = new HashMap<String, Nullable>();

    private static final Map<String, String> imsiTableColumns = new HashMap<String, String>();

    private static final Map<String, String> DimTableColumns = new HashMap<String, String>();

    private static final Map<String, String> imsiMsisdnColumns = new HashMap<String, String>();

    private SubsessionBIResource subsessionBIResource;

    private static final String TEST_GROUP_NAME = "VIP";

    private static final String TEST_MAX_ROWS = "50";

    private final static long TEST_IMSI_LTE = 312030419990004L;

    private final static int TEST_EVENT_ID = 7;

    private final static int TEST_EVENT_SUBTYPE_ID = 3;

    private final static String TEST_HIERARCHY_1 = "LTE25ERBS00002-4";
    
    private final static String TEST_HIER321_ID = "6079299740152738411";

    private final static String TEST_HIER3_ID = "3135210477467174988";

    private static final long TEST_MSISDN = 312030410000004L;

    private final static String TEST_VENDOR = "Ericsson";

    private final static String TEST_VENDOR_UNKNOWN = "Unknown";

    private static String TEST_DATETIME = DateTimeUtilities.getDateTimeMinus30Minutes();

    private static String TEST_LOCAL_DATETIME = DateTimeUtilities.getDateTimeMinus30Minutes().substring(0, 10);

    private static String TEST_DATETIME_WEEK = DateTimeUtilities.getDateTimeMinus36Hours();

    private static String TEST_LOCAL_DATETIME_WEEK = DateTimeUtilities.getDateTimeMinus36Hours().substring(0, 10);


    public Map<String, String> getGroupColumnsForImsiGroupTable(){
        groupColumnsForIMSIGroupTable.put(IMSI,UNSIGNED_BIGINT);
        groupColumnsForIMSIGroupTable.put(GROUP_NAME, VARCHAR_64);
        return groupColumnsForIMSIGroupTable;
    }

    public Map<String, Nullable> getHier321IdDimNullableColumns(){
        dimNullableColumns.put(VENDOR, Nullable.CAN_BE_NULL);
        dimNullableColumns.put(HIERARCHY_1,Nullable.CAN_BE_NULL);
        dimNullableColumns.put(HIER321_ID, Nullable.CANNOT_BE_NULL);
        return dimNullableColumns;
    }

    public Map<String, String> getErrRawTableColumns(){
        rawTableColumns.put(IMSI, UNSIGNED_BIGINT);
        rawTableColumns.put(EVENT_ID, TINYINT);
        rawTableColumns.put(EVENT_SUBTYPE_ID, TINYINT);
        rawTableColumns.put(HIERARCHY_1, VARCHAR_64);
        rawTableColumns.put(VENDOR, VARCHAR_64);
        rawTableColumns.put(DATETIME_ID, TIMESTAMP);
        rawTableColumns.put(LOCAL_DATE_ID, DATE);
        return rawTableColumns;
    }

    public Map<String, String> getImsiAggTableColumns(){
        imsiTableColumns.put(IMSI, UNSIGNED_BIGINT);
        imsiTableColumns.put(EVENT_ID, TINYINT);
        imsiTableColumns.put(EVENT_SUBTYPE_ID, TINYINT);
        imsiTableColumns.put(HIER3_ID, UNSIGNED_BIGINT);
        imsiTableColumns.put(HIER321_ID, UNSIGNED_BIGINT);
        imsiTableColumns.put(NO_OF_SUCCESSES, UNSIGNED_BIGINT);
        imsiTableColumns.put(DATETIME_ID, TIMESTAMP);
        return imsiTableColumns;
    }

    public Map<String, String> getDimTableColumns(){
        DimTableColumns.put(VENDOR, VARCHAR_64);
        DimTableColumns.put(HIER321_ID, UNSIGNED_BIGINT);
        DimTableColumns.put(HIERARCHY_1, VARCHAR_64);
        return DimTableColumns;
    }

    public Map<String, String> getMsisdnColumns(){
        imsiMsisdnColumns.put(IMSI, UNSIGNED_BIGINT);
        imsiMsisdnColumns.put(MSISDN, UNSIGNED_BIGINT);
        return imsiMsisdnColumns;
    }


    @Override
    public void onSetUp() throws Exception {
        super.onSetUp();
        subsessionBIResource = new SubsessionBIResource();

        createTemporaryTableWithColumnTypes(TEMP_EVENT_E_LTE_ERR_RAW, getErrRawTableColumns());

        createTemporaryTableWithColumnTypes(TEMP_EVENT_E_LTE_IMSI_SUC_RAW, getImsiAggTableColumns());

        createTemporaryTableWithColumnTypes(TEMP_DIM_E_IMSI_MSISDN, getMsisdnColumns());

        createTemporaryTableWithColumnTypes(TEMP_GROUP_TYPE_E_IMSI, getGroupColumnsForImsiGroupTable());

        createTemporaryTable(TEMP_DIM_E_LTE_HIER321, getHier321IdDimNullableColumns());

        populateTemporaryTables();

        attachDependencies(subsessionBIResource);
    }

    private void populateTemporaryTables() throws SQLException {
        populateLteTables();
        populateImsiGroupTables();
        populateDimTables();
        populateImsiAggTables();
    }


    public void populateImsiAggTables() throws SQLException {
        Map<String, Object> rowSpecificValues = new HashMap<String, Object>();
        int noOfSuccesses = 1;
        rowSpecificValues.put(IMSI, TEST_IMSI_LTE);
        rowSpecificValues.put(EVENT_ID, TEST_EVENT_ID);
        rowSpecificValues.put(EVENT_SUBTYPE_ID, TEST_EVENT_SUBTYPE_ID);
        rowSpecificValues.put(HIER3_ID, TEST_HIER3_ID);
        rowSpecificValues.put(HIER321_ID, TEST_HIER321_ID);
        rowSpecificValues.put(NO_OF_SUCCESSES, noOfSuccesses);
        rowSpecificValues.put(DATETIME_ID, TEST_DATETIME);

        insertRow(TEMP_EVENT_E_LTE_IMSI_SUC_RAW, rowSpecificValues);

        Map<String, Object> rowSpecificValues2 = new HashMap<String, Object>();
        noOfSuccesses = 2;
        rowSpecificValues2.put(IMSI, TEST_IMSI_LTE);
        rowSpecificValues2.put(EVENT_ID, TEST_EVENT_ID);
        rowSpecificValues2.put(EVENT_SUBTYPE_ID, TEST_EVENT_SUBTYPE_ID);
        rowSpecificValues2.put(HIER3_ID, TEST_HIER3_ID);
        rowSpecificValues2.put(HIER321_ID, TEST_HIER321_ID);
        rowSpecificValues2.put(NO_OF_SUCCESSES, noOfSuccesses);
        rowSpecificValues2.put(DATETIME_ID, TEST_DATETIME);
        insertRow(TEMP_EVENT_E_LTE_IMSI_SUC_RAW, rowSpecificValues2);

        rowSpecificValues.put(DATETIME_ID, TEST_DATETIME_WEEK);
        insertRow(TEMP_EVENT_E_LTE_IMSI_SUC_RAW, rowSpecificValues);

        rowSpecificValues2.put(DATETIME_ID, TEST_DATETIME_WEEK);
        insertRow(TEMP_EVENT_E_LTE_IMSI_SUC_RAW, rowSpecificValues2);
    }

    public void populateDimTables() throws SQLException {
        Map<String, Object> rowSpecificValues = new HashMap<String, Object>();
        rowSpecificValues.put(VENDOR, TEST_VENDOR);
        rowSpecificValues.put(HIER321_ID, TEST_HIER321_ID);
        rowSpecificValues.put(HIERARCHY_1, TEST_HIERARCHY_1);
        insertRow(TEMP_DIM_E_LTE_HIER321, rowSpecificValues);

        Map<String, Object> rowSpecificValues2 = new HashMap<String, Object>();
        rowSpecificValues2.put(HIER321_ID, TEST_HIER321_ID);
        insertRow(TEMP_DIM_E_LTE_HIER321, rowSpecificValues2);

        rowSpecificValues.clear();
        rowSpecificValues.put(MSISDN, TEST_MSISDN);
        rowSpecificValues.put(IMSI, TEST_IMSI_LTE);
        insertRow(TEMP_DIM_E_IMSI_MSISDN, rowSpecificValues);

    }

    public void populateLteTables() throws SQLException {
        Map<String, Object> rowSpecificValues = new HashMap<String, Object>();

        rowSpecificValues.put(IMSI, TEST_IMSI_LTE);
        rowSpecificValues.put(EVENT_ID, TEST_EVENT_ID);
        rowSpecificValues.put(EVENT_SUBTYPE_ID, TEST_EVENT_SUBTYPE_ID);
        rowSpecificValues.put(HIERARCHY_1, TEST_HIERARCHY_1);
        rowSpecificValues.put(VENDOR, TEST_VENDOR);
        rowSpecificValues.put(DATETIME_ID, TEST_DATETIME);
        rowSpecificValues.put(LOCAL_DATE_ID, TEST_LOCAL_DATETIME);
        insertRow(TEMP_EVENT_E_LTE_ERR_RAW, rowSpecificValues);

        Map<String, Object> rowSpecificValues2 = new HashMap<String, Object>();

        rowSpecificValues2.put(IMSI, TEST_IMSI_LTE);
        rowSpecificValues2.put(EVENT_ID, TEST_EVENT_ID);
        rowSpecificValues2.put(EVENT_SUBTYPE_ID, TEST_EVENT_SUBTYPE_ID);
        rowSpecificValues2.put(HIERARCHY_1, TEST_HIERARCHY_1);
        rowSpecificValues2.put(VENDOR, TEST_VENDOR);
        rowSpecificValues2.put(DATETIME_ID, TEST_DATETIME);
        rowSpecificValues2.put(LOCAL_DATE_ID, TEST_LOCAL_DATETIME);
        insertRow(TEMP_EVENT_E_LTE_ERR_RAW, rowSpecificValues2);

        rowSpecificValues.put(DATETIME_ID, TEST_DATETIME_WEEK);
        rowSpecificValues.put(LOCAL_DATE_ID, TEST_LOCAL_DATETIME_WEEK);
        insertRow(TEMP_EVENT_E_LTE_ERR_RAW, rowSpecificValues);

        rowSpecificValues2.put(DATETIME_ID, TEST_DATETIME_WEEK);
        rowSpecificValues2.put(LOCAL_DATE_ID, TEST_LOCAL_DATETIME_WEEK);
        insertRow(TEMP_EVENT_E_LTE_ERR_RAW, rowSpecificValues2);

    }

    public void populateImsiGroupTables() throws SQLException{
        Map<String, Object> rowSpecificValues = new HashMap<String, Object>();

        rowSpecificValues.put(GROUP_NAME, TEST_GROUP_NAME);
        rowSpecificValues.put(IMSI, TEST_IMSI_LTE);
        insertRow(TEMP_GROUP_TYPE_E_IMSI, rowSpecificValues);

    }

    public MultivaluedMap<String, String> getRequestParameters(){
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        map.putSingle(TZ_OFFSET, TZ_OFFSET_OF_ZERO);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        return map;
    }

    @Test
    public void testGetSubBIHandoverDataGroup() throws Exception {
        jndiProperties.disableSucRawJNDIProperty();
        final MultivaluedMap<String, String> map = getRequestParameters();
        map.putSingle(GROUP_NAME_PARAM, TEST_GROUP_NAME);
        map.putSingle(TIME_QUERY_PARAM, THIRTY_MINUTES);
        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String result = subsessionBIResource.getSubBIHandoverData();
        System.out.println(result);

        final List<SubBIHandoverResult> results = getTranslator().translateResult(result, SubBIHandoverResult.class);

        validateResults(results);
    }

    @Test
    public void testGetSubBIHandoverDataGroupOneWeek() throws Exception {
        jndiProperties.disableSucRawJNDIProperty();
        final MultivaluedMap<String, String> map = getRequestParameters();
        map.putSingle(GROUP_NAME_PARAM, TEST_GROUP_NAME);
        map.putSingle(TIME_QUERY_PARAM, ONE_WEEK);
        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String result = subsessionBIResource.getSubBIHandoverData();
        System.out.println(result);

        final List<SubBIHandoverResult> results = getTranslator().translateResult(result, SubBIHandoverResult.class);

        validateResults(results);
    }

    @Test
    public void testGetSubBIHandoverData() throws Exception {
        jndiProperties.disableSucRawJNDIProperty();
        final MultivaluedMap<String, String> map = getRequestParameters();
        map.putSingle(IMSI_PARAM, Long.toString(TEST_IMSI_LTE));
        map.putSingle(TIME_QUERY_PARAM, THIRTY_MINUTES);
        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String result = subsessionBIResource.getSubBIHandoverData();
        System.out.println(result);

        final List<SubBIHandoverResult> results = getTranslator().translateResult(result, SubBIHandoverResult.class);

        validateResults(results);
    }

    @Test
    public void testGetSubBIHandoverDataOneWeek() throws Exception {
        jndiProperties.disableSucRawJNDIProperty();
        final MultivaluedMap<String, String> map = getRequestParameters();
        map.putSingle(IMSI_PARAM, Long.toString(TEST_IMSI_LTE));
        map.putSingle(TIME_QUERY_PARAM, ONE_WEEK);
        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String result = subsessionBIResource.getSubBIHandoverData();
        System.out.println(result);

        final List<SubBIHandoverResult> results = getTranslator().translateResult(result, SubBIHandoverResult.class);

        validateResults(results);
    }

    @Test
    public void testGetSubBIHandoverDataMsisdn() throws Exception {
        jndiProperties.disableSucRawJNDIProperty();
        final MultivaluedMap<String, String> map = getRequestParameters();
        map.putSingle(TYPE_PARAM, TYPE_MSISDN);
        map.putSingle(MSISDN_PARAM,  Long.toString(TEST_MSISDN));
        map.putSingle(TIME_QUERY_PARAM, THIRTY_MINUTES);
        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String result = subsessionBIResource.getSubBIHandoverData();
        System.out.println(result);

        final List<SubBIHandoverResult> results = getTranslator().translateResult(result, SubBIHandoverResult.class);

        validateResults(results);
    }


    private void validateResults(final List<SubBIHandoverResult> results) {
        assertThat(results.size(), is(2));

        final SubBIHandoverResult firstSubBIHandoverResult = results.get(0);
        assertThat(firstSubBIHandoverResult.getEcell(), is(TEST_HIER321_ID + "," + TEST_VENDOR_UNKNOWN));
        assertThat(firstSubBIHandoverResult.getSuccesses(), is(3));
        assertThat(firstSubBIHandoverResult.getFailures(), is(0));

        final SubBIHandoverResult SecondSubBIHandoverResult = results.get(1);
        assertThat(SecondSubBIHandoverResult.getEcell(), is(TEST_HIERARCHY_1 + "," + TEST_VENDOR));
        assertThat(SecondSubBIHandoverResult.getSuccesses(), is(3));
        assertThat(SecondSubBIHandoverResult.getFailures(), is(2));
    }

}
