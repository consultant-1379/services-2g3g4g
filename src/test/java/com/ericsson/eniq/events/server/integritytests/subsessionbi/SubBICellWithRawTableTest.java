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
import static com.ericsson.eniq.events.server.common.EventIDConstants.*;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.*;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.*;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.sql.SQLException;
import java.util.*;

import javax.ws.rs.core.MultivaluedMap;

import org.junit.Test;

import com.ericsson.eniq.events.server.resources.SubsessionBIResource;
import com.ericsson.eniq.events.server.resources.TestsWithTemporaryTablesBaseTestCase;
import com.ericsson.eniq.events.server.test.queryresults.SubBIResult;
import com.ericsson.eniq.events.server.test.stubs.DummyUriInfoImpl;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;
import com.sun.jersey.core.util.MultivaluedMapImpl;

public class SubBICellWithRawTableTest extends
        TestsWithTemporaryTablesBaseTestCase<SubBIResult> {

    private static final String UNKNOWN_VENDOR = "Unknown";

    private static final String UNKNOWN_HIERARCHY_1 = "2";

    private static final String UNKNOWN_HIERARCHY_3 = "1";

    private static final int HIER3_ID_VAL_G2 = 9101122;

    private static final int HIER321_ID_VAL_G2 = 910111222;

    private static final int HIER321_ID_VAL_G1 = 111222;

    private static final int HIER3_ID_VAL_G1 = 101122;

    private static final String TEST_IMSI = "312030410000004";

    private static final long TEST_MSISDN = 312030410000005L;

    private static final String TEST_MSISDN_STRING = "312030410000005";

    public static final String HIERARCHY_3_STR_G1 = "h3_group1";

    public static final String HIERARCHY_3_STR_G2 = "h3_group2";

    public static final String HIERARCHY_1_STR = "h1_string";

    SubsessionBIResource subsessionBIResource;

    private final List<String> tempDataTables = new ArrayList<String>();

    private final Collection<String> rawTableColumns = new ArrayList<String>();

    private static final Collection<String> imsiMsisdnColumns = new ArrayList<String>();

    private static final Collection<String> hier321Columns = new ArrayList<String>();

    private static final Map<String, Object> imsiMsisdnTableValues = new HashMap<String, Object>();

    private final List<String> tempDataIMSITables = new ArrayList<String>();

    private final Collection<String> imsiRawTableColumns = new ArrayList<String>();
    private final Collection<String> groupImsiColumns = new ArrayList<String>();

    private final long testIMSI = Long.valueOf(TEST_IMSI);

    @Override
    public void onSetUp() throws Exception {
        super.onSetUp();

        createTablesForTest();

        subsessionBIResource = new SubsessionBIResource();

        for (final String tempTable : tempDataIMSITables) {
            createTemporaryTable(tempTable, imsiRawTableColumns);
        }

        for (final String tempTable : tempDataTables) {
            createTemporaryTable(tempTable, rawTableColumns);
        }
        createTemporaryTable(TEMP_DIM_E_IMSI_MSISDN, imsiMsisdnColumns);
        createTemporaryTable(TEMP_DIM_E_LTE_HIER321, hier321Columns);
        createTemporaryTable(TEMP_DIM_E_SGEH_HIER321, hier321Columns);
        createTemporaryTable(TEMP_GROUP_TYPE_E_IMSI, groupImsiColumns);

        populateTemporaryTables();

        attachDependencies(subsessionBIResource);
    }

    private void createTablesForTest() {
        tempDataTables.add(TEMP_EVENT_E_SGEH_ERR_RAW);
        tempDataTables.add(TEMP_EVENT_E_SGEH_SUC_RAW);
        tempDataTables.add(TEMP_EVENT_E_LTE_ERR_RAW);
        tempDataTables.add(TEMP_EVENT_E_LTE_SUC_RAW);

        tempDataIMSITables.add(TEMP_EVENT_E_SGEH_IMSI_SUC_RAW);
        tempDataIMSITables.add(TEMP_EVENT_E_LTE_IMSI_SUC_RAW);

        imsiMsisdnColumns.add(IMSI);
        imsiMsisdnColumns.add(MSISDN);

        hier321Columns.add(HIERARCHY_3);
        hier321Columns.add(HIERARCHY_2);
        hier321Columns.add(HIERARCHY_1);
        hier321Columns.add(VENDOR);
        hier321Columns.add(RAT);
        hier321Columns.add(HIER3_ID);
        hier321Columns.add(HIER321_ID);

        rawTableColumns.add(IMSI);
        rawTableColumns.add(EVENT_ID);
        rawTableColumns.add(DATETIME_ID);
        rawTableColumns.add(RAT);
        rawTableColumns.add(VENDOR);
        rawTableColumns.add(HIERARCHY_3);
        rawTableColumns.add(HIERARCHY_2);
        rawTableColumns.add(HIERARCHY_1);
        rawTableColumns.add(LOCAL_DATE_ID);

        imsiRawTableColumns.add(IMSI);
        imsiRawTableColumns.add(EVENT_ID);
        imsiRawTableColumns.add(DATETIME_ID);
        imsiRawTableColumns.add(RAT);
        imsiRawTableColumns.add(NO_OF_SUCCESSES);
        imsiRawTableColumns.add(HIER3_ID);
        imsiRawTableColumns.add(HIER321_ID);

        groupImsiColumns.add(IMSI);
        groupImsiColumns.add(GROUP_NAME);
    }

    @Test
    public void testGetSUBBICell_OneWeek_TypeEqualToIMSI() throws Exception {
        jndiProperties.useSucRawJNDIProperty();
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(DISPLAY_PARAM, CHART_PARAM);
        map.putSingle(TIME_QUERY_PARAM, ONE_WEEK);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        map.putSingle(IMSI_PARAM, TEST_IMSI);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_FIVE_THIRTY);
        map.putSingle(MAX_ROWS, "20");

        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String json = subsessionBIResource.getSubBICellData();
        System.out.println(json);

        final List<SubBIResult> results = getTranslator().translateResult(json,
                SubBIResult.class);
        validateResults(results);
    }

    @Test
    public void testGetSUBBICell_OneWeek_TypeEqualToMSISDN() throws Exception {
        jndiProperties.useSucRawJNDIProperty();

        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(DISPLAY_PARAM, CHART_PARAM);
        map.putSingle(TIME_QUERY_PARAM, ONE_WEEK);
        map.putSingle(TYPE_PARAM, TYPE_MSISDN);
        map.putSingle(MSISDN_PARAM, TEST_MSISDN_STRING);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_FIVE_THIRTY);
        map.putSingle(MAX_ROWS, "20");

        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String json = subsessionBIResource.getSubBICellData();
        System.out.println(json);

        final List<SubBIResult> results = getTranslator().translateResult(json,
                SubBIResult.class);
        validateResults(results);
    }

    @Test
    public void testGetSUBBICell_OneWeek_TypeEqualToIMSIGroup()
            throws Exception {
        jndiProperties.useSucRawJNDIProperty();
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(DISPLAY_PARAM, CHART_PARAM);
        map.putSingle(TIME_QUERY_PARAM, ONE_WEEK);
        map.putSingle(GROUP_NAME_PARAM, SAMPLE_IMSI_GROUP);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_FIVE_THIRTY);
        map.putSingle(MAX_ROWS, "20");

        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String json = subsessionBIResource.getSubBICellData();
        System.out.println(json);

        final List<SubBIResult> results = getTranslator().translateResult(json,
                SubBIResult.class);
        validateResults(results);
    }

    @Test
    public void testGetIMSIAggregationSUBBICell_OneWeek_TypeEqualToMSISDN()
            throws Exception {
        jndiProperties.disableSucRawJNDIProperty();

        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(DISPLAY_PARAM, CHART_PARAM);
        map.putSingle(TIME_QUERY_PARAM, ONE_WEEK);
        map.putSingle(TYPE_PARAM, TYPE_MSISDN);
        map.putSingle(MSISDN_PARAM, TEST_MSISDN_STRING);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_FIVE_THIRTY);
        map.putSingle(MAX_ROWS, "20");

        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String json = subsessionBIResource.getSubBICellData();
        System.out.println(json);

        final List<SubBIResult> results = getTranslator().translateResult(json,
                SubBIResult.class);
        validateResults(results);
    }

    @Test
    public void testGetIMSIAggregationSUBBICell_OneWeek_TypeEqualToIMSI()
            throws Exception {
        jndiProperties.disableSucRawJNDIProperty();
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(DISPLAY_PARAM, CHART_PARAM);
        map.putSingle(TIME_QUERY_PARAM, ONE_WEEK);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        map.putSingle(IMSI_PARAM, TEST_IMSI);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_FIVE_THIRTY);
        map.putSingle(MAX_ROWS, "20");

        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String json = subsessionBIResource.getSubBICellData();
        System.out.println(json);

        final List<SubBIResult> results = getTranslator().translateResult(json,
                SubBIResult.class);
        validateResults(results);
    }

    @Test
    public void testGetIMSIAggregationSUBBICell_OneWeek_TypeEqualToIMSIGroup()
            throws Exception {
        jndiProperties.disableSucRawJNDIProperty();
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(DISPLAY_PARAM, CHART_PARAM);
        map.putSingle(TIME_QUERY_PARAM, ONE_WEEK);
        map.putSingle(GROUP_NAME_PARAM, SAMPLE_IMSI_GROUP);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_FIVE_THIRTY);
        map.putSingle(MAX_ROWS, "20");

        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String json = subsessionBIResource.getSubBICellData();
        System.out.println(json);

        final List<SubBIResult> results = getTranslator().translateResult(json,
                SubBIResult.class);
        validateResults(results);
    }

    private void validateResults(final List<SubBIResult> results) {
        assertThat(results.size(), is(3));
        final SubBIResult firstSubBIResult = results.get(0);
        assertThat(firstSubBIResult.getXAxisLabel(), is(HIERARCHY_1_STR + ",,"
                + HIERARCHY_3_STR_G1 + "," + ERICSSON + ",1"));
        assertThat(firstSubBIResult.getSuccessCount(), is("10"));
        assertThat(firstSubBIResult.getFailureCount(), is("9"));

        final SubBIResult secondSubBIResult = results.get(1);
        assertThat(secondSubBIResult.getXAxisLabel(), is(HIERARCHY_1_STR + ",,"
                + HIERARCHY_3_STR_G2 + "," + ERICSSON + ",1"));
        assertThat(secondSubBIResult.getSuccessCount(), is("4"));
        assertThat(secondSubBIResult.getFailureCount(), is("5"));

        final SubBIResult thirdSubBIResult = results.get(2);
        assertThat(thirdSubBIResult.getXAxisLabel(), is(UNKNOWN_HIERARCHY_1
                + ",," + UNKNOWN_HIERARCHY_3 + "," + UNKNOWN_VENDOR + ",1"));
        assertThat(thirdSubBIResult.getSuccessCount(), is("2"));
        assertThat(thirdSubBIResult.getFailureCount(), is("4"));

    }

    private void populateTemporaryTables() throws SQLException {

        imsiMsisdnTableValues.put(MSISDN, TEST_MSISDN);
        imsiMsisdnTableValues.put(IMSI, TEST_IMSI);
        insertRow(TEMP_DIM_E_IMSI_MSISDN, imsiMsisdnTableValues);

        Map<String, Object> hier321Values = new HashMap<String, Object>();
        hier321Values.put(RAT, 1);
        hier321Values.put(VENDOR, ERICSSON);
        hier321Values.put(HIERARCHY_3, HIERARCHY_3_STR_G1);
        hier321Values.put(HIERARCHY_2, "");
        hier321Values.put(HIERARCHY_1, HIERARCHY_1_STR);
        hier321Values.put(HIER3_ID, HIER3_ID_VAL_G1);
        hier321Values.put(HIER321_ID, HIER321_ID_VAL_G1);
        insertRow(TEMP_DIM_E_LTE_HIER321, hier321Values);
        insertRow(TEMP_DIM_E_SGEH_HIER321, hier321Values);

        hier321Values = new HashMap<String, Object>();
        hier321Values.put(RAT, 1);
        hier321Values.put(VENDOR, ERICSSON);
        hier321Values.put(HIERARCHY_3, HIERARCHY_3_STR_G2);
        hier321Values.put(HIERARCHY_2, "");
        hier321Values.put(HIERARCHY_1, HIERARCHY_1_STR);
        hier321Values.put(HIER3_ID, HIER3_ID_VAL_G2);
        hier321Values.put(HIER321_ID, HIER321_ID_VAL_G2);
        insertRow(TEMP_DIM_E_LTE_HIER321, hier321Values);
        insertRow(TEMP_DIM_E_SGEH_HIER321, hier321Values);

        Map<String, Object> values = new HashMap<String, Object>();
        values.put(GROUP_NAME, SAMPLE_IMSI_GROUP);
        values.put(IMSI, testIMSI);
        insertRow(TEMP_GROUP_TYPE_E_IMSI, values);

        values = new HashMap<String, Object>();
        values.put(IMSI, testIMSI);
        values.put(EVENT_ID, ACTIVATE_IN_2G_AND_3G);
        values.put(RAT, 1);
        values.put(VENDOR, ERICSSON);
        values.put(HIERARCHY_3, HIERARCHY_3_STR_G1);
        values.put(HIERARCHY_2, "");
        values.put(HIERARCHY_1, HIERARCHY_1_STR);
        values.put(DATETIME_ID, DateTimeUtilities.getDateTimeMinusDay(1));
        values.put(LOCAL_DATE_ID, DateTimeUtilities.getDateTimeMinusDay(1)
                .substring(0, 10));
        insertRow(TEMP_EVENT_E_SGEH_ERR_RAW, values);
        insertRow(TEMP_EVENT_E_SGEH_ERR_RAW, values);
        insertRow(TEMP_EVENT_E_SGEH_SUC_RAW, values);

        values = new HashMap<String, Object>();
        values.put(IMSI, testIMSI);
        values.put(EVENT_ID, ACTIVATE_IN_2G_AND_3G);
        values.put(RAT, 1);
        values.put(DATETIME_ID, DateTimeUtilities.getDateTimeMinusDay(2));
        values.put(NO_OF_SUCCESSES, 1);
        values.put(HIER3_ID, HIER3_ID_VAL_G1);
        values.put(HIER321_ID, HIER321_ID_VAL_G1);
        insertRow(TEMP_EVENT_E_SGEH_IMSI_SUC_RAW, values);

        values = new HashMap<String, Object>();
        values.put(IMSI, testIMSI);
        values.put(EVENT_ID, ACTIVATE_IN_2G_AND_3G);
        values.put(RAT, 1);
        values.put(VENDOR, UNKNOWN_VENDOR);
        values.put(HIERARCHY_3, UNKNOWN_HIERARCHY_3);
        values.put(HIERARCHY_2, "");
        values.put(HIERARCHY_1, UNKNOWN_HIERARCHY_1);
        values.put(DATETIME_ID, DateTimeUtilities.getDateTimeMinusDay(1));
        values.put(LOCAL_DATE_ID, DateTimeUtilities.getDateTimeMinusDay(1)
                .substring(0, 10));
        insertRow(TEMP_EVENT_E_SGEH_ERR_RAW, values);
        insertRow(TEMP_EVENT_E_SGEH_ERR_RAW, values);
        insertRow(TEMP_EVENT_E_SGEH_SUC_RAW, values);

        values = new HashMap<String, Object>();
        values.put(IMSI, testIMSI);
        values.put(EVENT_ID, ACTIVATE_IN_2G_AND_3G);
        values.put(RAT, 1);
        values.put(DATETIME_ID, DateTimeUtilities.getDateTimeMinusDay(2));
        values.put(NO_OF_SUCCESSES, 1);
        values.put(HIER3_ID, UNKNOWN_HIERARCHY_3);
        values.put(HIER321_ID, UNKNOWN_HIERARCHY_1);
        insertRow(TEMP_EVENT_E_SGEH_IMSI_SUC_RAW, values);

        values = new HashMap<String, Object>();
        values.put(IMSI, testIMSI);
        values.put(EVENT_ID, DEACTIVATE_IN_2G_AND_3G);
        values.put(RAT, 1);
        values.put(DATETIME_ID, DateTimeUtilities.getDateTimeMinusDay(2));
        values.put(NO_OF_SUCCESSES, 2);
        values.put(HIER3_ID, HIER3_ID_VAL_G1);
        values.put(HIER321_ID, HIER321_ID_VAL_G1);
        insertRow(TEMP_EVENT_E_SGEH_IMSI_SUC_RAW, values);

        values = new HashMap<String, Object>();
        values.put(IMSI, testIMSI);
        values.put(EVENT_ID, DEACTIVATE_IN_2G_AND_3G);
        values.put(DATETIME_ID, DateTimeUtilities.getDateTimeMinusDay(2));
        values.put(LOCAL_DATE_ID, DateTimeUtilities.getDateTimeMinusDay(2)
                .substring(0, 10));
        values.put(RAT, 1);
        values.put(VENDOR, ERICSSON);
        values.put(HIERARCHY_3, HIERARCHY_3_STR_G1);
        values.put(HIERARCHY_2, "");
        values.put(HIERARCHY_1, HIERARCHY_1_STR);
        insertRow(TEMP_EVENT_E_SGEH_ERR_RAW, values);
        insertRow(TEMP_EVENT_E_SGEH_SUC_RAW, values);
        insertRow(TEMP_EVENT_E_SGEH_SUC_RAW, values);

        values = new HashMap<String, Object>();
        values.put(IMSI, testIMSI);
        values.put(EVENT_ID, ATTACH_IN_4G);
        values.put(DATETIME_ID, DateTimeUtilities.getDateTimeMinusDay(3));
        values.put(LOCAL_DATE_ID, DateTimeUtilities.getDateTimeMinusDay(3)
                .substring(0, 10));
        values.put(RAT, 1);
        values.put(VENDOR, ERICSSON);
        values.put(HIERARCHY_3, HIERARCHY_3_STR_G1);
        values.put(HIERARCHY_2, "");
        values.put(HIERARCHY_1, HIERARCHY_1_STR);
        insertRow(TEMP_EVENT_E_LTE_ERR_RAW, values);
        insertRow(TEMP_EVENT_E_LTE_SUC_RAW, values);

        values = new HashMap<String, Object>();
        values.put(IMSI, testIMSI);
        values.put(EVENT_ID, ATTACH_IN_4G);
        values.put(RAT, 1);
        values.put(DATETIME_ID, DateTimeUtilities.getDateTimeMinusDay(2));
        values.put(NO_OF_SUCCESSES, 1);
        values.put(HIER3_ID, HIER3_ID_VAL_G1);
        values.put(HIER321_ID, HIER321_ID_VAL_G1);
        insertRow(TEMP_EVENT_E_LTE_IMSI_SUC_RAW, values);

        values = new HashMap<String, Object>();
        values.put(IMSI, testIMSI);
        values.put(EVENT_ID, ACTIVATE_IN_2G_AND_3G);
        values.put(RAT, 1);
        values.put(VENDOR, UNKNOWN_VENDOR);
        values.put(HIERARCHY_3, UNKNOWN_HIERARCHY_3);
        values.put(HIERARCHY_2, "");
        values.put(HIERARCHY_1, UNKNOWN_HIERARCHY_1);
        values.put(DATETIME_ID, DateTimeUtilities.getDateTimeMinusDay(1));
        values.put(LOCAL_DATE_ID, DateTimeUtilities.getDateTimeMinusDay(1)
                .substring(0, 10));
        insertRow(TEMP_EVENT_E_LTE_ERR_RAW, values);
        insertRow(TEMP_EVENT_E_LTE_ERR_RAW, values);
        insertRow(TEMP_EVENT_E_LTE_SUC_RAW, values);

        values = new HashMap<String, Object>();
        values.put(IMSI, testIMSI);
        values.put(EVENT_ID, ACTIVATE_IN_2G_AND_3G);
        values.put(RAT, 1);
        values.put(DATETIME_ID, DateTimeUtilities.getDateTimeMinusDay(2));
        values.put(NO_OF_SUCCESSES, 1);
        values.put(HIER3_ID, UNKNOWN_HIERARCHY_3);
        values.put(HIER321_ID, UNKNOWN_HIERARCHY_1);
        insertRow(TEMP_EVENT_E_LTE_IMSI_SUC_RAW, values);

        values = new HashMap<String, Object>();
        values.put(IMSI, testIMSI);
        values.put(EVENT_ID, DEDICATED_BEARER_ACTIVATE_IN_4G);
        values.put(DATETIME_ID, DateTimeUtilities.getDateTimeMinusDay(3));
        values.put(LOCAL_DATE_ID, DateTimeUtilities.getDateTimeMinusDay(3)
                .substring(0, 10));
        values.put(RAT, 1);
        values.put(VENDOR, ERICSSON);
        values.put(HIERARCHY_3, HIERARCHY_3_STR_G1);
        values.put(HIERARCHY_2, "");
        values.put(HIERARCHY_1, HIERARCHY_1_STR);
        insertRow(TEMP_EVENT_E_LTE_ERR_RAW, values);
        insertRow(TEMP_EVENT_E_LTE_SUC_RAW, values);

        values = new HashMap<String, Object>();
        values.put(IMSI, testIMSI);
        values.put(EVENT_ID, DEDICATED_BEARER_ACTIVATE_IN_4G);
        values.put(RAT, 1);
        values.put(DATETIME_ID, DateTimeUtilities.getDateTimeMinusDay(2));
        values.put(NO_OF_SUCCESSES, 1);
        values.put(HIER3_ID, HIER3_ID_VAL_G1);
        values.put(HIER321_ID, HIER321_ID_VAL_G1);
        insertRow(TEMP_EVENT_E_LTE_IMSI_SUC_RAW, values);

        values = new HashMap<String, Object>();
        values.put(IMSI, testIMSI);
        values.put(EVENT_ID, DEDICATED_BEARER_DEACTIVATE_IN_4G);
        values.put(DATETIME_ID, DateTimeUtilities.getDateTimeMinusDay(3));
        values.put(LOCAL_DATE_ID, DateTimeUtilities.getDateTimeMinusDay(3)
                .substring(0, 10));
        values.put(RAT, 1);
        values.put(VENDOR, ERICSSON);
        values.put(HIERARCHY_3, HIERARCHY_3_STR_G1);
        values.put(HIERARCHY_2, "");
        values.put(HIERARCHY_1, HIERARCHY_1_STR);
        insertRow(TEMP_EVENT_E_LTE_ERR_RAW, values);
        insertRow(TEMP_EVENT_E_LTE_SUC_RAW, values);

        values = new HashMap<String, Object>();
        values.put(IMSI, testIMSI);
        values.put(EVENT_ID, DEDICATED_BEARER_DEACTIVATE_IN_4G);
        values.put(RAT, 1);
        values.put(DATETIME_ID, DateTimeUtilities.getDateTimeMinusDay(2));
        values.put(NO_OF_SUCCESSES, 1);
        values.put(HIER3_ID, HIER3_ID_VAL_G1);
        values.put(HIER321_ID, HIER321_ID_VAL_G1);
        insertRow(TEMP_EVENT_E_LTE_IMSI_SUC_RAW, values);

        values = new HashMap<String, Object>();
        values.put(IMSI, testIMSI);
        values.put(EVENT_ID, PDN_CONNECT_IN_4G);
        values.put(DATETIME_ID, DateTimeUtilities.getDateTimeMinusDay(3));
        values.put(LOCAL_DATE_ID, DateTimeUtilities.getDateTimeMinusDay(3)
                .substring(0, 10));
        values.put(RAT, 1);
        values.put(VENDOR, ERICSSON);
        values.put(HIERARCHY_3, HIERARCHY_3_STR_G1);
        values.put(HIERARCHY_2, "");
        values.put(HIERARCHY_1, HIERARCHY_1_STR);
        insertRow(TEMP_EVENT_E_LTE_ERR_RAW, values);
        insertRow(TEMP_EVENT_E_LTE_SUC_RAW, values);

        values = new HashMap<String, Object>();
        values.put(IMSI, testIMSI);
        values.put(EVENT_ID, PDN_CONNECT_IN_4G);
        values.put(RAT, 1);
        values.put(DATETIME_ID, DateTimeUtilities.getDateTimeMinusDay(2));
        values.put(NO_OF_SUCCESSES, 1);
        values.put(HIER3_ID, HIER3_ID_VAL_G1);
        values.put(HIER321_ID, HIER321_ID_VAL_G1);
        insertRow(TEMP_EVENT_E_LTE_IMSI_SUC_RAW, values);

        values = new HashMap<String, Object>();
        values.put(IMSI, testIMSI);
        values.put(EVENT_ID, PDN_DISCONNECT_IN_4G);
        values.put(DATETIME_ID, DateTimeUtilities.getDateTimeMinusDay(4));
        values.put(LOCAL_DATE_ID, DateTimeUtilities.getDateTimeMinusDay(4)
                .substring(0, 10));
        values.put(RAT, 1);
        values.put(VENDOR, ERICSSON);
        values.put(HIERARCHY_3, HIERARCHY_3_STR_G1);
        values.put(HIERARCHY_2, "");
        values.put(HIERARCHY_1, HIERARCHY_1_STR);
        insertRow(TEMP_EVENT_E_LTE_ERR_RAW, values);
        insertRow(TEMP_EVENT_E_LTE_SUC_RAW, values);
        insertRow(TEMP_EVENT_E_LTE_SUC_RAW, values);

        values = new HashMap<String, Object>();
        values.put(IMSI, testIMSI);
        values.put(EVENT_ID, PDN_DISCONNECT_IN_4G);
        values.put(RAT, 1);
        values.put(DATETIME_ID, DateTimeUtilities.getDateTimeMinusDay(2));
        values.put(NO_OF_SUCCESSES, 2);
        values.put(HIER3_ID, HIER3_ID_VAL_G1);
        values.put(HIER321_ID, HIER321_ID_VAL_G1);
        insertRow(TEMP_EVENT_E_LTE_IMSI_SUC_RAW, values);

        /* these are included here and should be excluded by the sql query */
        values = new HashMap<String, Object>();
        values.put(IMSI, testIMSI);
        values.put(EVENT_ID, DETACH_IN_4G);
        values.put(DATETIME_ID, DateTimeUtilities.getDateTimeMinusDay(4));
        values.put(LOCAL_DATE_ID, DateTimeUtilities.getDateTimeMinusDay(4)
                .substring(0, 10));
        values.put(RAT, 1);
        values.put(VENDOR, ERICSSON);
        values.put(HIERARCHY_3, HIERARCHY_3_STR_G1);
        values.put(HIERARCHY_2, "");
        values.put(HIERARCHY_1, HIERARCHY_1_STR);
        insertRow(TEMP_EVENT_E_LTE_ERR_RAW, values);
        insertRow(TEMP_EVENT_E_LTE_SUC_RAW, values);

        values = new HashMap<String, Object>();
        values.put(IMSI, testIMSI);
        values.put(EVENT_ID, DETACH_IN_4G);
        values.put(RAT, 1);
        values.put(DATETIME_ID, DateTimeUtilities.getDateTimeMinusDay(2));
        values.put(NO_OF_SUCCESSES, 1);
        values.put(HIER3_ID, HIER3_ID_VAL_G1);
        values.put(HIER321_ID, HIER321_ID_VAL_G1);
        insertRow(TEMP_EVENT_E_LTE_IMSI_SUC_RAW, values);

        values = new HashMap<String, Object>();
        values.put(IMSI, testIMSI);
        values.put(EVENT_ID, HANDOVER_IN_4G);
        values.put(DATETIME_ID, DateTimeUtilities.getDateTimeMinusDay(4));
        values.put(LOCAL_DATE_ID, DateTimeUtilities.getDateTimeMinusDay(4)
                .substring(0, 10));
        values.put(RAT, 1);
        values.put(VENDOR, ERICSSON);
        values.put(HIERARCHY_3, HIERARCHY_3_STR_G2);
        values.put(HIERARCHY_2, "");
        values.put(HIERARCHY_1, HIERARCHY_1_STR);
        insertRow(TEMP_EVENT_E_LTE_ERR_RAW, values);
        insertRow(TEMP_EVENT_E_LTE_ERR_RAW, values);
        insertRow(TEMP_EVENT_E_LTE_SUC_RAW, values);

        values = new HashMap<String, Object>();
        values.put(IMSI, testIMSI);
        values.put(EVENT_ID, HANDOVER_IN_4G);
        values.put(RAT, 1);
        values.put(DATETIME_ID, DateTimeUtilities.getDateTimeMinusDay(2));
        values.put(NO_OF_SUCCESSES, 1);
        values.put(HIER3_ID, HIER3_ID_VAL_G2);
        values.put(HIER321_ID, HIER321_ID_VAL_G2);
        insertRow(TEMP_EVENT_E_LTE_IMSI_SUC_RAW, values);

        values = new HashMap<String, Object>();
        values.put(IMSI, testIMSI);
        values.put(EVENT_ID, TAU_IN_4G);
        values.put(DATETIME_ID, DateTimeUtilities.getDateTimeMinusDay(4));
        values.put(LOCAL_DATE_ID, DateTimeUtilities.getDateTimeMinusDay(4)
                .substring(0, 10));
        values.put(RAT, 1);
        values.put(VENDOR, ERICSSON);
        values.put(HIERARCHY_3, HIERARCHY_3_STR_G2);
        values.put(HIERARCHY_2, "");
        values.put(HIERARCHY_1, HIERARCHY_1_STR);
        insertRow(TEMP_EVENT_E_LTE_ERR_RAW, values);
        insertRow(TEMP_EVENT_E_LTE_SUC_RAW, values);

        values = new HashMap<String, Object>();
        values.put(IMSI, testIMSI);
        values.put(EVENT_ID, TAU_IN_4G);
        values.put(RAT, 1);
        values.put(DATETIME_ID, DateTimeUtilities.getDateTimeMinusDay(2));
        values.put(NO_OF_SUCCESSES, 1);
        values.put(HIER3_ID, HIER3_ID_VAL_G2);
        values.put(HIER321_ID, HIER321_ID_VAL_G2);
        insertRow(TEMP_EVENT_E_LTE_IMSI_SUC_RAW, values);

        values = new HashMap<String, Object>();
        values.put(IMSI, testIMSI);
        values.put(EVENT_ID, DETACH_IN_2G_AND_3G);
        values.put(DATETIME_ID, DateTimeUtilities.getDateTimeMinusDay(1));
        values.put(LOCAL_DATE_ID, DateTimeUtilities.getDateTimeMinusDay(1)
                .substring(0, 10));
        values.put(RAT, 1);
        values.put(VENDOR, ERICSSON);
        values.put(HIERARCHY_3, HIERARCHY_3_STR_G2);
        values.put(HIERARCHY_2, "");
        values.put(HIERARCHY_1, HIERARCHY_1_STR);
        insertRow(TEMP_EVENT_E_SGEH_ERR_RAW, values);
        insertRow(TEMP_EVENT_E_SGEH_SUC_RAW, values);

        values = new HashMap<String, Object>();
        values.put(IMSI, testIMSI);
        values.put(EVENT_ID, DETACH_IN_2G_AND_3G);
        values.put(RAT, 1);
        values.put(DATETIME_ID, DateTimeUtilities.getDateTimeMinusDay(2));
        values.put(NO_OF_SUCCESSES, 1);
        values.put(HIER3_ID, HIER3_ID_VAL_G2);
        values.put(HIER321_ID, HIER321_ID_VAL_G2);
        insertRow(TEMP_EVENT_E_SGEH_IMSI_SUC_RAW, values);

        values = new HashMap<String, Object>();
        values.put(IMSI, testIMSI);
        values.put(EVENT_ID, SERVICE_REQUEST_IN_2G_AND_3G);
        values.put(DATETIME_ID, DateTimeUtilities.getDateTimeMinusDay(2));
        values.put(LOCAL_DATE_ID, DateTimeUtilities.getDateTimeMinusDay(2)
                .substring(0, 10));
        values.put(RAT, 1);
        values.put(VENDOR, ERICSSON);
        values.put(HIERARCHY_3, HIERARCHY_3_STR_G2);
        values.put(HIERARCHY_2, "");
        values.put(HIERARCHY_1, HIERARCHY_1_STR);
        insertRow(TEMP_EVENT_E_SGEH_ERR_RAW, values);
        insertRow(TEMP_EVENT_E_SGEH_SUC_RAW, values);

        values = new HashMap<String, Object>();
        values.put(IMSI, testIMSI);
        values.put(EVENT_ID, SERVICE_REQUEST_IN_2G_AND_3G);
        values.put(RAT, 1);
        values.put(DATETIME_ID, DateTimeUtilities.getDateTimeMinusDay(2));
        values.put(NO_OF_SUCCESSES, 1);
        values.put(HIER3_ID, HIER3_ID_VAL_G2);
        values.put(HIER321_ID, HIER321_ID_VAL_G2);
        insertRow(TEMP_EVENT_E_SGEH_IMSI_SUC_RAW, values);

    }
}