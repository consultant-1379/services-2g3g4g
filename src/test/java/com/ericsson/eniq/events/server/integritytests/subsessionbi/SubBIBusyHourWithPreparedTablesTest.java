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

import com.ericsson.eniq.events.server.resources.SubsessionBIResource;
import com.ericsson.eniq.events.server.resources.TestsWithTemporaryTablesBaseTestCase;
import com.ericsson.eniq.events.server.test.queryresults.SubBIResult;
import com.ericsson.eniq.events.server.test.sql.SQLExecutor;
import com.ericsson.eniq.events.server.test.stubs.DummyUriInfoImpl;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;
import com.sun.jersey.core.util.MultivaluedMapImpl;

public class SubBIBusyHourWithPreparedTablesTest extends TestsWithTemporaryTablesBaseTestCase<SubBIResult> {

    private static final String TWENTY_MAX_ROWS = "20";

    SubsessionBIResource subsessionBIResource;

    private final static List<String> tempDataTables = new ArrayList<String>();
    private final static List<String> tempImsiRawDataTables = new ArrayList<String>();

    private final static Collection<String> rawTableColumns = new ArrayList<String>();
    private final static Collection<String> imsiRawTableColumns = new ArrayList<String>();

    private final static Collection<String> groupColumns = new ArrayList<String>();
    private final static Collection<String> msisdnColumns = new ArrayList<String>();

    private final long testIMSI = 312030410000004L;
    private final long testMSISDN = 11001100L;

    static {
        tempDataTables.add(TEMP_EVENT_E_SGEH_ERR_RAW);
        tempDataTables.add(TEMP_EVENT_E_LTE_ERR_RAW);

        tempImsiRawDataTables.add(TEMP_EVENT_E_SGEH_IMSI_SUC_RAW);
        tempImsiRawDataTables.add(TEMP_EVENT_E_LTE_IMSI_SUC_RAW);

        rawTableColumns.add(MSISDN);
        rawTableColumns.add(HOUR_ID);
        rawTableColumns.add(DAY_ID);
        rawTableColumns.add(IMSI);
        rawTableColumns.add(DATETIME_ID);

        imsiRawTableColumns.addAll(rawTableColumns);
        imsiRawTableColumns.add(NO_OF_SUCCESSES);

        groupColumns.add(IMSI);
        groupColumns.add(GROUP_NAME);

        msisdnColumns.add(IMSI);
        msisdnColumns.add(MSISDN);
    }

    @Override
    public void onSetUp() throws Exception {
        super.onSetUp();
        subsessionBIResource = new SubsessionBIResource();

        for (final String tempTable : tempDataTables) {
            createTemporaryTable(tempTable, rawTableColumns);
        }

        for (final String tempTable : tempImsiRawDataTables) {
            createTemporaryTable(tempTable, imsiRawTableColumns);
        }

        createTemporaryTable(TEMP_GROUP_TYPE_E_IMSI, groupColumns);
        createTemporaryTable(TEMP_DIM_E_IMSI_MSISDN, msisdnColumns);

        populateTemporaryTables();

        attachDependencies(subsessionBIResource);
    }

    @Test
    public void testSubBIBusyHour() throws Exception {
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        jndiProperties.disableSucRawJNDIProperty();
        map.putSingle(DISPLAY_PARAM, CHART_PARAM);
        map.putSingle(TIME_QUERY_PARAM, ONE_DAY);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        map.putSingle(IMSI_PARAM, Long.toString(testIMSI));
        map.putSingle(TZ_OFFSET, TZ_OFFSET_OF_ZERO);
        map.putSingle(MAX_ROWS, TWENTY_MAX_ROWS);
        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String json = subsessionBIResource.getSubBIBusyHourData();
        System.out.println(json);

        final List<SubBIResult> results = getTranslator().translateResult(json, SubBIResult.class);
        jndiProperties.useSucRawJNDIProperty();

        validateResults(results);
    }

    @Test
    public void testSubBIBusyHourGroup() throws Exception {
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        jndiProperties.disableSucRawJNDIProperty();
        map.putSingle(DISPLAY_PARAM, CHART_PARAM);
        map.putSingle(TIME_QUERY_PARAM, ONE_DAY);
        map.putSingle(GROUP_NAME_PARAM, SAMPLE_IMSI_GROUP);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        map.putSingle(TZ_OFFSET, TZ_OFFSET_OF_ZERO);
        map.putSingle(MAX_ROWS, TWENTY_MAX_ROWS);
        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String json = subsessionBIResource.getSubBIBusyHourData();
        System.out.println(json);

        final List<SubBIResult> results = getTranslator().translateResult(json, SubBIResult.class);
        jndiProperties.useSucRawJNDIProperty();

        validateResults(results);
    }

    @Test
    public void testSubBIBusyHourMSISDN() throws Exception {
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        jndiProperties.disableSucRawJNDIProperty();
        map.putSingle(DISPLAY_PARAM, CHART_PARAM);
        map.putSingle(TIME_QUERY_PARAM, ONE_DAY);
        map.putSingle(TYPE_PARAM, TYPE_MSISDN);
        map.putSingle(MSISDN_PARAM, Long.toString(testMSISDN));
        map.putSingle(TZ_OFFSET, TZ_OFFSET_OF_ZERO);
        map.putSingle(MAX_ROWS, TWENTY_MAX_ROWS);
        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String json = subsessionBIResource.getSubBIBusyHourData();
        System.out.println(json);

        final List<SubBIResult> results = getTranslator().translateResult(json, SubBIResult.class);
        jndiProperties.useSucRawJNDIProperty();

        validateResults(results);
    }

    /**
     * @param results
     */
    private void validateResults(final List<SubBIResult> results) {
        final SubBIResult expectedResultsZero = new SubBIResult();
        expectedResultsZero.setFailureCount("0");
        expectedResultsZero.setSuccessCount("0");

        final SubBIResult expectedResultsTwoSuccessOneFailure = new SubBIResult();
        expectedResultsTwoSuccessOneFailure.setFailureCount("2");
        expectedResultsTwoSuccessOneFailure.setSuccessCount("1");

        final SubBIResult expectedResultsOneSuccessTwoFailure = new SubBIResult();
        expectedResultsOneSuccessTwoFailure.setFailureCount("1");
        expectedResultsOneSuccessTwoFailure.setSuccessCount("2");

        for (int i = 0; i < results.size(); i++) {
            if (Integer.parseInt((results.get(i).getXAxisLabel())) == Integer.parseInt((DateTimeUtilities.getDateTimeMinusHours(2).substring(11, 13)))) {
                expectedResultsOneSuccessTwoFailure.setXAxisLabel("" + Integer.parseInt(results.get(i).getXAxisLabel()));
                assertEquals(expectedResultsOneSuccessTwoFailure, results.get(i));
            } else if(Integer.parseInt(results.get(i).getXAxisLabel()) == Integer.parseInt(DateTimeUtilities.getDateTimeMinusHours(1).substring(11, 13))) {
                expectedResultsTwoSuccessOneFailure.setXAxisLabel("" + Integer.parseInt(results.get(i).getXAxisLabel()));
                assertEquals(expectedResultsTwoSuccessOneFailure, results.get(i));
            }
        }
    }

    private void populateTemporaryTables() throws SQLException {
        SQLExecutor sqlExecutor = null;
        try {
            sqlExecutor = new SQLExecutor(connection);

            Map<String, Object> values = new HashMap<String, Object>();
            Map<String, Object> imsiValues = new HashMap<String, Object>();

            values.put("HOUR_ID", DateTimeUtilities.getDateTimeMinusHours(1).substring(11, 13));
            values.put("DAY_ID", DateTimeUtilities.getDateTimeMinusHours(1).substring(8, 10));
            values.put("MSISDN", testMSISDN);
            values.put("IMSI", testIMSI);
            values.put("DATETIME_ID", DateTimeUtilities.getDateTimeMinusHours(1));
            imsiValues.putAll(values);
            imsiValues.put("NO_OF_SUCCESSES", 1);

            insertRow(TEMP_EVENT_E_SGEH_ERR_RAW, values);
            insertRow(TEMP_EVENT_E_SGEH_ERR_RAW, values);
            insertRow(TEMP_EVENT_E_SGEH_IMSI_SUC_RAW, imsiValues);

            values = new HashMap<String, Object>();
            imsiValues = new HashMap<String, Object>();
            values.put("HOUR_ID", DateTimeUtilities.getDateTimeMinusHours(2).substring(11, 13));
            values.put("DAY_ID", DateTimeUtilities.getDateTimeMinusHours(2).substring(8, 10));
            values.put("MSISDN", testMSISDN);
            values.put("IMSI", testIMSI);
            values.put("DATETIME_ID", DateTimeUtilities.getDateTimeMinusHours(2));
            imsiValues.putAll(values);
            imsiValues.put("NO_OF_SUCCESSES", 2);

            insertRow(TEMP_EVENT_E_SGEH_ERR_RAW, values);
            insertRow(TEMP_EVENT_E_SGEH_IMSI_SUC_RAW, imsiValues);

            values = new HashMap<String, Object>();
            imsiValues = new HashMap<String, Object>();
            values.put("HOUR_ID", DateTimeUtilities.getDateTimeMinusHours(2).substring(11, 13));
            values.put("DAY_ID", DateTimeUtilities.getDateTimeMinusHours(2).substring(8, 10));
            values.put("MSISDN", testMSISDN);
            values.put("IMSI", testIMSI);
            values.put("DATETIME_ID", DateTimeUtilities.getDateTimeMinusHours(2));

            imsiValues.putAll(values);
            imsiValues.put("NO_OF_SUCCESSES", 2);
            insertRow(TEMP_EVENT_E_LTE_ERR_RAW, values);
            insertRow(TEMP_EVENT_E_LTE_IMSI_SUC_RAW, imsiValues);

            values = new HashMap<String, Object>();
            imsiValues = new HashMap<String, Object>();
            values.put("HOUR_ID", DateTimeUtilities.getDateTimeMinusHours(1).substring(11, 13));
            values.put("DAY_ID", DateTimeUtilities.getDateTimeMinusHours(1).substring(8, 10));
            values.put("MSISDN", testMSISDN);
            values.put("IMSI", testIMSI);
            values.put("DATETIME_ID", DateTimeUtilities.getDateTimeMinusHours(1));
            imsiValues.putAll(values);
            imsiValues.put("NO_OF_SUCCESSES", 1);

            insertRow(TEMP_EVENT_E_LTE_ERR_RAW, values);
            insertRow(TEMP_EVENT_E_LTE_ERR_RAW, values);
            insertRow(TEMP_EVENT_E_LTE_IMSI_SUC_RAW, imsiValues);

            values = new HashMap<String, Object>();
            values.put("GROUP_NAME", SAMPLE_IMSI_GROUP);
            values.put("IMSI", testIMSI);
            insertRow(TEMP_GROUP_TYPE_E_IMSI, values);

            values = new HashMap<String, Object>();
            values.put("IMSI", testIMSI);
            values.put("MSISDN", testMSISDN);
            insertRow(TEMP_DIM_E_IMSI_MSISDN, values);

        } finally {
            if (sqlExecutor != null) {
                sqlExecutor.close();
            }
        }
    }

}
