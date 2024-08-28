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

public class SubBIBusyDayWithPreparedTablesTest extends TestsWithTemporaryTablesBaseTestCase<SubBIResult> {

    private static final String STRING_ONE = "1";

    private static final String STRING_TWO = "2";

    private static final int TOTAL_NUMBER_OF_INSERT_RECORDS = 2;

    private static final String TWENTY_MAX_ROWS = "20";

    SubsessionBIResource subsessionBIResource;

    private final static List<String> tempDataTables = new ArrayList<String>();
    private final static List<String> tempImsiRawDataTables = new ArrayList<String>();

    private final static Collection<String> rawTableColumns = new ArrayList<String>();
    private final static Collection<String> imsiRawTableColumns = new ArrayList<String>();

    private final static Collection<String> groupColumns = new ArrayList<String>();
    private final static Collection<String> msisdnColumns = new ArrayList<String>();

    static final List<String> tempSGEHTablesList = new ArrayList<String>();
    static final List<String> tempLTETablesList = new ArrayList<String>();

    private final long testIMSI = 312030410000004L;
    private final long testMSISDN = 11001100L;

    static {
        tempSGEHTablesList.add(TEMP_EVENT_E_SGEH_ERR_RAW);
        tempSGEHTablesList.add(TEMP_EVENT_E_SGEH_IMSI_SUC_RAW);

        tempLTETablesList.add(TEMP_EVENT_E_LTE_ERR_RAW);
        tempLTETablesList.add(TEMP_EVENT_E_LTE_IMSI_SUC_RAW);

        tempDataTables.add(TEMP_EVENT_E_SGEH_ERR_RAW);
        tempDataTables.add(TEMP_EVENT_E_LTE_ERR_RAW);

        tempImsiRawDataTables.add(TEMP_EVENT_E_SGEH_IMSI_SUC_RAW);
        tempImsiRawDataTables.add(TEMP_EVENT_E_LTE_IMSI_SUC_RAW);

        rawTableColumns.add(MSISDN);
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

        populateTemporaryTables(tempSGEHTablesList, 2, DateTimeUtilities.getDateTimeMinusDay(2));
        populateTemporaryTables(tempLTETablesList, 2, DateTimeUtilities.getDateTimeMinusDay(3));
        populateTemporaryTables(tempLTETablesList, 1, DateTimeUtilities.getDateTimeMinusDay(4));
        populateTemporaryTables(tempSGEHTablesList, 1, DateTimeUtilities.getDateTime(Calendar.MINUTE, -30));
        populateDimMsisdnTables();
        populateImsiGroupTables();
        attachDependencies(subsessionBIResource);

    }

    @Test
    public void testSubBIBusyDayForOneWeek() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParameters(ONE_WEEK);
        jndiProperties.disableSucRawJNDIProperty();
        map.putSingle(IMSI_PARAM, Long.toString(testIMSI));

        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String json = subsessionBIResource.getSubBIBusyDayData();

        final List<SubBIResult> results = getTranslator().translateResult(json, SubBIResult.class);
        jndiProperties.useSucRawJNDIProperty();

        validateResults(results);
    }

    @Test
    public void testSubBIBusyDayForOneDay() throws Exception {
        populateTemporaryTables(tempSGEHTablesList, 1, DateTimeUtilities.getDateTimeMinusDay(1));
        final MultivaluedMap<String, String> map = getRequestParameters(ONE_DAY);
        jndiProperties.disableSucRawJNDIProperty();
        map.putSingle(IMSI_PARAM, Long.toString(testIMSI));

        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String json = subsessionBIResource.getSubBIBusyDayData();

        final List<SubBIResult> results = getTranslator().translateResult(json, SubBIResult.class);
        jndiProperties.useSucRawJNDIProperty();

        validateResultsForNonWeekly(results, true);
    }

    @Test
    public void testSubBIBusyDayForSixHours() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParameters(SIX_HOURS);
        jndiProperties.disableSucRawJNDIProperty();
        map.putSingle(IMSI_PARAM, Long.toString(testIMSI));

        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String json = subsessionBIResource.getSubBIBusyDayData();

        final List<SubBIResult> results = getTranslator().translateResult(json, SubBIResult.class);
        jndiProperties.useSucRawJNDIProperty();

        validateResultsForNonWeekly(results, false);
    }

    @Test
    public void testSubBIBusyDayForThirtyMinutes() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParameters(THIRTY_MINUTES);
        jndiProperties.disableSucRawJNDIProperty();
        map.putSingle(IMSI_PARAM, Long.toString(testIMSI));

        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String json = subsessionBIResource.getSubBIBusyDayData();

        final List<SubBIResult> results = getTranslator().translateResult(json, SubBIResult.class);
        jndiProperties.useSucRawJNDIProperty();

        validateResultsForNonWeekly(results, false);
    }

    @Test
    public void testSubBIBusyDayGroupForOneWeek() throws Exception {
        jndiProperties.disableSucRawJNDIProperty();
        final MultivaluedMap<String, String> map = getRequestParameters(ONE_WEEK);
        map.putSingle(GROUP_NAME_PARAM, SAMPLE_IMSI_GROUP);

        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String json = subsessionBIResource.getSubBIBusyDayData();

        final List<SubBIResult> results = getTranslator().translateResult(json, SubBIResult.class);
        jndiProperties.useSucRawJNDIProperty();

        validateResults(results);
    }

    @Test
    public void testSubBIBusyDayGroupForOneDay() throws Exception {
        populateTemporaryTables(tempSGEHTablesList, 1, DateTimeUtilities.getDateTimeMinusDay(1));
        jndiProperties.disableSucRawJNDIProperty();
        final MultivaluedMap<String, String> map = getRequestParameters(ONE_DAY);
        map.putSingle(GROUP_NAME_PARAM, SAMPLE_IMSI_GROUP);
        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String json = subsessionBIResource.getSubBIBusyDayData();

        final List<SubBIResult> results = getTranslator().translateResult(json, SubBIResult.class);
        jndiProperties.useSucRawJNDIProperty();
        validateResultsForNonWeekly(results, true);
    }

    @Test
    public void testSubBIBusyDayGroupForSixHours() throws Exception {
        jndiProperties.disableSucRawJNDIProperty();
        final MultivaluedMap<String, String> map = getRequestParameters(SIX_HOURS);
        map.putSingle(GROUP_NAME_PARAM, SAMPLE_IMSI_GROUP);
        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String json = subsessionBIResource.getSubBIBusyDayData();

        final List<SubBIResult> results = getTranslator().translateResult(json, SubBIResult.class);
        jndiProperties.useSucRawJNDIProperty();
        validateResultsForNonWeekly(results, false);
    }

    @Test
    public void testSubBIBusyDayGroupForThirtyMinutes() throws Exception {
        jndiProperties.disableSucRawJNDIProperty();
        final MultivaluedMap<String, String> map = getRequestParameters(THIRTY_MINUTES);
        map.putSingle(GROUP_NAME_PARAM, SAMPLE_IMSI_GROUP);
        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String json = subsessionBIResource.getSubBIBusyDayData();

        final List<SubBIResult> results = getTranslator().translateResult(json, SubBIResult.class);
        jndiProperties.useSucRawJNDIProperty();
        validateResultsForNonWeekly(results, false);
    }

    @Test
    public void testSubBIBusyDayMSISDNForOneWeek() throws Exception {
        jndiProperties.disableSucRawJNDIProperty();
        final MultivaluedMap<String, String> map = getRequestParameters(ONE_WEEK);
        map.putSingle(TYPE_PARAM, TYPE_MSISDN);
        map.putSingle(MSISDN_PARAM, Long.toString(testMSISDN));

        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String json = subsessionBIResource.getSubBIBusyDayData();
        final List<SubBIResult> results = getTranslator().translateResult(json, SubBIResult.class);
        jndiProperties.useSucRawJNDIProperty();

        validateResults(results);
    }

    @Test
    public void testSubBIBusyDayMSISDNForOneDay() throws Exception {
        populateTemporaryTables(tempSGEHTablesList, 1, DateTimeUtilities.getDateTimeMinusDay(1));
        jndiProperties.disableSucRawJNDIProperty();
        final MultivaluedMap<String, String> map = getRequestParameters(ONE_DAY);
        map.putSingle(TYPE_PARAM, TYPE_MSISDN);
        map.putSingle(MSISDN_PARAM, Long.toString(testMSISDN));
        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String json = subsessionBIResource.getSubBIBusyDayData();

        final List<SubBIResult> results = getTranslator().translateResult(json, SubBIResult.class);
        jndiProperties.useSucRawJNDIProperty();
        validateResultsForNonWeekly(results, true);
    }

    @Test
    public void testSubBIBusyDayMSISDNForSixHours() throws Exception {
        jndiProperties.disableSucRawJNDIProperty();
        final MultivaluedMap<String, String> map = getRequestParameters(SIX_HOURS);
        map.putSingle(TYPE_PARAM, TYPE_MSISDN);
        map.putSingle(MSISDN_PARAM, Long.toString(testMSISDN));
        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String json = subsessionBIResource.getSubBIBusyDayData();

        final List<SubBIResult> results = getTranslator().translateResult(json, SubBIResult.class);
        jndiProperties.useSucRawJNDIProperty();
        validateResultsForNonWeekly(results, false);
    }

    @Test
    public void testSubBIBusyDayMSISDNForThirtyMinutes() throws Exception {
        jndiProperties.disableSucRawJNDIProperty();
        final MultivaluedMap<String, String> map = getRequestParameters(THIRTY_MINUTES);
        map.putSingle(TYPE_PARAM, TYPE_MSISDN);
        map.putSingle(MSISDN_PARAM, Long.toString(testMSISDN));

        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String json = subsessionBIResource.getSubBIBusyDayData();

        final List<SubBIResult> results = getTranslator().translateResult(json, SubBIResult.class);
        jndiProperties.useSucRawJNDIProperty();
        validateResultsForNonWeekly(results, false);
    }

    private void validateResults(final List<SubBIResult> results) {
        final SubBIResult expectedResultsZero = new SubBIResult();
        expectedResultsZero.setFailureCount(STRING_ZERO);
        expectedResultsZero.setSuccessCount(STRING_ZERO);

        final SubBIResult expectedResultsTwoSuccessOneFailure = new SubBIResult();
        expectedResultsTwoSuccessOneFailure.setFailureCount(STRING_ONE);
        expectedResultsTwoSuccessOneFailure.setSuccessCount(STRING_TWO);

        final SubBIResult expectedResultsOneSuccessTwoFailure = new SubBIResult();
        expectedResultsOneSuccessTwoFailure.setFailureCount(STRING_TWO);
        expectedResultsOneSuccessTwoFailure.setSuccessCount(STRING_ONE);

        for (int resultIndex = 0; resultIndex < results.size(); resultIndex++) {
            final String dayOfWeek = DateTimeUtilities.getNameOfDayOfWeek(resultIndex + 1);
            if (resultIndex + 1 == DateTimeUtilities.getDayOfWeekMinusDay(4)) {
                expectedResultsOneSuccessTwoFailure.setXAxisLabel(dayOfWeek);
                assertEquals(expectedResultsOneSuccessTwoFailure, results.get(resultIndex));
            } else if (resultIndex + 1 == DateTimeUtilities.getDayOfWeekMinusDay(2)) {
                expectedResultsTwoSuccessOneFailure.setXAxisLabel(dayOfWeek);
                assertEquals(expectedResultsTwoSuccessOneFailure, results.get(resultIndex));
            } else if (resultIndex + 1 == DateTimeUtilities.getDayOfWeekMinusDay(3)) {
                expectedResultsTwoSuccessOneFailure.setXAxisLabel(dayOfWeek);
                assertEquals(expectedResultsTwoSuccessOneFailure, results.get(resultIndex));
            } else {
                expectedResultsZero.setXAxisLabel(dayOfWeek);
                assertEquals(expectedResultsZero, results.get(resultIndex));
            }
        }
    }

    private void populateTemporaryTables(List<String> tempTableList, int noOfSuc, String dateTime) throws SQLException {
        SQLExecutor sqlExecutor = null;
        try {
            sqlExecutor = new SQLExecutor(connection);

            Map<String, Object> values = new HashMap<String, Object>();
            Map<String, Object> imsiValues = new HashMap<String, Object>();

            values.put(IMSI_COLUMN, testIMSI);
            values.put(DATETIME_ID_TEST, dateTime);
            values.put(MSISDN, testMSISDN);
            imsiValues.putAll(values);
            imsiValues.put(NO_OF_SUCCESSES, noOfSuc);
            for (int i = noOfSuc; i <= TOTAL_NUMBER_OF_INSERT_RECORDS; i++) {
                insertRow(tempTableList.get(0), values);
            }
            insertRow(tempTableList.get(1), imsiValues);

        } finally {
            if (sqlExecutor != null) {
                sqlExecutor.close();
            }
        }
    }

    private void populateImsiGroupTables() throws SQLException {
        SQLExecutor sqlExecutor = null;
        try {
            sqlExecutor = new SQLExecutor(connection);
            Map<String, Object> values = new HashMap<String, Object>();
            values.put(GROUP_NAME, SAMPLE_IMSI_GROUP);
            values.put(IMSI_COLUMN, testIMSI);
            insertRow(TEMP_GROUP_TYPE_E_IMSI, values);

        } finally {
            if (sqlExecutor != null) {
                sqlExecutor.close();
            }
        }
    }

    private void populateDimMsisdnTables() throws SQLException {
        SQLExecutor sqlExecutor = null;
        try {
            sqlExecutor = new SQLExecutor(connection);
            Map<String, Object> values = new HashMap<String, Object>();
            values.put(IMSI_COLUMN, testIMSI);
            values.put(MSISDN, testMSISDN);
            insertRow(TEMP_DIM_E_IMSI_MSISDN, values);
        } finally {
            if (sqlExecutor != null) {
                sqlExecutor.close();
            }
        }
    }

    private MultivaluedMap<String, String> getRequestParameters(String timeParamValue) {
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(DISPLAY_PARAM, CHART_PARAM);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        map.putSingle(TIME_QUERY_PARAM, timeParamValue);
        map.putSingle(TZ_OFFSET, TZ_OFFSET_OF_ZERO);
        map.putSingle(MAX_ROWS, TWENTY_MAX_ROWS);
        return map;
    }

    private void validateResultsForNonWeekly(final List<SubBIResult> results, boolean isForOneDay) {

        final SubBIResult expectedResultsZero = new SubBIResult();
        expectedResultsZero.setFailureCount(STRING_ZERO);
        expectedResultsZero.setSuccessCount(STRING_ZERO);

        final SubBIResult expectedResultsOneSuccessTwoFailure = new SubBIResult();
        expectedResultsOneSuccessTwoFailure.setFailureCount(STRING_TWO);
        expectedResultsOneSuccessTwoFailure.setSuccessCount(STRING_ONE);

        for (int resultIndex = 0; resultIndex < results.size(); resultIndex++) {
            final String dayOfWeek = DateTimeUtilities.getNameOfDayOfWeek(resultIndex + 1);
            if (resultIndex + 1 == DateTimeUtilities.getDayOfWeekMinusDay(0)) {
                expectedResultsOneSuccessTwoFailure.setXAxisLabel(dayOfWeek);
                assertEquals(expectedResultsOneSuccessTwoFailure, results.get(resultIndex));
            } else if (isForOneDay && resultIndex + 1 == DateTimeUtilities.getDayOfWeekMinusDay(1)) {
                expectedResultsOneSuccessTwoFailure.setXAxisLabel(dayOfWeek);
                assertEquals(expectedResultsOneSuccessTwoFailure, results.get(resultIndex));
            } else {
                expectedResultsZero.setXAxisLabel(dayOfWeek);
                assertEquals(expectedResultsZero, results.get(resultIndex));
            }
        }
    }

}
