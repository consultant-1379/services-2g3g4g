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

public class SubBIBusyDayWithSucRawWithPreparedTablesTest extends TestsWithTemporaryTablesBaseTestCase<SubBIResult> {

    private static final String STRING_TWO = "2";

    private static final String STRING_ONE = "1";

    private static final String TWENTY_MAX_ROWS = "20";

    SubsessionBIResource subsessionBIResource;

    private final static List<String> tempDataTables = new ArrayList<String>();

    private final static List<String> tempSGEHTablesList = new ArrayList<String>();

    private final static List<String> tempLTETablesList = new ArrayList<String>();

    private final static Map<String, String> rawTableColumns = new HashMap<String, String>();

    private final static Map<String, String> groupColumns = new HashMap<String, String>();

    private final long testIMSI = 312030410000004L;

    static {
        tempSGEHTablesList.add(TEMP_EVENT_E_SGEH_ERR_RAW);
        tempSGEHTablesList.add(TEMP_EVENT_E_SGEH_SUC_RAW);

        tempLTETablesList.add(TEMP_EVENT_E_LTE_ERR_RAW);
        tempLTETablesList.add(TEMP_EVENT_E_LTE_SUC_RAW);

        tempDataTables.add(TEMP_EVENT_E_SGEH_ERR_RAW);
        tempDataTables.add(TEMP_EVENT_E_SGEH_SUC_RAW);
        tempDataTables.add(TEMP_EVENT_E_LTE_ERR_RAW);
        tempDataTables.add(TEMP_EVENT_E_LTE_SUC_RAW);

        rawTableColumns.put(IMSI_COLUMN, UNSIGNED_BIGINT);
        rawTableColumns.put("DATETIME_ID", TIMESTAMP);

        groupColumns.put(IMSI_COLUMN, UNSIGNED_BIGINT);
        groupColumns.put(GROUP_NAME, VARCHAR_64);
    }

    @Override
    public void onSetUp() throws Exception {
        super.onSetUp();
        subsessionBIResource = new SubsessionBIResource();

        for (final String tempTable : tempDataTables) {
            createTemporaryTableWithColumnTypes(tempTable, rawTableColumns);
        }

        createTemporaryTableWithColumnTypes(TEMP_GROUP_TYPE_E_IMSI, groupColumns);
        populateTemporaryTables(tempSGEHTablesList, 2, DateTimeUtilities.getDateTimeMinusDay(2));
        populateTemporaryTables(tempLTETablesList, 2, DateTimeUtilities.getDateTimeMinusDay(3));
        populateTemporaryTables(tempLTETablesList, 1, DateTimeUtilities.getDateTimeMinusDay(4));
        populateTemporaryTables(tempSGEHTablesList, 1, DateTimeUtilities.getDateTime(Calendar.MINUTE, -30));
        populateImsiGroupTables();
        attachDependencies(subsessionBIResource);
    }

    @Test
    public void testSubBIBusyDayForWeek() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParameters(ONE_WEEK);
        jndiProperties.useSucRawJNDIProperty();
        map.putSingle(IMSI_PARAM, Long.toString(testIMSI));

        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String json = subsessionBIResource.getSubBIBusyDayData();
        final List<SubBIResult> results = getTranslator().translateResult(json, SubBIResult.class);

        validateResults(results);
    }

    @Test
    public void testSubBIBusyDayForOneDay() throws Exception {
        populateTemporaryTables(tempSGEHTablesList, 1, DateTimeUtilities.getDateTimeMinusDay(1));
        final MultivaluedMap<String, String> map = getRequestParameters(ONE_DAY);
        jndiProperties.useSucRawJNDIProperty();
        map.putSingle(IMSI_PARAM, Long.toString(testIMSI));

        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String json = subsessionBIResource.getSubBIBusyDayData();
        final List<SubBIResult> results = getTranslator().translateResult(json, SubBIResult.class);

        validateResultsForNonWeekly(results, true);
    }

    @Test
    public void testSubBIBusyDayForSixHours() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParameters(SIX_HOURS);
        jndiProperties.useSucRawJNDIProperty();
        map.putSingle(IMSI_PARAM, Long.toString(testIMSI));

        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String json = subsessionBIResource.getSubBIBusyDayData();
        final List<SubBIResult> results = getTranslator().translateResult(json, SubBIResult.class);

        validateResultsForNonWeekly(results, false);
    }

    @Test
    public void testSubBIBusyDayForThirtyMinutes() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParameters(THIRTY_MINUTES);
        jndiProperties.useSucRawJNDIProperty();
        map.putSingle(IMSI_PARAM, Long.toString(testIMSI));

        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String json = subsessionBIResource.getSubBIBusyDayData();
        final List<SubBIResult> results = getTranslator().translateResult(json, SubBIResult.class);

        validateResultsForNonWeekly(results, false);
    }

    @Test
    public void testSubBIBusyDayGroupforOneWeek() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParameters(ONE_WEEK);
        jndiProperties.useSucRawJNDIProperty();
        map.putSingle(GROUP_NAME_PARAM, SAMPLE_IMSI_GROUP);

        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String json = subsessionBIResource.getSubBIBusyDayData();
        final List<SubBIResult> results = getTranslator().translateResult(json, SubBIResult.class);

        validateResults(results);
    }

    @Test
    public void testSubBIBusyDayGroupForOneDay() throws Exception {
        populateTemporaryTables(tempSGEHTablesList, 1, DateTimeUtilities.getDateTimeMinusDay(1));
        final MultivaluedMap<String, String> map = getRequestParameters(ONE_DAY);
        jndiProperties.useSucRawJNDIProperty();
        map.putSingle(GROUP_NAME_PARAM, SAMPLE_IMSI_GROUP);

        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String json = subsessionBIResource.getSubBIBusyDayData();
        final List<SubBIResult> results = getTranslator().translateResult(json, SubBIResult.class);

        validateResultsForNonWeekly(results, true);
    }

    @Test
    public void testSubBIBusyDayGroupForSixHours() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParameters(SIX_HOURS);
        jndiProperties.useSucRawJNDIProperty();
        map.putSingle(GROUP_NAME_PARAM, SAMPLE_IMSI_GROUP);

        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String json = subsessionBIResource.getSubBIBusyDayData();
        final List<SubBIResult> results = getTranslator().translateResult(json, SubBIResult.class);

        validateResultsForNonWeekly(results, false);
    }

    @Test
    public void testSubBIBusyDayGroupForThirtyMinutes() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParameters(THIRTY_MINUTES);
        jndiProperties.useSucRawJNDIProperty();
        map.putSingle(GROUP_NAME_PARAM, SAMPLE_IMSI_GROUP);

        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String json = subsessionBIResource.getSubBIBusyDayData();
        final List<SubBIResult> results = getTranslator().translateResult(json, SubBIResult.class);

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

    private void populateTemporaryTables(List<String> tempTableList, int noOfSucRaw, String dateTime) throws SQLException {
        SQLExecutor sqlExecutor = null;
        final int TOTAL_NUMBER_OF_INSERT_RECORDS = 3;
        int row = 1;
        try {
            sqlExecutor = new SQLExecutor(connection);

            Map<String, Object> values = new HashMap<String, Object>();
            Map<String, Object> imsiValues = new HashMap<String, Object>();

            values.put(IMSI_COLUMN, testIMSI);
            values.put(DATETIME_ID_TEST, dateTime);
            for (int i = 1; i <= TOTAL_NUMBER_OF_INSERT_RECORDS; i++) {
                if (row <= noOfSucRaw) {
                    insertRow(tempTableList.get(1), values);
                    row++;
                    continue;
                }
                insertRow(tempTableList.get(0), values);
            }

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
