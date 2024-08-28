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

public class SubBIAPNUsageTest extends
        TestsWithTemporaryTablesBaseTestCase<SubBIResult> {

    SubsessionBIResource subsessionBIResource;

    private static final String TEST_IMSI = "312030410000004";

    private final static List<String> tempDataTables = new ArrayList<String>();

    private final static List<String> tempDataIMSITables = new ArrayList<String>();

    private final static Map<String, String> rawTableColumns = new HashMap<String, String>();

    private final static Map<String, String> imsiRawTableColumns = new HashMap<String, String>();

    private final long testIMSI = Long.valueOf(TEST_IMSI);

    static {
        tempDataTables.add(TEMP_EVENT_E_SGEH_ERR_RAW);
        tempDataTables.add(TEMP_EVENT_E_SGEH_SUC_RAW);
        tempDataTables.add(TEMP_EVENT_E_LTE_ERR_RAW);
        tempDataTables.add(TEMP_EVENT_E_LTE_SUC_RAW);

        tempDataIMSITables.add(TEMP_EVENT_E_SGEH_IMSI_SUC_RAW);
        tempDataIMSITables.add(TEMP_EVENT_E_LTE_IMSI_SUC_RAW);

        rawTableColumns.put("IMSI", "unsigned bigint");
        rawTableColumns.put("EVENT_ID", "unsigned bigint");
        rawTableColumns.put("DATETIME_ID", "timestamp");
        rawTableColumns.put("APN", "varchar(127)");
        rawTableColumns.put("LOCAL_DATE_ID", "timestamp");

        imsiRawTableColumns.put("IMSI", "unsigned bigint");
        imsiRawTableColumns.put("EVENT_ID", "unsigned bigint");
        imsiRawTableColumns.put("DATETIME_ID", "timestamp");
        imsiRawTableColumns.put("APN", "varchar(127)");
        imsiRawTableColumns.put("NO_OF_SUCCESSES", "unsigned bigint");
    }

    @Override
    public void onSetUp() throws Exception {
        super.onSetUp();
        subsessionBIResource = new SubsessionBIResource();

        for (final String tempTable : tempDataTables) {
            createTemporaryTableWithColumnTypes(tempTable, rawTableColumns);
        }

        for (final String tempTable : tempDataIMSITables) {
            createTemporaryTableWithColumnTypes(tempTable, imsiRawTableColumns);
        }

        populateTemporaryTables();

        attachDependencies(subsessionBIResource);
    }

    @Test
    public void testGetSUBBIAPNUsage_OneWeek_SuccessRawEnabled()
            throws Exception {
        jndiProperties.useSucRawJNDIProperty();

        final MultivaluedMap<String, String> map = mapInputParam(CHART_PARAM,
                ONE_WEEK, TYPE_IMSI, TEST_IMSI, TZ_OFFSET_OF_ZERO, "20");

        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String json = subsessionBIResource.getSubBIAPNData();
        System.out.println(json);

        final List<SubBIResult> results = getTranslator().translateResult(json,
                SubBIResult.class);
        validateResultsForSuccessRaw(results);
    }

    @Test
    public void testGetSUBBIAPNUsage_OneWeek_Offset_SuccessRawEnabled()
            throws Exception {
        jndiProperties.useSucRawJNDIProperty();

        final MultivaluedMap<String, String> map = mapInputParam(CHART_PARAM,
                ONE_WEEK, TYPE_IMSI, TEST_IMSI,
                TIME_ZONE_OFFSET_OF_PLUS_FIVE_THIRTY, "20");

        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String json = subsessionBIResource.getSubBIAPNData();
        System.out.println(json);

        final List<SubBIResult> results = getTranslator().translateResult(json,
                SubBIResult.class);
        validateResultsForSuccessRaw(results);
    }

    @Test
    public void testGetSUBBIAPNUsage_OneWeek_SuccessRawDisabled()
            throws Exception {
        jndiProperties.disableSucRawJNDIProperty();

        final MultivaluedMap<String, String> map = mapInputParam(CHART_PARAM,
                ONE_WEEK, TYPE_IMSI, TEST_IMSI, TZ_OFFSET_OF_ZERO, "20");

        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String json = subsessionBIResource.getSubBIAPNData();
        System.out.println(json);

        final List<SubBIResult> results = getTranslator().translateResult(json,
                SubBIResult.class);
        validateResultsForIMSISuccessRaw(results);
    }

    @Test
    public void testGetSUBBIAPNUsage_OneWeek_Offset_SuccessRawDisabled()
            throws Exception {
        jndiProperties.disableSucRawJNDIProperty();

        final MultivaluedMap<String, String> map = mapInputParam(CHART_PARAM,
                ONE_WEEK, TYPE_IMSI, TEST_IMSI,
                TIME_ZONE_OFFSET_OF_PLUS_FIVE_THIRTY, "20");

        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String json = subsessionBIResource.getSubBIAPNData();
        System.out.println(json);

        final List<SubBIResult> results = getTranslator().translateResult(json,
                SubBIResult.class);
        validateResultsForIMSISuccessRaw(results);
    }

    private MultivaluedMap<String, String> mapInputParam(
            final String displayParam, final String timeQueryParam,
            final String typeParam, final String imsiParam,
            final String tzOffset, final String maxRows) {
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(DISPLAY_PARAM, displayParam);
        map.putSingle(TIME_QUERY_PARAM, timeQueryParam);
        map.putSingle(TYPE_PARAM, typeParam);
        map.putSingle(IMSI_PARAM, imsiParam);
        map.putSingle(TZ_OFFSET, tzOffset);
        map.putSingle(MAX_ROWS, maxRows);
        return map;
    }

    private void validateResultsForSuccessRaw(final List<SubBIResult> results) {
        assertThat(results.size(), is(2));
        final SubBIResult firstSubBIResult = results.get(0);
        assertThat(firstSubBIResult.getXAxisLabel(), is(SAMPLE_APN2));
        assertThat(firstSubBIResult.getSuccessCount(), is("5"));
        assertThat(firstSubBIResult.getFailureCount(), is("5"));

        final SubBIResult secondSubBIResult = results.get(1);
        assertThat(secondSubBIResult.getXAxisLabel(), is(SAMPLE_APN));
        assertThat(secondSubBIResult.getSuccessCount(), is("3"));
        assertThat(secondSubBIResult.getFailureCount(), is("3"));
    }

    private void validateResultsForIMSISuccessRaw(
            final List<SubBIResult> results) {
        assertThat(results.size(), is(2));
        final SubBIResult firstSubBIResult = results.get(0);
        assertThat(firstSubBIResult.getXAxisLabel(), is(SAMPLE_APN2));
        assertThat(firstSubBIResult.getSuccessCount(), is("50"));
        assertThat(firstSubBIResult.getFailureCount(), is("5"));

        final SubBIResult secondSubBIResult = results.get(1);
        assertThat(secondSubBIResult.getXAxisLabel(), is(SAMPLE_APN));
        assertThat(secondSubBIResult.getSuccessCount(), is("30"));
        assertThat(secondSubBIResult.getFailureCount(), is("3"));
    }

    private void populateTemporaryTables() throws SQLException {
        Map<String, Object> values = addValuesToRawTables(testIMSI, SAMPLE_APN,
                ACTIVATE_IN_2G_AND_3G,
                DateTimeUtilities.getDateTimeMinusDay(3), DateTimeUtilities
                        .getDateTimeMinusDay(3).substring(0, 10));

        insertRow(TEMP_EVENT_E_SGEH_ERR_RAW, values);
        insertRow(TEMP_EVENT_E_SGEH_ERR_RAW, values);
        insertRow(TEMP_EVENT_E_SGEH_SUC_RAW, values);

        Map<String, Object> valuesimsiSucRaw = addValuesToImsiSucTables(
                testIMSI, SAMPLE_APN, ACTIVATE_IN_2G_AND_3G, 10,
                DateTimeUtilities.getDateTimeMinusDay(3));
        insertRow(TEMP_EVENT_E_SGEH_IMSI_SUC_RAW, valuesimsiSucRaw);

        values = addValuesToRawTables(testIMSI, SAMPLE_APN,
                DEACTIVATE_IN_2G_AND_3G,
                DateTimeUtilities.getDateTimeMinusDay(2), DateTimeUtilities
                        .getDateTimeMinusDay(2).substring(0, 10));

        insertRow(TEMP_EVENT_E_SGEH_ERR_RAW, values);
        insertRow(TEMP_EVENT_E_SGEH_SUC_RAW, values);
        insertRow(TEMP_EVENT_E_SGEH_SUC_RAW, values);

        valuesimsiSucRaw = addValuesToImsiSucTables(testIMSI, SAMPLE_APN,
                DEACTIVATE_IN_2G_AND_3G, 20,
                DateTimeUtilities.getDateTimeMinusDay(2));
        insertRow(TEMP_EVENT_E_SGEH_IMSI_SUC_RAW, valuesimsiSucRaw);

        values = addValuesToRawTables(testIMSI, SAMPLE_APN2, ATTACH_IN_4G,
                DateTimeUtilities.getDateTimeMinusDay(3), DateTimeUtilities
                        .getDateTimeMinusDay(3).substring(0, 10));
        insertRow(TEMP_EVENT_E_LTE_ERR_RAW, values);
        insertRow(TEMP_EVENT_E_LTE_SUC_RAW, values);

        valuesimsiSucRaw = addValuesToImsiSucTables(testIMSI, SAMPLE_APN2,
                ATTACH_IN_4G, 10, DateTimeUtilities.getDateTimeMinusDay(3));
        insertRow(TEMP_EVENT_E_LTE_IMSI_SUC_RAW, valuesimsiSucRaw);

        values = addValuesToRawTables(testIMSI, SAMPLE_APN2,
                DEDICATED_BEARER_ACTIVATE_IN_4G,
                DateTimeUtilities.getDateTimeMinusDay(3), DateTimeUtilities
                        .getDateTimeMinusDay(3).substring(0, 10));

        insertRow(TEMP_EVENT_E_LTE_ERR_RAW, values);
        insertRow(TEMP_EVENT_E_LTE_SUC_RAW, values);

        valuesimsiSucRaw = addValuesToImsiSucTables(testIMSI, SAMPLE_APN2,
                DEDICATED_BEARER_ACTIVATE_IN_4G, 10,
                DateTimeUtilities.getDateTimeMinusDay(3));
        insertRow(TEMP_EVENT_E_LTE_IMSI_SUC_RAW, valuesimsiSucRaw);

        values = addValuesToRawTables(testIMSI, SAMPLE_APN2,
                DEDICATED_BEARER_DEACTIVATE_IN_4G,
                DateTimeUtilities.getDateTimeMinusDay(3), DateTimeUtilities
                        .getDateTimeMinusDay(3).substring(0, 10));

        insertRow(TEMP_EVENT_E_LTE_ERR_RAW, values);
        insertRow(TEMP_EVENT_E_LTE_SUC_RAW, values);

        valuesimsiSucRaw = addValuesToImsiSucTables(testIMSI, SAMPLE_APN2,
                DEDICATED_BEARER_DEACTIVATE_IN_4G, 10,
                DateTimeUtilities.getDateTimeMinusDay(3));
        insertRow(TEMP_EVENT_E_LTE_IMSI_SUC_RAW, valuesimsiSucRaw);

        values = addValuesToRawTables(testIMSI, SAMPLE_APN2, PDN_CONNECT_IN_4G,
                DateTimeUtilities.getDateTimeMinusDay(3), DateTimeUtilities
                        .getDateTimeMinusDay(3).substring(0, 10));

        insertRow(TEMP_EVENT_E_LTE_ERR_RAW, values);
        insertRow(TEMP_EVENT_E_LTE_SUC_RAW, values);

        valuesimsiSucRaw = addValuesToImsiSucTables(testIMSI, SAMPLE_APN2,
                PDN_CONNECT_IN_4G, 10, DateTimeUtilities.getDateTimeMinusDay(3));
        insertRow(TEMP_EVENT_E_LTE_IMSI_SUC_RAW, valuesimsiSucRaw);

        values = addValuesToRawTables(testIMSI, SAMPLE_APN2,
                PDN_DISCONNECT_IN_4G, DateTimeUtilities.getDateTimeMinusDay(4),
                DateTimeUtilities.getDateTimeMinusDay(4).substring(0, 10));

        insertRow(TEMP_EVENT_E_LTE_ERR_RAW, values);
        insertRow(TEMP_EVENT_E_LTE_SUC_RAW, values);

        valuesimsiSucRaw = addValuesToImsiSucTables(testIMSI, SAMPLE_APN2,
                PDN_DISCONNECT_IN_4G, 10,
                DateTimeUtilities.getDateTimeMinusDay(4));
        insertRow(TEMP_EVENT_E_LTE_IMSI_SUC_RAW, valuesimsiSucRaw);

        /* these are included here and should be excluded by the sql query */

        values = addValuesToRawTables(testIMSI, SAMPLE_APN2, DETACH_IN_4G,
                DateTimeUtilities.getDateTimeMinusDay(4), DateTimeUtilities
                        .getDateTimeMinusDay(4).substring(0, 10));
        insertRow(TEMP_EVENT_E_LTE_ERR_RAW, values);
        insertRow(TEMP_EVENT_E_LTE_SUC_RAW, values);

        valuesimsiSucRaw = addValuesToImsiSucTables(testIMSI, SAMPLE_APN2,
                DETACH_IN_4G, 10, DateTimeUtilities.getDateTimeMinusDay(4));
        insertRow(TEMP_EVENT_E_LTE_IMSI_SUC_RAW, valuesimsiSucRaw);

        values = addValuesToRawTables(testIMSI, SAMPLE_APN2, HANDOVER_IN_4G,
                DateTimeUtilities.getDateTimeMinusDay(4), DateTimeUtilities
                        .getDateTimeMinusDay(4).substring(0, 10));
        insertRow(TEMP_EVENT_E_LTE_ERR_RAW, values);
        insertRow(TEMP_EVENT_E_LTE_SUC_RAW, values);

        valuesimsiSucRaw = addValuesToImsiSucTables(testIMSI, SAMPLE_APN2,
                HANDOVER_IN_4G, 10, DateTimeUtilities.getDateTimeMinusDay(4));
        insertRow(TEMP_EVENT_E_LTE_IMSI_SUC_RAW, valuesimsiSucRaw);

        values = addValuesToRawTables(testIMSI, SAMPLE_APN2, TAU_IN_4G,
                DateTimeUtilities.getDateTimeMinusDay(4), DateTimeUtilities
                        .getDateTimeMinusDay(4).substring(0, 10));
        insertRow(TEMP_EVENT_E_LTE_ERR_RAW, values);
        insertRow(TEMP_EVENT_E_LTE_SUC_RAW, values);

        valuesimsiSucRaw = addValuesToImsiSucTables(testIMSI, SAMPLE_APN2,
                TAU_IN_4G, 10, DateTimeUtilities.getDateTimeMinusDay(4));
        insertRow(TEMP_EVENT_E_LTE_IMSI_SUC_RAW, valuesimsiSucRaw);

        values = addValuesToRawTables(testIMSI, SAMPLE_APN,
                DETACH_IN_2G_AND_3G, DateTimeUtilities.getDateTimeMinusDay(3),
                DateTimeUtilities.getDateTimeMinusDay(3).substring(0, 10));

        insertRow(TEMP_EVENT_E_SGEH_ERR_RAW, values);
        insertRow(TEMP_EVENT_E_SGEH_SUC_RAW, values);

        valuesimsiSucRaw = addValuesToImsiSucTables(testIMSI, SAMPLE_APN,
                DETACH_IN_2G_AND_3G, 1,
                DateTimeUtilities.getDateTimeMinusDay(3));
        insertRow(TEMP_EVENT_E_SGEH_IMSI_SUC_RAW, valuesimsiSucRaw);

        values = addValuesToRawTables(testIMSI, SAMPLE_APN,
                SERVICE_REQUEST_IN_2G_AND_3G,
                DateTimeUtilities.getDateTimeMinusDay(2), DateTimeUtilities
                        .getDateTimeMinusDay(2).substring(0, 10));

        insertRow(TEMP_EVENT_E_SGEH_ERR_RAW, values);
        insertRow(TEMP_EVENT_E_SGEH_SUC_RAW, values);

        valuesimsiSucRaw = addValuesToImsiSucTables(testIMSI, SAMPLE_APN,
                SERVICE_REQUEST_IN_2G_AND_3G, 10,
                DateTimeUtilities.getDateTimeMinusDay(2));
        insertRow(TEMP_EVENT_E_SGEH_IMSI_SUC_RAW, valuesimsiSucRaw);

    }

    private Map<String, Object> addValuesToRawTables(final long imsi,
            final String apn, final int eventId, final String dateTimeId,
            final String localDateId) {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(IMSI, imsi);
        values.put(APN, apn);
        values.put(EVENT_ID, eventId);
        values.put(DATETIME_ID, dateTimeId);
        values.put(LOCAL_DATE_ID, localDateId);
        return values;
    }

    private Map<String, Object> addValuesToImsiSucTables(final long imsi,
            final String apn, final int eventId, final int successValue,
            final String dateTimeId) {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(IMSI, imsi);
        values.put(APN, apn);
        values.put(EVENT_ID, eventId);
        values.put(NO_OF_SUCCESSES, successValue);
        values.put(DATETIME_ID, dateTimeId);
        return values;
    }
}
