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
package com.ericsson.eniq.events.server.integritytests.dtput;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.common.RequestParametersUtilities.*;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.*;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.*;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.net.URISyntaxException;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import javax.ws.rs.core.MultivaluedMap;

import org.junit.Test;

import com.ericsson.eniq.events.server.resources.TestsWithTemporaryTablesBaseTestCase;
import com.ericsson.eniq.events.server.resources.dtput.NetworkDataVolumeResource;
import com.ericsson.eniq.events.server.test.queryresults.datavolume.SubscriberDataVolumeResult;
import com.ericsson.eniq.events.server.test.stubs.DummyUriInfoImpl;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;
import com.sun.jersey.core.util.MultivaluedMapImpl;

public class SubscriberDataVolumeWithPreparedTablesTest extends TestsWithTemporaryTablesBaseTestCase<SubscriberDataVolumeResult> {

    private MultivaluedMap<String, String> map;

    private NetworkDataVolumeResource networkDataVolumeResource;

    private static final long MEGABYTE = 1024L * 1024L;

    private static final int KB_MULTIPLIER = 1024;

    private static final long MILLISECONDUNIT = 1000L;

    private static final String MESSAGE = "CANCEL_REQUEST_NOT_SUPPORTED";

    private static final String IMSI_GROUP1 = "imsi_group1";

    private static final String APN1 = "apn1";

    private static final String APN3 = "apn3";

    private static final long TAC1 = 10000000L;

    private static final long TAC2 = 20000000L;

    private static final long TAC3 = 30000000L;

    private static final long TAC4 = 40000000L;

    private static final long IMSI1 = 12300000000L;

    private static final long IMSI2 = 22900000000L;

    private static final long IMSI3 = 32100000000L;

    private static final long IMSI4 = 29200000000L;

    private static final long MSISDN1 = 10000000000L;

    private static final long MSISDN2 = 20000000000L;

    private static final long MSISDN3 = 30000000000L;

    private static final long MSISDN4 = 40000000000L;

    private static final long DURATION1 = 100000L;

    private static final long DURATION2 = 200000L;

    private static final long DURATION3 = 300000L;

    private static final long DURATION4 = 100000L;

    private static final long DURATION5 = 200000L;

    private static final long DURATION6 = 300000L;

    private static final int PDN_ID1 = 10000;

    private static final int PDN_ID2 = 20000;

    private static final int PDN_ID3 = 15000;

    private static final int PDN_ID4 = 25000;

    private static final long GGSN_IPADDRESS1 = 12300000000L;

    private static final long GGSN_IPADDRESS2 = 45600000000L;

    private static final long GGSN_IPADDRESS3 = 12300000000L;

    private static final long GGSN_IPADDRESS4 = 45600000000L;

    private static final String TEST_APN_01 = APN1;

    private static final long TEST_TAC_01 = TAC1;

    private static final long TEST_IMSI_01 = IMSI1;

    private static final long TEST_MSISDN_01 = MSISDN1;

    private static final long TEST_DURATION_01 = DURATION1;

    private static final long TEST_GGSN_IPADDRESS_01 = GGSN_IPADDRESS1;

    private static final int TEST_PDN_ID_01 = PDN_ID2;

    private static final double TEST_DL_01 = 1000.000;

    private static final double TEST_UL_01 = 2000.000;

    private static final String TEST_DATETIME_ID_01 = DateTimeUtilities.getDateTime(DATE_TIME_FORMAT, Calendar.MINUTE, -45);

    private static final String TEST_DATETIME_ID_DAY_01 = DateTimeUtilities.getDateTime(DATE_TIME_FORMAT, Calendar.DAY_OF_MONTH, -1);

    private static final String TEST_APN_02 = APN1;

    private static final long TEST_TAC_02 = TAC2;

    private static final long TEST_IMSI_02 = IMSI2;

    private static final long TEST_MSISDN_02 = MSISDN2;

    private static final long TEST_DURATION_02 = DURATION2;

    private static final long TEST_GGSN_IPADDRESS_02 = GGSN_IPADDRESS1;

    private static final int TEST_PDN_ID_02 = PDN_ID2;

    private static final double TEST_DL_02 = 2000.000;

    private static final double TEST_UL_02 = 3000.000;

    private static final String TEST_DATETIME_ID_02 = TEST_DATETIME_ID_01;

    private static final String TEST_APN_03 = APN1;

    private static final long TEST_TAC_03 = TAC2;

    private static final long TEST_IMSI_03 = IMSI1;

    private static final long TEST_MSISDN_03 = MSISDN2;

    private static final long TEST_DURATION_03 = DURATION3;

    private static final long TEST_GGSN_IPADDRESS_03 = GGSN_IPADDRESS2;

    private static final int TEST_PDN_ID_03 = PDN_ID1;

    private static final double TEST_DL_03 = 4000.000;

    private static final double TEST_UL_03 = 5000.000;

    private static final String TEST_DATETIME_ID_03 = TEST_DATETIME_ID_01;

    private static final String TEST_APN_04 = APN3;

    private static final long TEST_TAC_04 = TAC3;

    private static final long TEST_IMSI_04 = IMSI3;

    private static final long TEST_MSISDN_04 = MSISDN3;

    private static final long TEST_DURATION_04 = DURATION4;

    private static final long TEST_GGSN_IPADDRESS_04 = GGSN_IPADDRESS3;

    private static final int TEST_PDN_ID_04 = PDN_ID4;

    private static final double TEST_DL_04 = 1000.000;

    private static final double TEST_UL_04 = 2000.000;

    private static final String TEST_DATETIME_ID_04 = DateTimeUtilities.getDateTime(DATE_TIME_FORMAT, Calendar.MINUTE, -45);

    private static final String TEST_APN_05 = APN3;

    private static final long TEST_TAC_05 = TAC4;

    private static final long TEST_IMSI_05 = IMSI4;

    private static final long TEST_MSISDN_05 = MSISDN4;

    private static final long TEST_DURATION_05 = DURATION5;

    private static final long TEST_GGSN_IPADDRESS_05 = GGSN_IPADDRESS3;

    private static final int TEST_PDN_ID_05 = PDN_ID4;

    private static final double TEST_DL_05 = 2000.000;

    private static final double TEST_UL_05 = 3000.000;

    private static final String TEST_DATETIME_ID_05 = TEST_DATETIME_ID_04;

    private static final String TEST_APN_06 = APN3;

    private static final long TEST_TAC_06 = TAC4;

    private static final long TEST_IMSI_06 = IMSI3;

    private static final long TEST_MSISDN_06 = MSISDN4;

    private static final long TEST_DURATION_06 = DURATION6;

    private static final long TEST_GGSN_IPADDRESS_06 = GGSN_IPADDRESS4;

    private static final int TEST_PDN_ID_06 = PDN_ID3;

    private static final double TEST_DL_06 = 4000.000;

    private static final double TEST_UL_06 = 5000.000;

    private static final String TEST_DATETIME_ID_06 = TEST_DATETIME_ID_04;

    private final static int EXPECTED_IMSI_TOTAL_RESULTS = 1;

    private final static double EXPECTED_IMSI_TOTAL_DL_1 = TEST_DL_01 + TEST_DL_03;

    private final static double EXPECTED_IMSI_TOTAL_UL_1 = TEST_UL_01 + TEST_UL_03;

    private final static double TOTAL_IMSI_DURATION_1 = TEST_DURATION_01 + TEST_DURATION_03;

    private final static double EXPECTED_IMSI_DL_THROUGHPUT_1 = (double) Math
            .round(((EXPECTED_IMSI_TOTAL_DL_1 * 8.00) / (TOTAL_IMSI_DURATION_1 / 1000.00)) * 100) / 100;

    private final static double EXPECTED_IMSI_UL_THROUGHPUT_1 = (double) Math
            .round(((EXPECTED_IMSI_TOTAL_UL_1 * 8.00) / (TOTAL_IMSI_DURATION_1 / 1000.00)) * 100) / 100;

    private final static double EXPECTED_IMSI_TOTAL_SESSION_1 = 2;

    private final static int EXPECTED_IMSI_GROUP_TOTAL_RESULTS = 1;

    private final static double EXPECTED_IMSI_GROUP_TOTAL_DL_1 = TEST_DL_01 + TEST_DL_03 + TEST_DL_04 + TEST_DL_06;

    private final static double EXPECTED_IMSI_GROUP_TOTAL_UL_1 = TEST_UL_01 + TEST_UL_03 + TEST_UL_04 + TEST_UL_06;

    private final static double TOTAL_IMSI_GROUP_DURATION_1 = TEST_DURATION_01 + TEST_DURATION_03 + TEST_DURATION_04 + TEST_DURATION_06;

    private final static double EXPECTED_IMSI_GROUP_DL_THROUGHPUT_1 = (double) Math
            .round(((EXPECTED_IMSI_GROUP_TOTAL_DL_1 * 8.00) / (TOTAL_IMSI_GROUP_DURATION_1 / 1000.00)) * 100) / 100;

    private final static double EXPECTED_IMSI_GROUP_UL_THROUGHPUT_1 = (double) Math
            .round(((EXPECTED_IMSI_GROUP_TOTAL_UL_1 * 8.00) / (TOTAL_IMSI_GROUP_DURATION_1 / 1000.00)) * 100) / 100;

    private final static double EXPECTED_IMSI_GROUP_TOTAL_SESSION_1 = 4;

    @Override
    public void onSetUp() {
        try {
            super.onSetUp();
        } catch (Exception e) {
            e.printStackTrace();
        }
        networkDataVolumeResource = new NetworkDataVolumeResource();

        final Collection<String> dtRawColumns = new ArrayList<String>();
        dtRawColumns.add(DATAVOL_DL);
        dtRawColumns.add(DATAVOL_UL);
        dtRawColumns.add(APN);
        dtRawColumns.add(TAC);
        dtRawColumns.add(IMSI);
        dtRawColumns.add(MSISDN);
        dtRawColumns.add(GGSN_IPADDRESS);
        dtRawColumns.add(PDN_ID);
        dtRawColumns.add(DURATION);
        dtRawColumns.add(DATETIME_ID);
        dtRawColumns.add(DATE_ID);
        dtRawColumns.add(PDNID_GGSNIP);
        try {
            createTemporaryTable(TEMP_EVENT_E_DVTP_DT_RAW, dtRawColumns);
        } catch (Exception e) {
            e.printStackTrace();
        }

        final Collection<String> dtIMSI_15min_AggregationColumns = new ArrayList<String>();
        dtIMSI_15min_AggregationColumns.add(IMSI);
        dtIMSI_15min_AggregationColumns.add(DATAVOL_DL);
        dtIMSI_15min_AggregationColumns.add(DATAVOL_UL);
        dtIMSI_15min_AggregationColumns.add(DURATION);
        dtIMSI_15min_AggregationColumns.add(DATETIME_ID);
        try {
            createTemporaryTable(TEMP_EVENT_E_DVTP_DT_IMSI_RANK_15MIN, dtIMSI_15min_AggregationColumns);
        } catch (Exception e) {
            e.printStackTrace();
        }

        final Collection<String> dtIMSI_Day_AggregationColumns = new ArrayList<String>();
        dtIMSI_Day_AggregationColumns.add(IMSI);
        dtIMSI_Day_AggregationColumns.add(DATAVOL_DL);
        dtIMSI_Day_AggregationColumns.add(DATAVOL_UL);
        dtIMSI_Day_AggregationColumns.add(DURATION);
        dtIMSI_Day_AggregationColumns.add(DATETIME_ID);
        dtIMSI_Day_AggregationColumns.add(DATE_ID);
        try {
            createTemporaryTable(TEMP_EVENT_E_DVTP_DT_IMSI_RANK_DAY, dtIMSI_Day_AggregationColumns);
        } catch (Exception e) {
            e.printStackTrace();
        }

        final Collection<String> dtIMSIGroupColumns = new ArrayList<String>();
        dtIMSIGroupColumns.add(GROUP_NAME);
        dtIMSIGroupColumns.add(IMSI);
        try {
            createTemporaryTable(TEMP_GROUP_TYPE_E_IMSI, dtIMSIGroupColumns);
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            populateTemporaryTables();
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }
        attachDependencies(networkDataVolumeResource);
        map = new MultivaluedMapImpl();
        try {
            DummyUriInfoImpl.setUriInfo(map, networkDataVolumeResource);
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        networkDataVolumeResource.setTechPackCXCMappingService(techPackCXCMappingService);
    }

    private void insertRecordInDVTPRaw(final String apn, final long tac, final long imsi, final long msisdn, final long duration,
                                       final long ggsnIPAddr, final long pdnId, final double dl, final double ul, final String dateTime)
            throws SQLException, ParseException {
        Map<String, Object> tableValues = new HashMap<String, Object>();
        tableValues = new HashMap<String, Object>();
        tableValues.put(APN, apn);
        tableValues.put(TAC, tac);
        tableValues.put(IMSI, imsi);
        tableValues.put(MSISDN, msisdn);
        tableValues.put(DURATION, duration);
        tableValues.put(GGSN_IPADDRESS, ggsnIPAddr);
        tableValues.put(PDN_ID, pdnId);
        tableValues.put(DATAVOL_DL, dl);
        tableValues.put(DATAVOL_UL, ul);
        tableValues.put(DATETIME_ID, dateTime);
        String date = new SimpleDateFormat(DATE_FORMAT).format(new SimpleDateFormat(DATE_TIME_FORMAT).parse(dateTime));
        tableValues.put(DATE_ID, date);
        tableValues.put(PDNID_GGSNIP, pdnId + "-" + ggsnIPAddr);
        insertRow(TEMP_EVENT_E_DVTP_DT_RAW, tableValues);
    }

    private void insertRecordInIMSIGroup(final String imsiGroup, final long imsi) throws SQLException {
        Map<String, Object> tableValues = new HashMap<String, Object>();
        tableValues.put(GROUP_NAME, imsiGroup);
        tableValues.put(IMSI, imsi);
        insertRow(TEMP_GROUP_TYPE_E_IMSI, tableValues);
    }

    private void insertRecordInIMSI_15min(final long imsi, final double dl, final double ul, final long duration, final String dateTime)
            throws SQLException {
        Map<String, Object> tableValues = new HashMap<String, Object>();
        tableValues = new HashMap<String, Object>();
        tableValues.put(IMSI, imsi);
        tableValues.put(DATAVOL_DL, dl);
        tableValues.put(DATAVOL_UL, ul);
        tableValues.put(DURATION, duration);
        tableValues.put(DATETIME_ID, dateTime);
        insertRow(TEMP_EVENT_E_DVTP_DT_IMSI_RANK_15MIN, tableValues);
    }

    private void insertRecordInIMSI_Day(final long imsi, final double dl, final double ul, final long duration, final String dateTime)
            throws SQLException, ParseException {
        Map<String, Object> tableValues = new HashMap<String, Object>();
        tableValues = new HashMap<String, Object>();
        tableValues.put(IMSI, imsi);
        tableValues.put(DATAVOL_DL, dl);
        tableValues.put(DATAVOL_UL, ul);
        tableValues.put(DURATION, duration);
        tableValues.put(DATETIME_ID, dateTime);
        String date = new SimpleDateFormat(DATE_FORMAT).format(new SimpleDateFormat(DATE_TIME_FORMAT).parse(dateTime));
        tableValues.put(DATE_ID, date);
        insertRow(TEMP_EVENT_E_DVTP_DT_IMSI_RANK_DAY, tableValues);
    }

    private void populateTemporaryTables() throws SQLException, ParseException {

        insertRecordInDVTPRaw(TEST_APN_01, TEST_TAC_01, TEST_IMSI_01, TEST_MSISDN_01, TEST_DURATION_01, TEST_GGSN_IPADDRESS_01, TEST_PDN_ID_01,
                TEST_DL_01 * MEGABYTE, TEST_UL_01 * MEGABYTE, TEST_DATETIME_ID_01);
        insertRecordInDVTPRaw(TEST_APN_02, TEST_TAC_02, TEST_IMSI_02, TEST_MSISDN_02, TEST_DURATION_02, TEST_GGSN_IPADDRESS_02, TEST_PDN_ID_02,
                TEST_DL_02 * MEGABYTE, TEST_UL_02 * MEGABYTE, TEST_DATETIME_ID_02);
        insertRecordInDVTPRaw(TEST_APN_03, TEST_TAC_03, TEST_IMSI_03, TEST_MSISDN_03, TEST_DURATION_03, TEST_GGSN_IPADDRESS_03, TEST_PDN_ID_03,
                TEST_DL_03 * MEGABYTE, TEST_UL_03 * MEGABYTE, TEST_DATETIME_ID_03);
        insertRecordInDVTPRaw(TEST_APN_04, TEST_TAC_04, TEST_IMSI_04, TEST_MSISDN_04, TEST_DURATION_04, TEST_GGSN_IPADDRESS_04, TEST_PDN_ID_04,
                TEST_DL_04 * MEGABYTE, TEST_UL_04 * MEGABYTE, TEST_DATETIME_ID_04);
        insertRecordInDVTPRaw(TEST_APN_05, TEST_TAC_05, TEST_IMSI_05, TEST_MSISDN_05, TEST_DURATION_05, TEST_GGSN_IPADDRESS_05, TEST_PDN_ID_05,
                TEST_DL_05 * MEGABYTE, TEST_UL_05 * MEGABYTE, TEST_DATETIME_ID_05);
        insertRecordInDVTPRaw(TEST_APN_06, TEST_TAC_06, TEST_IMSI_06, TEST_MSISDN_06, TEST_DURATION_06, TEST_GGSN_IPADDRESS_06, TEST_PDN_ID_06,
                TEST_DL_06 * MEGABYTE, TEST_UL_06 * MEGABYTE, TEST_DATETIME_ID_06);
        insertRecordInDVTPRaw(TEST_APN_01, TEST_TAC_01, TEST_IMSI_01, TEST_MSISDN_01, TEST_DURATION_01, TEST_GGSN_IPADDRESS_01, TEST_PDN_ID_01,
                TEST_DL_01 * MEGABYTE, TEST_UL_01 * MEGABYTE, TEST_DATETIME_ID_DAY_01);
        insertRecordInDVTPRaw(TEST_APN_02, TEST_TAC_02, TEST_IMSI_02, TEST_MSISDN_02, TEST_DURATION_02, TEST_GGSN_IPADDRESS_02, TEST_PDN_ID_02,
                TEST_DL_02 * MEGABYTE, TEST_UL_02 * MEGABYTE, TEST_DATETIME_ID_DAY_01);
        insertRecordInDVTPRaw(TEST_APN_03, TEST_TAC_03, TEST_IMSI_03, TEST_MSISDN_03, TEST_DURATION_03, TEST_GGSN_IPADDRESS_03, TEST_PDN_ID_03,
                TEST_DL_03 * MEGABYTE, TEST_UL_03 * MEGABYTE, TEST_DATETIME_ID_DAY_01);
        insertRecordInDVTPRaw(TEST_APN_04, TEST_TAC_04, TEST_IMSI_04, TEST_MSISDN_04, TEST_DURATION_04, TEST_GGSN_IPADDRESS_04, TEST_PDN_ID_04,
                TEST_DL_04 * MEGABYTE, TEST_UL_04 * MEGABYTE, TEST_DATETIME_ID_DAY_01);
        insertRecordInDVTPRaw(TEST_APN_05, TEST_TAC_05, TEST_IMSI_05, TEST_MSISDN_05, TEST_DURATION_05, TEST_GGSN_IPADDRESS_05, TEST_PDN_ID_05,
                TEST_DL_05 * MEGABYTE, TEST_UL_05 * MEGABYTE, TEST_DATETIME_ID_DAY_01);
        insertRecordInDVTPRaw(TEST_APN_06, TEST_TAC_06, TEST_IMSI_06, TEST_MSISDN_06, TEST_DURATION_06, TEST_GGSN_IPADDRESS_06, TEST_PDN_ID_06,
                TEST_DL_06 * MEGABYTE, TEST_UL_06 * MEGABYTE, TEST_DATETIME_ID_DAY_01);

        insertRecordInIMSIGroup(IMSI_GROUP1, IMSI1);
        insertRecordInIMSIGroup(IMSI_GROUP1, IMSI3);

        insertRecordInIMSI_15min(TEST_IMSI_01, TEST_DL_01 * MEGABYTE, TEST_UL_01 * MEGABYTE, TEST_DURATION_01 / MILLISECONDUNIT, TEST_DATETIME_ID_01);
        insertRecordInIMSI_15min(TEST_IMSI_02, TEST_DL_02 * MEGABYTE, TEST_UL_02 * MEGABYTE, TEST_DURATION_02 / MILLISECONDUNIT, TEST_DATETIME_ID_02);
        insertRecordInIMSI_15min(TEST_IMSI_03, TEST_DL_03 * MEGABYTE, TEST_UL_03 * MEGABYTE, TEST_DURATION_03 / MILLISECONDUNIT, TEST_DATETIME_ID_03);
        insertRecordInIMSI_15min(TEST_IMSI_04, TEST_DL_04 * MEGABYTE, TEST_UL_04 * MEGABYTE, TEST_DURATION_04 / MILLISECONDUNIT, TEST_DATETIME_ID_01);
        insertRecordInIMSI_15min(TEST_IMSI_05, TEST_DL_05 * MEGABYTE, TEST_UL_05 * MEGABYTE, TEST_DURATION_05 / MILLISECONDUNIT, TEST_DATETIME_ID_02);
        insertRecordInIMSI_15min(TEST_IMSI_06, TEST_DL_06 * MEGABYTE, TEST_UL_06 * MEGABYTE, TEST_DURATION_06 / MILLISECONDUNIT, TEST_DATETIME_ID_03);

        insertRecordInIMSI_Day(TEST_IMSI_01, TEST_DL_01 * MEGABYTE, TEST_UL_01 * MEGABYTE, TEST_DURATION_01 / MILLISECONDUNIT,
                TEST_DATETIME_ID_DAY_01);
        insertRecordInIMSI_Day(TEST_IMSI_02, TEST_DL_02 * MEGABYTE, TEST_UL_02 * MEGABYTE, TEST_DURATION_02 / MILLISECONDUNIT,
                TEST_DATETIME_ID_DAY_01);
        insertRecordInIMSI_Day(TEST_IMSI_03, TEST_DL_03 * MEGABYTE, TEST_UL_03 * MEGABYTE, TEST_DURATION_03 / MILLISECONDUNIT,
                TEST_DATETIME_ID_DAY_01);
        insertRecordInIMSI_Day(TEST_IMSI_04, TEST_DL_04 * MEGABYTE, TEST_UL_04 * MEGABYTE, TEST_DURATION_04 / MILLISECONDUNIT,
                TEST_DATETIME_ID_DAY_01);
        insertRecordInIMSI_Day(TEST_IMSI_05, TEST_DL_05 * MEGABYTE, TEST_UL_05 * MEGABYTE, TEST_DURATION_05 / MILLISECONDUNIT,
                TEST_DATETIME_ID_DAY_01);
        insertRecordInIMSI_Day(TEST_IMSI_06, TEST_DL_06 * MEGABYTE, TEST_UL_06 * MEGABYTE, TEST_DURATION_06 / MILLISECONDUNIT,
                TEST_DATETIME_ID_DAY_01);

    }

    private void validateResults(final String jsonResult, final String entityType) {
        jsonAssertUtils.assertJSONSucceeds(jsonResult);

        List<SubscriberDataVolumeResult> subscriberDataVolumeResults;
        try {
            subscriberDataVolumeResults = getTranslator().translateResult(jsonResult, SubscriberDataVolumeResult.class);

            if (entityType.equals(TYPE_IMSI)) {
                assertThat(subscriberDataVolumeResults.size(), is(EXPECTED_IMSI_TOTAL_RESULTS));
            } else if (entityType.equals(TYPE_IMSI_GROUP)) {
                assertThat(subscriberDataVolumeResults.size(), is(EXPECTED_IMSI_GROUP_TOTAL_RESULTS));
            }

            for (final SubscriberDataVolumeResult subscriberDataVolumeResult : subscriberDataVolumeResults) {
                validateResult(subscriberDataVolumeResult, entityType);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void validateResult(final SubscriberDataVolumeResult subscriberDataVolumeResult, final String entityType) {

        if (entityType.equals(TYPE_IMSI)) {
            getValidateResult(subscriberDataVolumeResult, EXPECTED_IMSI_TOTAL_DL_1 * KB_MULTIPLIER, EXPECTED_IMSI_TOTAL_UL_1 * KB_MULTIPLIER,
                    EXPECTED_IMSI_DL_THROUGHPUT_1 * KB_MULTIPLIER, EXPECTED_IMSI_UL_THROUGHPUT_1 * KB_MULTIPLIER, EXPECTED_IMSI_TOTAL_SESSION_1);
        } else if (entityType.equals(TYPE_IMSI_GROUP)) {
            getValidateResult(subscriberDataVolumeResult, EXPECTED_IMSI_GROUP_TOTAL_DL_1 * KB_MULTIPLIER, EXPECTED_IMSI_GROUP_TOTAL_UL_1
                    * KB_MULTIPLIER, EXPECTED_IMSI_GROUP_DL_THROUGHPUT_1 * KB_MULTIPLIER, EXPECTED_IMSI_GROUP_UL_THROUGHPUT_1 * KB_MULTIPLIER,
                    EXPECTED_IMSI_GROUP_TOTAL_SESSION_1);
        }
    }

    @Test
    public void testDrillDownByIMSI_1hour() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForIMSI(String.valueOf(IMSI1), CHART_PARAM, ONE_HOUR,
                TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        final String jsonResult = networkDataVolumeResource.getNetworkDataVolumeResults(MESSAGE, map);
        validateResults(jsonResult, TYPE_IMSI);
    }

    @Test
    public void testDrillDownByIMSI_2hours() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForIMSI(String.valueOf(IMSI1), GRID_PARAM, TWO_HOURS,
                TIME_ZONE_OFFSET_OF_MINUS_EIGHT_HOUR);
        final String jsonResult = networkDataVolumeResource.getNetworkDataVolumeResults(MESSAGE, map);
        validateResults(jsonResult, TYPE_IMSI);
    }

    @Test
    public void testDrillDownByIMSI_6hours() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForIMSI(String.valueOf(IMSI1), CHART_PARAM, SIX_HOURS,
                TIME_ZONE_OFFSET_OF_MINUS_EIGHT_HOUR);
        final String jsonResult = networkDataVolumeResource.getNetworkDataVolumeResults(MESSAGE, map);
        validateResults(jsonResult, TYPE_IMSI);
    }

    @Test
    public void testDrillDownByIMSI_12hours() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForIMSI(String.valueOf(IMSI1), CHART_PARAM, TWELVE_HOURS,
                TIME_ZONE_OFFSET_OF_MINUS_EIGHT_HOUR);
        final String jsonResult = networkDataVolumeResource.getNetworkDataVolumeResults(MESSAGE, map);
        validateResults(jsonResult, TYPE_IMSI);
    }

    @Test
    public void testDrillDownByIMSI_1day() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForIMSI(String.valueOf(IMSI1), CHART_PARAM, ONE_DAY,
                TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        final String jsonResult = networkDataVolumeResource.getNetworkDataVolumeResults(MESSAGE, map);
        validateResults(jsonResult, TYPE_IMSI);
    }

    @Test
    public void testDrillDownByIMSI_1week() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForIMSI(String.valueOf(IMSI1), CHART_PARAM, ONE_WEEK,
                TIME_ZONE_OFFSET_OF_MINUS_EIGHT_HOUR);
        final String jsonResult = networkDataVolumeResource.getNetworkDataVolumeResults(MESSAGE, map);
        validateResults(jsonResult, TYPE_IMSI);
    }

    @Test
    public void testDrillDownByIMSIGroup_1hour() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForGroup(TYPE_IMSI, IMSI_GROUP1, CHART_PARAM, ONE_HOUR,
                TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        final String jsonResult = networkDataVolumeResource.getNetworkDataVolumeResults(MESSAGE, map);
        validateResults(jsonResult, TYPE_IMSI_GROUP);
    }

    @Test
    public void testDrillDownByIMSIGroup_2hours() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForGroup(TYPE_IMSI, IMSI_GROUP1, GRID_PARAM, TWO_HOURS,
                TIME_ZONE_OFFSET_OF_MINUS_EIGHT_HOUR);
        final String jsonResult = networkDataVolumeResource.getNetworkDataVolumeResults(MESSAGE, map);
        validateResults(jsonResult, TYPE_IMSI_GROUP);
    }

    @Test
    public void testDrillDownByIMSIGroup_6hours() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForGroup(TYPE_IMSI, IMSI_GROUP1, CHART_PARAM, SIX_HOURS,
                TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        final String jsonResult = networkDataVolumeResource.getNetworkDataVolumeResults(MESSAGE, map);
        validateResults(jsonResult, TYPE_IMSI_GROUP);
    }

    @Test
    public void testDrillDownByIMSIGroup_12hours() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForGroup(TYPE_IMSI, IMSI_GROUP1, GRID_PARAM, TWELVE_HOURS,
                TIME_ZONE_OFFSET_OF_MINUS_EIGHT_HOUR);
        final String jsonResult = networkDataVolumeResource.getNetworkDataVolumeResults(MESSAGE, map);
        validateResults(jsonResult, TYPE_IMSI_GROUP);
    }

    @Test
    public void testDrillDownByIMSIGroup_1day() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForGroup(TYPE_IMSI, IMSI_GROUP1, CHART_PARAM, ONE_DAY,
                TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        final String jsonResult = networkDataVolumeResource.getNetworkDataVolumeResults(MESSAGE, map);
        validateResults(jsonResult, TYPE_IMSI_GROUP);
    }

    @Test
    public void testDrillDownByIMSIGroup_1week() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForGroup(TYPE_IMSI, IMSI_GROUP1, GRID_PARAM, ONE_WEEK,
                TIME_ZONE_OFFSET_OF_MINUS_EIGHT_HOUR);
        final String jsonResult = networkDataVolumeResource.getNetworkDataVolumeResults(MESSAGE, map);
        validateResults(jsonResult, TYPE_IMSI_GROUP);
    }

    @Test
    public void testDrillDownByMSISDN_1hour() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForMSISDN(String.valueOf(MSISDN2), CHART_PARAM, ONE_HOUR,
                TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        final String jsonResult = networkDataVolumeResource.getNetworkDataVolumeResults(MESSAGE, map);
        validateResults(jsonResult, TYPE_MSISDN);
    }

    @Test
    public void testDrillDownByMSISDN_2hours() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForMSISDN(String.valueOf(MSISDN2), GRID_PARAM, TWO_HOURS,
                TIME_ZONE_OFFSET_OF_MINUS_EIGHT_HOUR);
        final String jsonResult = networkDataVolumeResource.getNetworkDataVolumeResults(MESSAGE, map);
        validateResults(jsonResult, TYPE_MSISDN);
    }

    @Test
    public void testDrillDownByMSISDN_6hours() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForMSISDN(String.valueOf(MSISDN2), CHART_PARAM, SIX_HOURS,
                TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        final String jsonResult = networkDataVolumeResource.getNetworkDataVolumeResults(MESSAGE, map);
        validateResults(jsonResult, TYPE_MSISDN);
    }

    @Test
    public void testDrillDownByMSISDN_12hours() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForMSISDN(String.valueOf(MSISDN2), GRID_PARAM, TWELVE_HOURS,
                TIME_ZONE_OFFSET_OF_MINUS_EIGHT_HOUR);
        final String jsonResult = networkDataVolumeResource.getNetworkDataVolumeResults(MESSAGE, map);
        validateResults(jsonResult, TYPE_MSISDN);
    }

    @Test
    public void testDrillDownByMSISDN_1day() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForMSISDN(String.valueOf(MSISDN2), CHART_PARAM, ONE_DAY,
                TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        final String jsonResult = networkDataVolumeResource.getNetworkDataVolumeResults(MESSAGE, map);
        validateResults(jsonResult, TYPE_MSISDN);
    }

    @Test
    public void testDrillDownByMSISDN_1week() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForMSISDN(String.valueOf(MSISDN2), GRID_PARAM, ONE_WEEK,
                TIME_ZONE_OFFSET_OF_MINUS_EIGHT_HOUR);
        final String jsonResult = networkDataVolumeResource.getNetworkDataVolumeResults(MESSAGE, map);
        validateResults(jsonResult, TYPE_MSISDN);
    }

    private void getValidateResult(final SubscriberDataVolumeResult subscriberDataVolumeResult, final double total_dl, final double total_ul,
                                   final double dl_throughput, final double ul_throughput, final double total_session) {
        assertThat(subscriberDataVolumeResult.getDownlinkVolume(), is(total_dl));
        assertThat(subscriberDataVolumeResult.getUplinkVolume(), is(total_ul));
        assertThat(subscriberDataVolumeResult.getDownlinkThroughput(), is(dl_throughput));
        assertThat(subscriberDataVolumeResult.getUplinkThroughput(), is(ul_throughput));
        assertThat(subscriberDataVolumeResult.getNumberOfSession(), is(total_session));
    }
}
