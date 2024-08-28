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
import com.ericsson.eniq.events.server.test.queryresults.datavolume.NetworkDataVolumeResult;
import com.ericsson.eniq.events.server.test.stubs.DummyUriInfoImpl;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;
import com.sun.jersey.core.util.MultivaluedMapImpl;

public class NetworkDataVolumeWithPreparedTablesTest extends TestsWithTemporaryTablesBaseTestCase<NetworkDataVolumeResult> {
    private MultivaluedMap<String, String> map;

    private NetworkDataVolumeResource networkDataVolumeResource;

    private static final long MEGABYTE = 1024L * 1024L;

    private static final String MESSAGE = "CANCEL_REQUEST_NOT_SUPPORTED";

    private static final String APN_GROUP1 = "apn_group1";

    private static final String TAC_GROUP1 = "tac_group1";

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

    private static final String TEST_DATETIME_ID_DAY_02 = TEST_DATETIME_ID_DAY_01;

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

    private static final String TEST_DATETIME_ID_DAY_03 = TEST_DATETIME_ID_DAY_01;

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

    private final static int EXPECTED_APN_TOTAL_RESULTS = 1;

    private final static double EXPECTED_APN_TOTAL_DL_1 = TEST_DL_01 + TEST_DL_02 + TEST_DL_03;

    private final static double EXPECTED_APN_TOTAL_UL_1 = TEST_UL_01 + TEST_UL_02 + TEST_UL_03;

    private final static double TOTAL_APN_DURATION_1 = TEST_DURATION_01 + TEST_DURATION_02 + TEST_DURATION_03;

    private final static double EXPECTED_APN_TOTAL_SUBSCRIBER_1 = 2;

    private final static double EXPECTED_APN_TOTAL_SESSION_1 = 2;

    private final static int EXPECTED_TAC_TOTAL_RESULTS = 1;

    private final static double EXPECTED_TAC_TOTAL_DL_1 = TEST_DL_02 + TEST_DL_03;

    private final static double EXPECTED_TAC_TOTAL_UL_1 = TEST_UL_02 + TEST_UL_03;

    private final static double TOTAL_TAC_DURATION_1 = TEST_DURATION_02 + TEST_DURATION_03;

    private final static double EXPECTED_TAC_TOTAL_SUBSCRIBER_1 = 2;

    private final static double EXPECTED_TAC_TOTAL_SESSION_1 = 2;

    private final static int EXPECTED_MSISDN_TOTAL_RESULTS = 1;

    private final static double EXPECTED_MSISDN_TOTAL_DL_1 = TEST_DL_02 + TEST_DL_03;

    private final static double EXPECTED_MSISDN_TOTAL_UL_1 = TEST_UL_02 + TEST_UL_03;

    private final static double TOTAL_MSISDN_DURATION_1 = TEST_DURATION_02 + TEST_DURATION_03;

    private final static double EXPECTED_MSISDN_TOTAL_SESSION_1 = 2;

    private final static int EXPECTED_APN_GROUP_TOTAL_RESULTS = 1;

    private final static double EXPECTED_APN_GROUP_TOTAL_DL_1 = TEST_DL_01 + TEST_DL_02 + TEST_DL_03 + TEST_DL_04 + TEST_DL_05 + TEST_DL_06;

    private final static double EXPECTED_APN_GROUP_TOTAL_UL_1 = TEST_UL_01 + TEST_UL_02 + TEST_UL_03 + TEST_UL_04 + +TEST_UL_05 + TEST_UL_06;

    private final static double TOTAL_APN_GROUP_DURATION_1 = TEST_DURATION_01 + TEST_DURATION_02 + TEST_DURATION_03 + TEST_DURATION_04
            + TEST_DURATION_05 + TEST_DURATION_06;

    private final static double EXPECTED_APN_GROUP_TOTAL_SESSION_1 = 4;

    private static final double EXPECTED_APN_GROUP_TOTAL_SUBSCRIBER_1 = 4;

    private final static int EXPECTED_TAC_GROUP_TOTAL_RESULTS = 1;

    private final static double EXPECTED_TAC_GROUP_TOTAL_DL_1 = TEST_DL_02 + TEST_DL_03 + TEST_DL_05 + TEST_DL_06;

    private final static double EXPECTED_TAC_GROUP_TOTAL_UL_1 = TEST_UL_02 + TEST_UL_03 + TEST_UL_05 + TEST_UL_06;

    private final static double TOTAL_TAC_GROUP_DURATION_1 = TEST_DURATION_03 + TEST_DURATION_02 + TEST_DURATION_05 + TEST_DURATION_06;

    private final static double EXPECTED_TAC_GROUP_TOTAL_SESSION_1 = 4;

    private static final double EXPECTED_TAC_GROUP_TOTAL_SUBSCRIBER_1 = 4;

    private final static int EXPECTED_NETWORK_TOTAL_RESULTS = 1;

    private final static double EXPECTED_NETWORK_TOTAL_DL_1 = TEST_DL_01 + TEST_DL_02 + TEST_DL_03 + TEST_DL_04 + TEST_DL_05 + TEST_DL_06;

    private final static double EXPECTED_NETWORK_TOTAL_UL_1 = TEST_UL_01 + TEST_UL_02 + TEST_UL_03 + TEST_UL_04 + +TEST_UL_05 + TEST_UL_06;

    private final static double TOTAL_NETWORK_DURATION_1 = TEST_DURATION_01 + TEST_DURATION_02 + TEST_DURATION_03 + TEST_DURATION_04
            + TEST_DURATION_05 + TEST_DURATION_06;

    private final static double EXPECTED_NETWORK_TOTAL_SUBSCRIBER_1 = 4;

    private final static double EXPECTED_NETWORK_TOTAL_SESSION_1 = 4;

    @Override
    public void onSetUp() {
        try {
            super.onSetUp();
        } catch (Exception e1) {
            e1.printStackTrace();
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

        final Collection<String> dtAPNGroupColumns = new ArrayList<String>();
        dtAPNGroupColumns.add(GROUP_NAME);
        dtAPNGroupColumns.add(APN);
        try {
            createTemporaryTable(TEMP_GROUP_TYPE_E_APN, dtAPNGroupColumns);
        } catch (Exception e) {
            e.printStackTrace();
        }

        final Collection<String> dtAPN_15min_AggregationColumns = new ArrayList<String>();
        dtAPN_15min_AggregationColumns.add(APN);
        dtAPN_15min_AggregationColumns.add(DATAVOL_DL);
        dtAPN_15min_AggregationColumns.add(DATAVOL_UL);
        dtAPN_15min_AggregationColumns.add(DURATION);
        dtAPN_15min_AggregationColumns.add(DATETIME_ID);
        try {
            createTemporaryTable(TEMP_EVENT_E_DVTP_DT_APN_15MIN, dtAPN_15min_AggregationColumns);
        } catch (Exception e) {
            e.printStackTrace();
        }

        final Collection<String> dtAPN_Day_AggregationColumns = new ArrayList<String>();
        dtAPN_Day_AggregationColumns.add(APN);
        dtAPN_Day_AggregationColumns.add(DATAVOL_DL);
        dtAPN_Day_AggregationColumns.add(DATAVOL_UL);
        dtAPN_Day_AggregationColumns.add(DURATION);
        dtAPN_Day_AggregationColumns.add(DATETIME_ID);
        dtAPN_Day_AggregationColumns.add(DATE_ID);

        try {
            createTemporaryTable(TEMP_EVENT_E_DVTP_DT_APN_DAY, dtAPN_Day_AggregationColumns);
        } catch (Exception e) {
            e.printStackTrace();
        }

        final Collection<String> dtTAC_15min_AggregationColumns = new ArrayList<String>();
        dtTAC_15min_AggregationColumns.add(TAC);
        dtTAC_15min_AggregationColumns.add(DATAVOL_DL);
        dtTAC_15min_AggregationColumns.add(DATAVOL_UL);
        dtTAC_15min_AggregationColumns.add(DURATION);
        dtTAC_15min_AggregationColumns.add(DATETIME_ID);
        try {
            createTemporaryTable(TEMP_EVENT_E_DVTP_DT_TERM_15MIN, dtTAC_15min_AggregationColumns);
        } catch (Exception e) {
            e.printStackTrace();
        }

        final Collection<String> dtTAC_Day_AggregationColumns = new ArrayList<String>();
        dtTAC_Day_AggregationColumns.add(TAC);
        dtTAC_Day_AggregationColumns.add(DATAVOL_DL);
        dtTAC_Day_AggregationColumns.add(DATAVOL_UL);
        dtTAC_Day_AggregationColumns.add(DURATION);
        dtTAC_Day_AggregationColumns.add(DATETIME_ID);
        dtTAC_Day_AggregationColumns.add(DATE_ID);
        try {
            createTemporaryTable(TEMP_EVENT_E_DVTP_DT_TERM_DAY, dtTAC_Day_AggregationColumns);
        } catch (Exception e) {
            e.printStackTrace();
        }

        final Collection<String> dtNetwork_15min_AggregationColumns = new ArrayList<String>();
        dtNetwork_15min_AggregationColumns.add(DATAVOL_DL);
        dtNetwork_15min_AggregationColumns.add(DATAVOL_UL);
        dtNetwork_15min_AggregationColumns.add(DURATION);
        dtNetwork_15min_AggregationColumns.add(DATETIME_ID);
        try {
            createTemporaryTable(TEMP_EVENT_E_DVTP_DT_NETWORK_15MIN, dtNetwork_15min_AggregationColumns);
        } catch (Exception e) {
            e.printStackTrace();
        }

        final Collection<String> dtNetwork_Day_AggregationColumns = new ArrayList<String>();
        dtNetwork_Day_AggregationColumns.add(DATAVOL_DL);
        dtNetwork_Day_AggregationColumns.add(DATAVOL_UL);
        dtNetwork_Day_AggregationColumns.add(DURATION);
        dtNetwork_Day_AggregationColumns.add(DATETIME_ID);
        dtNetwork_Day_AggregationColumns.add(DATE_ID);
        try {
            createTemporaryTable(TEMP_EVENT_E_DVTP_DT_NETWORK_DAY, dtNetwork_Day_AggregationColumns);
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

    private void insertRecordInAPNGroup(final String apnGroup, final String apn) throws SQLException {
        Map<String, Object> tableValues = new HashMap<String, Object>();
        tableValues.put(GROUP_NAME, apnGroup);
        tableValues.put(APN, apn);
        insertRow(TEMP_GROUP_TYPE_E_APN, tableValues);
    }

    private void insertRecordInTACGroup(final String tacGroup, final long tac) throws SQLException {
        Map<String, Object> tableValues = new HashMap<String, Object>();
        tableValues.put(GROUP_NAME, tacGroup);
        tableValues.put(TAC, tac);
        insertRow(TEMP_GROUP_TYPE_E_TAC, tableValues);
    }

    private void insertRecordInAPN_15min_Agg(final String apn, final double dl, final double ul, final long duration, final String dateTime)
            throws SQLException {
        Map<String, Object> tableValues = new HashMap<String, Object>();
        tableValues = new HashMap<String, Object>();
        tableValues.put(APN, apn);
        tableValues.put(DATAVOL_DL, dl);
        tableValues.put(DATAVOL_UL, ul);
        tableValues.put(DURATION, duration);
        tableValues.put(DATETIME_ID, dateTime);
        insertRow(TEMP_EVENT_E_DVTP_DT_APN_15MIN, tableValues);
    }

    private void insertRecordInAPN_Day_Agg(final String apn, final double dl, final double ul, final long duration, final String dateTime)
            throws SQLException, ParseException {
        Map<String, Object> tableValues = new HashMap<String, Object>();
        tableValues = new HashMap<String, Object>();
        tableValues.put(APN, apn);
        tableValues.put(DATAVOL_DL, dl);
        tableValues.put(DATAVOL_UL, ul);
        tableValues.put(DURATION, duration);
        tableValues.put(DATETIME_ID, dateTime);
        String date = new SimpleDateFormat(DATE_FORMAT).format(new SimpleDateFormat(DATE_TIME_FORMAT).parse(dateTime));
        tableValues.put(DATE_ID, date);
        insertRow(TEMP_EVENT_E_DVTP_DT_APN_DAY, tableValues);
    }

    private void insertRecordInTAC_15min_Agg(final long tac, final double dl, final double ul, final long duration, final String dateTime)
            throws SQLException {
        Map<String, Object> tableValues = new HashMap<String, Object>();
        tableValues = new HashMap<String, Object>();
        tableValues.put(TAC, tac);
        tableValues.put(DATAVOL_DL, dl);
        tableValues.put(DATAVOL_UL, ul);
        tableValues.put(DURATION, duration);
        tableValues.put(DATETIME_ID, dateTime);
        insertRow(TEMP_EVENT_E_DVTP_DT_TERM_15MIN, tableValues);
    }

    private void insertRecordInTAC_Day_Agg(final long tac, final double dl, final double ul, final long duration, final String dateTime)
            throws SQLException, ParseException {
        Map<String, Object> tableValues = new HashMap<String, Object>();
        tableValues = new HashMap<String, Object>();
        tableValues.put(TAC, tac);
        tableValues.put(DATAVOL_DL, dl);
        tableValues.put(DATAVOL_UL, ul);
        tableValues.put(DURATION, duration);
        tableValues.put(DATETIME_ID, dateTime);
        String date = new SimpleDateFormat(DATE_FORMAT).format(new SimpleDateFormat(DATE_TIME_FORMAT).parse(dateTime));
        tableValues.put(DATE_ID, date);
        insertRow(TEMP_EVENT_E_DVTP_DT_TERM_DAY, tableValues);
    }

    private void insertRecordInNetwork_15min_Agg(final double dl, final double ul, final long duration, final String dateTime) throws SQLException {
        Map<String, Object> tableValues = new HashMap<String, Object>();
        tableValues = new HashMap<String, Object>();
        tableValues.put(DATAVOL_DL, dl);
        tableValues.put(DATAVOL_UL, ul);
        tableValues.put(DURATION, duration);
        tableValues.put(DATETIME_ID, dateTime);
        insertRow(TEMP_EVENT_E_DVTP_DT_NETWORK_15MIN, tableValues);
    }

    private void insertRecordInNetwork_Day_Agg(final double dl, final double ul, final long duration, final String dateTime) throws SQLException,
            ParseException {
        Map<String, Object> tableValues = new HashMap<String, Object>();
        tableValues = new HashMap<String, Object>();
        tableValues.put(DATAVOL_DL, dl);
        tableValues.put(DATAVOL_UL, ul);
        tableValues.put(DURATION, duration);
        tableValues.put(DATETIME_ID, dateTime);
        String date = new SimpleDateFormat(DATE_FORMAT).format(new SimpleDateFormat(DATE_TIME_FORMAT).parse(dateTime));
        tableValues.put(DATE_ID, date);
        insertRow(TEMP_EVENT_E_DVTP_DT_NETWORK_DAY, tableValues);
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
                TEST_DL_02 * MEGABYTE, TEST_UL_02 * MEGABYTE, TEST_DATETIME_ID_DAY_02);
        insertRecordInDVTPRaw(TEST_APN_03, TEST_TAC_03, TEST_IMSI_03, TEST_MSISDN_03, TEST_DURATION_03, TEST_GGSN_IPADDRESS_03, TEST_PDN_ID_03,
                TEST_DL_03 * MEGABYTE, TEST_UL_03 * MEGABYTE, TEST_DATETIME_ID_DAY_03);
        insertRecordInDVTPRaw(TEST_APN_04, TEST_TAC_04, TEST_IMSI_04, TEST_MSISDN_04, TEST_DURATION_04, TEST_GGSN_IPADDRESS_04, TEST_PDN_ID_04,
                TEST_DL_04 * MEGABYTE, TEST_UL_04 * MEGABYTE, TEST_DATETIME_ID_DAY_01);
        insertRecordInDVTPRaw(TEST_APN_05, TEST_TAC_05, TEST_IMSI_05, TEST_MSISDN_05, TEST_DURATION_05, TEST_GGSN_IPADDRESS_05, TEST_PDN_ID_05,
                TEST_DL_05 * MEGABYTE, TEST_UL_05 * MEGABYTE, TEST_DATETIME_ID_DAY_02);
        insertRecordInDVTPRaw(TEST_APN_06, TEST_TAC_06, TEST_IMSI_06, TEST_MSISDN_06, TEST_DURATION_06, TEST_GGSN_IPADDRESS_06, TEST_PDN_ID_06,
                TEST_DL_06 * MEGABYTE, TEST_UL_06 * MEGABYTE, TEST_DATETIME_ID_DAY_03);

        insertRecordInAPNGroup(APN_GROUP1, APN1);
        insertRecordInAPNGroup(APN_GROUP1, APN3);

        insertRecordInTACGroup(TAC_GROUP1, TAC2);
        insertRecordInTACGroup(TAC_GROUP1, TAC4);

        insertRecordInAPN_15min_Agg(TEST_APN_01, TEST_DL_01 * MEGABYTE, TEST_UL_01 * MEGABYTE, TEST_DURATION_01, TEST_DATETIME_ID_01);
        insertRecordInAPN_15min_Agg(TEST_APN_02, TEST_DL_02 * MEGABYTE, TEST_UL_02 * MEGABYTE, TEST_DURATION_02, TEST_DATETIME_ID_02);
        insertRecordInAPN_15min_Agg(TEST_APN_03, TEST_DL_03 * MEGABYTE, TEST_UL_03 * MEGABYTE, TEST_DURATION_03, TEST_DATETIME_ID_03);
        insertRecordInAPN_15min_Agg(TEST_APN_04, TEST_DL_04 * MEGABYTE, TEST_UL_04 * MEGABYTE, TEST_DURATION_04, TEST_DATETIME_ID_01);
        insertRecordInAPN_15min_Agg(TEST_APN_05, TEST_DL_05 * MEGABYTE, TEST_UL_05 * MEGABYTE, TEST_DURATION_05, TEST_DATETIME_ID_02);
        insertRecordInAPN_15min_Agg(TEST_APN_06, TEST_DL_06 * MEGABYTE, TEST_UL_06 * MEGABYTE, TEST_DURATION_06, TEST_DATETIME_ID_03);

        insertRecordInAPN_Day_Agg(TEST_APN_01, TEST_DL_01 * MEGABYTE, TEST_UL_01 * MEGABYTE, TEST_DURATION_01, TEST_DATETIME_ID_DAY_01);
        insertRecordInAPN_Day_Agg(TEST_APN_02, TEST_DL_02 * MEGABYTE, TEST_UL_02 * MEGABYTE, TEST_DURATION_02, TEST_DATETIME_ID_DAY_02);
        insertRecordInAPN_Day_Agg(TEST_APN_03, TEST_DL_03 * MEGABYTE, TEST_UL_03 * MEGABYTE, TEST_DURATION_03, TEST_DATETIME_ID_DAY_03);
        insertRecordInAPN_Day_Agg(TEST_APN_04, TEST_DL_04 * MEGABYTE, TEST_UL_04 * MEGABYTE, TEST_DURATION_04, TEST_DATETIME_ID_DAY_01);
        insertRecordInAPN_Day_Agg(TEST_APN_05, TEST_DL_05 * MEGABYTE, TEST_UL_05 * MEGABYTE, TEST_DURATION_05, TEST_DATETIME_ID_DAY_02);
        insertRecordInAPN_Day_Agg(TEST_APN_06, TEST_DL_06 * MEGABYTE, TEST_UL_06 * MEGABYTE, TEST_DURATION_06, TEST_DATETIME_ID_DAY_03);

        insertRecordInTAC_15min_Agg(TEST_TAC_01, TEST_DL_01 * MEGABYTE, TEST_UL_01 * MEGABYTE, TEST_DURATION_01, TEST_DATETIME_ID_01);
        insertRecordInTAC_15min_Agg(TEST_TAC_02, TEST_DL_02 * MEGABYTE, TEST_UL_02 * MEGABYTE, TEST_DURATION_02, TEST_DATETIME_ID_02);
        insertRecordInTAC_15min_Agg(TEST_TAC_03, TEST_DL_03 * MEGABYTE, TEST_UL_03 * MEGABYTE, TEST_DURATION_03, TEST_DATETIME_ID_03);
        insertRecordInTAC_15min_Agg(TEST_TAC_04, TEST_DL_04 * MEGABYTE, TEST_UL_04 * MEGABYTE, TEST_DURATION_04, TEST_DATETIME_ID_01);
        insertRecordInTAC_15min_Agg(TEST_TAC_05, TEST_DL_05 * MEGABYTE, TEST_UL_05 * MEGABYTE, TEST_DURATION_05, TEST_DATETIME_ID_02);
        insertRecordInTAC_15min_Agg(TEST_TAC_06, TEST_DL_06 * MEGABYTE, TEST_UL_06 * MEGABYTE, TEST_DURATION_06, TEST_DATETIME_ID_03);

        insertRecordInTAC_Day_Agg(TEST_TAC_01, TEST_DL_01 * MEGABYTE, TEST_UL_01 * MEGABYTE, TEST_DURATION_01, TEST_DATETIME_ID_DAY_01);
        insertRecordInTAC_Day_Agg(TEST_TAC_02, TEST_DL_02 * MEGABYTE, TEST_UL_02 * MEGABYTE, TEST_DURATION_02, TEST_DATETIME_ID_DAY_02);
        insertRecordInTAC_Day_Agg(TEST_TAC_03, TEST_DL_03 * MEGABYTE, TEST_UL_03 * MEGABYTE, TEST_DURATION_03, TEST_DATETIME_ID_DAY_03);
        insertRecordInTAC_Day_Agg(TEST_TAC_04, TEST_DL_04 * MEGABYTE, TEST_UL_04 * MEGABYTE, TEST_DURATION_04, TEST_DATETIME_ID_DAY_01);
        insertRecordInTAC_Day_Agg(TEST_TAC_05, TEST_DL_05 * MEGABYTE, TEST_UL_05 * MEGABYTE, TEST_DURATION_05, TEST_DATETIME_ID_DAY_02);
        insertRecordInTAC_Day_Agg(TEST_TAC_06, TEST_DL_06 * MEGABYTE, TEST_UL_06 * MEGABYTE, TEST_DURATION_06, TEST_DATETIME_ID_DAY_03);

        insertRecordInNetwork_15min_Agg(TEST_DL_01 * MEGABYTE, TEST_UL_01 * MEGABYTE, TEST_DURATION_01, TEST_DATETIME_ID_01);
        insertRecordInNetwork_15min_Agg(TEST_DL_02 * MEGABYTE, TEST_UL_02 * MEGABYTE, TEST_DURATION_02, TEST_DATETIME_ID_02);
        insertRecordInNetwork_15min_Agg(TEST_DL_03 * MEGABYTE, TEST_UL_03 * MEGABYTE, TEST_DURATION_03, TEST_DATETIME_ID_03);
        insertRecordInNetwork_15min_Agg(TEST_DL_04 * MEGABYTE, TEST_UL_04 * MEGABYTE, TEST_DURATION_04, TEST_DATETIME_ID_01);
        insertRecordInNetwork_15min_Agg(TEST_DL_05 * MEGABYTE, TEST_UL_05 * MEGABYTE, TEST_DURATION_05, TEST_DATETIME_ID_02);
        insertRecordInNetwork_15min_Agg(TEST_DL_06 * MEGABYTE, TEST_UL_06 * MEGABYTE, TEST_DURATION_06, TEST_DATETIME_ID_03);

        insertRecordInNetwork_Day_Agg(TEST_DL_01 * MEGABYTE, TEST_UL_01 * MEGABYTE, TEST_DURATION_01, TEST_DATETIME_ID_DAY_01);
        insertRecordInNetwork_Day_Agg(TEST_DL_02 * MEGABYTE, TEST_UL_02 * MEGABYTE, TEST_DURATION_02, TEST_DATETIME_ID_DAY_02);
        insertRecordInNetwork_Day_Agg(TEST_DL_03 * MEGABYTE, TEST_UL_03 * MEGABYTE, TEST_DURATION_03, TEST_DATETIME_ID_DAY_03);
        insertRecordInNetwork_Day_Agg(TEST_DL_04 * MEGABYTE, TEST_UL_04 * MEGABYTE, TEST_DURATION_04, TEST_DATETIME_ID_DAY_01);
        insertRecordInNetwork_Day_Agg(TEST_DL_05 * MEGABYTE, TEST_UL_05 * MEGABYTE, TEST_DURATION_05, TEST_DATETIME_ID_DAY_02);
        insertRecordInNetwork_Day_Agg(TEST_DL_06 * MEGABYTE, TEST_UL_06 * MEGABYTE, TEST_DURATION_06, TEST_DATETIME_ID_DAY_03);

    }

    private void validateResults(final String jsonResult, final String entityType, final String view) {
        jsonAssertUtils.assertJSONSucceeds(jsonResult);

        List<NetworkDataVolumeResult> networkDataVolumeResults;
        try {
            networkDataVolumeResults = getTranslator().translateResult(jsonResult, NetworkDataVolumeResult.class);

            if (entityType.equals(TYPE_APN)) {
                assertThat(networkDataVolumeResults.size(), is(EXPECTED_APN_TOTAL_RESULTS));
            } else if (entityType.equals(TYPE_TAC)) {
                assertThat(networkDataVolumeResults.size(), is(EXPECTED_TAC_TOTAL_RESULTS));
            } else if (entityType.equals(TYPE_MSISDN)) {
                assertThat(networkDataVolumeResults.size(), is(EXPECTED_MSISDN_TOTAL_RESULTS));
            } else if (entityType.equals(TYPE_TAC_GROUP)) {
                assertThat(networkDataVolumeResults.size(), is(EXPECTED_TAC_GROUP_TOTAL_RESULTS));
            } else if (entityType.equals(TYPE_APN_GROUP)) {
                assertThat(networkDataVolumeResults.size(), is(EXPECTED_APN_GROUP_TOTAL_RESULTS));
            } else if (entityType.equals(NETWORK)) {
                assertThat(networkDataVolumeResults.size(), is(EXPECTED_NETWORK_TOTAL_RESULTS));
            }

            for (final NetworkDataVolumeResult networkDataVolumeResult : networkDataVolumeResults) {
                validateResult(networkDataVolumeResult, entityType, view);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void validateResult(final NetworkDataVolumeResult networkDataVolumeResult, final String entityType, final String view) {

        if (entityType.equals(TYPE_APN)) {
            getValidResult(networkDataVolumeResult, EXPECTED_APN_TOTAL_DL_1, EXPECTED_APN_TOTAL_UL_1,
                    getThroughput(EXPECTED_APN_TOTAL_DL_1, TOTAL_APN_DURATION_1, view),
                    getThroughput(EXPECTED_APN_TOTAL_UL_1, TOTAL_APN_DURATION_1, view), EXPECTED_APN_TOTAL_SESSION_1, EXPECTED_APN_TOTAL_SUBSCRIBER_1);
        } else if (entityType.equals(TYPE_TAC)) {
            getValidResult(networkDataVolumeResult, EXPECTED_TAC_TOTAL_DL_1, EXPECTED_TAC_TOTAL_UL_1,
                    getThroughput(EXPECTED_TAC_TOTAL_DL_1, TOTAL_TAC_DURATION_1, view),
                    getThroughput(EXPECTED_TAC_TOTAL_UL_1, TOTAL_TAC_DURATION_1, view), EXPECTED_TAC_TOTAL_SESSION_1, EXPECTED_TAC_TOTAL_SUBSCRIBER_1);
        } else if (entityType.equals(TYPE_MSISDN)) {
            getValidResultForGroup(networkDataVolumeResult, EXPECTED_MSISDN_TOTAL_DL_1, EXPECTED_MSISDN_TOTAL_UL_1,
                    getThroughput(EXPECTED_MSISDN_TOTAL_DL_1, TOTAL_MSISDN_DURATION_1, view),
                    getThroughput(EXPECTED_MSISDN_TOTAL_UL_1, TOTAL_MSISDN_DURATION_1, view), EXPECTED_MSISDN_TOTAL_SESSION_1);
        } else if (entityType.equals(TYPE_TAC_GROUP)) {
            getValidResult(networkDataVolumeResult, EXPECTED_TAC_GROUP_TOTAL_DL_1, EXPECTED_TAC_GROUP_TOTAL_UL_1,
                    getThroughput(EXPECTED_TAC_GROUP_TOTAL_DL_1, TOTAL_TAC_GROUP_DURATION_1, view),
                    getThroughput(EXPECTED_TAC_GROUP_TOTAL_UL_1, TOTAL_TAC_GROUP_DURATION_1, view), EXPECTED_TAC_GROUP_TOTAL_SESSION_1,
                    EXPECTED_TAC_GROUP_TOTAL_SUBSCRIBER_1);
        } else if (entityType.equals(TYPE_APN_GROUP)) {
            getValidResult(networkDataVolumeResult, EXPECTED_APN_GROUP_TOTAL_DL_1, EXPECTED_APN_GROUP_TOTAL_UL_1,
                    getThroughput(EXPECTED_APN_GROUP_TOTAL_DL_1, TOTAL_APN_GROUP_DURATION_1, view),
                    getThroughput(EXPECTED_APN_GROUP_TOTAL_UL_1, TOTAL_APN_GROUP_DURATION_1, view), EXPECTED_APN_GROUP_TOTAL_SESSION_1,
                    EXPECTED_APN_GROUP_TOTAL_SUBSCRIBER_1);
        } else if (entityType.equals(NETWORK)) {
            getValidResult(networkDataVolumeResult, EXPECTED_NETWORK_TOTAL_DL_1, EXPECTED_NETWORK_TOTAL_UL_1,
                    getThroughput(EXPECTED_NETWORK_TOTAL_DL_1, TOTAL_NETWORK_DURATION_1, view),
                    getThroughput(EXPECTED_NETWORK_TOTAL_UL_1, TOTAL_NETWORK_DURATION_1, view), EXPECTED_NETWORK_TOTAL_SESSION_1,
                    EXPECTED_NETWORK_TOTAL_SUBSCRIBER_1);
        }
    }

    @Test
    public void testDrillDownByAPN_1hour() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForAPN(APN1, CHART_PARAM, ONE_HOUR, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        final String jsonResult = networkDataVolumeResource.getNetworkDataVolumeResults(MESSAGE, map);
        validateResults(jsonResult, TYPE_APN, RAW);
    }

    @Test
    public void testDrillDownByAPN_2hours() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForAPN(APN1, GRID_PARAM, TWO_HOURS, TIME_ZONE_OFFSET_OF_MINUS_EIGHT_HOUR);
        final String jsonResult = networkDataVolumeResource.getNetworkDataVolumeResults(MESSAGE, map);
        validateResults(jsonResult, TYPE_APN, RAW);
    }

    @Test
    public void testDrillDownByAPNGroup_1hour() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForGroup(TYPE_APN, APN_GROUP1, CHART_PARAM, ONE_HOUR,
                TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        final String jsonResult = networkDataVolumeResource.getNetworkDataVolumeResults(MESSAGE, map);
        validateResults(jsonResult, TYPE_APN_GROUP, RAW);
    }

    @Test
    public void testDrillDownByAPNGroup_2hours() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForGroup(TYPE_APN, APN_GROUP1, GRID_PARAM, TWO_HOURS,
                TIME_ZONE_OFFSET_OF_MINUS_EIGHT_HOUR);
        final String jsonResult = networkDataVolumeResource.getNetworkDataVolumeResults(MESSAGE, map);
        validateResults(jsonResult, TYPE_APN_GROUP, RAW);
    }

    @Test
    public void testDrillDownByAPN_6hours() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForAPN(APN1, CHART_PARAM, SIX_HOURS, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        final String jsonResult = networkDataVolumeResource.getNetworkDataVolumeResults(MESSAGE, map);
        validateResults(jsonResult, TYPE_APN, DAY);
    }

    @Test
    public void testDrillDownByAPN_12hours() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForAPN(APN1, GRID_PARAM, TWELVE_HOURS, TIME_ZONE_OFFSET_OF_MINUS_EIGHT_HOUR);
        final String jsonResult = networkDataVolumeResource.getNetworkDataVolumeResults(MESSAGE, map);
        validateResults(jsonResult, TYPE_APN, DAY);
    }

    @Test
    public void testDrillDownByAPN_1day() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForAPN(APN1, GRID_PARAM, ONE_DAY, TIME_ZONE_OFFSET_OF_MINUS_EIGHT_HOUR);
        final String jsonResult = networkDataVolumeResource.getNetworkDataVolumeResults(MESSAGE, map);
        validateResults(jsonResult, TYPE_APN, DAY);
    }

    @Test
    public void testDrillDownByAPN_1week() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForAPN(APN1, GRID_PARAM, ONE_WEEK, TIME_ZONE_OFFSET_OF_MINUS_EIGHT_HOUR);
        final String jsonResult = networkDataVolumeResource.getNetworkDataVolumeResults(MESSAGE, map);
        validateResults(jsonResult, TYPE_APN, DAY);
    }

    @Test
    public void testDrillDownByAPNGroup_6hour() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForGroup(TYPE_APN, APN_GROUP1, CHART_PARAM, SIX_HOURS,
                TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        final String jsonResult = networkDataVolumeResource.getNetworkDataVolumeResults(MESSAGE, map);
        validateResults(jsonResult, TYPE_APN_GROUP, DAY);
    }

    @Test
    public void testDrillDownByAPNGroup_12hours() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForGroup(TYPE_APN, APN_GROUP1, GRID_PARAM, TWELVE_HOURS,
                TIME_ZONE_OFFSET_OF_MINUS_EIGHT_HOUR);
        final String jsonResult = networkDataVolumeResource.getNetworkDataVolumeResults(MESSAGE, map);
        validateResults(jsonResult, TYPE_APN_GROUP, DAY);
    }

    @Test
    public void testDrillDownByAPNGroup_1day() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForGroup(TYPE_APN, APN_GROUP1, CHART_PARAM, ONE_DAY,
                TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        final String jsonResult = networkDataVolumeResource.getNetworkDataVolumeResults(MESSAGE, map);
        validateResults(jsonResult, TYPE_APN_GROUP, DAY);
    }

    @Test
    public void testDrillDownByAPNGroup_1week() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForGroup(TYPE_APN, APN_GROUP1, GRID_PARAM, ONE_WEEK,
                TIME_ZONE_OFFSET_OF_MINUS_EIGHT_HOUR);
        final String jsonResult = networkDataVolumeResource.getNetworkDataVolumeResults(MESSAGE, map);
        validateResults(jsonResult, TYPE_APN_GROUP, DAY);
    }

    @Test
    public void testDrillDownByTAC_6hours() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForTAC(String.valueOf(TAC2), CHART_PARAM, SIX_HOURS,
                TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        final String jsonResult = networkDataVolumeResource.getNetworkDataVolumeResults(MESSAGE, map);
        validateResults(jsonResult, TYPE_TAC, DAY);
    }

    @Test
    public void testDrillDownByTAC_12hours() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForTAC(String.valueOf(TAC2), GRID_PARAM, TWELVE_HOURS,
                TIME_ZONE_OFFSET_OF_MINUS_EIGHT_HOUR);
        final String jsonResult = networkDataVolumeResource.getNetworkDataVolumeResults(MESSAGE, map);
        validateResults(jsonResult, TYPE_TAC, DAY);
    }

    @Test
    public void testDrillDownByTAC_1day() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForTAC(String.valueOf(TAC2), CHART_PARAM, ONE_DAY,
                TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        final String jsonResult = networkDataVolumeResource.getNetworkDataVolumeResults(MESSAGE, map);
        validateResults(jsonResult, TYPE_TAC, DAY);
    }

    @Test
    public void testDrillDownByTAC_1week() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForTAC(String.valueOf(TAC2), GRID_PARAM, ONE_WEEK,
                TIME_ZONE_OFFSET_OF_MINUS_EIGHT_HOUR);
        final String jsonResult = networkDataVolumeResource.getNetworkDataVolumeResults(MESSAGE, map);
        validateResults(jsonResult, TYPE_TAC, DAY);
    }

    @Test
    public void testDrillDownByTACGroup_6hour() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForGroup(TYPE_TAC, TAC_GROUP1, CHART_PARAM, SIX_HOURS,
                TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        final String jsonResult = networkDataVolumeResource.getNetworkDataVolumeResults(MESSAGE, map);
        validateResults(jsonResult, TYPE_TAC_GROUP, DAY);
    }

    @Test
    public void testDrillDownByTACGroup_12hours() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForGroup(TYPE_TAC, TAC_GROUP1, GRID_PARAM, TWELVE_HOURS,
                TIME_ZONE_OFFSET_OF_MINUS_EIGHT_HOUR);
        final String jsonResult = networkDataVolumeResource.getNetworkDataVolumeResults(MESSAGE, map);
        validateResults(jsonResult, TYPE_TAC_GROUP, DAY);

    }

    @Test
    public void testDrillDownByTACGroup_1day() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForGroup(TYPE_TAC, TAC_GROUP1, CHART_PARAM, ONE_DAY,
                TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        final String jsonResult = networkDataVolumeResource.getNetworkDataVolumeResults(MESSAGE, map);
        validateResults(jsonResult, TYPE_TAC_GROUP, DAY);
    }

    @Test
    public void testDrillDownByTACGroup_1week() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForGroup(TYPE_TAC, TAC_GROUP1, GRID_PARAM, ONE_WEEK,
                TIME_ZONE_OFFSET_OF_MINUS_EIGHT_HOUR);
        final String jsonResult = networkDataVolumeResource.getNetworkDataVolumeResults(MESSAGE, map);
        validateResults(jsonResult, TYPE_TAC_GROUP, DAY);
    }

    @Test
    public void testDrillDownByTAC_1hour() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForTAC(String.valueOf(TAC2), CHART_PARAM, ONE_HOUR,
                TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        final String jsonResult = networkDataVolumeResource.getNetworkDataVolumeResults(MESSAGE, map);
        validateResults(jsonResult, TYPE_TAC, RAW);
    }

    @Test
    public void testDrillDownByTAC_2hours() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForTAC(String.valueOf(TAC2), GRID_PARAM, TWO_HOURS,
                TIME_ZONE_OFFSET_OF_MINUS_EIGHT_HOUR);
        final String jsonResult = networkDataVolumeResource.getNetworkDataVolumeResults(MESSAGE, map);
        validateResults(jsonResult, TYPE_TAC, RAW);
    }

    @Test
    public void testDrillDownByTACGroup_1hour() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForGroup(TYPE_TAC, TAC_GROUP1, CHART_PARAM, ONE_HOUR,
                TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        final String jsonResult = networkDataVolumeResource.getNetworkDataVolumeResults(MESSAGE, map);
        validateResults(jsonResult, TYPE_TAC_GROUP, RAW);
    }

    @Test
    public void testDrillDownByTACGroup_2hours() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForGroup(TYPE_TAC, TAC_GROUP1, GRID_PARAM, TWO_HOURS,
                TIME_ZONE_OFFSET_OF_MINUS_EIGHT_HOUR);
        final String jsonResult = networkDataVolumeResource.getNetworkDataVolumeResults(MESSAGE, map);
        validateResults(jsonResult, TYPE_TAC_GROUP, RAW);
    }

    @Test
    public void testDrillDownByNetwork_1hour() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForNetwork(CHART_PARAM, ONE_HOUR, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        final String jsonResult = networkDataVolumeResource.getNetworkDataVolumeResults(MESSAGE, map);
        validateResults(jsonResult, TYPE_NETWORK, RAW);
    }

    @Test
    public void testDrillDownByNetwork_2hours() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForNetwork(GRID_PARAM, TWO_HOURS, TIME_ZONE_OFFSET_OF_MINUS_EIGHT_HOUR);
        final String jsonResult = networkDataVolumeResource.getNetworkDataVolumeResults(MESSAGE, map);
        validateResults(jsonResult, TYPE_NETWORK, RAW);
    }

    @Test
    public void testDrillDownByNetwork_6hour() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForNetwork(CHART_PARAM, SIX_HOURS, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        final String jsonResult = networkDataVolumeResource.getNetworkDataVolumeResults(MESSAGE, map);
        validateResults(jsonResult, TYPE_NETWORK, DAY);
    }

    @Test
    public void testDrillDownByNetwork_12hours() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForNetwork(GRID_PARAM, TWELVE_HOURS, TIME_ZONE_OFFSET_OF_MINUS_EIGHT_HOUR);
        final String jsonResult = networkDataVolumeResource.getNetworkDataVolumeResults(MESSAGE, map);
        validateResults(jsonResult, TYPE_NETWORK, DAY);
    }

    @Test
    public void testDrillDownByNetwork_1day() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForNetwork(CHART_PARAM, ONE_DAY, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        final String jsonResult = networkDataVolumeResource.getNetworkDataVolumeResults(MESSAGE, map);
        validateResults(jsonResult, TYPE_NETWORK, DAY);
    }

    @Test
    public void testDrillDownByNetwork_1week() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForNetwork(GRID_PARAM, ONE_WEEK, TIME_ZONE_OFFSET_OF_MINUS_EIGHT_HOUR);
        final String jsonResult = networkDataVolumeResource.getNetworkDataVolumeResults(MESSAGE, map);
        validateResults(jsonResult, TYPE_NETWORK, DAY);
    }

    private void getValidResult(final NetworkDataVolumeResult networkDataVolumeResult, final double total_dl, final double total_ul,
                                final double dl_throughput, final double ul_throughput, final double total_session, final double total_subscriber) {
        assertThat(networkDataVolumeResult.getDownlinkVolume(), is(total_dl));
        assertThat(networkDataVolumeResult.getUplinkVolume(), is(total_ul));
        assertThat(networkDataVolumeResult.getDownlinkThroughput(), is(dl_throughput));
        assertThat(networkDataVolumeResult.getUplinkThroughput(), is(ul_throughput));
        assertThat(networkDataVolumeResult.getNumberOfSession(), is(total_session));
        assertThat(networkDataVolumeResult.getNumberOfSubscriber(), is(total_subscriber));
    }

    private void getValidResultForGroup(final NetworkDataVolumeResult networkDataVolumeResult, final double total_dl, final double total_ul,
                                        final double dl_throughput, final double ul_throughput, final double total_session) {
        assertThat(networkDataVolumeResult.getDownlinkVolume(), is(total_dl));
        assertThat(networkDataVolumeResult.getUplinkVolume(), is(total_ul));
        assertThat(networkDataVolumeResult.getDownlinkThroughput(), is(dl_throughput));
        assertThat(networkDataVolumeResult.getUplinkThroughput(), is(ul_throughput));
        assertThat(networkDataVolumeResult.getNumberOfSession(), is(total_session));
    }

}
