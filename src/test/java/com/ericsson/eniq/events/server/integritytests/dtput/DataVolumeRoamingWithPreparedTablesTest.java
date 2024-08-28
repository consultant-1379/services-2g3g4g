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
import static com.ericsson.eniq.events.server.integritytests.stubs.ReplaceTablesWithTempTablesTemplateUtils.*;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.*;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.*;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.sql.SQLException;
import java.util.*;

import javax.ws.rs.core.MultivaluedMap;

import org.junit.Test;

import com.ericsson.eniq.events.server.common.RequestParametersUtilities;
import com.ericsson.eniq.events.server.resources.TestsWithTemporaryTablesBaseTestCase;
import com.ericsson.eniq.events.server.resources.dtput.DatavolRoamingAnalysisResource;
import com.ericsson.eniq.events.server.test.queryresults.datavolume.DataVolumeRoamingResult;
import com.ericsson.eniq.events.server.test.stubs.DummyUriInfoImpl;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;
import com.sun.jersey.core.util.MultivaluedMapImpl;

public class DataVolumeRoamingWithPreparedTablesTest extends TestsWithTemporaryTablesBaseTestCase<DataVolumeRoamingResult> {
    private DatavolRoamingAnalysisResource datavolRoamingAnalysisResource;

    private MultivaluedMap<String, String> map;

    private static final String TIME_FROM = "0000";

    private static final String TIME_TO = "0000";

    private static final String DATE_FROM = "01012000";

    private static final String DATE_TO = "01013000";

    private static final long MEGABYTE = 1024L * 1024L;

    private static final long IMSI1 = 12300000000L;

    private static final long IMSI2 = 22900000000L;

    private static final int PDN_ID1 = 10000;

    private static final int PDN_ID2 = 20000;

    private static final long DURATION1 = 100000L;

    private static final long DURATION2 = 200000L;

    private static final long DURATION3 = 300000L;

    private static final long DURATION4 = 100000L;

    private static final long GGSN_IPADDRESS1 = 12300000000L;

    private static final long GGSN_IPADDRESS2 = 45600000000L;

    private static final long GGSN_IPADDRESS3 = 12300000000L;

    private static final long GGSN_IPADDRESS4 = 45600000000L;

    private static final String TEST_MCC_01 = "1";

    private static final String TEST_MNC_01 = "1";

    private static final String TEST_COUNTRY_01 = "Ireland";

    private static final String TEST_OPERATOR_01 = "Vodafone";

    private static final long TEST_IMSI_01 = IMSI1;

    private static final double TEST_DL_01 = 1000L;

    private static final double TEST_UL_01 = 3000L;

    private static final long TEST_DURATION_01 = DURATION1;

    private static final int TEST_ROAMING_01 = 1;

    private static final long TEST_GGSN_IPADDRESS_01 = GGSN_IPADDRESS1;

    private static final long TEST_PDN_ID_01 = PDN_ID1;

    private static final String TEST_DATETIME_ID_01 = DateTimeUtilities.getDateTime(DATE_TIME_FORMAT, Calendar.MINUTE, -45);

    private static final String TEST_DATETIME_ID_DAY_01 = DateTimeUtilities.getDateTime(DATE_TIME_FORMAT, Calendar.DAY_OF_MONTH, -1);

    private static final String TEST_MCC_02 = "2";

    private static final String TEST_MNC_02 = "2";

    private static final String TEST_COUNTRY_02 = "Hungary";

    private static final String TEST_OPERATOR_02 = "T-Mobile";

    private static final long TEST_IMSI_02 = IMSI2;

    private static final double TEST_DL_02 = 2000L;

    private static final double TEST_UL_02 = 4000L;

    private static final long TEST_DURATION_02 = DURATION2;

    private static final int TEST_ROAMING_02 = 1;

    private static final long TEST_GGSN_IPADDRESS_02 = GGSN_IPADDRESS2;

    private static final long TEST_PDN_ID_02 = PDN_ID2;

    private static final String TEST_DATETIME_ID_02 = TEST_DATETIME_ID_01;

    private static final String TEST_DATETIME_ID_DAY_02 = TEST_DATETIME_ID_DAY_01;

    private static final String TEST_MCC_03 = "1";

    private static final String TEST_MNC_03 = "2";

    private static final long TEST_IMSI_03 = IMSI1;

    private static final double TEST_DL_03 = 2400L;

    private static final double TEST_UL_03 = 4200L;

    private static final long TEST_DURATION_03 = DURATION3;

    private static final int TEST_ROAMING_03 = 1;

    private static final long TEST_GGSN_IPADDRESS_03 = GGSN_IPADDRESS3;

    private static final long TEST_PDN_ID_03 = PDN_ID1;

    private static final String TEST_DATETIME_ID_03 = TEST_DATETIME_ID_01;

    private static final String TEST_DATETIME_ID_DAY_03 = TEST_DATETIME_ID_DAY_01;

    private static final String TEST_MCC_04 = "1";

    private static final String TEST_MNC_04 = "2";

    private static final long TEST_IMSI_04 = IMSI2;

    private static final double TEST_DL_04 = 2400L;

    private static final double TEST_UL_04 = 4200L;

    private static final long TEST_DURATION_04 = DURATION4;

    private static final int TEST_ROAMING_04 = 1;

    private static final long TEST_GGSN_IPADDRESS_04 = GGSN_IPADDRESS4;

    private static final long TEST_PDN_ID_04 = PDN_ID2;

    private static final String TEST_DATETIME_ID_04 = TEST_DATETIME_ID_01;

    private static final String TEST_DATETIME_ID_DAY_04 = TEST_DATETIME_ID_DAY_01;

    private final static int EXPECTED_TOTAL_RESULTS = 2;

    private final static double EXPECTED_TOTAL_DL_COUNTRY_01 = TEST_DL_01 + TEST_DL_03 + TEST_DL_04;

    private final static double EXPECTED_TOTAL_DL_COUNTRY_02 = TEST_DL_02;

    private final static double EXPECTED_TOTAL_UL_COUNTRY_01 = TEST_UL_01 + TEST_UL_03 + TEST_UL_04;

    private final static double EXPECTED_TOTAL_UL_COUNTRY_02 = TEST_UL_02;

    private final static double TOTAL_DURATION_COUNTRY_01 = TEST_DURATION_01 + TEST_DURATION_03 + TEST_DURATION_04;

    private final static double TOTAL_DURATION_COUNTRY_02 = TEST_DURATION_02;

    private final static double EXPECTED_TOTAL_SESSION_COUNTRY_01 = 2;

    private final static double EXPECTED_TOTAL_SESSION_COUNTRY_02 = 1;

    private final static double EXPECTED_TOTAL_SUBSCRIBER_COUNTRY_01 = 2;

    private final static double EXPECTED_TOTAL_SUBSCRIBER_COUNTRY_02 = 1;

    private final static double EXPECTED_TOTAL_DL_OPERATOR_01 = TEST_DL_01;

    private final static double EXPECTED_TOTAL_DL_OPERATOR_02 = TEST_DL_02 + TEST_DL_03 + TEST_DL_04;

    private final static double EXPECTED_TOTAL_UL_OPERATOR_01 = TEST_UL_01;

    private final static double EXPECTED_TOTAL_UL_OPERATOR_02 = TEST_UL_02 + TEST_UL_03 + TEST_UL_04;

    private final static double TOTAL_DURATION_OPERATOR_01 = TEST_DURATION_01;

    private final static double TOTAL_DURATION_OPERATOR_02 = TEST_DURATION_02 + TEST_DURATION_03 + TEST_DURATION_04;

    private final static double EXPECTED_TOTAL_SESSION_OPERATOR_01 = 1;

    private final static double EXPECTED_TOTAL_SESSION_OPERATOR_02 = 2;

    private final static double EXPECTED_TOTAL_SUBSCRIBER_OPERATOR_01 = 1;

    private final static double EXPECTED_TOTAL_SUBSCRIBER_OPERATOR_02 = 2;

    @Override
    public void onSetUp() throws Exception {
        super.onSetUp();
        datavolRoamingAnalysisResource = new DatavolRoamingAnalysisResource();
        addTableNameToReplace(DIM_E_SGEH_MCCMNC, TEMP_DIM_E_SGEH_MCCMNC);

        final Collection<String> dtRawColumns = new ArrayList<String>();
        dtRawColumns.add(IMSI_MCC);
        dtRawColumns.add(IMSI_MNC);
        dtRawColumns.add(IMSI);
        dtRawColumns.add(DATAVOL_DL);
        dtRawColumns.add(DATAVOL_UL);
        dtRawColumns.add(DURATION);
        dtRawColumns.add(ROAMING);
        dtRawColumns.add(GGSN_IPADDRESS);
        dtRawColumns.add(PDN_ID);
        dtRawColumns.add(DATETIME_ID);
        dtRawColumns.add(PDNID_GGSNIP);
        createTemporaryTable(TEMP_EVENT_E_DVTP_DT_RAW, dtRawColumns);

        addTableNameToReplace(DIM_E_SGEH_MCCMNC, TEMP_DIM_E_SGEH_MCCMNC);

        final Collection<String> dt_15min_AggregationColumns = new ArrayList<String>();
        dt_15min_AggregationColumns.add(MCC);
        dt_15min_AggregationColumns.add(MNC);
        dt_15min_AggregationColumns.add(DATAVOL_DL);
        dt_15min_AggregationColumns.add(DATAVOL_UL);
        dt_15min_AggregationColumns.add(DURATION);
        dt_15min_AggregationColumns.add(ROAMING);
        dt_15min_AggregationColumns.add(DATETIME_ID);
        createTemporaryTable(TEMP_EVENT_E_DVTP_DT_NETWORK_15MIN, dt_15min_AggregationColumns);

        final Collection<String> dt_day_AggregationColumns = new ArrayList<String>();
        dt_day_AggregationColumns.add(MCC);
        dt_day_AggregationColumns.add(MNC);
        dt_day_AggregationColumns.add(DATAVOL_DL);
        dt_day_AggregationColumns.add(DATAVOL_UL);
        dt_day_AggregationColumns.add(DURATION);
        dt_day_AggregationColumns.add(ROAMING);
        dt_day_AggregationColumns.add(DATETIME_ID);
        createTemporaryTable(TEMP_EVENT_E_DVTP_DT_NETWORK_DAY, dt_day_AggregationColumns);

        populateTemporaryTables();
        attachDependencies(datavolRoamingAnalysisResource);
        datavolRoamingAnalysisResource.setTechPackCXCMappingService(techPackCXCMappingService);
        map = new MultivaluedMapImpl();
        DummyUriInfoImpl.setUriInfo(map, datavolRoamingAnalysisResource);
    }

    private void insertRecordInDVTPRaw(String mcc, String mnc, long imsi, double dl, double ul, long duration, int roaming, long ggsnIPAddr,
                                       long pdnId, String dateTime) throws SQLException {
        Map<String, Object> tableValues = new HashMap<String, Object>();
        tableValues = new HashMap<String, Object>();
        tableValues.put(IMSI_MCC, mcc);
        tableValues.put(IMSI_MNC, mnc);
        tableValues.put(IMSI, imsi);
        tableValues.put(DATAVOL_DL, dl);
        tableValues.put(DATAVOL_UL, ul);
        tableValues.put(DURATION, duration);
        tableValues.put(ROAMING, roaming);
        tableValues.put(GGSN_IPADDRESS, ggsnIPAddr);
        tableValues.put(PDN_ID, pdnId);
        tableValues.put(DATETIME_ID, dateTime);
        tableValues.put(PDNID_GGSNIP, pdnId + "-" + ggsnIPAddr);
        insertRow(TEMP_EVENT_E_DVTP_DT_RAW, tableValues);
    }

    private void insertRecordInMccMnc(String mcc, String mnc, String country, String operator) throws SQLException {
        Map<String, Object> tableValues = new HashMap<String, Object>();
        tableValues = new HashMap<String, Object>();
        tableValues.put(MCC, mcc);
        tableValues.put(MNC, mnc);
        tableValues.put(COUNTRY, country);
        tableValues.put(OPERATOR, operator);
        insertRow(TEMP_DIM_E_SGEH_MCCMNC, tableValues);
    }

    private void insertRecordInDVTP_15min_Aggregation(String mcc, String mnc, double dl, double ul, long duration, int roaming, String dateTime)
            throws SQLException {
        Map<String, Object> tableValues = new HashMap<String, Object>();
        tableValues = new HashMap<String, Object>();
        tableValues.put(MCC, mcc);
        tableValues.put(MNC, mnc);
        tableValues.put(DATAVOL_DL, dl);
        tableValues.put(DATAVOL_UL, ul);
        tableValues.put(DURATION, duration);
        tableValues.put(ROAMING, roaming);
        tableValues.put(DATETIME_ID, dateTime);
        insertRow(TEMP_EVENT_E_DVTP_DT_NETWORK_15MIN, tableValues);
    }

    private void insertRecordInDVTP_Day_Aggregation(String mcc, String mnc, double dl, double ul, long duration, int roaming, String dateTime)
            throws SQLException {
        Map<String, Object> tableValues = new HashMap<String, Object>();
        tableValues = new HashMap<String, Object>();
        tableValues.put(MCC, mcc);
        tableValues.put(MNC, mnc);
        tableValues.put(DATAVOL_DL, dl);
        tableValues.put(DATAVOL_UL, ul);
        tableValues.put(DURATION, duration);
        tableValues.put(ROAMING, roaming);
        tableValues.put(DATETIME_ID, dateTime);
        insertRow(TEMP_EVENT_E_DVTP_DT_NETWORK_DAY, tableValues);
    }

    private void populateTemporaryTables() throws SQLException {
        insertRecordInDVTPRaw(TEST_MCC_01, TEST_MNC_01, TEST_IMSI_01, TEST_DL_01 * MEGABYTE, TEST_UL_01 * MEGABYTE, TEST_DURATION_01,
                TEST_ROAMING_01, TEST_GGSN_IPADDRESS_01, TEST_PDN_ID_01, TEST_DATETIME_ID_01);
        insertRecordInDVTPRaw(TEST_MCC_02, TEST_MNC_02, TEST_IMSI_02, TEST_DL_02 * MEGABYTE, TEST_UL_02 * MEGABYTE, TEST_DURATION_02,
                TEST_ROAMING_02, TEST_GGSN_IPADDRESS_02, TEST_PDN_ID_02, TEST_DATETIME_ID_02);
        insertRecordInDVTPRaw(TEST_MCC_03, TEST_MNC_03, TEST_IMSI_03, TEST_DL_03 * MEGABYTE, TEST_UL_03 * MEGABYTE, TEST_DURATION_03,
                TEST_ROAMING_03, TEST_GGSN_IPADDRESS_03, TEST_PDN_ID_03, TEST_DATETIME_ID_03);
        insertRecordInDVTPRaw(TEST_MCC_04, TEST_MNC_04, TEST_IMSI_04, TEST_DL_04 * MEGABYTE, TEST_UL_04 * MEGABYTE, TEST_DURATION_04,
                TEST_ROAMING_04, TEST_GGSN_IPADDRESS_04, TEST_PDN_ID_04, TEST_DATETIME_ID_04);

        insertRecordInDVTPRaw(TEST_MCC_01, TEST_MNC_01, TEST_IMSI_01, TEST_DL_01 * MEGABYTE, TEST_UL_01 * MEGABYTE, TEST_DURATION_01,
                TEST_ROAMING_01, TEST_GGSN_IPADDRESS_01, TEST_PDN_ID_01, TEST_DATETIME_ID_DAY_01);
        insertRecordInDVTPRaw(TEST_MCC_02, TEST_MNC_02, TEST_IMSI_02, TEST_DL_02 * MEGABYTE, TEST_UL_02 * MEGABYTE, TEST_DURATION_02,
                TEST_ROAMING_02, TEST_GGSN_IPADDRESS_02, TEST_PDN_ID_02, TEST_DATETIME_ID_DAY_02);
        insertRecordInDVTPRaw(TEST_MCC_03, TEST_MNC_03, TEST_IMSI_03, TEST_DL_03 * MEGABYTE, TEST_UL_03 * MEGABYTE, TEST_DURATION_03,
                TEST_ROAMING_03, TEST_GGSN_IPADDRESS_03, TEST_PDN_ID_03, TEST_DATETIME_ID_DAY_03);
        insertRecordInDVTPRaw(TEST_MCC_04, TEST_MNC_04, TEST_IMSI_04, TEST_DL_04 * MEGABYTE, TEST_UL_04 * MEGABYTE, TEST_DURATION_04,
                TEST_ROAMING_04, TEST_GGSN_IPADDRESS_04, TEST_PDN_ID_04, TEST_DATETIME_ID_DAY_04);

        insertRecordInMccMnc(TEST_MCC_01, TEST_MNC_01, TEST_COUNTRY_01, TEST_OPERATOR_01);
        insertRecordInMccMnc(TEST_MCC_02, TEST_MNC_02, TEST_COUNTRY_02, TEST_OPERATOR_02);
        insertRecordInMccMnc(TEST_MCC_01, TEST_MNC_02, TEST_COUNTRY_01, TEST_OPERATOR_02);

        insertRecordInDVTP_15min_Aggregation(TEST_MCC_01, TEST_MNC_01, TEST_DL_01 * MEGABYTE, TEST_UL_01 * MEGABYTE, TEST_DURATION_01,
                TEST_ROAMING_01, TEST_DATETIME_ID_01);
        insertRecordInDVTP_15min_Aggregation(TEST_MCC_02, TEST_MNC_02, TEST_DL_02 * MEGABYTE, TEST_UL_02 * MEGABYTE, TEST_DURATION_02,
                TEST_ROAMING_02, TEST_DATETIME_ID_02);
        insertRecordInDVTP_15min_Aggregation(TEST_MCC_03, TEST_MNC_03, TEST_DL_03 * MEGABYTE, TEST_UL_03 * MEGABYTE, TEST_DURATION_03,
                TEST_ROAMING_03, TEST_DATETIME_ID_03);
        insertRecordInDVTP_15min_Aggregation(TEST_MCC_04, TEST_MNC_04, TEST_DL_04 * MEGABYTE, TEST_UL_04 * MEGABYTE, TEST_DURATION_04,
                TEST_ROAMING_04, TEST_DATETIME_ID_04);

        insertRecordInDVTP_Day_Aggregation(TEST_MCC_01, TEST_MNC_01, TEST_DL_01 * MEGABYTE, TEST_UL_01 * MEGABYTE, TEST_DURATION_01, TEST_ROAMING_01,
                TEST_DATETIME_ID_DAY_01);
        insertRecordInDVTP_Day_Aggregation(TEST_MCC_02, TEST_MNC_02, TEST_DL_02 * MEGABYTE, TEST_UL_02 * MEGABYTE, TEST_DURATION_02, TEST_ROAMING_02,
                TEST_DATETIME_ID_DAY_02);
        insertRecordInDVTP_Day_Aggregation(TEST_MCC_03, TEST_MNC_03, TEST_DL_03 * MEGABYTE, TEST_UL_03 * MEGABYTE, TEST_DURATION_03, TEST_ROAMING_03,
                TEST_DATETIME_ID_DAY_03);
        insertRecordInDVTP_Day_Aggregation(TEST_MCC_04, TEST_MNC_04, TEST_DL_04 * MEGABYTE, TEST_UL_04 * MEGABYTE, TEST_DURATION_04, TEST_ROAMING_04,
                TEST_DATETIME_ID_DAY_04);

    }

    @Test
    public void testForCountry_30minutes() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForNetwork(GRID_PARAM, THIRTY_MINUTES, TIME_ZONE_OFFSET_OF_MINUS_EIGHT_HOUR);
        validateResults(datavolRoamingAnalysisResource.getRoamingResults(CANCEL_REQUEST_NOT_SUPPORTED, TYPE_ROAMING_COUNTRY, map));
    }

    @Test
    public void testForCountry_2hours() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForNetwork(GRID_PARAM, TWO_HOURS, TIME_ZONE_OFFSET_OF_MINUS_EIGHT_HOUR);
        validateResults(datavolRoamingAnalysisResource.getRoamingResults(CANCEL_REQUEST_NOT_SUPPORTED, TYPE_ROAMING_COUNTRY, map));
    }

    @Test
    public void testForCountry_6hours() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForNetwork(GRID_PARAM, SIX_HOURS, TIME_ZONE_OFFSET_OF_MINUS_EIGHT_HOUR);
        validateResults(datavolRoamingAnalysisResource.getRoamingResults(CANCEL_REQUEST_NOT_SUPPORTED, TYPE_ROAMING_COUNTRY, map));
    }

    @Test
    public void testForCountry_12hours() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForNetwork(GRID_PARAM, TWELVE_HOURS, TIME_ZONE_OFFSET_OF_MINUS_EIGHT_HOUR);
        validateResults(datavolRoamingAnalysisResource.getRoamingResults(CANCEL_REQUEST_NOT_SUPPORTED, TYPE_ROAMING_COUNTRY, map));
    }

    @Test
    public void testForCountry_1day() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForNetwork(GRID_PARAM, ONE_DAY, TIME_ZONE_OFFSET_OF_MINUS_EIGHT_HOUR);
        validateResults(datavolRoamingAnalysisResource.getRoamingResults(CANCEL_REQUEST_NOT_SUPPORTED, TYPE_ROAMING_COUNTRY, map));
    }

    @Test
    public void testForCountry_1week() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForNetwork(GRID_PARAM, ONE_WEEK, TIME_ZONE_OFFSET_OF_MINUS_EIGHT_HOUR);
        validateResults(datavolRoamingAnalysisResource.getRoamingResults(CANCEL_REQUEST_NOT_SUPPORTED, TYPE_ROAMING_COUNTRY, map));
    }

    @Test
    public void testForCountry_CustomTime() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForNetwork(GRID_PARAM, THREE_DAY, TIME_ZONE_OFFSET_OF_MINUS_EIGHT_HOUR);
        validateResults(datavolRoamingAnalysisResource.getRoamingResults(CANCEL_REQUEST_NOT_SUPPORTED, TYPE_ROAMING_COUNTRY, map));
    }

    @Test
    public void testForOperator_30minutes() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForNetwork(GRID_PARAM, THIRTY_MINUTES, TIME_ZONE_OFFSET_OF_MINUS_EIGHT_HOUR);
        validateResults(datavolRoamingAnalysisResource.getRoamingResults(CANCEL_REQUEST_NOT_SUPPORTED, TYPE_ROAMING_OPERATOR, map));
    }

    @Test
    public void testForOperator_1hour() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForNetwork(CHART_PARAM, ONE_HOUR, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        validateResults(datavolRoamingAnalysisResource.getRoamingResults(CANCEL_REQUEST_NOT_SUPPORTED, TYPE_ROAMING_OPERATOR, map));
    }

    @Test
    public void testForOperator_2hours() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForNetwork(GRID_PARAM, TWO_HOURS, TIME_ZONE_OFFSET_OF_MINUS_EIGHT_HOUR);
        validateResults(datavolRoamingAnalysisResource.getRoamingResults(CANCEL_REQUEST_NOT_SUPPORTED, TYPE_ROAMING_OPERATOR, map));
    }

    @Test
    public void testForOperator_6hours() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForNetwork(GRID_PARAM, SIX_HOURS, TIME_ZONE_OFFSET_OF_MINUS_EIGHT_HOUR);
        validateResults(datavolRoamingAnalysisResource.getRoamingResults(CANCEL_REQUEST_NOT_SUPPORTED, TYPE_ROAMING_OPERATOR, map));
    }

    @Test
    public void testForOperator_12hours() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForNetwork(GRID_PARAM, TWELVE_HOURS, TIME_ZONE_OFFSET_OF_MINUS_EIGHT_HOUR);
        validateResults(datavolRoamingAnalysisResource.getRoamingResults(CANCEL_REQUEST_NOT_SUPPORTED, TYPE_ROAMING_OPERATOR, map));
    }

    @Test
    public void testForOperator_1day() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForNetwork(GRID_PARAM, ONE_DAY, TIME_ZONE_OFFSET_OF_MINUS_EIGHT_HOUR);
        validateResults(datavolRoamingAnalysisResource.getRoamingResults(CANCEL_REQUEST_NOT_SUPPORTED, TYPE_ROAMING_OPERATOR, map));
    }

    @Test
    public void testForOperator_1week() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForNetwork(GRID_PARAM, ONE_WEEK, TIME_ZONE_OFFSET_OF_MINUS_EIGHT_HOUR);
        validateResults(datavolRoamingAnalysisResource.getRoamingResults(CANCEL_REQUEST_NOT_SUPPORTED, TYPE_ROAMING_OPERATOR, map));
    }

    @Test
    public void testForOperator_CustomTime() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForNetwork(GRID_PARAM, THREE_DAY, TIME_ZONE_OFFSET_OF_MINUS_EIGHT_HOUR);
        validateResults(datavolRoamingAnalysisResource.getRoamingResults(CANCEL_REQUEST_NOT_SUPPORTED, TYPE_ROAMING_OPERATOR, map));

    }

    private void validateResults(final String jsonResult) throws Exception {
        System.out.println(jsonResult);
        jsonAssertUtils.assertJSONSucceeds(jsonResult);

        final List<DataVolumeRoamingResult> dataVolumeRoamingResults = getTranslator().translateResult(jsonResult, DataVolumeRoamingResult.class);
        assertThat(dataVolumeRoamingResults.size(), is(EXPECTED_TOTAL_RESULTS));

        for (final DataVolumeRoamingResult dataVolumeRoamingResult : dataVolumeRoamingResults) {
            validateResult(dataVolumeRoamingResult);
        }
    }

    private void validateResult(final DataVolumeRoamingResult dataVolumeRoamingResult) {

        if (TEST_MCC_01.equals(dataVolumeRoamingResult.getCountryOrOperator())) {
            getValidResult(dataVolumeRoamingResult, EXPECTED_TOTAL_DL_COUNTRY_01, EXPECTED_TOTAL_UL_COUNTRY_01,
                    RequestParametersUtilities.getThroughput(EXPECTED_TOTAL_DL_COUNTRY_01, TOTAL_DURATION_COUNTRY_01, RAW),
                    RequestParametersUtilities.getThroughput(EXPECTED_TOTAL_UL_COUNTRY_01, TOTAL_DURATION_COUNTRY_01, RAW),
                    EXPECTED_TOTAL_SESSION_COUNTRY_01, EXPECTED_TOTAL_SUBSCRIBER_COUNTRY_01);
        } else if (TEST_MCC_02.equals(dataVolumeRoamingResult.getCountryOrOperator())) {
            getValidResult(dataVolumeRoamingResult, EXPECTED_TOTAL_DL_COUNTRY_02, EXPECTED_TOTAL_UL_COUNTRY_02,
                    RequestParametersUtilities.getThroughput(EXPECTED_TOTAL_DL_COUNTRY_02, TOTAL_DURATION_COUNTRY_02, FIFTEEN_MINUTES),
                    RequestParametersUtilities.getThroughput(EXPECTED_TOTAL_UL_COUNTRY_02, TOTAL_DURATION_COUNTRY_02, FIFTEEN_MINUTES),
                    EXPECTED_TOTAL_SESSION_COUNTRY_02, EXPECTED_TOTAL_SUBSCRIBER_COUNTRY_02);
        } else if (TEST_MNC_01.equals(dataVolumeRoamingResult.getCountryOrOperator())) {
            getValidResult(dataVolumeRoamingResult, EXPECTED_TOTAL_DL_OPERATOR_01, EXPECTED_TOTAL_UL_OPERATOR_01,
                    RequestParametersUtilities.getThroughput(EXPECTED_TOTAL_DL_OPERATOR_01, TOTAL_DURATION_OPERATOR_01, RAW),
                    RequestParametersUtilities.getThroughput(EXPECTED_TOTAL_UL_OPERATOR_01, TOTAL_DURATION_OPERATOR_01, RAW),
                    EXPECTED_TOTAL_SESSION_OPERATOR_01, EXPECTED_TOTAL_SUBSCRIBER_OPERATOR_01);
        } else if (TEST_MNC_02.equals(dataVolumeRoamingResult.getCountryOrOperator())) {
            getValidResult(dataVolumeRoamingResult, EXPECTED_TOTAL_DL_OPERATOR_02, EXPECTED_TOTAL_UL_OPERATOR_02,
                    RequestParametersUtilities.getThroughput(EXPECTED_TOTAL_DL_OPERATOR_02, TOTAL_DURATION_OPERATOR_02, FIFTEEN_MINUTES),
                    RequestParametersUtilities.getThroughput(EXPECTED_TOTAL_UL_OPERATOR_02, TOTAL_DURATION_OPERATOR_02, FIFTEEN_MINUTES),
                    EXPECTED_TOTAL_SESSION_OPERATOR_02, EXPECTED_TOTAL_SUBSCRIBER_OPERATOR_02);

        }
    }

    private void getValidResult(final DataVolumeRoamingResult dataVolumeRoamingResult, double total_dl, double total_ul, double dl_throughput,
                                double ul_throughput, double total_session, double total_subscriber) {
        assertThat(dataVolumeRoamingResult.getDownlinkVolume(), is(total_dl));
        assertThat(dataVolumeRoamingResult.getUplinkVolume(), is(total_ul));
        assertThat(dataVolumeRoamingResult.getDownlinkThroughput(), is(dl_throughput));
        assertThat(dataVolumeRoamingResult.getUplinkThroughput(), is(ul_throughput));
        assertThat(dataVolumeRoamingResult.getNumberOfSession(), is(total_session));
        assertThat(dataVolumeRoamingResult.getNumberOfSubscriber(), is(total_subscriber));
    }
}