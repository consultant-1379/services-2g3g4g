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
package com.ericsson.eniq.events.server.integritytests.kpiratio;

import java.net.URISyntaxException;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.ericsson.eniq.events.server.resources.KPIRatioResource;
import com.ericsson.eniq.events.server.resources.TestsWithTemporaryTablesBaseTestCase;
import com.ericsson.eniq.events.server.test.queryresults.KPIByAPNAndSGSNResult;
import com.ericsson.eniq.events.server.test.stubs.DummyUriInfoImpl;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;
import com.sun.jersey.core.util.MultivaluedMapImpl;
import org.junit.Test;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.common.EventIDConstants.*;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.*;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.*;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class KPIRatioDrillByAPNAndSGSNTest extends
        TestsWithTemporaryTablesBaseTestCase<KPIByAPNAndSGSNResult> {

    private KPIRatioResource kpiRatioResource;

    private final SimpleDateFormat dateOnlyFormatter = new SimpleDateFormat(
            DATE_FORMAT);

    private final int noOfErrors = 1;

    private final int noOfSuccess = 1;

    private final int noOfSuccess1 = 0;

    private final int noOfOccurences = 2;

    private final int noOfOccurences1 = 1;

    private final int noOfSubscribers = 1;

    @Override
    public void onSetUp() throws Exception {
        super.onSetUp();
        kpiRatioResource = new KPIRatioResource();
        attachDependencies(kpiRatioResource);
    }

    @Test
    public void testTypeAPNDrilltypeSGSN2G3GEventId() throws Exception {
        createAggregationTables();
        final String timestamp = DateTimeUtilities.getDateTimeMinus48Hours();
        final String localDateId = dateOnlyFormatter.format(dateOnlyFormatter
                .parse(timestamp));
        createAndPopulateRawTablesForSgehQuery(timestamp, localDateId);
        populateAggregationTablesForSgehQuery(timestamp);
        final String result = runQuery(SAMPLE_SGSN, ISRAU_IN_2G_AND_3G,
                TWO_WEEKS);
        validateResultForSGEHQuery(result);
    }

    @Test
    public void testTypeAPNDrilltypeSGSN2G3GEventIdWithDataTiering()
            throws Exception {
        jndiProperties.setUpDataTieringJNDIProperty();
        createAggregationTables();
        final String timestamp = DateTimeUtilities.getDateTimeMinus25Minutes();
        final String localDateId = dateOnlyFormatter.format(dateOnlyFormatter
                .parse(timestamp));
        createAndPopulateRawTablesForSgehQuery(timestamp, localDateId);
        populateSuccessAggregationTablesForSgehQuery(timestamp);
        final String json = runQuery(SAMPLE_SGSN, ISRAU_IN_2G_AND_3G,
                THIRTY_MINUTES);
        validateResult(json);
    }

    @Test
    public void testTypeAPNDrilltypeSGSNBSC2G3GEventIdWithDataTiering()
            throws Exception {
        jndiProperties.setUpDataTieringJNDIProperty();
        final String timestamp = DateTimeUtilities.getDateTimeMinus25Minutes();
        final String localDateId = dateOnlyFormatter.format(dateOnlyFormatter
                .parse(timestamp));
        createAndPopulateRawTablesForSgehQuery(timestamp, localDateId);
        final String json = runQueryTypeAPNDrilltypeSGSNBSC(SAMPLE_SGSN,
                ISRAU_IN_2G_AND_3G, THIRTY_MINUTES);
        validateResult(json);

    }

    @Test
    public void testTypeAPNDrilltypeSGSN2G3GEventIdWithDataTiering6Hours()
            throws Exception {
        jndiProperties.setUpDataTieringJNDIProperty();
        createAggregationTables();
        final String timestamp = DateTimeUtilities.getDateTimeMinus55Minutes();
        final String localDateId = dateOnlyFormatter.format(dateOnlyFormatter
                .parse(timestamp));
        createAndPopulateRawTablesForSgehQuery(timestamp, localDateId);
        populateSuccessAggregationTablesForSgehQuery(timestamp);
        final String json = runQuery(SAMPLE_SGSN, ISRAU_IN_2G_AND_3G, SIX_HOURS);
        validateResult(json);
    }

    @Test
    public void testTypeAPNDrilltypeSGSN4GEventId() throws Exception {
        createAggregationTables();
        final String timestamp = DateTimeUtilities.getDateTimeMinus48Hours();
        final String localDateId = dateOnlyFormatter.format(dateOnlyFormatter
                .parse(timestamp));
        createAndPopulateRawTablesForLteQuery(timestamp, localDateId);
        populateAggregationTablesForLteQuery(timestamp);
        final String result = runQuery(SAMPLE_MME, TAU_IN_4G, TWO_WEEKS);
        validateResultForLTEQuery(result);
    }

    private void validateResult(final String json) throws Exception {
        final List<KPIByAPNAndSGSNResult> results = getTranslator()
                .translateResult(json, KPIByAPNAndSGSNResult.class);
        assertThat(results.size(), is(1));
        final KPIByAPNAndSGSNResult result = results.get(0);
        validateRowResult(result, RAT_FOR_GSM, SAMPLE_APN, SAMPLE_SGSN,
                ERICSSON, SAMPLE_BSC, GSM, ISRAU_IN_2G_AND_3G, ISRAU,
                noOfErrors, noOfSuccess, noOfOccurences, noOfSubscribers);
    }

    private void validateRowResult(final KPIByAPNAndSGSNResult result,
            final int rat, final String apn, final String sgsn,
            final String vendor, final String bsc, final String ratDesc,
            final int eventID, final String eventDesc, final int errors,
            final int success, final int occurences, final int subscribers)
            throws Exception {
        assertThat(result.getRAT(), is(rat));
        assertThat(result.getAPN(), is(apn));
        assertThat(result.getSGSN(), is(sgsn));
        assertThat(result.getVendor(), is(vendor));
        assertThat(result.getController(), is(bsc));
        assertThat(result.getRATDesc(), is(ratDesc));
        assertThat(result.getEvendID(), is(eventID));
        assertThat(result.getEventDesc(), is(eventDesc));
        assertThat(result.getNoErrors(), is(errors));
        assertThat(result.getNoSuccesses(), is(success));
        assertThat(result.getOccurrences(), is(occurences));
        assertThat(result.getNoTotalErrSubscribers(), is(subscribers));
        jndiProperties.setUpJNDIPropertiesForTest();
    }

    private String runQueryTypeAPNDrilltypeSGSNBSC(final String sgsnOrMME,
            final int eventId, final String time) throws URISyntaxException {
        final MultivaluedMapImpl map = new MultivaluedMapImpl();
        map.putSingle(TYPE_PARAM, TYPE_APN);
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(TIME_QUERY_PARAM, time);
        map.putSingle(APN_PARAM, SAMPLE_APN);
        map.putSingle(BSC_PARAM, SAMPLE_BSC);
        map.putSingle(SGSN_PARAM, sgsnOrMME);
        map.putSingle(EVENT_ID_PARAM, eventId);
        map.putSingle(RAT_PARAM, RAT_FOR_GSM);
        map.putSingle(VENDOR_PARAM, ERICSSON);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, "50");
        DummyUriInfoImpl.setUriInfo(map, kpiRatioResource);
        final String result = kpiRatioResource.getData();
        System.out.println(result);
        return result;
    }

    private String runQuery(final String sgsnOrMME, final int eventId,
            final String time) throws URISyntaxException {
        final MultivaluedMapImpl map = new MultivaluedMapImpl();
        map.putSingle(TYPE_PARAM, TYPE_APN);
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(TIME_QUERY_PARAM, time);
        map.putSingle(APN_PARAM, SAMPLE_APN);
        map.putSingle(SGSN_PARAM, sgsnOrMME);
        map.putSingle(EVENT_ID_PARAM, eventId);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, "50");
        DummyUriInfoImpl.setUriInfo(map, kpiRatioResource);
        final String result = kpiRatioResource.getData();
        System.out.println(result);
        return result;
    }

    private void createAggregationTables() throws Exception {
        final Collection<String> columnsForAggTable = new ArrayList<String>();
        columnsForAggTable.add(APN);
        columnsForAggTable.add(EVENT_SOURCE_NAME);
        columnsForAggTable.add(RAT);
        columnsForAggTable.add(VENDOR_PARAM_UPPER_CASE);
        columnsForAggTable.add(HIERARCHY_3);
        columnsForAggTable.add(EVENT_ID);
        columnsForAggTable.add(NO_OF_ERRORS);
        columnsForAggTable.add(NO_OF_SUCCESSES);
        columnsForAggTable.add(NO_OF_NET_INIT_DEACTIVATES);
        columnsForAggTable.add(DATETIME_ID);
        createTemporaryTable(
                TEMP_EVENT_E_SGEH_APN_EVENTID_EVNTSRC_VEND_HIER3_ERR_DAY,
                columnsForAggTable);
        createTemporaryTable(
                TEMP_EVENT_E_SGEH_APN_EVENTID_EVNTSRC_VEND_HIER3_SUC_DAY,
                columnsForAggTable);
        createTemporaryTable(
                TEMP_EVENT_E_LTE_APN_EVENTID_EVNTSRC_VEND_HIER3_ERR_DAY,
                columnsForAggTable);
        createTemporaryTable(
                TEMP_EVENT_E_LTE_APN_EVENTID_EVNTSRC_VEND_HIER3_SUC_DAY,
                columnsForAggTable);
        createTemporaryTable(
                TEMP_EVENT_E_LTE_APN_EVENTID_EVNTSRC_VEND_HIER3_SUC_15MIN,
                columnsForAggTable);
        createTemporaryTable(
                TEMP_EVENT_E_SGEH_APN_EVENTID_EVNTSRC_VEND_HIER3_SUC_15MIN,
                columnsForAggTable);
    }

    private void validateResultForLTEQuery(final String json) throws Exception {
        final List<KPIByAPNAndSGSNResult> results = getTranslator()
                .translateResult(json, KPIByAPNAndSGSNResult.class);
        assertThat(results.size(), is(1));
        final KPIByAPNAndSGSNResult result = results.get(0);
        assertThat(result.getRAT(), is(RAT_FOR_LTE));
        assertThat(result.getAPN(), is(SAMPLE_APN));
        assertThat(result.getSGSN(), is(SAMPLE_MME));
        assertThat(result.getVendor(), is(ERICSSON));
        assertThat(result.getController(), is(SAMPLE_ERBS));
        assertThat(result.getRATDesc(), is(LTE));
        assertThat(result.getEvendID(), is(TAU_IN_4G));
        assertThat(result.getEventDesc(), is(L_TAU));
        assertThat(result.getNoErrors(), is(noOfErrors));
        assertThat(result.getNoSuccesses(), is(noOfSuccess1));
        assertThat(result.getOccurrences(), is(noOfOccurences1));
        assertThat(result.getNoTotalErrSubscribers(), is(noOfSubscribers));
    }

    private void populateAggregationTablesForLteQuery(final String timestamp)
            throws SQLException {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(APN, SAMPLE_APN);
        values.put(EVENT_SOURCE_NAME, SAMPLE_MME);
        values.put(RAT, RAT_FOR_LTE);
        values.put(VENDOR_PARAM_UPPER_CASE, ERICSSON);
        values.put(HIERARCHY_3, SAMPLE_ERBS);
        values.put(EVENT_ID, TAU_IN_4G);
        values.put(NO_OF_ERRORS, 1);
        values.put(NO_OF_SUCCESSES, 0);
        values.put(NO_OF_NET_INIT_DEACTIVATES, 1);
        values.put(DATETIME_ID, timestamp);
        insertRow(TEMP_EVENT_E_LTE_APN_EVENTID_EVNTSRC_VEND_HIER3_ERR_DAY,
                values);
    }

    private void createAndPopulateRawTablesForLteQuery(final String timestamp,
            final String localDateId) throws Exception {
        final Map<String, Object> columnsAndValuesForRawTable = new HashMap<String, Object>();
        columnsAndValuesForRawTable.put(APN, SAMPLE_APN);
        columnsAndValuesForRawTable.put(EVENT_SOURCE_NAME, SAMPLE_MME);
        columnsAndValuesForRawTable.put(VENDOR_PARAM_UPPER_CASE, ERICSSON);
        columnsAndValuesForRawTable.put(IMSI, SAMPLE_IMSI);
        columnsAndValuesForRawTable.put(HIERARCHY_3, SAMPLE_ERBS);
        columnsAndValuesForRawTable.put(EVENT_ID, TAU_IN_4G);
        columnsAndValuesForRawTable.put(RAT, RAT_FOR_LTE);
        columnsAndValuesForRawTable.put(TAC, SAMPLE_TAC);
        columnsAndValuesForRawTable.put(DEACTIVATION_TRIGGER, 1);
        columnsAndValuesForRawTable.put(DATETIME_ID, timestamp);
        columnsAndValuesForRawTable.put(LOCAL_DATE_ID, localDateId);
        createTemporaryTable(TEMP_EVENT_E_SGEH_ERR_RAW,
                columnsAndValuesForRawTable.keySet());
        createTemporaryTable(TEMP_EVENT_E_LTE_ERR_RAW,
                columnsAndValuesForRawTable.keySet());
        insertRow(TEMP_EVENT_E_LTE_ERR_RAW, columnsAndValuesForRawTable);
    }

    private void validateResultForSGEHQuery(final String json) throws Exception {
        final List<KPIByAPNAndSGSNResult> results = getTranslator()
                .translateResult(json, KPIByAPNAndSGSNResult.class);
        assertThat(results.size(), is(1));
        final KPIByAPNAndSGSNResult result = results.get(0);
        assertThat(result.getRAT(), is(RAT_FOR_GSM));
        assertThat(result.getAPN(), is(SAMPLE_APN));
        assertThat(result.getSGSN(), is(SAMPLE_SGSN));
        assertThat(result.getVendor(), is(ERICSSON));
        assertThat(result.getController(), is(SAMPLE_BSC));
        assertThat(result.getRATDesc(), is(GSM));
        assertThat(result.getEvendID(), is(ISRAU_IN_2G_AND_3G));
        assertThat(result.getEventDesc(), is(ISRAU));
        assertThat(result.getNoErrors(), is(1));
        assertThat(result.getNoSuccesses(), is(0));
        assertThat(result.getOccurrences(), is(1));
        assertThat(result.getNoTotalErrSubscribers(), is(1));
    }

    private void populateAggregationTablesForSgehQuery(final String timestamp)
            throws SQLException {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(APN, SAMPLE_APN);
        values.put(EVENT_SOURCE_NAME, SAMPLE_SGSN);
        values.put(RAT, RAT_FOR_GSM);
        values.put(VENDOR_PARAM_UPPER_CASE, ERICSSON);
        values.put(HIERARCHY_3, SAMPLE_BSC);
        values.put(EVENT_ID, ISRAU_IN_2G_AND_3G);
        values.put(NO_OF_ERRORS, 1);
        values.put(NO_OF_SUCCESSES, 0);
        values.put(NO_OF_NET_INIT_DEACTIVATES, 1);
        values.put(DATETIME_ID, timestamp);
        insertRow(TEMP_EVENT_E_SGEH_APN_EVENTID_EVNTSRC_VEND_HIER3_ERR_DAY,
                values);
        insertRow(TEMP_EVENT_E_SGEH_APN_EVENTID_EVNTSRC_VEND_HIER3_SUC_15MIN,
                values);
    }

    private void populateSuccessAggregationTablesForSgehQuery(
            final String timestamp) throws SQLException {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(APN, SAMPLE_APN);
        values.put(EVENT_SOURCE_NAME, SAMPLE_SGSN);
        values.put(RAT, RAT_FOR_GSM);
        values.put(VENDOR_PARAM_UPPER_CASE, ERICSSON);
        values.put(HIERARCHY_3, SAMPLE_BSC);
        values.put(EVENT_ID, ISRAU_IN_2G_AND_3G);
        values.put(NO_OF_ERRORS, 0);
        values.put(NO_OF_SUCCESSES, 1);
        values.put(NO_OF_NET_INIT_DEACTIVATES, 1);
        values.put(DATETIME_ID, timestamp);
        insertRow(TEMP_EVENT_E_SGEH_APN_EVENTID_EVNTSRC_VEND_HIER3_SUC_15MIN,
                values);
    }

    private void createAndPopulateRawTablesForSgehQuery(final String timestamp,
            final String localDateId) throws SQLException, Exception {
        final Map<String, Object> columnsAndValuesForRawTable = new HashMap<String, Object>();
        columnsAndValuesForRawTable.put(APN, SAMPLE_APN);
        columnsAndValuesForRawTable.put(EVENT_SOURCE_NAME, SAMPLE_SGSN);
        columnsAndValuesForRawTable.put(VENDOR_PARAM_UPPER_CASE, ERICSSON);
        columnsAndValuesForRawTable.put(IMSI, SAMPLE_IMSI);
        columnsAndValuesForRawTable.put(HIERARCHY_3, SAMPLE_BSC);
        columnsAndValuesForRawTable.put(HIERARCHY_1, SAMPLE_BSC_CELL);
        columnsAndValuesForRawTable.put(EVENT_ID, ISRAU_IN_2G_AND_3G);
        columnsAndValuesForRawTable.put(RAT, RAT_FOR_GSM);
        columnsAndValuesForRawTable.put(TAC, SAMPLE_TAC);
        columnsAndValuesForRawTable.put(DEACTIVATION_TRIGGER, 1);
        columnsAndValuesForRawTable.put(DATETIME_ID, timestamp);
        columnsAndValuesForRawTable.put(LOCAL_DATE_ID, localDateId);
        createTemporaryTable(TEMP_EVENT_E_LTE_ERR_RAW,
                columnsAndValuesForRawTable.keySet());
        createTemporaryTable(TEMP_EVENT_E_SGEH_ERR_RAW,
                columnsAndValuesForRawTable.keySet());
        createTemporaryTable(TEMP_EVENT_E_LTE_SUC_RAW,
                columnsAndValuesForRawTable.keySet());
        createTemporaryTable(TEMP_EVENT_E_SGEH_SUC_RAW,
                columnsAndValuesForRawTable.keySet());
        insertRow(TEMP_EVENT_E_LTE_SUC_RAW, columnsAndValuesForRawTable);
        insertRow(TEMP_EVENT_E_SGEH_ERR_RAW, columnsAndValuesForRawTable);
        insertRow(TEMP_EVENT_E_SGEH_SUC_RAW, columnsAndValuesForRawTable);
        columnsAndValuesForRawTable.put(TAC, SAMPLE_EXCLUSIVE_TAC_2);
        insertRow(TEMP_EVENT_E_SGEH_SUC_RAW, columnsAndValuesForRawTable);
    }
}