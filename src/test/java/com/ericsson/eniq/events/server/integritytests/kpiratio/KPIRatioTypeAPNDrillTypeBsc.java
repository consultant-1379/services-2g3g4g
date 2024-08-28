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

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.common.EventIDConstants.*;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.*;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.*;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.*;

import org.junit.Test;

import com.ericsson.eniq.events.server.common.ApplicationConstants;
import com.ericsson.eniq.events.server.resources.KPIRatioBscResource;
import com.ericsson.eniq.events.server.resources.TestsWithTemporaryTablesBaseTestCase;
import com.ericsson.eniq.events.server.test.queryresults.KPIByAPNFromBSCResult;
import com.ericsson.eniq.events.server.test.schema.Nullable;
import com.ericsson.eniq.events.server.test.stubs.DummyUriInfoImpl;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;
import com.sun.jersey.core.util.MultivaluedMapImpl;

public class KPIRatioTypeAPNDrillTypeBsc extends TestsWithTemporaryTablesBaseTestCase<KPIByAPNFromBSCResult> {

    private static final String UNKNOWN_ECELL_ID = "3322";
    private static final String UNKNOWN_ENODEB_ID = "111";
    private static final String UNKNOWN_GSM_CELL_ID = "121";

    private KPIRatioBscResource kpiRatioResource;

    @Override
    public void onSetUp() throws Exception {
        super.onSetUp();
        kpiRatioResource = new KPIRatioBscResource();
        attachDependencies(kpiRatioResource);
    }

    @Test
    public void testTypeAPNDrilltypeBSC_2G3GEventId() throws Exception {
        final String timestamp = DateTimeUtilities.getDateTimeMinus48Hours();
        createAndPopulateRawTablesForSgehQuery(timestamp, 12, 2);
        final String result = runQuery(SAMPLE_SGSN, ISRAU_IN_2G_AND_3G, TWO_WEEKS, ERICSSON, SAMPLE_BSC, Integer.toString(RAT_FOR_GSM));
        validateResultForSGEHQuery(result);
    }

    @Test
    public void testTypeAPNDrilltypeSGSN_2G3GEventIdWithDataTiering() throws Exception {
        jndiProperties.setUpDataTieringJNDIProperty();
        final String timestamp = DateTimeUtilities.getDateTimeMinus25Minutes();
        createAndPopulateRawTablesForSgehQuery(timestamp, 1, 1);
        final String json = runQuery(SAMPLE_SGSN, ISRAU_IN_2G_AND_3G, THIRTY_MINUTES, ERICSSON, SAMPLE_BSC, Integer.toString(RAT_FOR_GSM));

        final List<KPIByAPNFromBSCResult> results = getTranslator().translateResult(json, KPIByAPNFromBSCResult.class);

        assertThat(results.size(), is(1));
        final KPIByAPNFromBSCResult result = results.get(0);
        assertThat(result.getRAT(), is(RAT_FOR_GSM));
        assertThat(result.getSGSN(), is(SAMPLE_SGSN));
        assertThat(result.getVendor(), is(ERICSSON));
        assertThat(result.getController(), is(SAMPLE_BSC));
        assertThat(result.getAccessArea(), is(SAMPLE_BSC_CELL));
        assertThat(result.getRATDesc(), is(GSM));
        assertThat(result.getEvendID(), is(ISRAU_IN_2G_AND_3G));
        assertThat(result.getEventDesc(), is(ISRAU));
        assertThat(result.getNoErrors(), is(1));
        assertThat(result.getNoSuccesses(), is(1));
        assertThat(result.getOccurrences(), is(2));
        assertThat(result.getNoTotalErrSubscribers(), is(1));
        assertThat(result.getSuccessRatio(), is(50.0));
        assertThat(result.getAPN(), is(SAMPLE_APN));

        jndiProperties.setUpJNDIPropertiesForTest();
    }

    @Test
    public void testTypeAPNDrilltypeSGSN_2G3GEventIdWithDataTiering_6Hours() throws Exception {
        jndiProperties.setUpDataTieringJNDIProperty();
        final String timestamp = DateTimeUtilities.getDateTimeMinus55Minutes();
        createAndPopulateRawTablesForSgehQuery(timestamp, 1, 1);
        final String json = runQuery(SAMPLE_SGSN, ISRAU_IN_2G_AND_3G, SIX_HOURS, ERICSSON, SAMPLE_BSC, Integer.toString(RAT_FOR_GSM));

        final List<KPIByAPNFromBSCResult> results = getTranslator().translateResult(json, KPIByAPNFromBSCResult.class);
        assertThat(results.size(), is(1));
        final KPIByAPNFromBSCResult result = results.get(0);
        assertThat(result.getRAT(), is(RAT_FOR_GSM));
        assertThat(result.getSGSN(), is(SAMPLE_SGSN));
        assertThat(result.getVendor(), is(ERICSSON));
        assertThat(result.getController(), is(SAMPLE_BSC));
        assertThat(result.getAccessArea(), is(SAMPLE_BSC_CELL));
        assertThat(result.getRATDesc(), is(GSM));
        assertThat(result.getEvendID(), is(ISRAU_IN_2G_AND_3G));
        assertThat(result.getEventDesc(), is(ISRAU));
        assertThat(result.getNoErrors(), is(1));
        assertThat(result.getNoSuccesses(), is(1));
        assertThat(result.getOccurrences(), is(2));
        assertThat(result.getNoTotalErrSubscribers(), is(1));
        assertThat(result.getSuccessRatio(), is(50.0));
        assertThat(result.getAPN(), is(SAMPLE_APN));

        jndiProperties.setUpJNDIPropertiesForTest();
    }

    @Test
    public void testTypeAPNDrilltypeSGSN_4GEventId() throws Exception {
        final String timestamp = DateTimeUtilities.getDateTimeMinus48Hours();
        createAndPopulateRawTablesForLteQuery(timestamp, 8, 10);
        final String result = runQuery(SAMPLE_MME, TAU_IN_4G, TWO_WEEKS, ERICSSON, SAMPLE_ERBS, Integer.toString(RAT_FOR_LTE));
        validateResultForLTEQuery(result);
    }

    @Test
    public void testTypeAPNDrilltypeSGSN_2G3GEventIdWithDataTiering_6HoursWithImsiAgg() throws Exception {
        jndiProperties.disableSucRawJNDIProperty();
        createImsiAggregationTables();
        createTopologyTables();
        populateTopologyForSgehQuery();
        populateTopologyForLteQuery();
        final String timestamp = DateTimeUtilities.getDateTimeMinus55Minutes();
        populateSuccessImsiAggregationTablesForLteQuery(timestamp);
        populateSuccessImsiAggregationTablesForSgehQuery(timestamp);
        createAndPopulateRawTablesForSgehQuery(timestamp, 1, 1);
        final String json = runQuery(SAMPLE_SGSN, ISRAU_IN_2G_AND_3G, SIX_HOURS, ERICSSON, SAMPLE_BSC, Integer.toString(RAT_FOR_GSM));

        final List<KPIByAPNFromBSCResult> results = getTranslator().translateResult(json, KPIByAPNFromBSCResult.class);
        assertThat(results.size(), is(1));
        final KPIByAPNFromBSCResult result = results.get(0);
        assertThat(result.getRAT(), is(RAT_FOR_GSM));
        assertThat(result.getSGSN(), is(SAMPLE_SGSN));
        assertThat(result.getVendor(), is(ERICSSON));
        assertThat(result.getController(), is(SAMPLE_BSC));
        assertThat(result.getAccessArea(), is(SAMPLE_BSC_CELL));
        assertThat(result.getRATDesc(), is(GSM));
        assertThat(result.getEvendID(), is(ISRAU_IN_2G_AND_3G));
        assertThat(result.getEventDesc(), is(ISRAU));
        assertThat(result.getNoErrors(), is(1));
        assertThat(result.getNoSuccesses(), is(4));
        assertThat(result.getOccurrences(), is(5));
        assertThat(result.getNoTotalErrSubscribers(), is(1));
        assertThat(result.getSuccessRatio(), is(80.0));
        assertThat(result.getAPN(), is(SAMPLE_APN));

        jndiProperties.setUpJNDIPropertiesForTest();
    }

    @Test
    public void testTypeAPNDrilltypeSGSN_2G3GEventIdWithDataTiering_6HoursWithImsiAggWithUnknownToplogy() throws Exception {
        jndiProperties.disableSucRawJNDIProperty();
        createImsiAggregationTables();
        createTopologyTables();
        populateTopologyForSgehQuery();
        populateTopologyForLteQuery();
        final String timestamp = DateTimeUtilities.getDateTimeMinus55Minutes();
        populateSuccessImsiAggregationTablesForLteQuery(timestamp);
        populateSuccessImsiAggregationTablesForSgehQuery(timestamp);
        createAndPopulateRawTablesForSgehQuery(timestamp, 1, 1);
        final String json = runQuery(SAMPLE_SGSN, ISRAU_IN_2G_AND_3G, SIX_HOURS, ApplicationConstants.UNKNOWN, ApplicationConstants.UNKNOWN,
                Integer.toString(RAT_FOR_GSM));

        final List<KPIByAPNFromBSCResult> results = getTranslator().translateResult(json, KPIByAPNFromBSCResult.class);
        assertThat(results.size(), is(1));
        final KPIByAPNFromBSCResult result = results.get(0);
        assertThat(result.getRAT(), is(RAT_FOR_GSM));
        assertThat(result.getSGSN(), is(SAMPLE_SGSN));
        assertThat(result.getVendor(), is(ApplicationConstants.UNKNOWN));
        assertThat(result.getController(), is(ApplicationConstants.UNKNOWN));
        assertThat(result.getAccessArea(), is(UNKNOWN_GSM_CELL_ID));
        assertThat(result.getRATDesc(), is(GSM));
        assertThat(result.getEvendID(), is(ISRAU_IN_2G_AND_3G));
        assertThat(result.getEventDesc(), is(ISRAU));
        assertThat(result.getNoErrors(), is(21));
        assertThat(result.getNoSuccesses(), is(41));
        assertThat(result.getOccurrences(), is(62));
        assertThat(result.getNoTotalErrSubscribers(), is(1));
        assertThat(result.getSuccessRatio(), is(66.13));
        assertThat(result.getAPN(), is(SAMPLE_APN));

        jndiProperties.setUpJNDIPropertiesForTest();
    }

    @Test
    public void testTypeAPNDrilltypeSGSN_4GEventIdWithImsiAgg() throws Exception {
        jndiProperties.disableSucRawJNDIProperty();
        createImsiAggregationTables();
        createTopologyTables();
        populateTopologyForSgehQuery();
        populateTopologyForLteQuery();
        final String timestamp = DateTimeUtilities.getDateTimeMinus48Hours();
        populateSuccessImsiAggregationTablesForLteQuery(timestamp);
        populateSuccessImsiAggregationTablesForSgehQuery(timestamp);
        createAndPopulateRawTablesForLteQuery(timestamp, 8, 10);
        final String result = runQuery(SAMPLE_MME, TAU_IN_4G, TWO_WEEKS, ERICSSON, SAMPLE_ERBS, Integer.toString(RAT_FOR_LTE));
        validateResultForLTEQuery(result);
    }

    @Test
    public void testTypeAPNDrilltypeSGSN_4GEventIdWithImsiAggWithUnknownToplogy() throws Exception {
        jndiProperties.disableSucRawJNDIProperty();
        createImsiAggregationTables();
        createTopologyTables();
        populateTopologyForSgehQuery();
        populateTopologyForLteQuery();
        final String timestamp = DateTimeUtilities.getDateTimeMinus48Hours();
        populateSuccessImsiAggregationTablesForLteQuery(timestamp);
        populateSuccessImsiAggregationTablesForSgehQuery(timestamp);
        createAndPopulateRawTablesForLteQuery(timestamp, 8, 10);
        final String result = runQuery(SAMPLE_MME, TAU_IN_4G, TWO_WEEKS, ApplicationConstants.UNKNOWN, UNKNOWN_ENODEB_ID,
                Integer.toString(RAT_FOR_LTE));
        validateResultForLTEQueryUnknownTopology(result);
    }

    private String runQuery(final String sgsnOrMME, final int eventId, final String time, final String vendor, final String hier3, final String rat)
            throws URISyntaxException {
        final MultivaluedMapImpl map = new MultivaluedMapImpl();
        map.putSingle(TYPE_PARAM, TYPE_APN);
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(TIME_QUERY_PARAM, time);
        map.putSingle(APN_PARAM, SAMPLE_APN);
        map.putSingle(SGSN_PARAM, sgsnOrMME);
        map.putSingle(BSC_PARAM, hier3);
        map.putSingle(VENDOR_PARAM, vendor);
        map.putSingle(RAT, rat);
        map.putSingle(EVENT_ID_PARAM, eventId);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, "50");
        DummyUriInfoImpl.setUriInfoForMss(map, kpiRatioResource);
        final String result = kpiRatioResource.getData();
        System.out.println(result);
        return result;
    }

    private void createImsiAggregationTables() throws Exception {
        final Map<String, Nullable> columnsForAggTable = new HashMap<String, Nullable>();
        columnsForAggTable.put(APN, Nullable.CAN_BE_NULL);
        columnsForAggTable.put(EVENT_SOURCE_NAME, Nullable.CAN_BE_NULL);
        columnsForAggTable.put(RAT, Nullable.CAN_BE_NULL);
        columnsForAggTable.put(VENDOR_PARAM_UPPER_CASE, Nullable.CAN_BE_NULL);
        columnsForAggTable.put(HIER3_ID, Nullable.CAN_BE_NULL);
        columnsForAggTable.put(HIER321_ID, Nullable.CAN_BE_NULL);
        columnsForAggTable.put(EVENT_ID, Nullable.CAN_BE_NULL);
        columnsForAggTable.put(NO_OF_ERRORS, Nullable.CAN_BE_NULL);
        columnsForAggTable.put(NO_OF_SUCCESSES, Nullable.CAN_BE_NULL);
        columnsForAggTable.put(NO_OF_NET_INIT_DEACTIVATES, Nullable.CAN_BE_NULL);
        columnsForAggTable.put(DATETIME_ID, Nullable.CAN_BE_NULL);
        columnsForAggTable.put(TAC, Nullable.CAN_BE_NULL);
        createTemporaryTable(TEMP_EVENT_E_SGEH_IMSI_SUC_RAW, columnsForAggTable);
        createTemporaryTable(TEMP_EVENT_E_LTE_IMSI_SUC_RAW, columnsForAggTable);
    }

    private void createTopologyTables() throws Exception {
        final Collection<String> columnsForTopologyTable = new ArrayList<String>();
        columnsForTopologyTable.add(RAT);
        columnsForTopologyTable.add(VENDOR_PARAM_UPPER_CASE);
        columnsForTopologyTable.add(HIERARCHY_3);
        columnsForTopologyTable.add(HIERARCHY_1);
        columnsForTopologyTable.add(HIER3_ID);
        columnsForTopologyTable.add(HIER321_ID);
        createTemporaryTable(TEMP_DIM_E_LTE_HIER321, columnsForTopologyTable);
        createTemporaryTable(TEMP_DIM_E_SGEH_HIER321, columnsForTopologyTable);
    }

    private void populateTopologyForSgehQuery() throws SQLException {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(RAT, RAT_FOR_GSM);
        values.put(VENDOR_PARAM_UPPER_CASE, ERICSSON);
        values.put(HIERARCHY_3, SAMPLE_BSC);
        values.put(HIERARCHY_1, SAMPLE_BSC_CELL);
        values.put(HIER3_ID, 111);
        values.put(HIER321_ID, 11122);
        insertRow(TEMP_DIM_E_SGEH_HIER321, values);
    }

    private void populateSuccessImsiAggregationTablesForSgehQuery(final String timestamp) throws SQLException {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(APN, SAMPLE_APN);
        values.put(EVENT_SOURCE_NAME, SAMPLE_SGSN);
        values.put(RAT, RAT_FOR_GSM);
        values.put(VENDOR_PARAM_UPPER_CASE, ERICSSON);
        values.put(HIER3_ID, 111);
        values.put(HIER321_ID, 11122);
        values.put(NO_OF_ERRORS, 0);
        values.put(NO_OF_SUCCESSES, 4);
        values.put(NO_OF_NET_INIT_DEACTIVATES, 1);
        values.put(DATETIME_ID, timestamp);
        values.put(EVENT_ID, ISRAU_IN_2G_AND_3G);
        values.put(TAC, SAMPLE_TAC);
        insertRow(TEMP_EVENT_E_SGEH_IMSI_SUC_RAW, values);
        values.put(APN, SAMPLE_APN);
        values.put(EVENT_SOURCE_NAME, SAMPLE_SGSN);
        values.put(RAT, RAT_FOR_GSM);
        values.put(VENDOR_PARAM_UPPER_CASE, ApplicationConstants.UNKNOWN);
        values.put(HIER3_ID, null);
        values.put(HIER321_ID, UNKNOWN_GSM_CELL_ID);
        values.put(NO_OF_ERRORS, 0);
        values.put(NO_OF_SUCCESSES, 41);
        values.put(NO_OF_NET_INIT_DEACTIVATES, 1);
        values.put(DATETIME_ID, timestamp);
        values.put(EVENT_ID, ISRAU_IN_2G_AND_3G);
        values.put(TAC, SAMPLE_TAC);
        insertRow(TEMP_EVENT_E_SGEH_IMSI_SUC_RAW, values);
    }

    private void populateTopologyForLteQuery() throws SQLException {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(RAT, RAT_FOR_LTE);
        values.put(VENDOR_PARAM_UPPER_CASE, ERICSSON);
        values.put(HIERARCHY_3, SAMPLE_ERBS);
        values.put(HIERARCHY_1, SAMPLE_ERBS_CELL);
        values.put(HIER3_ID, 33);
        values.put(HIER321_ID, 3322);
        insertRow(TEMP_DIM_E_LTE_HIER321, values);
    }

    private void populateSuccessImsiAggregationTablesForLteQuery(final String timestamp) throws SQLException {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(APN, SAMPLE_APN);
        values.put(EVENT_SOURCE_NAME, SAMPLE_MME);
        values.put(RAT, RAT_FOR_LTE);
        values.put(VENDOR_PARAM_UPPER_CASE, ERICSSON);
        values.put(HIER3_ID, 33);
        values.put(HIER321_ID, 3322);
        values.put(NO_OF_ERRORS, 0);
        values.put(NO_OF_SUCCESSES, 10);
        values.put(NO_OF_NET_INIT_DEACTIVATES, 1);
        values.put(DATETIME_ID, timestamp);
        values.put(EVENT_ID, TAU_IN_4G);
        values.put(TAC, SAMPLE_TAC);
        insertRow(TEMP_EVENT_E_LTE_IMSI_SUC_RAW, values);
        values.put(APN, SAMPLE_APN);
        values.put(EVENT_SOURCE_NAME, SAMPLE_MME);
        values.put(RAT, RAT_FOR_LTE);
        values.put(VENDOR_PARAM_UPPER_CASE, ApplicationConstants.UNKNOWN);
        values.put(HIER3_ID, UNKNOWN_ENODEB_ID);
        values.put(HIER321_ID, UNKNOWN_ECELL_ID);
        values.put(NO_OF_ERRORS, 0);
        values.put(NO_OF_SUCCESSES, 11);
        values.put(NO_OF_NET_INIT_DEACTIVATES, 1);
        values.put(DATETIME_ID, timestamp);
        values.put(EVENT_ID, TAU_IN_4G);
        values.put(TAC, SAMPLE_TAC);
        insertRow(TEMP_EVENT_E_LTE_IMSI_SUC_RAW, values);
    }

    private void validateResultForLTEQuery(final String json) throws Exception {
        final List<KPIByAPNFromBSCResult> results = getTranslator().translateResult(json, KPIByAPNFromBSCResult.class);
        assertThat(results.size(), is(1));
        final KPIByAPNFromBSCResult result = results.get(0);
        assertThat(result.getRAT(), is(RAT_FOR_LTE));
        assertThat(result.getSGSN(), is(SAMPLE_MME));
        assertThat(result.getVendor(), is(ERICSSON));
        assertThat(result.getController(), is(SAMPLE_ERBS));
        assertThat(result.getAccessArea(), is(SAMPLE_ERBS_CELL));
        assertThat(result.getRATDesc(), is(LTE));
        assertThat(result.getEvendID(), is(TAU_IN_4G));
        assertThat(result.getEventDesc(), is(L_TAU));
        assertThat(result.getNoErrors(), is(8));
        assertThat(result.getNoSuccesses(), is(10));
        assertThat(result.getOccurrences(), is(18));
        assertThat(result.getNoTotalErrSubscribers(), is(1));
        assertThat(result.getSuccessRatio(), is(55.56));
        assertThat(result.getAPN(), is(SAMPLE_APN));
    }

    private void validateResultForLTEQueryUnknownTopology(final String json) throws Exception {
        final List<KPIByAPNFromBSCResult> results = getTranslator().translateResult(json, KPIByAPNFromBSCResult.class);
        assertThat(results.size(), is(1));
        final KPIByAPNFromBSCResult result = results.get(0);
        assertThat(result.getRAT(), is(RAT_FOR_LTE));
        assertThat(result.getSGSN(), is(SAMPLE_MME));
        assertThat(result.getVendor(), is(ApplicationConstants.UNKNOWN));
        assertThat(result.getController(), is(UNKNOWN_ENODEB_ID));
        assertThat(result.getAccessArea(), is(UNKNOWN_ECELL_ID));
        assertThat(result.getRATDesc(), is(LTE));
        assertThat(result.getEvendID(), is(TAU_IN_4G));
        assertThat(result.getEventDesc(), is(L_TAU));
        assertThat(result.getNoErrors(), is(2));
        assertThat(result.getNoSuccesses(), is(11));
        assertThat(result.getOccurrences(), is(13));
        assertThat(result.getNoTotalErrSubscribers(), is(1));
        assertThat(result.getSuccessRatio(), is(84.62));
        assertThat(result.getAPN(), is(SAMPLE_APN));
    }

    private void createAndPopulateRawTablesForLteQuery(final String timestamp, final int noOfErrorRowsInRaw, final int noOfSuccessRowsInRaw)
            throws Exception {
        final Map<String, Object> columnsAndValuesForRawTable = new HashMap<String, Object>();
        columnsAndValuesForRawTable.put(APN, SAMPLE_APN);
        columnsAndValuesForRawTable.put(EVENT_SOURCE_NAME, SAMPLE_MME);
        columnsAndValuesForRawTable.put(VENDOR_PARAM_UPPER_CASE, ERICSSON);
        columnsAndValuesForRawTable.put(IMSI, SAMPLE_IMSI);
        columnsAndValuesForRawTable.put(HIERARCHY_3, SAMPLE_ERBS);
        columnsAndValuesForRawTable.put(HIERARCHY_1, SAMPLE_ERBS_CELL);
        columnsAndValuesForRawTable.put(EVENT_ID, TAU_IN_4G);
        columnsAndValuesForRawTable.put(RAT, RAT_FOR_LTE);
        columnsAndValuesForRawTable.put(TAC, SAMPLE_TAC);
        columnsAndValuesForRawTable.put(DATETIME_ID, timestamp);
        columnsAndValuesForRawTable.put(DEACTIVATION_TRIGGER, 1);

        createTemporaryTable(TEMP_EVENT_E_SGEH_ERR_RAW, columnsAndValuesForRawTable.keySet());
        createTemporaryTable(TEMP_EVENT_E_SGEH_SUC_RAW, columnsAndValuesForRawTable.keySet());
        createTemporaryTable(TEMP_EVENT_E_LTE_ERR_RAW, columnsAndValuesForRawTable.keySet());
        createTemporaryTable(TEMP_EVENT_E_LTE_SUC_RAW, columnsAndValuesForRawTable.keySet());

        for (int i = 0; i < noOfErrorRowsInRaw; i++) {
            insertRow(TEMP_EVENT_E_LTE_ERR_RAW, columnsAndValuesForRawTable);
        }

        for (int i = 0; i < noOfSuccessRowsInRaw; i++) {
            insertRow(TEMP_EVENT_E_LTE_SUC_RAW, columnsAndValuesForRawTable);
        }

        //Add some unknown topology
        columnsAndValuesForRawTable.put(HIERARCHY_3, UNKNOWN_ENODEB_ID);
        columnsAndValuesForRawTable.put(HIERARCHY_1, UNKNOWN_ECELL_ID);
        columnsAndValuesForRawTable.put(VENDOR_PARAM_UPPER_CASE, ApplicationConstants.UNKNOWN);

        for (int i = 0; i < 2; i++) {
            insertRow(TEMP_EVENT_E_LTE_ERR_RAW, columnsAndValuesForRawTable);
        }

        for (int i = 0; i < 3; i++) {
            insertRow(TEMP_EVENT_E_LTE_SUC_RAW, columnsAndValuesForRawTable);
        }
    }

    private void validateResultForSGEHQuery(final String json) throws Exception {
        final List<KPIByAPNFromBSCResult> results = getTranslator().translateResult(json, KPIByAPNFromBSCResult.class);
        assertThat(results.size(), is(1));
        final KPIByAPNFromBSCResult result = results.get(0);
        assertThat(result.getRAT(), is(RAT_FOR_GSM));
        assertThat(result.getSGSN(), is(SAMPLE_SGSN));
        assertThat(result.getVendor(), is(ERICSSON));
        assertThat(result.getController(), is(SAMPLE_BSC));
        assertThat(result.getAccessArea(), is(SAMPLE_BSC_CELL));
        assertThat(result.getRATDesc(), is(GSM));
        assertThat(result.getEvendID(), is(ISRAU_IN_2G_AND_3G));
        assertThat(result.getEventDesc(), is(ISRAU));
        assertThat(result.getNoErrors(), is(12));
        assertThat(result.getNoSuccesses(), is(2));
        assertThat(result.getOccurrences(), is(14));
        assertThat(result.getNoTotalErrSubscribers(), is(1));
        assertThat(result.getSuccessRatio(), is(14.29));
        assertThat(result.getAPN(), is(SAMPLE_APN));
    }

    private void createAndPopulateRawTablesForSgehQuery(final String timestamp, final int noOfErrorRowsInRaw, final int noOfSuccessRowsInRaw)
            throws SQLException, Exception {
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
        columnsAndValuesForRawTable.put(DEACTIVATION_TRIGGER, 1);

        createTemporaryTable(TEMP_EVENT_E_LTE_ERR_RAW, columnsAndValuesForRawTable.keySet());
        createTemporaryTable(TEMP_EVENT_E_LTE_SUC_RAW, columnsAndValuesForRawTable.keySet());
        createTemporaryTable(TEMP_EVENT_E_SGEH_ERR_RAW, columnsAndValuesForRawTable.keySet());
        createTemporaryTable(TEMP_EVENT_E_SGEH_SUC_RAW, columnsAndValuesForRawTable.keySet());
        for (int i = 0; i < noOfErrorRowsInRaw; i++) {
            insertRow(TEMP_EVENT_E_SGEH_ERR_RAW, columnsAndValuesForRawTable);
        }

        for (int i = 0; i < noOfSuccessRowsInRaw; i++) {
            insertRow(TEMP_EVENT_E_SGEH_SUC_RAW, columnsAndValuesForRawTable);
        }

        //Add some unknown topology
        columnsAndValuesForRawTable.put(HIERARCHY_3, ApplicationConstants.UNKNOWN);
        columnsAndValuesForRawTable.put(HIERARCHY_1, UNKNOWN_GSM_CELL_ID);
        columnsAndValuesForRawTable.put(VENDOR_PARAM_UPPER_CASE, ApplicationConstants.UNKNOWN);

        for (int i = 0; i < 21; i++) {
            insertRow(TEMP_EVENT_E_SGEH_ERR_RAW, columnsAndValuesForRawTable);
        }

        for (int i = 0; i < 3; i++) {
            insertRow(TEMP_EVENT_E_SGEH_SUC_RAW, columnsAndValuesForRawTable);
        }
    }
}
