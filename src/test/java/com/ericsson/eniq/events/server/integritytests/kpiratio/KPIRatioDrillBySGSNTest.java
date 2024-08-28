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
import java.text.SimpleDateFormat;
import java.util.*;

import org.junit.Test;

import com.ericsson.eniq.events.server.resources.KPIRatioSgsnResource;
import com.ericsson.eniq.events.server.resources.TestsWithTemporaryTablesBaseTestCase;
import com.ericsson.eniq.events.server.test.queryresults.KPIBySGSNResult;
import com.ericsson.eniq.events.server.test.stubs.DummyUriInfoImpl;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;
import com.sun.jersey.core.util.MultivaluedMapImpl;

public class KPIRatioDrillBySGSNTest extends TestsWithTemporaryTablesBaseTestCase<KPIBySGSNResult> {

    private KPIRatioSgsnResource kpiRatioResource;

    private final SimpleDateFormat dateOnlyFormatter = new SimpleDateFormat(DATE_FORMAT);

    @Override
    public void onSetUp() throws Exception {
        super.onSetUp();
        kpiRatioResource = new KPIRatioSgsnResource();
        attachDependencies(kpiRatioResource);
    }

    @Test
    public void testTypeSGSNDrilltypeSGSN_2G3GEventId() throws Exception {
        createAggregationTables();
        final String timestamp = DateTimeUtilities.getDateTimeMinus48Hours();
        final String localDateId = dateOnlyFormatter.format(dateOnlyFormatter.parse(timestamp));
        createAndPopulateRawTablesForSgehQuery(timestamp,localDateId);
        populateAggregationTablesForSgehQuery(timestamp);
        final String result = runQuery(SAMPLE_SGSN, ISRAU_IN_2G_AND_3G, TWO_WEEKS);
        validateResultForSGEHQuery(result);
    }

    @Test
    public void testTypeSGSNDrilltypeSGSN_2G3GEventIdWithDataTiering() throws Exception {
        jndiProperties.setUpDataTieringJNDIProperty();
        createAggregationTables();
        final String timestamp = DateTimeUtilities.getDateTimeMinus25Minutes();
        final String localDateId = dateOnlyFormatter.format(dateOnlyFormatter.parse(timestamp));
        createAndPopulateRawTablesForSgehQuery(timestamp, localDateId);
        populateSuccessAggregationTablesForSgehQuery(timestamp);
        final String json = runQuery(SAMPLE_SGSN, ISRAU_IN_2G_AND_3G, THIRTY_MINUTES);

        final List<KPIBySGSNResult> results = getTranslator().translateResult(json, KPIBySGSNResult.class);

        assertThat(results.size(), is(1));
        final KPIBySGSNResult result = results.get(0);
        assertThat(result.getRAT(), is(RAT_FOR_GSM));
        assertThat(result.getSGSN(), is(SAMPLE_SGSN));
        assertThat(result.getVendor(), is(ERICSSON));
        assertThat(result.getController(), is(SAMPLE_BSC));
        assertThat(result.getRATDesc(), is(GSM));
        assertThat(result.getEvendID(), is(ISRAU_IN_2G_AND_3G));
        assertThat(result.getEventDesc(), is(ISRAU));
        assertThat(result.getNoErrors(), is(1));
        assertThat(result.getNoSuccesses(), is(1));
        assertThat(result.getOccurrences(), is(2));
        assertThat(result.getNoTotalErrSubscribers(), is(1));
        assertThat(result.getSuccessRatio(), is(50.0));

        jndiProperties.setUpJNDIPropertiesForTest();
    }

    @Test
    public void testTypeSGSNDrilltypeSGSN_2G3GEventIdWithDataTiering_6Hours() throws Exception {
        jndiProperties.setUpDataTieringJNDIProperty();
        createAggregationTables();
        final String timestamp = DateTimeUtilities.getDateTimeMinus55Minutes();
        final String localDateId = dateOnlyFormatter.format(dateOnlyFormatter.parse(timestamp));
        createAndPopulateRawTablesForSgehQuery(timestamp, localDateId);
        populateSuccessAggregationTablesForSgehQuery(timestamp);
        final String json = runQuery(SAMPLE_SGSN, ISRAU_IN_2G_AND_3G, SIX_HOURS);

        final List<KPIBySGSNResult> results = getTranslator().translateResult(json, KPIBySGSNResult.class);
        assertThat(results.size(), is(1));
        final KPIBySGSNResult result = results.get(0);
        assertThat(result.getRAT(), is(RAT_FOR_GSM));
        assertThat(result.getSGSN(), is(SAMPLE_SGSN));
        assertThat(result.getVendor(), is(ERICSSON));
        assertThat(result.getController(), is(SAMPLE_BSC));
        assertThat(result.getRATDesc(), is(GSM));
        assertThat(result.getEvendID(), is(ISRAU_IN_2G_AND_3G));
        assertThat(result.getEventDesc(), is(ISRAU));
        assertThat(result.getNoErrors(), is(1));
        assertThat(result.getNoSuccesses(), is(1));
        assertThat(result.getOccurrences(), is(2));
        assertThat(result.getNoTotalErrSubscribers(), is(1));
        assertThat(result.getSuccessRatio(), is(50.0));

        jndiProperties.setUpJNDIPropertiesForTest();
    }

    @Test
    public void testTypeSGSNDrilltypeSGSN_4GEventId() throws Exception {
        createAggregationTables();
        final String timestamp = DateTimeUtilities.getDateTimeMinus48Hours();
        final String localDateId = dateOnlyFormatter.format(dateOnlyFormatter.parse(timestamp));
        createAndPopulateRawTablesForLteQuery(timestamp,localDateId);
        populateAggregationTablesForLteQuery(timestamp);
        final String result = runQuery(SAMPLE_MME, TAU_IN_4G, TWO_WEEKS);
        validateResultForLTEQuery(result);
    }

    private String runQuery(final String sgsnOrMME, final int eventId, final String time) throws URISyntaxException {
        final MultivaluedMapImpl map = new MultivaluedMapImpl();
        map.putSingle(TYPE_PARAM, TYPE_SGSN);
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(TIME_QUERY_PARAM, time);
        map.putSingle(SGSN_PARAM, sgsnOrMME);
        map.putSingle(EVENT_ID_PARAM, eventId);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, "50");
        DummyUriInfoImpl.setUriInfoMss(map, kpiRatioResource);
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
        createTemporaryTable(TEMP_EVENT_E_SGEH_APN_EVENTID_EVNTSRC_VEND_HIER3_ERR_DAY, columnsForAggTable);
        createTemporaryTable(TEMP_EVENT_E_SGEH_APN_EVENTID_EVNTSRC_VEND_HIER3_SUC_DAY, columnsForAggTable);
        createTemporaryTable(TEMP_EVENT_E_LTE_APN_EVENTID_EVNTSRC_VEND_HIER3_ERR_DAY, columnsForAggTable);
        createTemporaryTable(TEMP_EVENT_E_LTE_APN_EVENTID_EVNTSRC_VEND_HIER3_SUC_DAY, columnsForAggTable);
        createTemporaryTable(TEMP_EVENT_E_LTE_APN_EVENTID_EVNTSRC_VEND_HIER3_SUC_15MIN, columnsForAggTable);
        createTemporaryTable(TEMP_EVENT_E_SGEH_APN_EVENTID_EVNTSRC_VEND_HIER3_SUC_15MIN, columnsForAggTable);
    }

    private void validateResultForLTEQuery(final String json) throws Exception {
        final List<KPIBySGSNResult> results = getTranslator().translateResult(json, KPIBySGSNResult.class);
        assertThat(results.size(), is(1));
        final KPIBySGSNResult result = results.get(0);
        assertThat(result.getRAT(), is(RAT_FOR_LTE));
        assertThat(result.getSGSN(), is(SAMPLE_MME));
        assertThat(result.getVendor(), is(ERICSSON));
        assertThat(result.getController(), is(SAMPLE_ERBS));
        assertThat(result.getRATDesc(), is(LTE));
        assertThat(result.getEvendID(), is(TAU_IN_4G));
        assertThat(result.getEventDesc(), is(L_TAU));
        assertThat(result.getNoErrors(), is(8));
        assertThat(result.getNoSuccesses(), is(10));
        assertThat(result.getOccurrences(), is(18));
        assertThat(result.getNoTotalErrSubscribers(), is(1));
        assertThat(result.getSuccessRatio(), is(55.56));
    }

    private void populateAggregationTablesForLteQuery(final String timestamp) throws SQLException {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(APN, SAMPLE_APN);
        values.put(EVENT_SOURCE_NAME, SAMPLE_MME);
        values.put(RAT, RAT_FOR_LTE);
        values.put(VENDOR_PARAM_UPPER_CASE, ERICSSON);
        values.put(HIERARCHY_3, SAMPLE_ERBS);
        values.put(EVENT_ID, TAU_IN_4G);
        values.put(NO_OF_ERRORS, 8);
        values.put(NO_OF_SUCCESSES, 0);
        values.put(NO_OF_NET_INIT_DEACTIVATES, 1);
        values.put(DATETIME_ID, timestamp);
        insertRow(TEMP_EVENT_E_LTE_APN_EVENTID_EVNTSRC_VEND_HIER3_ERR_DAY, values);

        values.put(APN, SAMPLE_APN);
        values.put(EVENT_SOURCE_NAME, SAMPLE_MME);
        values.put(RAT, RAT_FOR_LTE);
        values.put(VENDOR_PARAM_UPPER_CASE, ERICSSON);
        values.put(HIERARCHY_3, SAMPLE_ERBS);
        values.put(EVENT_ID, TAU_IN_4G);
        values.put(NO_OF_ERRORS, 0);
        values.put(NO_OF_SUCCESSES, 10);
        values.put(NO_OF_NET_INIT_DEACTIVATES, 1);
        values.put(DATETIME_ID, timestamp);
        insertRow(TEMP_EVENT_E_LTE_APN_EVENTID_EVNTSRC_VEND_HIER3_SUC_DAY, values);
    }

    private void createAndPopulateRawTablesForLteQuery(final String timestamp, final String localDateId) throws Exception {
        final Map<String, Object> columnsAndValuesForRawTable = new HashMap<String, Object>();
        columnsAndValuesForRawTable.put(APN, SAMPLE_APN);
        columnsAndValuesForRawTable.put(EVENT_SOURCE_NAME, SAMPLE_MME);
        columnsAndValuesForRawTable.put(VENDOR_PARAM_UPPER_CASE, ERICSSON);
        columnsAndValuesForRawTable.put(IMSI, SAMPLE_IMSI);
        columnsAndValuesForRawTable.put(HIERARCHY_3, SAMPLE_ERBS);
        columnsAndValuesForRawTable.put(EVENT_ID, TAU_IN_4G);
        columnsAndValuesForRawTable.put(RAT, RAT_FOR_LTE);
        columnsAndValuesForRawTable.put(TAC, SAMPLE_TAC);
        columnsAndValuesForRawTable.put(DATETIME_ID, timestamp);
        columnsAndValuesForRawTable.put(LOCAL_DATE_ID, localDateId);
        createTemporaryTable(TEMP_EVENT_E_SGEH_ERR_RAW, columnsAndValuesForRawTable.keySet());
        createTemporaryTable(TEMP_EVENT_E_LTE_ERR_RAW, columnsAndValuesForRawTable.keySet());
        insertRow(TEMP_EVENT_E_LTE_ERR_RAW, columnsAndValuesForRawTable);

    }

    private void validateResultForSGEHQuery(final String json) throws Exception {
        final List<KPIBySGSNResult> results = getTranslator().translateResult(json, KPIBySGSNResult.class);
        assertThat(results.size(), is(1));
        final KPIBySGSNResult result = results.get(0);
        assertThat(result.getRAT(), is(RAT_FOR_GSM));
        assertThat(result.getSGSN(), is(SAMPLE_SGSN));
        assertThat(result.getVendor(), is(ERICSSON));
        assertThat(result.getController(), is(SAMPLE_BSC));
        assertThat(result.getRATDesc(), is(GSM));
        assertThat(result.getEvendID(), is(ISRAU_IN_2G_AND_3G));
        assertThat(result.getEventDesc(), is(ISRAU));
        assertThat(result.getNoErrors(), is(12));
        assertThat(result.getNoSuccesses(), is(2));
        assertThat(result.getOccurrences(), is(14));
        assertThat(result.getNoTotalErrSubscribers(), is(1));
        assertThat(result.getSuccessRatio(), is(14.29));
    }

    private void populateAggregationTablesForSgehQuery(final String timestamp) throws SQLException {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(APN, SAMPLE_APN);
        values.put(EVENT_SOURCE_NAME, SAMPLE_SGSN);
        values.put(RAT, RAT_FOR_GSM);
        values.put(VENDOR_PARAM_UPPER_CASE, ERICSSON);
        values.put(HIERARCHY_3, SAMPLE_BSC);
        values.put(EVENT_ID, ISRAU_IN_2G_AND_3G);
        values.put(NO_OF_ERRORS, 12);
        values.put(NO_OF_SUCCESSES, 0);
        values.put(NO_OF_NET_INIT_DEACTIVATES, 1);
        values.put(DATETIME_ID, timestamp);
        insertRow(TEMP_EVENT_E_SGEH_APN_EVENTID_EVNTSRC_VEND_HIER3_ERR_DAY, values);

        values.put(APN, SAMPLE_APN);
        values.put(EVENT_SOURCE_NAME, SAMPLE_SGSN);
        values.put(RAT, RAT_FOR_GSM);
        values.put(VENDOR_PARAM_UPPER_CASE, ERICSSON);
        values.put(HIERARCHY_3, SAMPLE_BSC);
        values.put(EVENT_ID, ISRAU_IN_2G_AND_3G);
        values.put(NO_OF_ERRORS, 0);
        values.put(NO_OF_SUCCESSES, 2);
        values.put(NO_OF_NET_INIT_DEACTIVATES, 1);
        values.put(DATETIME_ID, timestamp);
        insertRow(TEMP_EVENT_E_SGEH_APN_EVENTID_EVNTSRC_VEND_HIER3_SUC_DAY, values);

    }

    private void populateSuccessAggregationTablesForSgehQuery(final String timestamp) throws SQLException {
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
        insertRow(TEMP_EVENT_E_SGEH_APN_EVENTID_EVNTSRC_VEND_HIER3_SUC_15MIN, values);
    }

    private void createAndPopulateRawTablesForSgehQuery(final String timestamp, final String localDateId) throws SQLException, Exception {
        final Map<String, Object> columnsAndValuesForRawTable = new HashMap<String, Object>();
        columnsAndValuesForRawTable.put(APN, SAMPLE_APN);
        columnsAndValuesForRawTable.put(EVENT_SOURCE_NAME, SAMPLE_SGSN);
        columnsAndValuesForRawTable.put(VENDOR_PARAM_UPPER_CASE, ERICSSON);
        columnsAndValuesForRawTable.put(IMSI, SAMPLE_IMSI);
        columnsAndValuesForRawTable.put(HIERARCHY_3, SAMPLE_BSC);
        columnsAndValuesForRawTable.put(EVENT_ID, ISRAU_IN_2G_AND_3G);
        columnsAndValuesForRawTable.put(RAT, RAT_FOR_GSM);
        columnsAndValuesForRawTable.put(TAC, SAMPLE_TAC);
        columnsAndValuesForRawTable.put(DEACTIVATION_TRIGGER, 1);
        columnsAndValuesForRawTable.put(DATETIME_ID, timestamp);
        columnsAndValuesForRawTable.put(LOCAL_DATE_ID, localDateId);
        createTemporaryTable(TEMP_EVENT_E_LTE_ERR_RAW, columnsAndValuesForRawTable.keySet());
        createTemporaryTable(TEMP_EVENT_E_SGEH_ERR_RAW, columnsAndValuesForRawTable.keySet());
        insertRow(TEMP_EVENT_E_SGEH_ERR_RAW, columnsAndValuesForRawTable);
    }
}
