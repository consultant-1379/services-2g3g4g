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
package com.ericsson.eniq.events.server.integritytests.eventanalysis.apn;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.*;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.*;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.ws.rs.core.MultivaluedMap;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.ericsson.eniq.events.server.integritytests.stubs.ReplaceTablesWithTempTablesTemplateUtils;
import com.ericsson.eniq.events.server.resources.BaseDataIntegrityTest;
import com.ericsson.eniq.events.server.serviceprovider.impl.eventanalysis.EventAnalysisService;
import com.ericsson.eniq.events.server.test.queryresults.APNEventAnalysisSummaryResult;
import com.ericsson.eniq.events.server.test.queryresults.ResultTranslator;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;
import com.sun.jersey.core.util.MultivaluedMapImpl;

public class EventAnalysisSummaryRawTablesAPNTest extends BaseDataIntegrityTest<APNEventAnalysisSummaryResult> {

    private final EventAnalysisService service = new EventAnalysisService();

    private final MultivaluedMap<String, String> requestParameters = new MultivaluedMapImpl();
    private final MultivaluedMap<String, String> requestParametersForSuccessRatioTest = new MultivaluedMapImpl();
    private final String SAMPLE_APN1 = "sampleAPN1";
    private final int NO_OF_SUCCESS_COUNT = 999780; 
    private final int SUCCESS_RATIO_RESULT_COUNT = 1;
    private final double SUCCESS_RATIO_VALUE = 99.99;

    @Before
    public void setUp() throws Exception {
        attachDependencies(service);
        ReplaceTablesWithTempTablesTemplateUtils.addTableNameToReplace(DIM_E_SGEH_EVENTTYPE, TEMP_DIM_E_SGEH_EVENTTYPE);
        ReplaceTablesWithTempTablesTemplateUtils.addTableNameToReplace(DIM_E_LTE_EVENTTYPE, TEMP_DIM_E_LTE_EVENTTYPE);
        setURLParams();
        jndiProperties.setUpDataTieringJNDIProperty();
    }

    @After
    public void tearDown() throws Exception {
        jndiProperties.setUpJNDIPropertiesForTest();
    }

    @Test
    public void getAPNEventSummaryWithDataTieringOn30MIN() throws Exception {
        createRawTable();
        insertRowData();
        createAggTable();
        insertAggData();

        final String result = runQuery(service, requestParameters);
        validateAgainstGridDefinition(result, "NETWORK_EVENT_ANALYSIS_APN");

        final ResultTranslator<APNEventAnalysisSummaryResult> rt = getTranslator();
        final List<APNEventAnalysisSummaryResult> resultList = rt.translateResult(result, APNEventAnalysisSummaryResult.class);
        assertThat(resultList.size(), is(1));

        final APNEventAnalysisSummaryResult summaryResult = resultList.get(0);
        assertThat(summaryResult.getApn(), is(SAMPLE_APN));
        assertThat(summaryResult.getEventId(), is(SGEH_DEACTIVATE_EVENT_ID));
        assertThat(summaryResult.getEventType(), is(DEACTIVATE));
        assertThat(summaryResult.getNoOfErrors(), is(1));
        assertThat(summaryResult.getNoOfSuccesses(), is(8));
        assertThat(summaryResult.getOccurrences(), is(9));
        assertThat(summaryResult.getSuccessRatio(), is(88.89));
        assertThat(summaryResult.getImpactedSubscribers(), is(1));
    }

    @Test
    public void getAPNEventSummaryWithDataTieringOn30MINForSuccessRatioTest() throws Exception {
        createRawTable();
        createAggTable();
        insertRowandAggDataForSuccessRatioTest();

        final String result = runQuery(service, requestParametersForSuccessRatioTest);
        validateAgainstGridDefinition(result, "NETWORK_EVENT_ANALYSIS_APN");

        final ResultTranslator<APNEventAnalysisSummaryResult> rt = getTranslator();
        final List<APNEventAnalysisSummaryResult> resultList = rt.translateResult(result, APNEventAnalysisSummaryResult.class);
        assertThat(resultList.size(), is(SUCCESS_RATIO_RESULT_COUNT));

        final APNEventAnalysisSummaryResult summaryResult = resultList.get(0);
        assertThat(summaryResult.getSuccessRatio(), is(SUCCESS_RATIO_VALUE));
    }

    private void insertRowandAggDataForSuccessRatioTest() throws Exception {
        final Map<String, Object> valuesForAggTable = new HashMap<String, Object>();
        final String dateTimeForAggTable = DateTimeUtilities.getDateTimeMinusMinutes(20 + SGEH_LATENCY_ON_THIRTY_MIN_QUERY);

        valuesForAggTable.put(APN, SAMPLE_APN1);
        valuesForAggTable.put(EVENT_ID, SGEH_DEACTIVATE_EVENT_ID);
        valuesForAggTable.put(NO_OF_SUCCESSES, NO_OF_SUCCESS_COUNT);
        valuesForAggTable.put(DATETIME_ID, dateTimeForAggTable);
        valuesForAggTable.put(DEACTIVATION_TRIGGER, 0);
        insertRow(TEMP_EVENT_E_LTE_APN_EVENTID_SUC_15MIN, valuesForAggTable);

        final Map<String, Object> valuesForRowTable = new HashMap<String, Object>();
        final String dateTimeRowTable = DateTimeUtilities.getDateTimeMinusMinutes(20 + SGEH_LATENCY_ON_THIRTY_MIN_QUERY);

        valuesForRowTable.put(APN, SAMPLE_APN1);
        valuesForRowTable.put(EVENT_ID, SGEH_DEACTIVATE_EVENT_ID);
        valuesForRowTable.put(IMSI, SAMPLE_IMSI);
        valuesForRowTable.put(TAC, SAMPLE_TAC);
        valuesForRowTable.put(DATETIME_ID, dateTimeRowTable);
        valuesForRowTable.put(DEACTIVATION_TRIGGER, 0);
        insertRow(TEMP_EVENT_E_LTE_ERR_RAW, valuesForRowTable);

    }

    private void insertAggData() throws Exception {
        final Map<String, Object> values = new HashMap<String, Object>();
        final String dateTime = DateTimeUtilities.getDateTimeMinusMinutes(20 + SGEH_LATENCY_ON_THIRTY_MIN_QUERY);

        values.put(APN, SAMPLE_APN);
        values.put(EVENT_ID, SGEH_DEACTIVATE_EVENT_ID);
        values.put(NO_OF_SUCCESSES, 8);
        values.put(DATETIME_ID, dateTime);
        values.put(DEACTIVATION_TRIGGER, 0);
        insertRow(TEMP_EVENT_E_SGEH_APN_EVENTID_SUC_15MIN, values);
    }

    private void createAggTable() throws Exception {
        final Collection<String> columns = new ArrayList<String>();
        columns.add(APN);
        columns.add(EVENT_ID);
        columns.add(NO_OF_SUCCESSES);
        columns.add(DATETIME_ID);
        columns.add(DEACTIVATION_TRIGGER);
        createTemporaryTable(TEMP_EVENT_E_SGEH_APN_EVENTID_SUC_15MIN, columns);
        createTemporaryTable(TEMP_EVENT_E_LTE_APN_EVENTID_SUC_15MIN, columns);
    }

    private void createRawTable() throws Exception {
        final Collection<String> columns = new ArrayList<String>();
        columns.add(APN);
        columns.add(EVENT_ID);
        columns.add(IMSI);
        columns.add(TAC);
        columns.add(DATETIME_ID);
        columns.add(DEACTIVATION_TRIGGER);
        createTemporaryTable(TEMP_EVENT_E_SGEH_ERR_RAW, columns);
        createTemporaryTable(TEMP_EVENT_E_LTE_ERR_RAW, columns);
        createTemporaryTable(TEMP_EVENT_E_SGEH_SUC_RAW, columns);
        createTemporaryTable(TEMP_EVENT_E_LTE_SUC_RAW, columns);

        createAndPopulateLookupTable(TEMP_DIM_E_SGEH_EVENTTYPE);
        createAndPopulateLookupTable(TEMP_DIM_E_LTE_EVENTTYPE);
    }

    private void insertRowData() throws Exception {
        final Map<String, Object> values = new HashMap<String, Object>();
        final String dateTime = DateTimeUtilities.getDateTimeMinusMinutes(20 + SGEH_LATENCY_ON_THIRTY_MIN_QUERY);

        values.put(APN, SAMPLE_APN);
        values.put(EVENT_ID, SGEH_DEACTIVATE_EVENT_ID);
        values.put(IMSI, SAMPLE_IMSI);
        values.put(TAC, SAMPLE_TAC);
        values.put(DATETIME_ID, dateTime);
        values.put(DEACTIVATION_TRIGGER, 0);
        insertRow(TEMP_EVENT_E_SGEH_ERR_RAW, values);
        insertRow(TEMP_EVENT_E_SGEH_SUC_RAW, values);
    }

    private void setURLParams() {
        requestParameters.putSingle(TIME_QUERY_PARAM, THIRTY_MINUTES);
        requestParameters.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        requestParameters.putSingle(TYPE_PARAM, TYPE_APN);
        requestParameters.putSingle(NODE_PARAM, SAMPLE_APN);
        requestParameters.putSingle(DISPLAY_PARAM, GRID);
        requestParameters.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        requestParameters.putSingle(MAX_ROWS, "500");

        requestParametersForSuccessRatioTest.putSingle(TIME_QUERY_PARAM, THIRTY_MINUTES);
        requestParametersForSuccessRatioTest.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        requestParametersForSuccessRatioTest.putSingle(TYPE_PARAM, TYPE_APN);
        requestParametersForSuccessRatioTest.putSingle(NODE_PARAM, SAMPLE_APN1);
        requestParametersForSuccessRatioTest.putSingle(DISPLAY_PARAM, GRID);
        requestParametersForSuccessRatioTest.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        requestParametersForSuccessRatioTest.putSingle(MAX_ROWS, "500");
    }

}
