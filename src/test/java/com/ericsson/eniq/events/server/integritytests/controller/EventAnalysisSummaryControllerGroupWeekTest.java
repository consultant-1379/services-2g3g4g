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
package com.ericsson.eniq.events.server.integritytests.controller;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.common.EventIDConstants.*;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.*;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.*;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.sql.SQLException;
import java.util.*;

import javax.ws.rs.core.MultivaluedMap;

import org.junit.Before;
import org.junit.Test;

import com.ericsson.eniq.events.server.resources.BaseDataIntegrityTest;
import com.ericsson.eniq.events.server.serviceprovider.impl.eventanalysis.EventAnalysisService;
import com.ericsson.eniq.events.server.test.queryresults.*;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;
import com.sun.jersey.core.util.MultivaluedMapImpl;

public class EventAnalysisSummaryControllerGroupWeekTest extends BaseDataIntegrityTest<EventAnalysisControllerGroupSummaryResult>{

    private static final int TEST_DEACTIVATE_TRIGGER_VALUE = 1;

    private static final int HIER3_ID_VALUE_LTE = 9101122;

    private static final String MIDNIGHT = " 00:00:000";

    public static final String CONTROLLER_GROUP_NAME_2G_3G = "controller_group1";

    public static final String CONTROLLER_GROUP_NAME_4G = "controller_group2";

    private static final int NO_OF_SUCCESSES_2G_3G = 425;

    private static final int NO_OF_SUCCESSES_4G = 327;

    private static final int NO_OF_ERRORS_2G_3G = 56;

    private static final int NO_OF_ERRORS_4G = 23;

    public static final String HIERARCHY_1_STR = "h1_string";

    private final  EventAnalysisService service = new EventAnalysisService();

    private final List<String> tempDataTables = new ArrayList<String>();

    private final Collection<String> rawTableColumns = new ArrayList<String>();

    private final Collection<String> groupRatVendHier3Columns = new ArrayList<String>();

    private final Collection<String> vendHier3EventIdDayColumns = new ArrayList<String>();

    private final Set<Integer> listOfEventTypesGroup1 = new TreeSet<Integer>();

    private final Set<Integer> listOfEventTypesGroup2 = new TreeSet<Integer>();

    @Before
    public void onSetUp() throws Exception {
        attachDependencies(service);

        createTablesForTest();

        for (final String tempTable : tempDataTables) {
            createTemporaryTable(tempTable, rawTableColumns);
        }

        createTemporaryTable(TEMP_GROUP_TYPE_E_RAT_VEND_HIER3, groupRatVendHier3Columns);
        createTemporaryTable(TEMP_EVENT_E_SGEH_VEND_HIER3_EVENTID_SUC_DAY, vendHier3EventIdDayColumns);
        createTemporaryTable(TEMP_EVENT_E_LTE_VEND_HIER3_EVENTID_SUC_DAY, vendHier3EventIdDayColumns);
        createTemporaryTable(TEMP_EVENT_E_SGEH_VEND_HIER3_EVENTID_ERR_DAY, vendHier3EventIdDayColumns);
        createTemporaryTable(TEMP_EVENT_E_LTE_VEND_HIER3_EVENTID_ERR_DAY, vendHier3EventIdDayColumns);

        createAndPopulateDimTables();
        populateControllerGroupTemporaryTables();
        populateErrRawTemporaryTables();
        populateDayAggTemporaryTables();
    }

    @Test
    public void testEventAnalysisSummaryForLTEControllerGroupInDayAgg() throws Exception {
        jndiProperties.useSucRawJNDIProperty();
        MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(TIME_QUERY_PARAM, ONE_WEEK);
        map.putSingle(TYPE_PARAM, TYPE_BSC);
        map.putSingle(GROUP_NAME_PARAM, CONTROLLER_GROUP_NAME_4G);
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, DEFAULT_MAX_ROWS);

        String result = runQuery(service, map);
        validateAgainstGridDefinition(result, "NETWORK_EVENT_ANALYSIS_BSC_GROUP");

        ResultTranslator<EventAnalysisControllerGroupSummaryResult> rt = getTranslator();
        List<EventAnalysisControllerGroupSummaryResult> resultList = rt.translateResult(result, EventAnalysisControllerGroupSummaryResult.class);

        validResults(resultList,false);
    }

    @Test
    public void testEventAnalysisSummaryForSGEHControllerGroupInDayAgg() throws Exception {
        jndiProperties.useSucRawJNDIProperty();
        MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(TIME_QUERY_PARAM, ONE_WEEK);
        map.putSingle(TYPE_PARAM, TYPE_BSC);
        map.putSingle(GROUP_NAME_PARAM, CONTROLLER_GROUP_NAME_2G_3G);
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, "20");

        String result = runQuery(service, map);
        validateAgainstGridDefinition(result, "NETWORK_EVENT_ANALYSIS_BSC_GROUP");

        ResultTranslator<EventAnalysisControllerGroupSummaryResult> rt = getTranslator();
        List<EventAnalysisControllerGroupSummaryResult> resultList = rt.translateResult(result, EventAnalysisControllerGroupSummaryResult.class);

        validResults(resultList,true);
    }


    private void validResults(final List<EventAnalysisControllerGroupSummaryResult> resultList, boolean isSgeh) {

        if(isSgeh){
            assertThat(resultList.size(), is(listOfEventTypesGroup1.size()));
        }else{
            assertThat(resultList.size(), is(listOfEventTypesGroup2.size()));
        }

        for (EventAnalysisControllerGroupSummaryResult summaryResult : resultList) {
            if (summaryResult.getEventId() == DEACTIVATE_IN_2G_AND_3G) {
                verifyDeactivate2Gand3GRow(summaryResult);
            } else if (summaryResult.getEventId() == HANDOVER_IN_4G){
                verifyHandover4GRow(summaryResult);
            }
        }
    }

    private void verifyHandover4GRow(EventAnalysisControllerGroupSummaryResult summaryResult) {
        int expectedTotal = NO_OF_SUCCESSES_4G + NO_OF_ERRORS_4G;

        assertThat(summaryResult.getGroupName(), is(CONTROLLER_GROUP_NAME_4G));
        assertThat(summaryResult.getEventId(), is(HANDOVER_IN_4G));
        assertThat(summaryResult.getEventIdDesc(), is(L_HANDOVER));
        assertThat(summaryResult.getErrorCount(), is(NO_OF_ERRORS_4G));
        assertThat(summaryResult.getSuccessCount(), is(NO_OF_SUCCESSES_4G));
        assertThat(summaryResult.getTotalEvents(), is(expectedTotal));
        assertThat(summaryResult.getSuccessRatio(), is(summaryResult.calculateExpectedSuccessRatio()));
        assertThat(summaryResult.getErrorSubscriberCount(), is(2));
    }

    private void verifyDeactivate2Gand3GRow(EventAnalysisControllerGroupSummaryResult summaryResult) {
        int expectedTotal = NO_OF_SUCCESSES_2G_3G  + NO_OF_ERRORS_2G_3G;

        assertThat(summaryResult.getGroupName(), is(CONTROLLER_GROUP_NAME_2G_3G));
        assertThat(summaryResult.getEventId(), is(DEACTIVATE_IN_2G_AND_3G));
        assertThat(summaryResult.getEventIdDesc(), is(DEACTIVATE));
        assertThat(summaryResult.getErrorCount(), is(NO_OF_ERRORS_2G_3G));
        assertThat(summaryResult.getSuccessCount(), is(NO_OF_SUCCESSES_2G_3G));
        assertThat(summaryResult.getTotalEvents(), is(expectedTotal));
        assertThat(summaryResult.getSuccessRatio(), is(summaryResult.calculateExpectedSuccessRatio()));
        assertThat(summaryResult.getErrorSubscriberCount(), is(1));
    }

    private void createTablesForTest() {
        tempDataTables.add(TEMP_EVENT_E_SGEH_ERR_RAW);
        tempDataTables.add(TEMP_EVENT_E_SGEH_SUC_RAW);
        tempDataTables.add(TEMP_EVENT_E_LTE_ERR_RAW);
        tempDataTables.add(TEMP_EVENT_E_LTE_SUC_RAW);

        addRawColumnsForTableCreation();
        addControllerGroupColumnsForTableCreation();
        addDayAggColumnsForTableCreation();
    }
    private void addRawColumnsForTableCreation(){

        rawTableColumns.add(IMSI);
        rawTableColumns.add(EVENT_ID);
        rawTableColumns.add(DATETIME_ID);
        rawTableColumns.add(LOCAL_DATE_ID);
        rawTableColumns.add(RAT);
        rawTableColumns.add(VENDOR);
        rawTableColumns.add(HIERARCHY_3);
        rawTableColumns.add(HIERARCHY_2);
        rawTableColumns.add(HIERARCHY_1);
        rawTableColumns.add(DEACTIVATION_TRIGGER);
        rawTableColumns.add(TAC);

    }

    private void addControllerGroupColumnsForTableCreation(){

        groupRatVendHier3Columns.add(HIER3_ID);
        groupRatVendHier3Columns.add(HIERARCHY_3);
        groupRatVendHier3Columns.add(GROUP_NAME);
        groupRatVendHier3Columns.add(VENDOR);
        groupRatVendHier3Columns.add(RAT);

    }

    private void addDayAggColumnsForTableCreation(){

        vendHier3EventIdDayColumns.add(RAT);
        vendHier3EventIdDayColumns.add(VENDOR);
        vendHier3EventIdDayColumns.add(HIERARCHY_3);
        vendHier3EventIdDayColumns.add(EVENT_ID);
        vendHier3EventIdDayColumns.add(NO_OF_SUCCESSES);
        vendHier3EventIdDayColumns.add(NO_OF_ERRORS);
        vendHier3EventIdDayColumns.add(DATETIME_ID);
        vendHier3EventIdDayColumns.add(NO_OF_NET_INIT_DEACTIVATES);

    }

    private void populateControllerGroupTemporaryTables() throws SQLException {

        Map<String, Object> values = new HashMap<String, Object>();
        values.put(GROUP_NAME, CONTROLLER_GROUP_NAME_2G_3G);
        values.put(HIER3_ID, TEST_VALUE_GSM_HIER3_ID);
        values.put(HIERARCHY_3, TEST_VALUE_GSM_CONTROLLER1_NAME);
        values.put(VENDOR, TEST_VALUE_VENDOR);
        values.put(RAT, RAT_INTEGER_VALUE_FOR_2G);
        insertRow(TEMP_GROUP_TYPE_E_RAT_VEND_HIER3, values);

        values = new HashMap<String, Object>();
        values.put(GROUP_NAME, CONTROLLER_GROUP_NAME_4G);
        values.put(HIER3_ID, HIER3_ID_VALUE_LTE);
        values.put(HIERARCHY_3, SAMPLE_HIERARCHY_3);
        values.put(VENDOR, TEST_VALUE_VENDOR);
        values.put(RAT, RAT_INTEGER_VALUE_FOR_4G);
        insertRow(TEMP_GROUP_TYPE_E_RAT_VEND_HIER3, values);
    }

    private void createAndPopulateDimTables() throws SQLException{

        try {
            createAndPopulateTempLookupTable(DIM_E_SGEH_EVENTTYPE);
            createAndPopulateTempLookupTable(DIM_E_LTE_EVENTTYPE);
        } catch (Exception e) {
            System.out.println("Could not create DIM_EVENTTYPE tables");
            e.printStackTrace();
        }
    }
    private void populateErrRawTemporaryTables() throws SQLException{

        Map<String, Object> values = new HashMap<String, Object>();
        values.put(IMSI, SAMPLE_IMSI);
        values.put(EVENT_ID, DEACTIVATE_IN_2G_AND_3G);
        values.put(DATETIME_ID, DateTimeUtilities.getDateTimeMinus48Hours());
        values.put(RAT, RAT_INTEGER_VALUE_FOR_3G);
        values.put(VENDOR, ERICSSON);
        values.put(HIERARCHY_3, TEST_VALUE_GSM_CONTROLLER1_NAME);
        values.put(HIERARCHY_2, "");
        values.put(HIERARCHY_1, HIERARCHY_1_STR);
        values.put(DEACTIVATION_TRIGGER, TEST_DEACTIVATE_TRIGGER_VALUE);
        values.put(TAC, SAMPLE_TAC);
        values.put(LOCAL_DATE_ID,DateTimeUtilities.getDateMinus48Hours());
        insertRow(TEMP_EVENT_E_SGEH_ERR_RAW, values);
        insertRow(TEMP_EVENT_E_SGEH_SUC_RAW, values);

        values = new HashMap<String, Object>();
        values.put(IMSI, SAMPLE_IMSI);
        values.put(EVENT_ID, ATTACH_IN_4G);
        values.put(DATETIME_ID, DateTimeUtilities.getDateTimeMinusDay(3));
        values.put(RAT, RAT_INTEGER_VALUE_FOR_4G);
        values.put(VENDOR, ERICSSON);
        values.put(HIERARCHY_3, SAMPLE_HIERARCHY_3);
        values.put(HIERARCHY_2, "");
        values.put(HIERARCHY_1, HIERARCHY_1_STR);
        values.put(DEACTIVATION_TRIGGER, TEST_DEACTIVATE_TRIGGER_VALUE);
        values.put(TAC, SAMPLE_TAC);
        values.put(LOCAL_DATE_ID,DateTimeUtilities.getDateMinus48Hours());
        insertRow(TEMP_EVENT_E_LTE_ERR_RAW, values);

        values = new HashMap<String, Object>();
        values.put(IMSI, SAMPLE_IMSI);
        values.put(EVENT_ID, HANDOVER_IN_4G);
        values.put(DATETIME_ID, DateTimeUtilities.getDateTimeMinus48Hours());
        values.put(RAT, RAT_INTEGER_VALUE_FOR_4G);
        values.put(VENDOR, ERICSSON);
        values.put(HIERARCHY_3, SAMPLE_HIERARCHY_3);
        values.put(HIERARCHY_2, "");
        values.put(HIERARCHY_1, HIERARCHY_1_STR);
        values.put(DEACTIVATION_TRIGGER, TEST_DEACTIVATE_TRIGGER_VALUE);
        values.put(TAC, SAMPLE_TAC);
        values.put(LOCAL_DATE_ID,DateTimeUtilities.getDateMinus48Hours());
        insertRow(TEMP_EVENT_E_LTE_ERR_RAW, values);
        insertRow(TEMP_EVENT_E_LTE_SUC_RAW, values);
        values.put(IMSI, SAMPLE_IMSI_2);
        insertRow(TEMP_EVENT_E_LTE_ERR_RAW, values);
        values.put(TAC,SAMPLE_EXCLUSIVE_TAC);
        insertRow(TEMP_EVENT_E_LTE_ERR_RAW, values);
    }

    private void populateDayAggTemporaryTables() throws SQLException{

        Map<String, Object> values = new HashMap<String, Object>();
        values.put(RAT, RAT_INTEGER_VALUE_FOR_4G);
        values.put(VENDOR, TEST_VALUE_VENDOR);
        values.put(HIERARCHY_3, SAMPLE_HIERARCHY_3);
        values.put(EVENT_ID, HANDOVER_IN_4G);
        values.put(NO_OF_SUCCESSES, NO_OF_SUCCESSES_4G);
        values.put(NO_OF_ERRORS, 0);
        values.put(DATETIME_ID, DateTimeUtilities.getDateMinus48Hours() + MIDNIGHT);
        values.put(NO_OF_NET_INIT_DEACTIVATES, 0);
        listOfEventTypesGroup2.add((Integer) values.get(EVENT_ID));
        insertRow(TEMP_EVENT_E_LTE_VEND_HIER3_EVENTID_SUC_DAY, values);
        values.put(NO_OF_SUCCESSES,0);
        values.put(NO_OF_ERRORS, NO_OF_ERRORS_4G);
        insertRow(TEMP_EVENT_E_LTE_VEND_HIER3_EVENTID_ERR_DAY, values);

        values = new HashMap<String, Object>();
        values.put(RAT, RAT_INTEGER_VALUE_FOR_3G);
        values.put(VENDOR, TEST_VALUE_VENDOR);
        values.put(HIERARCHY_3, TEST_VALUE_GSM_CONTROLLER1_NAME);
        values.put(EVENT_ID, DEACTIVATE_IN_2G_AND_3G);
        values.put(NO_OF_SUCCESSES, NO_OF_SUCCESSES_2G_3G);
        values.put(NO_OF_ERRORS, 0);
        values.put(DATETIME_ID, DateTimeUtilities.getDateMinus48Hours() + MIDNIGHT);
        values.put(NO_OF_NET_INIT_DEACTIVATES, 0);
        insertRow(TEMP_EVENT_E_SGEH_VEND_HIER3_EVENTID_SUC_DAY, values);
        values.put(NO_OF_SUCCESSES,0);
        values.put(NO_OF_ERRORS, NO_OF_ERRORS_2G_3G);
        listOfEventTypesGroup1.add((Integer) values.get(EVENT_ID));
        insertRow(TEMP_EVENT_E_SGEH_VEND_HIER3_EVENTID_ERR_DAY, values);
        values.put(EVENT_ID, DEACTIVATE_IN_2G_AND_3G);
        listOfEventTypesGroup1.add((Integer) values.get(EVENT_ID));
        insertRow(TEMP_EVENT_E_SGEH_VEND_HIER3_EVENTID_SUC_DAY, values);

    }

}
