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
package com.ericsson.eniq.events.server.integritytests.eventanalysis.imsi;

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
import com.ericsson.eniq.events.server.test.queryresults.EventAnalysisImsiGroupSummaryResult;
import com.ericsson.eniq.events.server.test.queryresults.ResultTranslator;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;
import com.sun.jersey.core.util.MultivaluedMapImpl;

public class EventAnalysisSummaryImsiGroupTest extends BaseDataIntegrityTest<EventAnalysisImsiGroupSummaryResult> {

    private static final int TEST_DEACTIVATE_TRIGGER_VALUE = 1;
    private static final int TEST_TAC_VALUE = 10010;

    private static final int HIER3_ID_VAL_G2 = 9101122;

    private static final int HIER321_ID_VAL_G2 = 910111222;

    private static final int HIER321_ID_VAL_G1 = 111222;

    private static final int HIER3_ID_VAL_G1 = 101122;

    public static final String HIERARCHY_3_STR_G1 = "h3_group1";

    public static final String HIERARCHY_3_STR_G2 = "h3_group2";

    public static final String HIERARCHY_1_STR = "h1_string";
    private final EventAnalysisService service = new EventAnalysisService();

    private final List<String> tempDataTables = new ArrayList<String>();

    private final Collection<String> rawTableColumns = new ArrayList<String>();

    private final List<String> tempDataIMSITables = new ArrayList<String>();

    private final Collection<String> imsiRawTableColumns = new ArrayList<String>();

    private final Collection<String> groupImsiColumns = new ArrayList<String>();

    private final Set<Integer> listOfEvents = new TreeSet<Integer>();

    private static final long TEST_IMSI = 312030410000004L;

    private static final long TEST_IMSI_1 = 3120304100001114L;

    @Before
    public void onSetUp() throws Exception {
        attachDependencies(service);

        createTablesForTest();

        for (final String tempTable : tempDataIMSITables) {
            createTemporaryTable(tempTable, imsiRawTableColumns);
        }

        for (final String tempTable : tempDataTables) {
            createTemporaryTable(tempTable, rawTableColumns);
        }
        createTemporaryTable(TEMP_GROUP_TYPE_E_IMSI, groupImsiColumns);

        populateTemporaryTables();
    }

    @Test
    public void testEventAnalysisSummaryForIMSIGroupInSuccessRaw() throws Exception {
        jndiProperties.useSucRawJNDIProperty();
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(TIME_QUERY_PARAM, ONE_WEEK);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        map.putSingle(GROUP_NAME_PARAM, SAMPLE_IMSI_GROUP);
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, "20");

        final String result = runQuery(service, map);
        validateAgainstGridDefinition(result, "NETWORK_EVENT_ANALYSIS_IMSI_GROUP");

        final ResultTranslator<EventAnalysisImsiGroupSummaryResult> rt = getTranslator();
        final List<EventAnalysisImsiGroupSummaryResult> resultList = rt.translateResult(result, EventAnalysisImsiGroupSummaryResult.class);

        validResults(resultList);
    }

    @Test
    public void testEventAnalysisSummaryForIMSIGroupInImsiAggregation() throws Exception {
        jndiProperties.setupDataTieringAndDisableSucRawJNDIProperties();
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(TIME_QUERY_PARAM, ONE_WEEK);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        map.putSingle(GROUP_NAME_PARAM, SAMPLE_IMSI_GROUP);
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, "20");

        final String result = runQuery(service, map);
        validateAgainstGridDefinition(result, "NETWORK_EVENT_ANALYSIS_IMSI_GROUP");

        final ResultTranslator<EventAnalysisImsiGroupSummaryResult> rt = getTranslator();
        final List<EventAnalysisImsiGroupSummaryResult> resultList = rt.translateResult(result, EventAnalysisImsiGroupSummaryResult.class);

        validResults(resultList);
    }


    private void validResults(final List<EventAnalysisImsiGroupSummaryResult> resultList) {
        assertThat(listOfEvents.size(), is(4));
        assertThat(resultList.size(), is(listOfEvents.size()));

        for (EventAnalysisImsiGroupSummaryResult summaryResult : resultList) {
            if (summaryResult.getEventId() == ATTACH_IN_4G) {
                verifyAttach4GRow(summaryResult);
            } else if (summaryResult.getEventId() == DEACTIVATE_IN_2G_AND_3G) {
                verifyDeactivate2Gand3GRow(summaryResult);
            } else if (summaryResult.getEventId() == DEDICATED_BEARER_DEACTIVATE_IN_4G) {
                verifyDedicatedBearerDeactivate4GRow(summaryResult);
            } else {
                verifyHandover4GRow(summaryResult);
            }
        }
    }

    private void verifyHandover4GRow(final EventAnalysisImsiGroupSummaryResult summaryResult) {
        assertThat(summaryResult.getGroupName(), is(SAMPLE_IMSI_GROUP));
        assertThat(summaryResult.getEventId(), is(HANDOVER_IN_4G));
        assertThat(summaryResult.getEventIdDesc(), is(L_HANDOVER));
        assertThat(summaryResult.getErrorCount(), is(2));
        assertThat(summaryResult.getSuccessCount(), is(1));
        assertThat(summaryResult.getOccurrences(), is(3));
        assertThat(summaryResult.getSuccessRatio(), is(33.33));
        assertThat(summaryResult.getErrorSubscriberCount(), is(2));
    }

    private void verifyDedicatedBearerDeactivate4GRow(final EventAnalysisImsiGroupSummaryResult summaryResult) {
        assertThat(summaryResult.getGroupName(), is(SAMPLE_IMSI_GROUP));
        assertThat(summaryResult.getEventId(), is(DEDICATED_BEARER_DEACTIVATE_IN_4G));
        assertThat(summaryResult.getEventIdDesc(), is(L_DEDICATED_BEARER_DEACTIVATE));
        assertThat(summaryResult.getErrorCount(), is(1));
        assertThat(summaryResult.getSuccessCount(), is(2));
        assertThat(summaryResult.getOccurrences(), is(3));
        assertThat(summaryResult.getSuccessRatio(), is(66.67));
        assertThat(summaryResult.getErrorSubscriberCount(), is(1));
    }

    private void verifyDeactivate2Gand3GRow(final EventAnalysisImsiGroupSummaryResult summaryResult) {
        assertThat(summaryResult.getGroupName(), is(SAMPLE_IMSI_GROUP));
        assertThat(summaryResult.getEventId(), is(DEACTIVATE_IN_2G_AND_3G));
        assertThat(summaryResult.getEventIdDesc(), is(DEACTIVATE));
        assertThat(summaryResult.getErrorCount(), is(1));
        assertThat(summaryResult.getSuccessCount(), is(2));
        assertThat(summaryResult.getOccurrences(), is(3));
        assertThat(summaryResult.getSuccessRatio(), is(66.67));
        assertThat(summaryResult.getErrorSubscriberCount(), is(1));
    }

    private void verifyAttach4GRow(final EventAnalysisImsiGroupSummaryResult summaryResult) {
        assertThat(summaryResult.getGroupName(), is(SAMPLE_IMSI_GROUP));
        assertThat(summaryResult.getEventId(), is(ATTACH_IN_4G));
        assertThat(summaryResult.getEventIdDesc(), is(L_ATTACH));
        assertThat(summaryResult.getErrorCount(), is(1));
        assertThat(summaryResult.getSuccessCount(), is(1));
        assertThat(summaryResult.getOccurrences(), is(2));
        assertThat(summaryResult.getSuccessRatio(), is(50.0));
        assertThat(summaryResult.getErrorSubscriberCount(), is(1));
    }

    private void createTablesForTest() {
        tempDataTables.add(TEMP_EVENT_E_SGEH_ERR_RAW);
        tempDataTables.add(TEMP_EVENT_E_SGEH_SUC_RAW);
        tempDataTables.add(TEMP_EVENT_E_LTE_ERR_RAW);
        tempDataTables.add(TEMP_EVENT_E_LTE_SUC_RAW);

        tempDataIMSITables.add(TEMP_EVENT_E_SGEH_IMSI_SUC_RAW);
        tempDataIMSITables.add(TEMP_EVENT_E_LTE_IMSI_SUC_RAW);

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

        imsiRawTableColumns.add(IMSI);
        imsiRawTableColumns.add(EVENT_ID);
        imsiRawTableColumns.add(DATETIME_ID);
        imsiRawTableColumns.add(RAT);
        imsiRawTableColumns.add(NO_OF_SUCCESSES);
        imsiRawTableColumns.add(HIER3_ID);
        imsiRawTableColumns.add(HIER321_ID);
        imsiRawTableColumns.add(TAC);

        groupImsiColumns.add(IMSI);
        groupImsiColumns.add(GROUP_NAME);
    }

    private void populateTemporaryTables() throws SQLException {

        Map<String, Object> values = new HashMap<String, Object>();
        values.put(GROUP_NAME, SAMPLE_IMSI_GROUP);
        values.put(IMSI, TEST_IMSI);
        insertRow(TEMP_GROUP_TYPE_E_IMSI, values);

        values = new HashMap<String, Object>();
        values.put(GROUP_NAME, SAMPLE_IMSI_GROUP);
        values.put(IMSI, TEST_IMSI_1);
        insertRow(TEMP_GROUP_TYPE_E_IMSI, values);

        values = new HashMap<String, Object>();
        values.put(IMSI, TEST_IMSI);
        values.put(EVENT_ID, DEACTIVATE_IN_2G_AND_3G);
        values.put(RAT, 1);
        values.put(DATETIME_ID, DateTimeUtilities.getDateTimeMinusDay(1));
        values.put(NO_OF_SUCCESSES, 2);
        values.put(HIER3_ID, HIER3_ID_VAL_G1);
        values.put(HIER321_ID, HIER321_ID_VAL_G1);
        values.put(TAC, TEST_TAC_VALUE);
        insertRow(TEMP_EVENT_E_SGEH_IMSI_SUC_RAW, values);
        listOfEvents.add((Integer) values.get(EVENT_ID));

        values = new HashMap<String, Object>();
        values.put(IMSI, TEST_IMSI);
        values.put(EVENT_ID, DEACTIVATE_IN_2G_AND_3G);
        values.put(DATETIME_ID, DateTimeUtilities.getDateTimeMinusDay(2));
        values.put(LOCAL_DATE_ID, DateTimeUtilities.getDateTimeMinusDay(2).substring(0, 10));
        values.put(RAT, 1);
        values.put(VENDOR, ERICSSON);
        values.put(HIERARCHY_3, HIERARCHY_3_STR_G1);
        values.put(HIERARCHY_2, "");
        values.put(HIERARCHY_1, HIERARCHY_1_STR);
        values.put(DEACTIVATION_TRIGGER, TEST_DEACTIVATE_TRIGGER_VALUE);
        values.put(TAC, TEST_TAC_VALUE);
        insertRow(TEMP_EVENT_E_SGEH_ERR_RAW, values);
        insertRow(TEMP_EVENT_E_SGEH_SUC_RAW, values);
        insertRow(TEMP_EVENT_E_SGEH_SUC_RAW, values);
        listOfEvents.add((Integer) values.get(EVENT_ID));

        values = new HashMap<String, Object>();
        values.put(IMSI, TEST_IMSI);
        values.put(EVENT_ID, ATTACH_IN_4G);
        values.put(DATETIME_ID, DateTimeUtilities.getDateTimeMinusDay(3));
        values.put(LOCAL_DATE_ID, DateTimeUtilities.getDateTimeMinusDay(3).substring(0, 10));
        values.put(RAT, 1);
        values.put(VENDOR, ERICSSON);
        values.put(HIERARCHY_3, HIERARCHY_3_STR_G1);
        values.put(HIERARCHY_2, "");
        values.put(HIERARCHY_1, HIERARCHY_1_STR);
        values.put(DEACTIVATION_TRIGGER, TEST_DEACTIVATE_TRIGGER_VALUE);
        values.put(TAC, TEST_TAC_VALUE);
        insertRow(TEMP_EVENT_E_LTE_ERR_RAW, values);
        insertRow(TEMP_EVENT_E_LTE_SUC_RAW, values);
        listOfEvents.add((Integer) values.get(EVENT_ID));

        values = new HashMap<String, Object>();
        values.put(IMSI, TEST_IMSI);
        values.put(EVENT_ID, ATTACH_IN_4G);
        values.put(RAT, 1);
        values.put(DATETIME_ID, DateTimeUtilities.getDateTimeMinusDay(1));
        values.put(NO_OF_SUCCESSES, 1);
        values.put(HIER3_ID, HIER3_ID_VAL_G1);
        values.put(HIER321_ID, HIER321_ID_VAL_G1);
        values.put(TAC, TEST_TAC_VALUE);
        insertRow(TEMP_EVENT_E_LTE_IMSI_SUC_RAW, values);
        listOfEvents.add((Integer) values.get(EVENT_ID));

        values = new HashMap<String, Object>();
        values.put(IMSI, TEST_IMSI);
        values.put(EVENT_ID, DEDICATED_BEARER_DEACTIVATE_IN_4G);
        values.put(DATETIME_ID, DateTimeUtilities.getDateTimeMinusDay(3));
        values.put(LOCAL_DATE_ID, DateTimeUtilities.getDateTimeMinusDay(3).substring(0, 10));
        values.put(RAT, 1);
        values.put(VENDOR, ERICSSON);
        values.put(HIERARCHY_3, HIERARCHY_3_STR_G1);
        values.put(HIERARCHY_2, "");
        values.put(HIERARCHY_1, HIERARCHY_1_STR);
        values.put(DEACTIVATION_TRIGGER, TEST_DEACTIVATE_TRIGGER_VALUE);
        values.put(TAC, TEST_TAC_VALUE);
        insertRow(TEMP_EVENT_E_LTE_ERR_RAW, values);
        insertRow(TEMP_EVENT_E_LTE_SUC_RAW, values);
        insertRow(TEMP_EVENT_E_LTE_SUC_RAW, values);
        listOfEvents.add((Integer) values.get(EVENT_ID));

        values = new HashMap<String, Object>();
        values.put(IMSI, TEST_IMSI);
        values.put(EVENT_ID, DEDICATED_BEARER_DEACTIVATE_IN_4G);
        values.put(RAT, 1);
        values.put(DATETIME_ID, DateTimeUtilities.getDateTimeMinusDay(1));
        values.put(NO_OF_SUCCESSES, 2);
        values.put(HIER3_ID, HIER3_ID_VAL_G1);
        values.put(HIER321_ID, HIER321_ID_VAL_G1);
        values.put(TAC, TEST_TAC_VALUE);
        insertRow(TEMP_EVENT_E_LTE_IMSI_SUC_RAW, values);
        listOfEvents.add((Integer) values.get(EVENT_ID));

        values = new HashMap<String, Object>();
        values.put(IMSI, TEST_IMSI);
        values.put(EVENT_ID, HANDOVER_IN_4G);
        values.put(DATETIME_ID, DateTimeUtilities.getDateTimeMinusDay(4));
        values.put(LOCAL_DATE_ID, DateTimeUtilities.getDateTimeMinusDay(4).substring(0, 10));
        values.put(RAT, 1);
        values.put(VENDOR, ERICSSON);
        values.put(HIERARCHY_3, HIERARCHY_3_STR_G2);
        values.put(HIERARCHY_2, "");
        values.put(HIERARCHY_1, HIERARCHY_1_STR);
        values.put(DEACTIVATION_TRIGGER, TEST_DEACTIVATE_TRIGGER_VALUE);
        values.put(TAC, TEST_TAC_VALUE);
        insertRow(TEMP_EVENT_E_LTE_ERR_RAW, values);
        insertRow(TEMP_EVENT_E_LTE_SUC_RAW, values);
        //Change IMSI in second row into ERR raw table
        values.put(IMSI, TEST_IMSI_1);
        insertRow(TEMP_EVENT_E_LTE_ERR_RAW, values);
        listOfEvents.add((Integer) values.get(EVENT_ID));

        values = new HashMap<String, Object>();
        values.put(IMSI, TEST_IMSI);
        values.put(EVENT_ID, HANDOVER_IN_4G);
        values.put(RAT, 1);
        values.put(DATETIME_ID, DateTimeUtilities.getDateTimeMinusDay(1));
        values.put(NO_OF_SUCCESSES, 1);
        values.put(HIER3_ID, HIER3_ID_VAL_G2);
        values.put(HIER321_ID, HIER321_ID_VAL_G2);
        values.put(TAC, TEST_TAC_VALUE);
        insertRow(TEMP_EVENT_E_LTE_IMSI_SUC_RAW, values);
        listOfEvents.add((Integer) values.get(EVENT_ID));
    }
}
