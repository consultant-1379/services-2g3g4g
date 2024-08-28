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

import java.text.SimpleDateFormat;
import java.util.*;

import javax.ws.rs.core.MultivaluedMap;

import org.junit.Before;
import org.junit.Test;

import com.ericsson.eniq.events.server.resources.BaseDataIntegrityTest;
import com.ericsson.eniq.events.server.serviceprovider.impl.eventanalysis.EventAnalysisService;
import com.ericsson.eniq.events.server.test.queryresults.EventAnalysisControllerGroupSummaryResult;
import com.ericsson.eniq.events.server.test.queryresults.EventAnalysisDetailedResult;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;
import com.sun.jersey.core.util.MultivaluedMapImpl;

public class EventAnalysisControllerGroup extends
BaseDataIntegrityTest<EventAnalysisDetailedResult>{

    private final String CONTROLLER_GROUP_NAME_2G_3G = "controllerGroup2g3g";

    private final String CONTROLLER_GROUP2_NAME_4G = "controllerGroup4g";

    private final int HIER3_VALUE = 54321;

    private final EventAnalysisService eventAnalysisService = new EventAnalysisService();

    private static Map<String, Object> columnsInLteRawTables = new HashMap<String, Object>();

    private static Map<String, Object> columnsInSgehRawTables = new HashMap<String, Object>();

    private static Map<String, Object> columnsInRawTables = new HashMap<String, Object>();

    private static Map<String, Object> columnsInCauseCodeTable = new HashMap<String, Object>();

    private static Map<String, Object> columnsInSubCauseCodeTable = new HashMap<String, Object>();

    private static Map<String, Object> columnsInLteCauseProtTypeTable = new HashMap<String, Object>();

    private static Map<String, Object> columnsInSgehCauseProtTypeTable = new HashMap<String, Object>();

    private static Map<String, Object> columnsInGroupRatVendHier3Table = new HashMap<String, Object>();

    private static Map<String, Object> columnsInDimTac = new HashMap<String, Object>();

    private final SimpleDateFormat dateOnlyFormatter = new SimpleDateFormat(DATE_FORMAT);

    @Before
    public void onSetUp() throws Exception {

        attachDependencies(eventAnalysisService);

        addLteSgehSpecificValuesForRawTables();
        setupRequiredDimTables();
        populateDimTables();
        setupAndPopulateGroupTables();
    }

    @Test
    public void testGetDetailedControllerGroupsSgeh1Week() throws Exception {
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(TIME_QUERY_PARAM, ONE_WEEK);
        map.putSingle(TYPE_PARAM, TYPE_BSC);
        map.putSingle(GROUP_NAME_PARAM, CONTROLLER_GROUP_NAME_2G_3G);
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, "500");
        map.putSingle(EVENT_ID_PARAM, String.valueOf(ATTACH_IN_2G_AND_3G));
        final String timestamp = DateTimeUtilities.getDateTimeMinusHours(48);
        createAndPopulateRawTables(timestamp);
        final String json = getData(eventAnalysisService, map);
        System.out.println(json);
        final List<EventAnalysisDetailedResult> detailedResult = getTranslator().translateResult(json,
                EventAnalysisDetailedResult.class);

        assertThat("Size of detailedResult should have been 1!", detailedResult.size(), is(1));

        validateResults(detailedResult);
    }

    @Test
    public void testGetDetailedControllerGroupLte1Week() throws Exception {
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(TIME_QUERY_PARAM, ONE_WEEK);
        map.putSingle(TYPE_PARAM, TYPE_BSC);
        map.putSingle(GROUP_NAME_PARAM, CONTROLLER_GROUP2_NAME_4G);
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, "500");
        map.putSingle(EVENT_ID_PARAM, String.valueOf(ATTACH_IN_4G));
        final String timestamp = DateTimeUtilities.getDateTimeMinusHours(48);
        createAndPopulateRawTables(timestamp);
        final String json = getData(eventAnalysisService, map);
        System.out.println(json);
        final List<EventAnalysisDetailedResult> detailedResult = getTranslator().translateResult(json,
                EventAnalysisDetailedResult.class);

        assertThat("Size of detailedResult should have been 1!", detailedResult.size(), is(1));

        validateResults(detailedResult);
    }

    @Test
    public void testGetDetailedControllerGroupsSgeh1Day() throws Exception {
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(TIME_QUERY_PARAM, SIX_HOURS);
        map.putSingle(TYPE_PARAM, TYPE_BSC);
        map.putSingle(GROUP_NAME_PARAM, CONTROLLER_GROUP_NAME_2G_3G);
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_TWO_HOUR);
        map.putSingle(MAX_ROWS, "500");
        map.putSingle(EVENT_ID_PARAM, String.valueOf(ATTACH_IN_2G_AND_3G));
        final String timestamp = DateTimeUtilities.getDateTimeMinusHours(5);
        createAndPopulateRawTables(timestamp);
        final String json = getData(eventAnalysisService, map);
        System.out.println(json);
        final List<EventAnalysisDetailedResult> detailedResult = getTranslator().translateResult(json,
                EventAnalysisDetailedResult.class);

        assertThat("Size of detailedResult should have been 1!", detailedResult.size(), is(1));

        validateResults(detailedResult);
    }

    @Test
    public void testGetDetailedControllerGroupLte1Day() throws Exception {
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(TIME_QUERY_PARAM, SIX_HOURS);
        map.putSingle(TYPE_PARAM, TYPE_BSC);
        map.putSingle(GROUP_NAME_PARAM, CONTROLLER_GROUP2_NAME_4G);
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_TWO_HOUR);
        map.putSingle(MAX_ROWS, "500");
        map.putSingle(EVENT_ID_PARAM, String.valueOf(ATTACH_IN_4G));
        final String timestamp = DateTimeUtilities.getDateTimeMinusHours(6);
        createAndPopulateRawTables(timestamp);
        final String json = getData(eventAnalysisService, map);
        System.out.println(json);
        final List<EventAnalysisDetailedResult> detailedResult = getTranslator().translateResult(json,
                EventAnalysisDetailedResult.class);

        assertThat("Size of detailedResult should have been 1!", detailedResult.size(), is(1));

        validateResults(detailedResult);
    }

    @Test
    public void testGetDetailedControllerGroupsSgeh1Hour() throws Exception {
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(TIME_QUERY_PARAM, ONE_HOUR);
        map.putSingle(TYPE_PARAM, TYPE_BSC);
        map.putSingle(GROUP_NAME_PARAM, CONTROLLER_GROUP_NAME_2G_3G);
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_FIVE_THIRTY);
        map.putSingle(MAX_ROWS, "500");
        map.putSingle(EVENT_ID_PARAM, String.valueOf(ATTACH_IN_2G_AND_3G));
        final String timestamp = DateTimeUtilities.getDateTimeMinusMinutes(30);
        createAndPopulateRawTables(timestamp);
        final String json = getData(eventAnalysisService, map);
        System.out.println(json);
        final List<EventAnalysisDetailedResult> detailedResult = getTranslator().translateResult(json,
                EventAnalysisDetailedResult.class);

        assertThat("Size of detailedResult should have been 1!", detailedResult.size(), is(1));

        validateResults(detailedResult);
    }


    @Test
    public void testGetDetailedControllerGroupLte1Hour() throws Exception {
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(TIME_QUERY_PARAM, ONE_HOUR);
        map.putSingle(TYPE_PARAM, TYPE_BSC);
        map.putSingle(GROUP_NAME_PARAM, CONTROLLER_GROUP2_NAME_4G);
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_FIVE_THIRTY);
        map.putSingle(MAX_ROWS, "500");
        map.putSingle(EVENT_ID_PARAM, String.valueOf(ATTACH_IN_4G));
        final String timestamp = DateTimeUtilities.getDateTimeMinusMinutes(30);
        createAndPopulateRawTables(timestamp);
        final String json = getData(eventAnalysisService, map);
        System.out.println(json);
        final List<EventAnalysisDetailedResult> detailedResult = getTranslator().translateResult(json,
                EventAnalysisDetailedResult.class);

        assertThat("Size of detailedResult should have been 1!", detailedResult.size(), is(1));

        validateResults(detailedResult);
    }

    private void validateResults(final List<EventAnalysisDetailedResult> resultList) {

        for (EventAnalysisDetailedResult summaryResult : resultList) {
            if (summaryResult.getEventIdDesc().equals(ATTACH)) {
                verifySgehRow(summaryResult);
            } else if (summaryResult.getEventIdDesc().equals(L_ATTACH)){
                verifyLteRow(summaryResult);
            }
        }
    }

    private void verifySgehRow(EventAnalysisDetailedResult summaryResult) {
        assertThat(summaryResult.getHierarchy3(), is(TEST_VALUE_GSM_CONTROLLER1_NAME));
        assertThat(summaryResult.getRat(), is(String.valueOf(RAT_FOR_GSM)));
        assertThat(summaryResult.getTac(), is(String.valueOf(SAMPLE_TAC)));
        assertThat(summaryResult.getApn(), is(SAMPLE_APN));
        assertThat(summaryResult.getImsi(), is(String.valueOf(SAMPLE_IMSI)));
        assertThat(summaryResult.getPdpGgsnName1(), is (PDP_GGSN_NAME_1_VALUE));
        assertThat(summaryResult.getPdpGgsnName2(), is (PDP_GGSN_NAME_2_VALUE));
    }

    private void verifyLteRow(EventAnalysisDetailedResult summaryResult) { 
        assertThat(summaryResult.getHierarchy3(), is(SAMPLE_HIERARCHY_3));
        assertThat(summaryResult.getRat(), is(String.valueOf(RAT_FOR_LTE)));
        assertThat(summaryResult.getTac(), is(String.valueOf(SAMPLE_TAC)));
        assertThat(summaryResult.getApn(), is(SAMPLE_APN));
        assertThat(summaryResult.getImsi(), is(String.valueOf(SAMPLE_IMSI)));
    }

    private void setupRequiredDimTables(){
        columnsInSgehCauseProtTypeTable.put(CAUSE_PROT_TYPE_COLUMN, "0");
        columnsInSgehCauseProtTypeTable.put(CAUSE_PROT_TYPE_DESC_COLUMN, "RIL3");

        columnsInLteCauseProtTypeTable.put(CAUSE_PROT_TYPE_COLUMN, "0");
        columnsInLteCauseProtTypeTable.put(CAUSE_PROT_TYPE_DESC_COLUMN, "S1AP");

        columnsInSubCauseCodeTable.put(SUBCAUSE_CODE_COLUMN, "0");
        columnsInSubCauseCodeTable.put(SUB_CAUSE_CODE_DESC_COLUMN, "No SubCauseCode available");
        columnsInSubCauseCodeTable.put(SUB_CAUSE_CODE_HELP_COLUMN, "");

        columnsInCauseCodeTable.put(CAUSE_CODE_COLUMN, "0");
        columnsInCauseCodeTable.put(CAUSE_CODE_DESC_COLUMN, "Success");
        columnsInCauseCodeTable.put(CAUSE_CODE_HELP_COLUMN, "");
        columnsInCauseCodeTable.put(CAUSE_PROT_TYPE_COLUMN, "0");

        columnsInDimTac.put(TAC_COLUMN, SAMPLE_TAC);
        columnsInDimTac.put(BAND, SAMPLE_BAND);
    }

    private void populateDimTables() throws Exception{

        createAndPopulateTempLookupTable(DIM_E_SGEH_EVENTTYPE);
        createAndPopulateTempLookupTable(DIM_E_SGEH_EVENTRESULT);
        createAndPopulateTempLookupTable(DIM_E_SGEH_ATTACHTYPE);
        createAndPopulateTempLookupTable(DIM_E_SGEH_DETACH_TRIGGER);
        createAndPopulateTempLookupTable(DIM_E_SGEH_DETACH_TYPE);
        createAndPopulateTempLookupTable(DIM_E_SGEH_DEACTIVATIONTRIGGER);
        createAndPopulateTempLookupTable(DIM_E_SGEH_SERVICE_REQ_TRIGGER);
        createAndPopulateTempLookupTable(DIM_E_SGEH_RAT);
        createAndPopulateTempLookupTable(DIM_E_SGEH_SGSN);
        createAndPopulateTempLookupTable(DIM_E_SGEH_GGSN );
        createAndPopulateTempLookupTable(DIM_E_LTE_EVENTTYPE);
        createAndPopulateTempLookupTable(DIM_E_LTE_ATTACHTYPE);
        createAndPopulateTempLookupTable(DIM_E_LTE_DETACH_TRIGGER);
        createAndPopulateTempLookupTable(DIM_E_LTE_DETACH_TYPE);
        createAndPopulateTempLookupTable(DIM_E_LTE_DEACTIVATIONTRIGGER);
        createAndPopulateTempLookupTable(DIM_E_LTE_EVENT_SUBTYPE);
        createAndPopulateTempLookupTable(DIM_E_LTE_PDNDISCONNECTTYPE);
        createAndPopulateTempLookupTable(DIM_E_LTE_SMS_ONLY);
        createAndPopulateTempLookupTable(DIM_E_LTE_COMBINED_TAU_TYPE);
        createAndPopulateTempLookupTable(DIM_E_LTE_ARP_PCI);
        createAndPopulateTempLookupTable(DIM_E_LTE_ARP_PVI);
        createAndPopulateTempLookupTable(DIM_E_LTE_BEARER_CAUSE);    

        createTemporaryTable(TEMP_DIM_E_SGEH_CAUSECODE, columnsInCauseCodeTable.keySet());
        insertRow(TEMP_DIM_E_SGEH_CAUSECODE, columnsInCauseCodeTable);

        createTemporaryTable(TEMP_DIM_E_SGEH_SUBCAUSECODE, columnsInSubCauseCodeTable.keySet());
        insertRow(TEMP_DIM_E_SGEH_SUBCAUSECODE, columnsInSubCauseCodeTable);

        createTemporaryTable(TEMP_DIM_E_LTE_CAUSECODE, columnsInCauseCodeTable.keySet());
        insertRow(TEMP_DIM_E_LTE_CAUSECODE, columnsInCauseCodeTable);

        createTemporaryTable(TEMP_DIM_E_LTE_SUBCAUSECODE, columnsInSubCauseCodeTable.keySet());
        insertRow(TEMP_DIM_E_LTE_SUBCAUSECODE, columnsInSubCauseCodeTable);

        createTemporaryTable(TEMP_DIM_E_LTE_CAUSE_PROT_TYPE, columnsInLteCauseProtTypeTable.keySet());
        insertRow(TEMP_DIM_E_LTE_CAUSE_PROT_TYPE, columnsInLteCauseProtTypeTable);

        createTemporaryTable(TEMP_DIM_E_SGEH_CAUSE_PROT_TYPE, columnsInSgehCauseProtTypeTable.keySet());
        insertRow(TEMP_DIM_E_SGEH_CAUSE_PROT_TYPE, columnsInSgehCauseProtTypeTable);

    }
    
    private void setupAndPopulateGroupTables() throws Exception{
        setupGroupTables(CONTROLLER_GROUP_NAME_2G_3G, TEST_VALUE_GSM_CONTROLLER1_NAME, RAT_FOR_GSM, TEST_VALUE_VENDOR, HIER3_VALUE);
        createTemporaryTable(TEMP_GROUP_TYPE_E_RAT_VEND_HIER3, columnsInGroupRatVendHier3Table.keySet());
        insertRow(TEMP_GROUP_TYPE_E_RAT_VEND_HIER3, columnsInGroupRatVendHier3Table);
        setupGroupTables(CONTROLLER_GROUP2_NAME_4G, SAMPLE_HIERARCHY_3, RAT_FOR_LTE, TEST_VALUE_VENDOR, HIER3_VALUE + 1);
        insertRow(TEMP_GROUP_TYPE_E_RAT_VEND_HIER3, columnsInGroupRatVendHier3Table);
    }
    
    private void setupGroupTables(final String groupName, final String hierarchy3, final int rat,
                                  final String vendor, final int hier3_id){
        columnsInGroupRatVendHier3Table.put(HIER3_ID, hier3_id);
        columnsInGroupRatVendHier3Table.put(HIERARCHY_3, hierarchy3);
        columnsInGroupRatVendHier3Table.put(GROUP_NAME, groupName);
        columnsInGroupRatVendHier3Table.put(VENDOR, vendor);
        columnsInGroupRatVendHier3Table.put(RAT, rat);      
    }
    
    private void createAndPopulateRawTables( String timestamp ) throws Exception{

        final String localDateId = dateOnlyFormatter.format(dateOnlyFormatter.parse(timestamp));
        
        setupRawTablesValues(timestamp, localDateId, ATTACH_IN_2G_AND_3G, RAT_FOR_GSM, 1, TEST_VALUE_GSM_CONTROLLER1_NAME, SAMPLE_TAC);
        final Collection<String> columnsToCreateInSgehTable = new ArrayList<String>();
        columnsToCreateInSgehTable.addAll(columnsInRawTables.keySet());
        columnsToCreateInSgehTable.addAll(columnsInSgehRawTables.keySet());
        createTemporaryTable(TEMP_EVENT_E_SGEH_ERR_RAW, columnsToCreateInSgehTable);
        Map<String, Object> valuesForSgehTable = new HashMap<String, Object>();
        valuesForSgehTable.putAll(columnsInRawTables);
        valuesForSgehTable.putAll(columnsInSgehRawTables);
        insertRow(TEMP_EVENT_E_SGEH_ERR_RAW, valuesForSgehTable);

        setupRawTablesValues(timestamp, localDateId, ATTACH_IN_2G_AND_3G, RAT_FOR_GSM, 1, TEST_VALUE_GSM_CONTROLLER1_NAME, SAMPLE_EXCLUSIVE_TAC);
        valuesForSgehTable = new HashMap<String, Object>();
        valuesForSgehTable.putAll(columnsInRawTables);
        valuesForSgehTable.putAll(columnsInSgehRawTables);
        insertRow(TEMP_EVENT_E_SGEH_ERR_RAW, valuesForSgehTable);

        setupRawTablesValues(timestamp, localDateId, ATTACH_IN_4G, RAT_FOR_LTE, 1, SAMPLE_HIERARCHY_3, SAMPLE_TAC);
        final Collection<String> columnsToCreateInLteTable = new ArrayList<String>();
        columnsToCreateInLteTable.addAll(columnsInRawTables.keySet());
        columnsToCreateInLteTable.addAll(columnsInLteRawTables.keySet());
        createTemporaryTable(TEMP_EVENT_E_LTE_ERR_RAW, columnsToCreateInLteTable);
        final Map<String, Object> valuesForLteTable = new HashMap<String, Object>();
        valuesForLteTable.putAll(columnsInRawTables);
        valuesForLteTable.putAll(columnsInLteRawTables);
        insertRow(TEMP_EVENT_E_LTE_ERR_RAW, valuesForLteTable);
    }
    
    private void setupRawTablesValues(final String timestamp, final String localDateId, final int eventId, final int rat,
                                      final int eventResult, final String hierarchy3, final int tac) {

         columnsInRawTables.put(EVENT_ID, eventId);
         columnsInRawTables.put(RAT, rat);
         columnsInRawTables.put(TAC, tac);
         columnsInRawTables.put(IMSI, SAMPLE_IMSI);
         columnsInRawTables.put(DATETIME_ID, timestamp);
         columnsInRawTables.put(LOCAL_DATE_ID, localDateId);
         columnsInRawTables.put(APN, SAMPLE_APN);
         columnsInRawTables.put(EVENT_TIME, timestamp);
         columnsInRawTables.put(EVENT_SOURCE_NAME, "");
         columnsInRawTables.put(HIERARCHY_1, "");
         columnsInRawTables.put(HIERARCHY_2, "");
         columnsInRawTables.put(HIERARCHY_3, hierarchy3);
         columnsInRawTables.put(VENDOR_PARAM_UPPER_CASE, "");
         columnsInRawTables.put(DURATION, 0);
         columnsInRawTables.put(CAUSE_PROT_TYPE_COLUMN, 0);
         columnsInRawTables.put(CAUSE_CODE_COLUMN, 0);
         columnsInRawTables.put(SUBCAUSE_CODE_COLUMN, 0);
         columnsInRawTables.put(EVENT_RESULT, 0);
         columnsInRawTables.put(ATTACH_TYPE, 0);
         columnsInRawTables.put(DETACH_TYPE, 0);
         columnsInRawTables.put(DEACTIVATION_TRIGGER, 0);
         columnsInRawTables.put(DETACH_TRIGGER, 0);
         columnsInRawTables.put(SERVICE_REQ_TRIGGER, 0);
         columnsInRawTables.put(PAGING_ATTEMPTS, 0);
         columnsInRawTables.put(REQUEST_RETRIES, 0);
         columnsInRawTables.put(MCC, "0");
         columnsInRawTables.put(MNC, "0");
         columnsInRawTables.put(OLD_MCC, "0");
         columnsInRawTables.put(OLD_MNC, "0");
         columnsInRawTables.put(OLD_RAC, 0);
         columnsInRawTables.put(OLD_LAC, 0);
         columnsInRawTables.put(TRANSFERRED_PDP, 0);
         columnsInRawTables.put(HLR, 0);
         columnsInRawTables.put(DROPPED_PDP, 0);
         columnsInRawTables.put(MSISDN, 0);
     }

     private void addLteSgehSpecificValuesForRawTables(){
         columnsInSgehRawTables.put(PTMSI, 3);
         columnsInSgehRawTables.put(LINKED_NSAPI, 0);
         columnsInSgehRawTables.put(PDP_NSAPI_1, 0);
         columnsInSgehRawTables.put(PDP_NSAPI_2, 0);
         columnsInSgehRawTables.put(PDP_GGSN_IPADDRESS_1, 0);
         columnsInSgehRawTables.put(PDP_GGSN_IPADDRESS_2, 0);
         columnsInSgehRawTables.put(PDP_MS_IPADDRESS_1, 0);
         columnsInSgehRawTables.put(PDP_MS_IPADDRESS_2, 0);
         columnsInSgehRawTables.put(PDP_GGSN_NAME_1, PDP_GGSN_NAME_1_VALUE);
         columnsInSgehRawTables.put(PDP_GGSN_NAME_2, PDP_GGSN_NAME_2_VALUE);
         columnsInSgehRawTables.put(RAC, 0);
         columnsInSgehRawTables.put(LAC, 0);
         columnsInSgehRawTables.put(UPDATE_TYPE, 0);
         columnsInSgehRawTables.put(OLD_SGSN_IPADDRESS, 0);

         columnsInLteRawTables.put(TRAC, 0);
         columnsInLteRawTables.put(OLD_TRAC, 0);
         columnsInLteRawTables.put(OLD_CELL_ID, 0);
         columnsInLteRawTables.put(OLD_L_MMEGI, 0);
         columnsInLteRawTables.put(OLD_L_MMEC, 0);
         columnsInLteRawTables.put(OLD_L_MTMSI, 0);
         columnsInLteRawTables.put(OLD_SGW_IPV4, 0);
         columnsInLteRawTables.put(OLD_SGW_IPV6, 0);
         columnsInLteRawTables.put(PDN_BEARER_ID_1, 0);
         columnsInLteRawTables.put(PDN_BEARER_ID_2, 0);
         columnsInLteRawTables.put(PDN_BEARER_ID_3, 0);
         columnsInLteRawTables.put(PDN_PAA_IPV4_1, 0);
         columnsInLteRawTables.put(PDN_PAA_IPV4_2, 0);
         columnsInLteRawTables.put(PDN_PAA_IPV4_3, 0);
         columnsInLteRawTables.put(PDN_PAA_IPV6_1, 0);
         columnsInLteRawTables.put(PDN_PAA_IPV6_2, 0);
         columnsInLteRawTables.put(PDN_PAA_IPV6_3, 0);
         columnsInLteRawTables.put(PDN_PGW_IPV4_1, 0);
         columnsInLteRawTables.put(PDN_PGW_IPV4_2, 0);
         columnsInLteRawTables.put(PDN_PGW_IPV4_3, 0);
         columnsInLteRawTables.put(PDN_PGW_IPV6_1, 0);
         columnsInLteRawTables.put(PDN_PGW_IPV6_2, 0);
         columnsInLteRawTables.put(PDN_PGW_IPV6_3, 0);
         columnsInLteRawTables.put(ARP_PL_1, 0);
         columnsInLteRawTables.put(ARP_PL_2, 0);
         columnsInLteRawTables.put(ARP_PL_3, 0);
         columnsInLteRawTables.put(GBR_UPLINK_1, 0);
         columnsInLteRawTables.put(GBR_UPLINK_2, 0);
         columnsInLteRawTables.put(GBR_UPLINK_3, 0);
         columnsInLteRawTables.put(GBR_DOWNLINK_1, 0);
         columnsInLteRawTables.put(GBR_DOWNLINK_2, 0);
         columnsInLteRawTables.put(GBR_DOWNLINK_3, 0);
         columnsInLteRawTables.put(L_DISCONNECT_PDN_TYPE, 0);
         columnsInLteRawTables.put(EVENT_SUBTYPE_ID, 0);
         columnsInLteRawTables.put(SMS_ONLY, 0);
         columnsInLteRawTables.put(COMBINED_TAU_TYPE, 0);
         columnsInLteRawTables.put(ARP_PCI_1, 0);
         columnsInLteRawTables.put(ARP_PCI_2, 0);
         columnsInLteRawTables.put(ARP_PCI_3, 0);
         columnsInLteRawTables.put(ARP_PVI_1, 0);
         columnsInLteRawTables.put(ARP_PVI_2, 0);
         columnsInLteRawTables.put(ARP_PVI_3, 0);
         columnsInLteRawTables.put(BEARER_CAUSE_1, 0);
         columnsInLteRawTables.put(BEARER_CAUSE_2, 0);
         columnsInLteRawTables.put(BEARER_CAUSE_3, 0);
         columnsInLteRawTables.put(QCI_ERR_1, 0);
         columnsInLteRawTables.put(QCI_ERR_2, 0);
         columnsInLteRawTables.put(QCI_ERR_3, 0);
         columnsInLteRawTables.put(QCI_ERR_4, 0);
         columnsInLteRawTables.put(QCI_ERR_5, 0);
         columnsInLteRawTables.put(QCI_ERR_6, 0);
         columnsInLteRawTables.put(QCI_ERR_7, 0);
         columnsInLteRawTables.put(QCI_ERR_8, 0);
         columnsInLteRawTables.put(QCI_ERR_9, 0);
         columnsInLteRawTables.put(QCI_ERR_10, 0);
         columnsInLteRawTables.put(QCI_SUC_1, 0);
         columnsInLteRawTables.put(QCI_SUC_2, 0);
         columnsInLteRawTables.put(QCI_SUC_3, 0);
         columnsInLteRawTables.put(QCI_SUC_4, 0);
         columnsInLteRawTables.put(QCI_SUC_5, 0);
         columnsInLteRawTables.put(QCI_SUC_6, 0);
         columnsInLteRawTables.put(QCI_SUC_7, 0);
         columnsInLteRawTables.put(QCI_SUC_8, 0);
         columnsInLteRawTables.put(QCI_SUC_9, 0);
         columnsInLteRawTables.put(QCI_SUC_10, 0);
         columnsInLteRawTables.put(EPS_BEARER_ID_1, 0);
         columnsInLteRawTables.put(EPS_BEARER_ID_2, 0);
         columnsInLteRawTables.put(EPS_BEARER_ID_3, 0); 
     }
}
