package com.ericsson.eniq.events.server.integritytests.subsessionbi;

/**
 * Date: 01/12/14
 */

import com.ericsson.eniq.events.server.resources.SubsessionBIResource;
import com.ericsson.eniq.events.server.resources.TestsWithTemporaryTablesBaseTestCase;
import com.ericsson.eniq.events.server.test.queryresults.EventAnalysisDetailedResult;
import com.ericsson.eniq.events.server.test.stubs.DummyUriInfoImpl;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;
import com.sun.jersey.core.util.MultivaluedMapImpl;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import javax.ws.rs.core.MultivaluedMap;
import java.sql.SQLException;
import java.util.*;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.*;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.*;



public class SubBiTerminalDrillDownTest extends TestsWithTemporaryTablesBaseTestCase<EventAnalysisDetailedResult>{

    private static final Collection<String> lteCauseCodeTableColumns = new ArrayList<String>();

    private static final Collection<String> lteCauseProtTypeColumns = new ArrayList<String>();

    private static final Collection<String> lteSubCauseCodeTableColumns = new ArrayList<String>();

    private static final Collection<String> dimImsiMsisdnColumns = new ArrayList<String>();

    private static final Collection<String>  eventLteErrRawColums = new ArrayList<String>();

    private static final Collection<String>  sgehCauseCodeColumns = new ArrayList<String>();

    private static final Collection<String>  sgehSubCauseCodeColumns = new ArrayList<String>();

    private static final Collection<String> sgehCauseProtTypeColumns = new ArrayList<String>();

    private static final Collection<String> eventSgehErrRawColums = new ArrayList<String>();

    private static final String TEST_TIME = "360";

    private static final String TEST_PARAM_IMSI_ONE = "88888888888";
    private static final String TEST_PARAM_IMSI_TWO = "99999999999";
    private static final String TEST_PARAM_TAC = "1111";
    private static final String TEST_PARAM_MSISDN = "12345678901234567";
    private static final String TEST_PARAM_IMEISV_ONE = "3333333333333333";
    private static final String TEST_PARAM_IMEISV_TWO = "4444444444444444";

    private SubsessionBIResource subsessionBIResource;

    static {
        final Map<String, Object> tempLteErrRawMap = setupTempLteErrRawTableValues("", "", "", "");
        final Map<String, Object> tempSgehErrRawMap = setupTempSgehErrRawTables("", "", "", "");

        lteCauseCodeTableColumns.add("CAUSE_CODE");
        lteCauseCodeTableColumns.add("CAUSE_CODE_DESC");
        lteCauseCodeTableColumns.add("CAUSE_CODE_HELP");
        lteCauseCodeTableColumns.add("CAUSE_PROT_TYPE");

        lteSubCauseCodeTableColumns.add("SUBCAUSE_CODE");
        lteSubCauseCodeTableColumns.add("SUBCAUSE_CODE_DESC");
        lteSubCauseCodeTableColumns.add("SUBCAUSE_CODE_HELP");

        lteCauseProtTypeColumns.add("CAUSE_PROT_TYPE");
        lteCauseProtTypeColumns.add("CAUSE_PROT_TYPE_DESC");

        dimImsiMsisdnColumns.add("IMSI");
        dimImsiMsisdnColumns.add("MSISDN");
        dimImsiMsisdnColumns.add("VENDOR");
        dimImsiMsisdnColumns.add("STATUS");
        dimImsiMsisdnColumns.add("CREATED");
        dimImsiMsisdnColumns.add("MODIFIED");
        dimImsiMsisdnColumns.add("MODIFIER");

        sgehCauseCodeColumns.add("CAUSE_CODE");
        sgehCauseCodeColumns.add("CAUSE_CODE_DESC");
        sgehCauseCodeColumns.add("CAUSE_CODE_HELP");
        sgehCauseCodeColumns.add("CAUSE_PROT_TYPE");

        sgehSubCauseCodeColumns.add("SUBCAUSE_CODE");
        sgehSubCauseCodeColumns.add("SUBCAUSE_CODE_DESC");
        sgehSubCauseCodeColumns.add("SUBCAUSE_CODE_HELP");

        sgehCauseProtTypeColumns.add("CAUSE_PROT_TYPE");
        sgehCauseProtTypeColumns.add("CAUSE_PROT_TYPE_DESC");

        eventLteErrRawColums.addAll(tempLteErrRawMap.keySet());
        eventSgehErrRawColums.addAll(tempSgehErrRawMap.keySet());
    }


    @Override
    public void onSetUp() throws Exception {
        super.onSetUp();
        subsessionBIResource = new SubsessionBIResource();
        createTemporaryTable(TEMP_EVENT_E_LTE_ERR_RAW, eventLteErrRawColums);
        createTemporaryTable(TEMP_DIM_E_IMSI_MSISDN, dimImsiMsisdnColumns);
        createTemporaryTable(TEMP_DIM_E_LTE_CAUSE_PROT_TYPE, lteCauseProtTypeColumns);
        createTemporaryTable(TEMP_DIM_E_LTE_SUBCAUSECODE, lteSubCauseCodeTableColumns);
        createTemporaryTable(TEMP_DIM_E_LTE_CAUSECODE, lteCauseCodeTableColumns);
        createTemporaryTable(TEMP_DIM_E_SGEH_CAUSECODE, sgehCauseCodeColumns);
        createTemporaryTable(TEMP_DIM_E_SGEH_SUBCAUSECODE, sgehSubCauseCodeColumns);
        createTemporaryTable(TEMP_DIM_E_SGEH_CAUSE_PROT_TYPE, sgehCauseProtTypeColumns);
        createTemporaryTable(TEMP_EVENT_E_SGEH_ERR_RAW, eventSgehErrRawColums);
        populateTables();
        attachDependencies(subsessionBIResource);
    }


    @Test
    public void testGetSubBITerminalDataMSISDN() throws Exception  {
        jndiProperties.disableSucRawJNDIProperty();
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(TYPE_PARAM, TYPE_MSISDN);
        map.putSingle(MSISDN_PARAM, TEST_PARAM_MSISDN);
        map.putSingle(TAC_PARAM, TEST_PARAM_TAC );
        map.putSingle(IMSI_PARAM, TEST_PARAM_IMSI_ONE);
        map.putSingle(IMEISV_PARAM, TEST_PARAM_IMEISV_ONE);
        map.putSingle(TZ_OFFSET, TZ_OFFSET_OF_ZERO);
        map.putSingle(MAX_ROWS, DEFAULT_MAX_ROWS);
        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String result = subsessionBIResource.getSubBITerminalData();
        final List<EventAnalysisDetailedResult> results = getTranslator().translateResult(result, EventAnalysisDetailedResult.class);

        final int expectedRowCount = 2;
        assertEquals(expectedRowCount, results.size());

        for (final EventAnalysisDetailedResult eadr : results) {

            assertFalse(TEST_PARAM_IMSI_TWO.equals(eadr.getImsi()));

            assertTrue(TEST_PARAM_IMSI_ONE.equals(eadr.getImsi()));
        }
    }

    private static Map <String, Object> setupTempLteErrRawTableValues(final String imsiParam, final String tacParam, final String msisdnParam, final String imeisvParam) {
        final Map <String, Object> map = new HashMap<String, Object>();
        final String timeStamp = DateTimeUtilities.getDateTimeMinus30Minutes();
        map.put(IMEISV, imeisvParam);
        map.put(DATETIME_ID, timeStamp);
        map.put(EVENT_TIME, timeStamp);
        map.put(MSISDN, msisdnParam);
        map.put(IMSI, imsiParam);
        map.put(PTMSI, 0);
        map.put(TAC, tacParam);
        map.put(EVENT_SOURCE_NAME, "");
        map.put(HIERARCHY_3, "0");
        map.put(HIERARCHY_2, "0");
        map.put(HIERARCHY_1, "0");
        map.put(VENDOR, "0");
        map.put(TRAC, 0);
        map.put(OLD_MCC, "0");
        map.put(OLD_MNC, "0");
        map.put(OLD_TRAC, 0);
        map.put(OLD_CELL_ID, 0);
        map.put(OLD_L_MMEGI, 0);
        map.put(OLD_L_MMEC, 0);
        map.put(OLD_L_MTMSI, 0);
        map.put(OLD_SGW_IPV4, 0);
        map.put(OLD_SGW_IPV6, 0);
        map.put(PDN_BEARER_ID_1, 0);
        map.put(PDN_BEARER_ID_2, 0);
        map.put(PDN_BEARER_ID_3, 0);
        map.put(PDN_PAA_IPV4_1, 0);
        map.put(PDN_PAA_IPV4_2, 0);
        map.put(PDN_PAA_IPV4_3, 0);
        map.put(PDN_PAA_IPV6_1, 0);
        map.put(PDN_PAA_IPV6_2, 0);
        map.put(PDN_PAA_IPV6_3, 0);
        map.put(PDN_PGW_IPV4_1, 0);
        map.put(PDN_PGW_IPV4_2, 0);
        map.put(PDN_PGW_IPV4_3, 0);
        map.put(PDN_PGW_IPV6_1, 0);
        map.put(PDN_PGW_IPV6_2, 0);
        map.put(PDN_PGW_IPV6_3, 0);
        map.put(PAGING_ATTEMPTS, 0);
        map.put(REQUEST_RETRIES, 0);
        map.put(APN, "0");
        map.put(MCC, "0");
        map.put(MNC, "0");
        map.put(CAUSE_CODE_COLUMN, 0);
        map.put(SUBCAUSE_CODE, 0);
        map.put(ARP_PL_1, 0);
        map.put(ARP_PL_2, 0);
        map.put(ARP_PL_3, 0);
        map.put(GBR_UPLINK_1, 0);
        map.put(GBR_UPLINK_2, 0);
        map.put(GBR_UPLINK_3, 0);
        map.put(GBR_DOWNLINK_1, 0);
        map.put(GBR_DOWNLINK_2, 0);
        map.put(GBR_DOWNLINK_3, 0);
        map.put(L_DISCONNECT_PDN_TYPE, 0);
        map.put(EVENT_SUBTYPE_ID, 0);
        map.put(SMS_ONLY, 0);
        map.put(COMBINED_TAU_TYPE, 0);
        map.put(ARP_PCI_1, 0);
        map.put(ARP_PCI_2, 0);
        map.put(ARP_PCI_3, 0);
        map.put(ARP_PVI_1, 0);
        map.put(ARP_PVI_2, 0);
        map.put(ARP_PVI_3, 0);
        map.put(BEARER_CAUSE_1, 0);
        map.put(BEARER_CAUSE_2, 0);
        map.put(BEARER_CAUSE_3, 0);
        map.put(QCI_ERR_1, 0);
        map.put(QCI_ERR_2, 0);
        map.put(QCI_ERR_3, 0);
        map.put(QCI_ERR_4, 0);
        map.put(QCI_ERR_5, 0);
        map.put(QCI_ERR_6, 0);
        map.put(QCI_ERR_7, 0);
        map.put(QCI_ERR_8, 0);
        map.put(QCI_ERR_9, 0);
        map.put(QCI_ERR_10, 0);
        map.put(QCI_SUC_1, 0);
        map.put(QCI_SUC_2, 0);
        map.put(QCI_SUC_3, 0);
        map.put(QCI_SUC_4, 0);
        map.put(QCI_SUC_5, 0);
        map.put(QCI_SUC_6, 0);
        map.put(QCI_SUC_7, 0);
        map.put(QCI_SUC_8, 0);
        map.put(QCI_SUC_9, 0);
        map.put(QCI_SUC_10, 0);
        map.put(EPS_BEARER_ID_1, 0);
        map.put(EPS_BEARER_ID_2, 0);
        map.put(EPS_BEARER_ID_3, 0);
        map.put(DURATION, 0);
        map.put(EVENT_ID, 5);
        map.put(TYPE_CAUSE_PROT_TYPE, 0);
        map.put(EVENT_RESULT, 0);
        map.put(ATTACH_TYPE, 0);
        map.put(DETACH_TRIGGER, 0);
        map.put(DETACH_TYPE, 0);
        map.put(DEACTIVATION_TRIGGER, 0);
        map.put(RAT, 2);
        map.put(SERVICE_REQ_TRIGGER, 0);
        return map;
    }

    private static Map <String, Object> setupTempSgehErrRawTables(final String imsiParam, final String tacParam, final String msisdnParam, final String imeisvParam) {
        final Map <String, Object> tempMap = new HashMap<String, Object>();
        final String timeStamp = DateTimeUtilities.getDateTimeMinus30Minutes();
        tempMap.put(IMEISV, imeisvParam);
        tempMap.put(EVENT_ID, 0);
        tempMap.put(RAT, 0);
        tempMap.put(TAC, tacParam);
        tempMap.put(IMSI, imsiParam);
        tempMap.put(DATETIME_ID, timeStamp);
        tempMap.put(APN, "0");
        tempMap.put(EVENT_TIME, timeStamp);
        tempMap.put(EVENT_SOURCE_NAME, "0");
        tempMap.put(HIERARCHY_1, "0");
        tempMap.put(HIERARCHY_2, "0");
        tempMap.put(HIERARCHY_3, "0");
        tempMap.put(VENDOR_PARAM_UPPER_CASE, "0");
        tempMap.put(DURATION, 0);
        tempMap.put(CAUSE_PROT_TYPE_COLUMN, 0);
        tempMap.put(CAUSE_CODE_COLUMN, 0);
        tempMap.put(SUBCAUSE_CODE_COLUMN, 0);
        tempMap.put(EVENT_RESULT, 0);
        tempMap.put(ATTACH_TYPE, 0);
        tempMap.put(DETACH_TYPE, 0);
        tempMap.put(DEACTIVATION_TRIGGER, 0);
        tempMap.put(DETACH_TRIGGER, 0);
        tempMap.put(SERVICE_REQ_TRIGGER, 0);
        tempMap.put(PAGING_ATTEMPTS, 0);
        tempMap.put(REQUEST_RETRIES, 0);
        tempMap.put(MCC, "0");
        tempMap.put(MNC, "0");
        tempMap.put(OLD_MCC, "0");
        tempMap.put(OLD_MNC, "0");
        tempMap.put(OLD_RAC, "0");
        tempMap.put(OLD_LAC, "0");
        tempMap.put(TRANSFERRED_PDP, 0);
        tempMap.put(HLR, 0);
        tempMap.put(DROPPED_PDP, 0);
        tempMap.put(MSISDN, msisdnParam);
        tempMap.put(PTMSI, 3);
        tempMap.put(LINKED_NSAPI, 0);
        tempMap.put(PDP_NSAPI_1, 0);
        tempMap.put(PDP_NSAPI_2, 0);
        tempMap.put(PDP_GGSN_IPADDRESS_1, 0);
        tempMap.put(PDP_GGSN_IPADDRESS_2, 0);
        tempMap.put(PDP_MS_IPADDRESS_1, 0);
        tempMap.put(PDP_MS_IPADDRESS_2, 0);
        tempMap.put(PDP_GGSN_NAME_1, "0");
        tempMap.put(PDP_GGSN_NAME_2, "0");
        tempMap.put(RAC, 0);
        tempMap.put(LAC, 0);
        tempMap.put(UPDATE_TYPE, 0);
        tempMap.put(OLD_SGSN_IPADDRESS, 0);
        return tempMap;
    }

    private void populateTables() throws Exception {
        populateLteErrRawTable();
        populateSgehErrRawTable();
        populateDimImsiMsisdnTable();
    }

    private void populateLteErrRawTable() throws SQLException {
        insertRow(TEMP_EVENT_E_LTE_ERR_RAW, setupTempLteErrRawTableValues(TEST_PARAM_IMSI_ONE, TEST_PARAM_TAC, TEST_PARAM_MSISDN, TEST_PARAM_IMEISV_ONE));
        insertRow(TEMP_EVENT_E_LTE_ERR_RAW, setupTempLteErrRawTableValues(TEST_PARAM_IMSI_ONE, TEST_PARAM_TAC, TEST_PARAM_MSISDN, TEST_PARAM_IMEISV_TWO));
        insertRow(TEMP_EVENT_E_LTE_ERR_RAW, setupTempLteErrRawTableValues(TEST_PARAM_IMSI_TWO, TEST_PARAM_TAC, TEST_PARAM_MSISDN, TEST_PARAM_IMEISV_TWO));
        insertRow(TEMP_EVENT_E_LTE_ERR_RAW, setupTempLteErrRawTableValues(TEST_PARAM_IMSI_TWO, TEST_PARAM_TAC, TEST_PARAM_MSISDN, TEST_PARAM_IMEISV_TWO));
    }

    private void populateSgehErrRawTable() throws SQLException {
        insertRow(TEMP_EVENT_E_SGEH_ERR_RAW, setupTempSgehErrRawTables(TEST_PARAM_IMSI_ONE, TEST_PARAM_TAC, TEST_PARAM_MSISDN, TEST_PARAM_IMEISV_ONE));
        insertRow(TEMP_EVENT_E_SGEH_ERR_RAW, setupTempSgehErrRawTables(TEST_PARAM_IMSI_ONE, TEST_PARAM_TAC, TEST_PARAM_MSISDN, TEST_PARAM_IMEISV_TWO));
        insertRow(TEMP_EVENT_E_SGEH_ERR_RAW, setupTempSgehErrRawTables(TEST_PARAM_IMSI_TWO, TEST_PARAM_TAC, TEST_PARAM_MSISDN, TEST_PARAM_IMEISV_TWO));
        insertRow(TEMP_EVENT_E_SGEH_ERR_RAW, setupTempSgehErrRawTables(TEST_PARAM_IMSI_TWO, TEST_PARAM_TAC, TEST_PARAM_MSISDN, TEST_PARAM_IMEISV_TWO));
    }

    private void populateDimImsiMsisdnTable() throws SQLException {
        insertRow(TEMP_DIM_E_IMSI_MSISDN, getImsiMsisdnMap(TEST_PARAM_IMSI_ONE, TEST_PARAM_MSISDN));
        insertRow(TEMP_DIM_E_IMSI_MSISDN, getImsiMsisdnMap(TEST_PARAM_IMSI_TWO, TEST_PARAM_MSISDN));
    }

    private Map<String, Object> getImsiMsisdnMap(final String imsi, final String msisdn) {
        final String timeStamp = DateTimeUtilities.getDateTimeMinus30Minutes();
        final Map<String, Object> map = new HashMap<String, Object>();
        map.put(IMSI, imsi);
        map.put(MSISDN, msisdn);
        map.put(VENDOR, "0");
        map.put(STATUS, "");
        map.put(MODIFIER, "");
        map.put(CREATED, timeStamp);
        map.put(MODIFIED, timeStamp);
        return map;
    }
}
