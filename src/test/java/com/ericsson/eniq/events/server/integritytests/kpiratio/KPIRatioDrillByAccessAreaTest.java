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
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.*;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.*;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.*;

import javax.ws.rs.core.MultivaluedMap;

import org.junit.Test;

import com.ericsson.eniq.events.server.resources.KPIRatioResource;
import com.ericsson.eniq.events.server.resources.TestsWithTemporaryTablesBaseTestCase;
import com.ericsson.eniq.events.server.test.populator.LookupTechPackPopulator;
import com.ericsson.eniq.events.server.test.queryresults.KPIRatioDrillByAccessAreaResult;
import com.ericsson.eniq.events.server.test.stubs.DummyUriInfoImpl;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;
import com.sun.jersey.core.util.MultivaluedMapImpl;

public class KPIRatioDrillByAccessAreaTest extends TestsWithTemporaryTablesBaseTestCase<KPIRatioDrillByAccessAreaResult> {

    private KPIRatioResource kpiRatioResource;

    private final int SAMPLE_CAUSE_CODE = 29;

    private final int SAMPLE_SUB_CAUSE_CODE = 352;

    private final String SAMPLE_ACCESS_AREA = "8378";

    @Override
    public void onSetUp() throws Exception {
        super.onSetUp();
        kpiRatioResource = new KPIRatioResource();
        attachDependencies(kpiRatioResource);
    }

    @Test
    public void testTypeAccessAreaDrilltypeCauseCode() throws Exception {
        final String timestamp = DateTimeUtilities.getDateTimeMinus5Minutes();
        createAndPopulateRawTablesForSgehQuery(timestamp);
        createDimTables();
        final String json = runQuery(SAMPLE_BSC, RAT_INTEGER_VALUE_FOR_3G);
        System.out.println(json);
        final List<KPIRatioDrillByAccessAreaResult> summaryResult = getTranslator().translateResult(json, KPIRatioDrillByAccessAreaResult.class);
        validateResultFromSGEHables(summaryResult);
    }

    private String runQuery(final String controller, final String rat) throws URISyntaxException {
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(TYPE_PARAM, TYPE_CELL);
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(TIME_QUERY_PARAM, THIRTY_MINUTES);
        map.putSingle(CELL_PARAM, "8378");
        map.putSingle(BSC_PARAM, controller);
        map.putSingle(VENDOR_PARAM, ERICSSON);
        map.putSingle(EVENT_ID_PARAM, "1");
        map.putSingle(RAT_PARAM, rat);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, "500");
        map.putSingle(TYPE_CAUSE_CODE, "29");
        map.putSingle(TYPE_SUB_CAUSE_CODE, "352");
        map.putSingle(CAUSE_PROT_TYPE_PARAM, "0");
        DummyUriInfoImpl.setUriInfo(map, kpiRatioResource);
        return kpiRatioResource.getData();
    }

    private void validateResultFromSGEHables(final List<KPIRatioDrillByAccessAreaResult> results) {
        assertThat(results.size(), is(1));
        final KPIRatioDrillByAccessAreaResult result = results.get(0);
        assertThat(result.getImsi(), is(SAMPLE_IMSI));
        assertThat(result.getVendor(), is(ERICSSON));
        assertThat(result.getController(), is(SAMPLE_BSC));
        assertThat(result.getAccessArea(), is(SAMPLE_ACCESS_AREA));
        assertThat(result.getApn(), is(SAMPLE_APN));

    }

    private void createAndPopulateRawTablesForSgehQuery(final String timestamp) throws SQLException, Exception {

        final Map<String, Object> columnsAndValuesForRawTables = new HashMap<String, Object>();
        columnsAndValuesForRawTables.put(HIERARCHY_1, "8378");
        columnsAndValuesForRawTables.put(HIERARCHY_3, SAMPLE_BSC);
        columnsAndValuesForRawTables.put(VENDOR_PARAM_UPPER_CASE, ERICSSON);
        columnsAndValuesForRawTables.put(CAUSE_CODE_COLUMN, SAMPLE_CAUSE_CODE);
        columnsAndValuesForRawTables.put(SUBCAUSE_CODE_COLUMN, SAMPLE_SUB_CAUSE_CODE);
        columnsAndValuesForRawTables.put(EVENT_ID, "1");
        columnsAndValuesForRawTables.put(RAT, RAT_FOR_3G);
        columnsAndValuesForRawTables.put(TAC, SAMPLE_TAC);
        columnsAndValuesForRawTables.put(IMSI, SAMPLE_IMSI);
        columnsAndValuesForRawTables.put(EVENT_TIME, timestamp);
        columnsAndValuesForRawTables.put(DATETIME_ID, timestamp);
        columnsAndValuesForRawTables.put(MSISDN, 0);
        columnsAndValuesForRawTables.put(EVENT_SOURCE_NAME, SAMPLE_MME);
        columnsAndValuesForRawTables.put(HIERARCHY_2, SAMPLE_ACCESS_AREA);
        columnsAndValuesForRawTables.put(TRAC, 0);
        columnsAndValuesForRawTables.put(DURATION, 0);
        columnsAndValuesForRawTables.put(CAUSE_PROT_TYPE_COLUMN, 0);
        columnsAndValuesForRawTables.put(EVENT_RESULT, 0);
        columnsAndValuesForRawTables.put(ATTACH_TYPE, 0);
        columnsAndValuesForRawTables.put(DETACH_TYPE, 0);
        columnsAndValuesForRawTables.put(DEACTIVATION_TRIGGER, 0);
        columnsAndValuesForRawTables.put(DETACH_TRIGGER, 0);
        columnsAndValuesForRawTables.put(SERVICE_REQ_TRIGGER, 0);
        columnsAndValuesForRawTables.put(PAGING_ATTEMPTS, 0);
        columnsAndValuesForRawTables.put(REQUEST_RETRIES, 0);
        columnsAndValuesForRawTables.put(MCC, "0");
        columnsAndValuesForRawTables.put(MNC, "0");
        columnsAndValuesForRawTables.put(OLD_MCC, "0");
        columnsAndValuesForRawTables.put(OLD_MNC, "0");
        columnsAndValuesForRawTables.put(OLD_RAC, 0);
        columnsAndValuesForRawTables.put(OLD_LAC, 0);
        columnsAndValuesForRawTables.put(TRANSFERRED_PDP, 0);
        columnsAndValuesForRawTables.put(HLR, 0);
        columnsAndValuesForRawTables.put(DROPPED_PDP, 0);
        columnsAndValuesForRawTables.put(PTMSI, 3);
        columnsAndValuesForRawTables.put(LINKED_NSAPI, 0);
        columnsAndValuesForRawTables.put(PDP_NSAPI_1, 0);
        columnsAndValuesForRawTables.put(PDP_NSAPI_2, 0);
        columnsAndValuesForRawTables.put(PDP_GGSN_IPADDRESS_1, 0);
        columnsAndValuesForRawTables.put(PDP_GGSN_IPADDRESS_2, 0);
        columnsAndValuesForRawTables.put(PDP_MS_IPADDRESS_1, 0);
        columnsAndValuesForRawTables.put(PDP_MS_IPADDRESS_2, 0);
        columnsAndValuesForRawTables.put(PDP_GGSN_NAME_1, "0");
        columnsAndValuesForRawTables.put(PDP_GGSN_NAME_2, "0");
        columnsAndValuesForRawTables.put(RAC, 0);
        columnsAndValuesForRawTables.put(LAC, 0);
        columnsAndValuesForRawTables.put(UPDATE_TYPE, 0);
        columnsAndValuesForRawTables.put(OLD_SGSN_IPADDRESS, 0);
        columnsAndValuesForRawTables.put(OLD_TRAC, 0);
        columnsAndValuesForRawTables.put(OLD_CELL_ID, 0);
        columnsAndValuesForRawTables.put(OLD_L_MMEGI, 0);
        columnsAndValuesForRawTables.put(OLD_L_MMEC, 0);
        columnsAndValuesForRawTables.put(OLD_L_MTMSI, 0);
        columnsAndValuesForRawTables.put(OLD_SGW_IPV4, 0);
        columnsAndValuesForRawTables.put(OLD_SGW_IPV6, 0);
        columnsAndValuesForRawTables.put(PDN_BEARER_ID_1, 0);
        columnsAndValuesForRawTables.put(PDN_BEARER_ID_2, 0);
        columnsAndValuesForRawTables.put(PDN_BEARER_ID_3, 0);
        columnsAndValuesForRawTables.put(PDN_PAA_IPV4_1, 0);
        columnsAndValuesForRawTables.put(PDN_PAA_IPV4_2, 0);
        columnsAndValuesForRawTables.put(PDN_PAA_IPV4_3, 0);
        columnsAndValuesForRawTables.put(PDN_PAA_IPV6_1, 0);
        columnsAndValuesForRawTables.put(PDN_PAA_IPV6_2, 0);
        columnsAndValuesForRawTables.put(PDN_PAA_IPV6_3, 0);
        columnsAndValuesForRawTables.put(PDN_PGW_IPV4_1, 0);
        columnsAndValuesForRawTables.put(PDN_PGW_IPV4_2, 0);
        columnsAndValuesForRawTables.put(PDN_PGW_IPV4_3, 0);
        columnsAndValuesForRawTables.put(PDN_PGW_IPV6_1, 0);
        columnsAndValuesForRawTables.put(PDN_PGW_IPV6_2, 0);
        columnsAndValuesForRawTables.put(PDN_PGW_IPV6_3, 0);
        columnsAndValuesForRawTables.put(ARP_PL_1, 0);
        columnsAndValuesForRawTables.put(ARP_PL_2, 0);
        columnsAndValuesForRawTables.put(ARP_PL_3, 0);
        columnsAndValuesForRawTables.put(GBR_UPLINK_1, 0);
        columnsAndValuesForRawTables.put(GBR_UPLINK_2, 0);
        columnsAndValuesForRawTables.put(GBR_UPLINK_3, 0);
        columnsAndValuesForRawTables.put(GBR_DOWNLINK_1, 0);
        columnsAndValuesForRawTables.put(GBR_DOWNLINK_2, 0);
        columnsAndValuesForRawTables.put(GBR_DOWNLINK_3, 0);
        columnsAndValuesForRawTables.put(L_DISCONNECT_PDN_TYPE, 0);
        columnsAndValuesForRawTables.put(EVENT_SUBTYPE_ID, 0);
        columnsAndValuesForRawTables.put(SMS_ONLY, 0);
        columnsAndValuesForRawTables.put(COMBINED_TAU_TYPE, 0);
        columnsAndValuesForRawTables.put(ARP_PCI_1, 0);
        columnsAndValuesForRawTables.put(ARP_PCI_2, 0);
        columnsAndValuesForRawTables.put(ARP_PCI_3, 0);
        columnsAndValuesForRawTables.put(ARP_PVI_1, 0);
        columnsAndValuesForRawTables.put(ARP_PVI_2, 0);
        columnsAndValuesForRawTables.put(ARP_PVI_3, 0);
        columnsAndValuesForRawTables.put(BEARER_CAUSE_1, 0);
        columnsAndValuesForRawTables.put(BEARER_CAUSE_2, 0);
        columnsAndValuesForRawTables.put(BEARER_CAUSE_3, 0);
        columnsAndValuesForRawTables.put(QCI_ERR_1, 0);
        columnsAndValuesForRawTables.put(QCI_ERR_2, 0);
        columnsAndValuesForRawTables.put(QCI_ERR_3, 0);
        columnsAndValuesForRawTables.put(QCI_ERR_4, 0);
        columnsAndValuesForRawTables.put(QCI_ERR_5, 0);
        columnsAndValuesForRawTables.put(QCI_ERR_6, 0);
        columnsAndValuesForRawTables.put(QCI_ERR_7, 0);
        columnsAndValuesForRawTables.put(QCI_ERR_8, 0);
        columnsAndValuesForRawTables.put(QCI_ERR_9, 0);
        columnsAndValuesForRawTables.put(QCI_ERR_10, 0);
        columnsAndValuesForRawTables.put(QCI_SUC_1, 0);
        columnsAndValuesForRawTables.put(QCI_SUC_2, 0);
        columnsAndValuesForRawTables.put(QCI_SUC_3, 0);
        columnsAndValuesForRawTables.put(QCI_SUC_4, 0);
        columnsAndValuesForRawTables.put(QCI_SUC_5, 0);
        columnsAndValuesForRawTables.put(QCI_SUC_6, 0);
        columnsAndValuesForRawTables.put(QCI_SUC_7, 0);
        columnsAndValuesForRawTables.put(QCI_SUC_8, 0);
        columnsAndValuesForRawTables.put(QCI_SUC_9, 0);
        columnsAndValuesForRawTables.put(QCI_SUC_10, 0);
        columnsAndValuesForRawTables.put(EPS_BEARER_ID_1, 0);
        columnsAndValuesForRawTables.put(EPS_BEARER_ID_2, 0);
        columnsAndValuesForRawTables.put(EPS_BEARER_ID_3, 0);
        columnsAndValuesForRawTables.put(APN, SAMPLE_APN);
        createTemporaryTable(TEMP_EVENT_E_LTE_ERR_RAW, columnsAndValuesForRawTables.keySet());
        createTemporaryTable(TEMP_EVENT_E_SGEH_ERR_RAW, columnsAndValuesForRawTables.keySet());
        insertRow(TEMP_EVENT_E_SGEH_ERR_RAW, columnsAndValuesForRawTables);

        columnsAndValuesForRawTables.put(TAC, SAMPLE_EXCLUSIVE_TAC);

        insertRow(TEMP_EVENT_E_SGEH_ERR_RAW, columnsAndValuesForRawTables);

    }

    private void createDimTables() throws Exception {
        new LookupTechPackPopulator().createAndPopulateLookupTable(connection, TEMP_DIM_E_SGEH_CAUSECODE);
        new LookupTechPackPopulator().createAndPopulateLookupTable(connection, TEMP_DIM_E_LTE_CAUSECODE);
        new LookupTechPackPopulator().createAndPopulateLookupTable(connection, TEMP_DIM_E_SGEH_SUBCAUSECODE);
        new LookupTechPackPopulator().createAndPopulateLookupTable(connection, TEMP_DIM_E_LTE_SUBCAUSECODE);
        new LookupTechPackPopulator().createAndPopulateLookupTable(connection, TEMP_DIM_E_LTE_CAUSE_PROT_TYPE);
        new LookupTechPackPopulator().createAndPopulateLookupTable(connection, TEMP_DIM_E_SGEH_CAUSE_PROT_TYPE);
    }

}
