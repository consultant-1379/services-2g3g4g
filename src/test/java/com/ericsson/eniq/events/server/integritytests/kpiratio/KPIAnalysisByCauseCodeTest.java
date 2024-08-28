package com.ericsson.eniq.events.server.integritytests.kpiratio;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.common.EventIDConstants.*;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.*;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.*;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.*;

import org.junit.Test;

import com.ericsson.eniq.events.server.resources.KPIRatioResource;
import com.ericsson.eniq.events.server.resources.TestsWithTemporaryTablesBaseTestCase;
import com.ericsson.eniq.events.server.test.queryresults.KPIAnalysisByCauseCodeResult;
import com.ericsson.eniq.events.server.test.stubs.DummyUriInfoImpl;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;
import com.sun.jersey.core.util.MultivaluedMapImpl;

/**
 * Created by eeikbe on 28/08/2014.
 */
public class KPIAnalysisByCauseCodeTest extends TestsWithTemporaryTablesBaseTestCase<KPIAnalysisByCauseCodeResult> {

    private static final int CAUSE_CODE_VALUE = 11;
    private static final int SUB_CAUSE_CODE_VALUE = 22;
    private static final int CAUSE_PROT_TYPE_VALUE = 33;

    private static final String EVENT_TIME_VALUE = "2014-08-29 08:12:00:1212";

    private KPIRatioResource kpiRatioResource;

    private final SimpleDateFormat dateOnlyFormatter = new SimpleDateFormat(DATE_FORMAT);

    @Override
    public void onSetUp() throws Exception {
        super.onSetUp();
        kpiRatioResource = new KPIRatioResource();
        attachDependencies(kpiRatioResource);
    }

    @Test
    public void testKPIAnalysisByCauseCode() throws Exception {
        final String timestamp = DateTimeUtilities.getDateTimeMinusDay(7);
        final String localDateId = dateOnlyFormatter.format(dateOnlyFormatter.parse(timestamp));
        createAndPopulateEVENT_E_SGEH_ERR_RAW(CAUSE_CODE_VALUE, SUB_CAUSE_CODE_VALUE, CAUSE_PROT_TYPE_VALUE, timestamp, localDateId);
        createAndPopulateEVENT_E_LTE_ERR_RAW(CAUSE_CODE_VALUE, SUB_CAUSE_CODE_VALUE, CAUSE_PROT_TYPE_VALUE, timestamp, localDateId);
        createAndPopulateDIMTables();
        final String result = runQueryKPIAnalysisByCauseCode(SAMPLE_CELL, SAMPLE_BSC, RAT_FOR_3G, SAMPLE_SGSN, ISRAU_IN_2G_AND_3G, ONE_WEEK);
        validateResultForKPIAnalysisByCauseCodeQuery(result);
    }

    private Map<String, Object> createCommonRows(final int causeCode, final int subCauseCode, final int causeProtType, final String timestamp,
                                                 final String localDateId) throws Exception {
        final Map<String, Object> columnsAndValuesForRawTable = new HashMap<String, Object>();
        columnsAndValuesForRawTable.put(APN, SAMPLE_APN);
        columnsAndValuesForRawTable.put(EVENT_SOURCE_NAME, SAMPLE_SGSN);
        columnsAndValuesForRawTable.put(VENDOR_PARAM_UPPER_CASE, ERICSSON);
        columnsAndValuesForRawTable.put(IMSI, SAMPLE_IMSI);
        columnsAndValuesForRawTable.put(HIERARCHY_3, SAMPLE_BSC);
        columnsAndValuesForRawTable.put(HIERARCHY_1, SAMPLE_CELL);
        columnsAndValuesForRawTable.put(EVENT_ID, ISRAU_IN_2G_AND_3G);
        columnsAndValuesForRawTable.put(RAT, RAT_FOR_3G);
        columnsAndValuesForRawTable.put(TAC, SAMPLE_TAC);
        columnsAndValuesForRawTable.put(DEACTIVATION_TRIGGER, 0);
        columnsAndValuesForRawTable.put(DATETIME_ID, timestamp);
        columnsAndValuesForRawTable.put(LOCAL_DATE_ID, localDateId);
        columnsAndValuesForRawTable.put(CAUSE_CODE_COLUMN, causeCode);
        columnsAndValuesForRawTable.put(SUBCAUSE_CODE, subCauseCode);
        columnsAndValuesForRawTable.put(CAUSE_PROT_TYPE_COLUMN, causeProtType);
        columnsAndValuesForRawTable.put(EVENT_TIME_COLUMN, EVENT_TIME_VALUE);
        columnsAndValuesForRawTable.put(MSISDN, SAMPLE_MSISDN);
        columnsAndValuesForRawTable.put(HIERARCHY_2, "");
        columnsAndValuesForRawTable.put(OLD_MNC, "440");
        columnsAndValuesForRawTable.put(OLD_MCC, "348");
        columnsAndValuesForRawTable.put(PAGING_ATTEMPTS, "0");
        columnsAndValuesForRawTable.put(REQUEST_RETRIES, "0");
        columnsAndValuesForRawTable.put(MCC, SAMPLE_MCC);
        columnsAndValuesForRawTable.put(MNC, SAMPLE_MNC);
        columnsAndValuesForRawTable.put(DURATION, "84");
        columnsAndValuesForRawTable.put(EVENT_RESULT, "1");
        columnsAndValuesForRawTable.put(ATTACH_TYPE, "1");
        columnsAndValuesForRawTable.put(DETACH_TRIGGER, "1");
        columnsAndValuesForRawTable.put(DETACH_TYPE, "1");
        columnsAndValuesForRawTable.put(SERVICE_REQ_TRIGGER, "1");
        columnsAndValuesForRawTable.put(L_DISCONNECT_PDN_TYPE, "1");
        return columnsAndValuesForRawTable;
    }

    /**
     * Populate a row of the EVENT_E_SGEH_ERR_RAW table.
     * 
     * @param causeCode
     * @param subCauseCode
     * @param causeProtType
     * @param timestamp
     * @param localDateId
     * @throws Exception
     */
    private void populateRowEVENT_E_SGEH_ERR_RAW(final int causeCode, final int subCauseCode, final int causeProtType, final String timestamp,
                                                 final String localDateId) throws Exception {
        createAndPopulateEVENT_E_SGEH_ERR_RAW(causeCode, subCauseCode, causeProtType, timestamp, localDateId, false);
    }

    /**
     * Create and populate a row of the EVENT_E_SGEH_ERR_RAW table.
     * 
     * @param causeCode
     * @param subCauseCode
     * @param causeProtType
     * @param timestamp
     * @param localDateId
     * @throws Exception
     */
    private void createAndPopulateEVENT_E_SGEH_ERR_RAW(final int causeCode, final int subCauseCode, final int causeProtType, final String timestamp,
                                                       final String localDateId) throws Exception {
        createAndPopulateEVENT_E_SGEH_ERR_RAW(causeCode, subCauseCode, causeProtType, timestamp, localDateId, true);
    }

    /**
     * Populate a row of the EVENT_E_LTE_ERR_RAW table.
     * 
     * @param causeCode
     * @param subCauseCode
     * @param causeProtType
     * @param timestamp
     * @param localDateId
     * @throws Exception
     */
    private void populateRowEVENT_E_LTE_ERR_RAW(final int causeCode, final int subCauseCode, final int causeProtType, final String timestamp,
                                                final String localDateId) throws Exception {
        createAndPopulateEVENT_E_LTE_ERR_RAW(causeCode, subCauseCode, causeProtType, timestamp, localDateId, false);
    }

    /**
     * Create and populate a row of the EVENT_E_LTE_ERR_RAW table.
     * 
     * @param causeCode
     * @param subCauseCode
     * @param causeProtType
     * @param timestamp
     * @param localDateId
     * @throws Exception
     */
    private void createAndPopulateEVENT_E_LTE_ERR_RAW(final int causeCode, final int subCauseCode, final int causeProtType, final String timestamp,
                                                      final String localDateId) throws Exception {
        createAndPopulateEVENT_E_LTE_ERR_RAW(causeCode, subCauseCode, causeProtType, timestamp, localDateId, true);
    }

    private void createAndPopulateEVENT_E_SGEH_ERR_RAW(final int causeCode, final int subCauseCode, final int causeProtType, final String timestamp,
                                                       final String localDateId, final boolean createTable) throws Exception {
        final Map<String, Object> columnsAndValuesForRawTable = createCommonRows(causeCode, subCauseCode, causeProtType, timestamp, localDateId);
        columnsAndValuesForRawTable.put(PTMSI, "0");
        columnsAndValuesForRawTable.put(LINKED_NSAPI, "0");
        columnsAndValuesForRawTable.put(PDP_NSAPI_1, "5");
        columnsAndValuesForRawTable.put(PDP_GGSN_IPADDRESS_1, "1");
        columnsAndValuesForRawTable.put(PDP_GGSN_IPADDRESS_2, "2");
        columnsAndValuesForRawTable.put(PDP_GGSN_NAME_1, "1");
        columnsAndValuesForRawTable.put(PDP_GGSN_NAME_2, "2");
        columnsAndValuesForRawTable.put(PDP_MS_IPADDRESS_1, "1");
        columnsAndValuesForRawTable.put(PDP_MS_IPADDRESS_2, "2");
        columnsAndValuesForRawTable.put(PDP_NSAPI_2, "0");
        columnsAndValuesForRawTable.put(RAC, "1");
        columnsAndValuesForRawTable.put(LAC, "1");
        columnsAndValuesForRawTable.put(UPDATE_TYPE, "2");
        columnsAndValuesForRawTable.put(OLD_SGSN_IPADDRESS, "2");
        columnsAndValuesForRawTable.put(OLD_RAC, "1");
        columnsAndValuesForRawTable.put(OLD_LAC, "2");
        columnsAndValuesForRawTable.put(TRANSFERRED_PDP, "2");
        columnsAndValuesForRawTable.put(DROPPED_PDP, "3");
        columnsAndValuesForRawTable.put(HLR, "1");

        if (createTable) {
            createTemporaryTable(TEMP_EVENT_E_SGEH_ERR_RAW, columnsAndValuesForRawTable.keySet());
        }

        insertRow(TEMP_EVENT_E_SGEH_ERR_RAW, columnsAndValuesForRawTable);
    }

    private void createAndPopulateEVENT_E_LTE_ERR_RAW(final int causeCode, final int subCauseCode, final int causeProtType, final String timestamp,
                                                      final String localDateId, final boolean createTable) throws Exception {
        final Map<String, Object> columnsAndValuesForRawTable = createCommonRows(causeCode, subCauseCode, causeProtType, timestamp, localDateId);
        columnsAndValuesForRawTable.put(TRAC, "1234");
        columnsAndValuesForRawTable.put(OLD_TRAC, "000");
        columnsAndValuesForRawTable.put(OLD_CELL_ID, "1");
        columnsAndValuesForRawTable.put(OLD_L_MMEGI, "11253");
        columnsAndValuesForRawTable.put(OLD_L_MMEC, "8");
        columnsAndValuesForRawTable.put(OLD_L_MTMSI, "3372249920");
        columnsAndValuesForRawTable.put(OLD_SGW_IPV4, "1");
        columnsAndValuesForRawTable.put(OLD_SGW_IPV6, "");
        columnsAndValuesForRawTable.put(PDN_BEARER_ID_1, "5");
        columnsAndValuesForRawTable.put(PDN_BEARER_ID_2, "6");
        columnsAndValuesForRawTable.put(PDN_BEARER_ID_3, "7");
        columnsAndValuesForRawTable.put(PDN_PAA_IPV4_1, "");
        columnsAndValuesForRawTable.put(PDN_PAA_IPV4_2, "");
        columnsAndValuesForRawTable.put(PDN_PAA_IPV4_3, "");
        columnsAndValuesForRawTable.put(PDN_PAA_IPV6_1, "");
        columnsAndValuesForRawTable.put(PDN_PAA_IPV6_2, "");
        columnsAndValuesForRawTable.put(PDN_PAA_IPV6_3, "");
        columnsAndValuesForRawTable.put(PDN_PGW_IPV4_1, "");
        columnsAndValuesForRawTable.put(PDN_PGW_IPV4_2, "");
        columnsAndValuesForRawTable.put(PDN_PGW_IPV4_3, "");
        columnsAndValuesForRawTable.put(PDN_PGW_IPV6_1, "");
        columnsAndValuesForRawTable.put(PDN_PGW_IPV6_2, "");
        columnsAndValuesForRawTable.put(PDN_PGW_IPV6_3, "");
        columnsAndValuesForRawTable.put(ARP_PL_1, "1");
        columnsAndValuesForRawTable.put(ARP_PL_2, "2");
        columnsAndValuesForRawTable.put(ARP_PL_3, "3");
        columnsAndValuesForRawTable.put(ARP_PCI_1, "1");
        columnsAndValuesForRawTable.put(ARP_PCI_2, "2");
        columnsAndValuesForRawTable.put(ARP_PCI_3, "3");
        columnsAndValuesForRawTable.put(ARP_PVI_1, "1");
        columnsAndValuesForRawTable.put(ARP_PVI_2, "2");
        columnsAndValuesForRawTable.put(ARP_PVI_3, "3");
        columnsAndValuesForRawTable.put(GBR_UPLINK_1, "1");
        columnsAndValuesForRawTable.put(GBR_UPLINK_2, "2");
        columnsAndValuesForRawTable.put(GBR_UPLINK_3, "3");
        columnsAndValuesForRawTable.put(GBR_DOWNLINK_1, "1");
        columnsAndValuesForRawTable.put(GBR_DOWNLINK_2, "2");
        columnsAndValuesForRawTable.put(GBR_DOWNLINK_3, "3");
        columnsAndValuesForRawTable.put(EPS_BEARER_ID_1, "1");
        columnsAndValuesForRawTable.put(EPS_BEARER_ID_2, "2");
        columnsAndValuesForRawTable.put(EPS_BEARER_ID_3, "3");
        columnsAndValuesForRawTable.put(QCI_ERR_1, "1");
        columnsAndValuesForRawTable.put(QCI_ERR_2, "2");
        columnsAndValuesForRawTable.put(QCI_ERR_3, "3");
        columnsAndValuesForRawTable.put(QCI_ERR_4, "4");
        columnsAndValuesForRawTable.put(QCI_ERR_5, "5");
        columnsAndValuesForRawTable.put(QCI_ERR_6, "6");
        columnsAndValuesForRawTable.put(QCI_ERR_7, "7");
        columnsAndValuesForRawTable.put(QCI_ERR_8, "8");
        columnsAndValuesForRawTable.put(QCI_ERR_9, "9");
        columnsAndValuesForRawTable.put(QCI_ERR_10, "10");
        columnsAndValuesForRawTable.put(QCI_SUC_1, "1");
        columnsAndValuesForRawTable.put(QCI_SUC_2, "2");
        columnsAndValuesForRawTable.put(QCI_SUC_3, "3");
        columnsAndValuesForRawTable.put(QCI_SUC_4, "4");
        columnsAndValuesForRawTable.put(QCI_SUC_5, "5");
        columnsAndValuesForRawTable.put(QCI_SUC_6, "6");
        columnsAndValuesForRawTable.put(QCI_SUC_7, "7");
        columnsAndValuesForRawTable.put(QCI_SUC_8, "8");
        columnsAndValuesForRawTable.put(QCI_SUC_9, "9");
        columnsAndValuesForRawTable.put(QCI_SUC_10, "10");
        columnsAndValuesForRawTable.put(EVENT_SUBTYPE_ID, "3");
        columnsAndValuesForRawTable.put(SMS_ONLY, "1");
        columnsAndValuesForRawTable.put(COMBINED_TAU_TYPE, "1");
        columnsAndValuesForRawTable.put(BEARER_CAUSE_1, "1");
        columnsAndValuesForRawTable.put(BEARER_CAUSE_2, "2");
        columnsAndValuesForRawTable.put(BEARER_CAUSE_3, "3");

        if (createTable) {
            createTemporaryTable(TEMP_EVENT_E_LTE_ERR_RAW, columnsAndValuesForRawTable.keySet());
        }

        insertRow(TEMP_EVENT_E_LTE_ERR_RAW, columnsAndValuesForRawTable);
    }

    private void createAndPopulateDIMTables() throws Exception {
        createAndPopulateDIM_CAUSECODE();
        createAndPopulateDIM_SUBCAUSECODE();
        createAndPopulateDIM_CAUSE_PROT_TYPE();
    }

    private void createAndPopulateDIM_CAUSECODE() throws Exception {
        final Map<String, Object> columnsAndValuesForDIMTable = new HashMap<String, Object>();
        columnsAndValuesForDIMTable.put(CAUSE_CODE_COLUMN, 11);
        columnsAndValuesForDIMTable.put(CAUSE_CODE_DESC_COLUMN, "REQUEST_IMSI");
        columnsAndValuesForDIMTable.put(CAUSE_CODE_HELP_COLUMN, "Bueller, anyone, anyone?");
        columnsAndValuesForDIMTable.put(CAUSE_PROT_TYPE_COLUMN, CAUSE_PROT_TYPE_VALUE);
        createTemporaryTable(TEMP_DIM_E_SGEH_CAUSECODE, columnsAndValuesForDIMTable.keySet());
        createTemporaryTable(TEMP_DIM_E_LTE_CAUSECODE, columnsAndValuesForDIMTable.keySet());
        insertRow(TEMP_DIM_E_SGEH_CAUSECODE, columnsAndValuesForDIMTable);
        insertRow(TEMP_DIM_E_LTE_CAUSECODE, columnsAndValuesForDIMTable);
    }

    private void createAndPopulateDIM_SUBCAUSECODE() throws Exception {
        final Map<String, Object> columnsAndValuesForDIMTable = new HashMap<String, Object>();
        columnsAndValuesForDIMTable.put(SUBCAUSE_CODE, 22);
        columnsAndValuesForDIMTable.put(SUB_CAUSE_CODE_DESC_COLUMN, "NO_VALUE");
        columnsAndValuesForDIMTable.put(SUB_CAUSE_CODE_HELP_COLUMN, "No suggested action.");
        columnsAndValuesForDIMTable.put(LONG_SUBCAUSE_CODE_DESC_COLUMN, "There is no sub cause code available");
        createTemporaryTable(TEMP_DIM_E_SGEH_SUBCAUSECODE, columnsAndValuesForDIMTable.keySet());
        createTemporaryTable(TEMP_DIM_E_LTE_SUBCAUSECODE, columnsAndValuesForDIMTable.keySet());
        insertRow(TEMP_DIM_E_SGEH_SUBCAUSECODE, columnsAndValuesForDIMTable);
        insertRow(TEMP_DIM_E_LTE_SUBCAUSECODE, columnsAndValuesForDIMTable);
    }

    private void createAndPopulateDIM_CAUSE_PROT_TYPE() throws Exception {
        final Map<String, Object> columnsAndValuesForDIMTable = new HashMap<String, Object>();
        columnsAndValuesForDIMTable.put(CAUSE_PROT_TYPE_COLUMN, CAUSE_PROT_TYPE_VALUE);
        columnsAndValuesForDIMTable.put(CAUSE_PROT_TYPE_DESC_COLUMN, "A Description of a Cause Prot Type.");
        createTemporaryTable(TEMP_DIM_E_SGEH_CAUSE_PROT_TYPE, columnsAndValuesForDIMTable.keySet());
        createTemporaryTable(TEMP_DIM_E_LTE_CAUSE_PROT_TYPE, columnsAndValuesForDIMTable.keySet());
        insertRow(TEMP_DIM_E_SGEH_CAUSE_PROT_TYPE, columnsAndValuesForDIMTable);
        insertRow(TEMP_DIM_E_LTE_CAUSE_PROT_TYPE, columnsAndValuesForDIMTable);
    }

    private String runQueryKPIAnalysisByCauseCode(final String cell, final String bsc, final int rat, final String sgsnOrMME, final int eventId,
                                                  final String time) throws URISyntaxException {
        final MultivaluedMapImpl map = new MultivaluedMapImpl();
        map.putSingle(TIME_QUERY_PARAM, time);
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, "500");
        map.putSingle(RAT_PARAM, rat);
        map.putSingle(APN_PARAM, SAMPLE_APN);
        map.putSingle(SGSN_PARAM, sgsnOrMME);
        map.putSingle(VENDOR_PARAM, ERICSSON);
        map.putSingle(BSC_PARAM, bsc);
        map.putSingle(CELL_PARAM, cell);
        map.putSingle(EVENT_ID_PARAM, eventId);
        map.putSingle(TYPE_PARAM, TYPE_APN);
        DummyUriInfoImpl.setUriInfo(map, kpiRatioResource);
        final String result = kpiRatioResource.getData();
        return result;
    }

    private void validateResultForKPIAnalysisByCauseCodeQuery(final String json) throws Exception {
        final List<KPIAnalysisByCauseCodeResult> results = getTranslator().translateResult(json, KPIAnalysisByCauseCodeResult.class);
        assertThat(results.size(), is(1));
        final KPIAnalysisByCauseCodeResult result = results.get(0);
        //01. RAT ID
        assertThat(result.getRatId(), is(1));
        //02. APN
        assertThat(result.getApn(), is(SAMPLE_APN));
        //03. SGSN-MME
        assertThat(result.getSGSN(), is(SAMPLE_SGSN));
        //04. Vendor
        assertThat(result.getVendor(), is(ERICSSON));
        //05. Controller
        assertThat(result.getController(), is(SAMPLE_BSC));
        //06. Access Area
        assertThat(result.getAccessArea(), is("cell1,,bsc1,ericsson,GSM"));
        //07. Cause Code ID
        assertThat(result.getCauseCodeId(), is(11));
        //08. SubCauseCodeID
        assertThat(result.getSubCauseCodeId(), is(22));
        //09. Cause Prot Type
        assertThat(result.getCauseProtType(), is(CAUSE_PROT_TYPE_VALUE));
        //10. Cause Code
        assertThat(result.getCauseCode(), is("REQUEST_IMSI"));
        //11. Sub Cause Code
        assertThat(result.getSubCauseCode(), is("NO_VALUE"));
        //12. RAT
        assertThat(result.getRat(), is("3G"));
        //13. EventID
        assertThat(result.getEventId(), is(3));
        //14. Event Type
        assertThat(result.getEventType(), is("ISRAU"));
        //15. Occurrences
        assertThat(result.getOccurrences(), is(1));
        //16. Impacted Subscribers
        assertThat(result.getImpactedSubscribers(), is(1));
    }

}
