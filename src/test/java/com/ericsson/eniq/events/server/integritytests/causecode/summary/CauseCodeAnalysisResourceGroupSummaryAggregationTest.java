/*------------------------------------------------------------------------------
 *******************************************************************************
 * COPYRIGHT Ericsson 2013
 *
 * The copyright to the computer program(s) herein is the property of
 * Ericsson Inc. The programs may be used and/or copied only with written
 * permission from Ericsson Inc. or in accordance with the terms and
 * conditions stipulated in the agreement/contract under which the
 * program(s) have been supplied.
 *******************************************************************************
 *----------------------------------------------------------------------------*/
package com.ericsson.eniq.events.server.integritytests.causecode.summary;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.*;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.*;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import javax.ws.rs.core.MultivaluedMap;

import org.junit.Test;

import com.ericsson.eniq.events.server.resources.CauseCodeAnalysisResource;
import com.ericsson.eniq.events.server.resources.TestsWithTemporaryTablesBaseTestCase;
import com.ericsson.eniq.events.server.test.queryresults.CauseCodeAnalysisSummaryResult;
import com.ericsson.eniq.events.server.test.sql.SQLCommand;
import com.ericsson.eniq.events.server.test.sql.SQLExecutor;
import com.ericsson.eniq.events.server.test.stubs.DummyUriInfoImpl;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;
import com.sun.jersey.core.util.MultivaluedMapImpl;

public class CauseCodeAnalysisResourceGroupSummaryAggregationTest extends
        TestsWithTemporaryTablesBaseTestCase<CauseCodeAnalysisSummaryResult> {

    /**
     * 
     */
    private static final String EMPTY_STRING = "";

    private final CauseCodeAnalysisResource causeCodeAnalysisResource = new CauseCodeAnalysisResource();

    private static final String DESC_FOR_CAUSE_PROT_TYPE_1_IN_2G = "GTP";

    private static final String LTE_ERR_APN = "#EVENT_E_LTE_APN_CC_SCC_ERR_DAY";

    private static final String LTE_ERR_RAW = "#EVENT_E_LTE_ERR_RAW";

    private static final String SGEH_ERR_APN = "#EVENT_E_SGEH_APN_CC_SCC_ERR_DAY";

    private static final String SGEH_ERR_RAW = "#EVENT_E_SGEH_ERR_RAW";

    private static final String SGEH_ERR_Controller = "#EVENT_E_SGEH_VEND_HIER3_CC_SCC_ERR_DAY";

    private static final String LTE_ERR_Controller = "#EVENT_E_LTE_VEND_HIER3_CC_SCC_ERR_DAY";

    private static final String SGEH_ERR_SGSN = "#EVENT_E_SGEH_EVNTSRC_CC_SCC_ERR_DAY";

    private static final String LTE_ERR_SGSN = "#EVENT_E_LTE_EVNTSRC_CC_SCC_ERR_DAY";

    private static final String DESC_FOR_CAUSE_PROT_TYPE_1_IN_4G = "NAS";

    private final static List<String> tempTables = new ArrayList<String>();

    private final static List<String> tempTablesController = new ArrayList<String>();

    private final static List<String> tempTablesSgsn = new ArrayList<String>();
    private final static List<String> tempRawTables = new ArrayList<String>();

    private final static int causeCode_38 = 38;

    private final static int subCauseCode_12 = 12;

    private final static int subCauseCode_42 = 42;

    private final static int causeCode_7 = 7;

    private final int causeProtTypeUsedInTable = 1;

    private final static List<String> tempTopologyTables = new ArrayList<String>();

    private final static Map<Integer, String> causeCodeMapping = new HashMap<Integer, String>();

    private final static Map<Integer, String> causeCodeHelpMapping = new HashMap<Integer, String>();

    private final static Map<Integer, String> subCauseCodeMapping = new HashMap<Integer, String>();

    private final static Map<Integer, String> subCauseCodeHelpMapping = new HashMap<Integer, String>();

    private final int noErrorsForSgehCauseCode_7 = 11;

    private final int noErrorsForLteCauseCode_7 = 8;

    private final int noErrorsForSgehCauseCode_38 = 6;

    private final int noErrorsForLteCauseCode_38 = 2;

    private static final int IMSI_1 = 123456;

    private static final int IMSI_2 = 123455;

    private static final String APN_1 = "blackberry.net";

    private static final String BSC_1 = "BSC1";

    private static final String SGSN_1 = "SGSN1";

    private static final String Vendor = "Ericsson";

    private static final int TAC_1 = 4321;

    private static final String GROUP_TYPE_E_APN = "#GROUP_TYPE_E_APN";

    private static final String GROUP_TYPE_E_RAT_VEND_HIER3 = "#GROUP_TYPE_E_RAT_VEND_HIER3";

    private static final String GROUP_TYPE_E_EVNTSRC = "#GROUP_TYPE_E_EVNTSRC";

    private final static HashMap<String, String> apnGroupTableColumns;

    private final static HashMap<String, String> bscGroupTableColumns;

    private final static HashMap<String, String> sgsnGroupTableColumns;
    private final static HashMap<String, Object> valuesForGroupTable;

    private final static HashMap<String, Object> valuesForBscGroupTable;

    private final static HashMap<String, Object> valuesForSgsnGroupTable;
    private final SimpleDateFormat dateOnlyFormatter = new SimpleDateFormat(
            DATE_FORMAT);

    // only applicable for LTE
    private final static int subCauseCode_522 = 522;

    private final static Map<Integer, String> causeCodeMappingLTE = new HashMap<Integer, String>();

    private final static Map<Integer, String> causeCodeHelpMappingLTE = new HashMap<Integer, String>();

    private final static Map<Integer, String> subCauseCodeMappingLTE = new HashMap<Integer, String>();

    private final static Map<Integer, String> subCauseCodeHelpMappingLTE = new HashMap<Integer, String>();

    static {
        causeCodeMapping.put(causeCode_7, "GPRS service not allowed");
        causeCodeMapping.put(causeCode_38, "Network Failure");

        causeCodeHelpMapping.put(causeCode_7, "cc help 7");
        causeCodeHelpMapping.put(causeCode_38, "cc help 38");

        subCauseCodeMapping.put(subCauseCode_12,
                "Auth failed: unknown subscriber; operator determined barring");
        subCauseCodeMapping
                .put(subCauseCode_42,
                        "GGSN responded with reject cause different from #192 during SGSN initiated modification");

        subCauseCodeHelpMapping.put(subCauseCode_12,
                "Check the subscription data in the HLR.");
        subCauseCodeHelpMapping.put(subCauseCode_42, EMPTY_STRING);

        tempTables.add(SGEH_ERR_APN);
        tempTables.add(LTE_ERR_APN);
        tempRawTables.add(SGEH_ERR_RAW);
        tempRawTables.add(LTE_ERR_RAW);

        tempTablesController.add(SGEH_ERR_Controller);
        tempTablesController.add(LTE_ERR_Controller);

        tempTablesSgsn.add(SGEH_ERR_SGSN);
        tempTablesSgsn.add(LTE_ERR_SGSN);

        apnGroupTableColumns = new HashMap<String, String>();
        apnGroupTableColumns.put(GROUP_NAME, VARCHAR_64);
        apnGroupTableColumns.put(APN, VARCHAR_127);
        bscGroupTableColumns = new HashMap<String, String>();
        bscGroupTableColumns.put(GROUP_NAME, VARCHAR_64);
        bscGroupTableColumns.put(HIERARCHY_3_SQL, VARCHAR_127);
        bscGroupTableColumns.put(VENDOR_PARAM_UPPER_CASE, VARCHAR_127);

        sgsnGroupTableColumns = new HashMap<String, String>();
        sgsnGroupTableColumns.put(GROUP_NAME, VARCHAR_64);
        sgsnGroupTableColumns.put(SGSN_SQL_NAME, VARCHAR_127);

        valuesForGroupTable = new HashMap<String, Object>();
        valuesForGroupTable.put(APN, APN_1);
        valuesForGroupTable.put(GROUP_NAME, SAMPLE_APN_GROUP);

        valuesForBscGroupTable = new HashMap<String, Object>();
        valuesForBscGroupTable.put(HIERARCHY_3_SQL, BSC_1);
        valuesForBscGroupTable.put(VENDOR_PARAM_UPPER_CASE, Vendor);
        valuesForBscGroupTable.put(GROUP_NAME, SAMPLE_BSC_GROUP);

        valuesForSgsnGroupTable = new HashMap<String, Object>();
        valuesForSgsnGroupTable.put(SGSN_SQL_NAME, SGSN_1);
        valuesForSgsnGroupTable.put(GROUP_NAME, SAMPLE_SGSN_GROUP);

        // Populate LTE cause codes/descriptions
        causeCodeMappingLTE
                .put(causeCode_38,
                        "This cause is used by the network to indicate that the requested service was rejected due to an error.");
        causeCodeMappingLTE
                .put(causeCode_7,
                        "This cause code is sent to the #UE# when there is no #EPS# subscription in the #HSS# for this #IMSI#");

        causeCodeHelpMappingLTE.put(causeCode_38, "cc lte 38");
        causeCodeHelpMappingLTE.put(causeCode_7, "cc lte 7");

        // LTE sub cause codes/descriptions
        subCauseCodeMappingLTE
                .put(subCauseCode_522,
                        "Used in the Dedicated bearer activation procedure, for CC #68, service not supported.");
        subCauseCodeMappingLTE.put(subCauseCode_12,
                "Auth failed: unknown subscriber, operator determined barring");

        subCauseCodeHelpMappingLTE.put(subCauseCode_42, EMPTY_STRING);
        subCauseCodeHelpMappingLTE.put(subCauseCode_12, EMPTY_STRING);
        subCauseCodeHelpMappingLTE.put(subCauseCode_522, EMPTY_STRING);

    }

    /*
     * (non-Javadoc)
     * 
     * @see com.ericsson.eniq.events.server.resources.
     * TestsWithTemporaryTablesBaseTestCase#onSetUp()
     */
    @Override
    public void onSetUp() throws Exception {
        super.onSetUp();
        attachDependencies(causeCodeAnalysisResource);

        for (final String tempTopologyTable : tempTopologyTables) {
            createTopologyTemporaryTable(tempTopologyTable);
        }

        for (final String tempTable : tempTables) {
            createTemporaryTable(tempTable);
        }

        for (final String tempTable : tempTablesController) {
            createTemporaryTableController(tempTable);
        }

        for (final String tempTable : tempTablesSgsn) {
            createTemporaryTableSgsn(tempTable);
        }
        for (final String tempRawTable : tempRawTables) {
            createRawTemporaryTable(tempRawTable);
        }

        populateTemporaryTables();
        createTemporaryTables();
    }

    @Test
    public void testGetSummaryData_CauseCode_APN() throws Exception {

        createTemporaryTableWithColumnTypes(GROUP_TYPE_E_APN,
                apnGroupTableColumns);
        insertRow(GROUP_TYPE_E_APN, valuesForGroupTable);

        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(MAX_ROWS, "500");
        map.putSingle(TYPE_PARAM, TYPE_APN);
        map.putSingle(TIME_QUERY_PARAM, TWO_WEEKS);
        map.putSingle(GROUP_NAME_PARAM, SAMPLE_APN_GROUP);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        DummyUriInfoImpl.setUriInfo(map, causeCodeAnalysisResource);
        final String json = causeCodeAnalysisResource.getData();
        System.out.println(json);
        validateResults(json, SAMPLE_APN_GROUP);
    }

    @Test
    public void testGetSummaryData_CauseCode_BSC() throws Exception {

        createTemporaryTableWithColumnTypes(GROUP_TYPE_E_RAT_VEND_HIER3,
                bscGroupTableColumns);
        insertRow(GROUP_TYPE_E_RAT_VEND_HIER3, valuesForBscGroupTable);

        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(MAX_ROWS, "500");
        map.putSingle(TYPE_PARAM, TYPE_BSC);
        map.putSingle(TIME_QUERY_PARAM, TWO_WEEKS);
        map.putSingle(GROUP_NAME_PARAM, SAMPLE_BSC_GROUP);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);

        DummyUriInfoImpl.setUriInfo(map, causeCodeAnalysisResource);

        final String json = causeCodeAnalysisResource.getData();
        validateResults(json, SAMPLE_BSC_GROUP);
    }

    @Test
    public void testGetSummaryData_CauseCode_SGSN() throws Exception {

        createTemporaryTableWithColumnTypes(GROUP_TYPE_E_EVNTSRC,
                sgsnGroupTableColumns);
        insertRow(GROUP_TYPE_E_EVNTSRC, valuesForSgsnGroupTable);

        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(MAX_ROWS, "500");
        map.putSingle(TYPE_PARAM, TYPE_SGSN);
        map.putSingle(TIME_QUERY_PARAM, TWO_WEEKS);
        map.putSingle(GROUP_NAME_PARAM, SAMPLE_SGSN_GROUP);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);

        DummyUriInfoImpl.setUriInfo(map, causeCodeAnalysisResource);

        final String json = causeCodeAnalysisResource.getData();
        validateResults(json, SAMPLE_SGSN_GROUP);
    }

    private void validateResults(final String json, final String groupName) {
        List<CauseCodeAnalysisSummaryResult> summaryResult;
        try {
            summaryResult = getTranslator().translateResult(json,
                    CauseCodeAnalysisSummaryResult.class);
            assertThat(summaryResult.size(), is(5));
            final CauseCodeAnalysisSummaryResult firstResult = summaryResult
                    .get(0);
            assertThat(firstResult.getNode(), is(groupName));
            assertThat(firstResult.getCauseProtocolType(),
                    is(Integer.toString(causeProtTypeUsedInTable)));
            assertThat(firstResult.getCauseProtocolTypeDescription(),
                    is(DESC_FOR_CAUSE_PROT_TYPE_1_IN_2G));
            assertThat(firstResult.getCauseCodeID(),
                    is(Integer.toString(causeCode_7)));
            assertThat(firstResult.getCauseCode(),
                    is(causeCodeMapping.get(causeCode_7)));
            assertThat(firstResult.getCauseCodeHelp(),
                    is(causeCodeHelpMapping.get(causeCode_7)));
            assertThat(firstResult.getSubCauseCodeID(),
                    is(Integer.toString(subCauseCode_12)));
            assertThat(firstResult.getSubCauseCode(),
                    is(subCauseCodeMapping.get(subCauseCode_12)));
            assertThat(firstResult.getSubCauseCodeHelp(),
                    is(subCauseCodeHelpMapping.get(subCauseCode_12)));
            assertThat(firstResult.getOccurrences(), is("11"));
            assertThat(firstResult.getImpactedSubscribers(), is("1"));

            final CauseCodeAnalysisSummaryResult secondResult = summaryResult
                    .get(1);
            assertThat(secondResult.getNode(), is(groupName));
            assertThat(secondResult.getCauseProtocolType(),
                    is(Integer.toString(causeProtTypeUsedInTable)));
            assertThat(secondResult.getCauseProtocolTypeDescription(),
                    is(DESC_FOR_CAUSE_PROT_TYPE_1_IN_4G));
            assertThat(secondResult.getCauseCodeID(),
                    is(Integer.toString(causeCode_7)));
            assertThat(secondResult.getCauseCode(),
                    is(causeCodeMappingLTE.get(causeCode_7)));
            assertThat(secondResult.getCauseCodeHelp(),
                    is(causeCodeHelpMappingLTE.get(causeCode_7)));
            assertThat(secondResult.getSubCauseCodeID(),
                    is(Integer.toString(subCauseCode_42)));
            assertThat(secondResult.getSubCauseCode(), is(EMPTY_STRING));
            assertThat(secondResult.getSubCauseCodeHelp(),
                    is(subCauseCodeHelpMappingLTE.get(subCauseCode_42)));
            assertThat(secondResult.getOccurrences(), is("8"));
            assertThat(secondResult.getImpactedSubscribers(), is("1"));

            final CauseCodeAnalysisSummaryResult thirdResult = summaryResult
                    .get(2);
            assertThat(thirdResult.getNode(), is(groupName));
            assertThat(thirdResult.getCauseProtocolType(),
                    is(Integer.toString(causeProtTypeUsedInTable)));
            assertThat(thirdResult.getCauseProtocolTypeDescription(),
                    is(DESC_FOR_CAUSE_PROT_TYPE_1_IN_2G));
            assertThat(thirdResult.getCauseCodeID(),
                    is(Integer.toString(causeCode_38)));
            assertThat(thirdResult.getCauseCode(),
                    is(causeCodeMapping.get(causeCode_38)));
            assertThat(thirdResult.getCauseCodeHelp(),
                    is(causeCodeHelpMapping.get(causeCode_38)));
            assertThat(thirdResult.getSubCauseCodeID(),
                    is(Integer.toString(subCauseCode_42)));
            assertThat(thirdResult.getSubCauseCode(),
                    is(subCauseCodeMapping.get(subCauseCode_42)));
            assertThat(thirdResult.getSubCauseCodeHelp(),
                    is(subCauseCodeHelpMapping.get(subCauseCode_42)));
            assertThat(thirdResult.getOccurrences(), is("6"));
            assertThat(thirdResult.getImpactedSubscribers(), is("1"));

            final CauseCodeAnalysisSummaryResult fourthResult = summaryResult
                    .get(3);
            assertThat(fourthResult.getNode(), is(groupName));
            assertThat(fourthResult.getCauseProtocolType(),
                    is(Integer.toString(causeProtTypeUsedInTable)));
            assertThat(fourthResult.getCauseProtocolTypeDescription(),
                    is(DESC_FOR_CAUSE_PROT_TYPE_1_IN_4G));
            assertThat(fourthResult.getCauseCodeID(),
                    is(Integer.toString(causeCode_38)));
            assertThat(fourthResult.getCauseCode(),
                    is(causeCodeMappingLTE.get(causeCode_38)));
            assertThat(fourthResult.getCauseCodeHelp(),
                    is(causeCodeHelpMappingLTE.get(causeCode_38)));
            assertThat(fourthResult.getSubCauseCodeID(),
                    is(Integer.toString(subCauseCode_12)));
            assertThat(fourthResult.getSubCauseCode(),
                    is(subCauseCodeMappingLTE.get(subCauseCode_12)));
            assertThat(fourthResult.getSubCauseCodeHelp(),
                    is(subCauseCodeHelpMappingLTE.get(subCauseCode_12)));
            assertThat(fourthResult.getOccurrences(), is("2"));
            assertThat(fourthResult.getImpactedSubscribers(), is("1"));

            final CauseCodeAnalysisSummaryResult fifthResult = summaryResult
                    .get(4);
            assertThat(fifthResult.getNode(), is(groupName));
            assertThat(fifthResult.getCauseProtocolType(),
                    is(Integer.toString(causeProtTypeUsedInTable)));
            assertThat(fifthResult.getCauseProtocolTypeDescription(),
                    is(DESC_FOR_CAUSE_PROT_TYPE_1_IN_4G));
            assertThat(fifthResult.getCauseCodeID(),
                    is(Integer.toString(causeCode_38)));
            assertThat(fifthResult.getCauseCode(),
                    is(causeCodeMappingLTE.get(causeCode_38)));
            assertThat(fifthResult.getCauseCodeHelp(),
                    is(causeCodeHelpMappingLTE.get(causeCode_38)));
            assertThat(fifthResult.getSubCauseCodeID(),
                    is(Integer.toString(subCauseCode_522)));
            assertThat(fifthResult.getSubCauseCode(),
                    is(subCauseCodeMappingLTE.get(subCauseCode_522)));
            assertThat(fifthResult.getSubCauseCodeHelp(),
                    is(subCauseCodeHelpMappingLTE.get(subCauseCode_522)));
            assertThat(fifthResult.getOccurrences(), is("4"));
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    private void populateTemporaryTables() throws SQLException, ParseException {
        SQLExecutor sqlExecutor = null;
        try {
            sqlExecutor = new SQLExecutor(connection);
            final String dateTime = DateTimeUtilities.getDateTime(
                    Calendar.MINUTE, -3200);

            insertRawRow(SGEH_ERR_RAW, causeCode_7, subCauseCode_12, IMSI_1,
                    APN_1, TAC_1, BSC_1, Vendor, SGSN_1, sqlExecutor, dateTime);
            insertRawRow(SGEH_ERR_RAW, causeCode_38, subCauseCode_42, IMSI_2,
                    APN_1, TAC_1, BSC_1, Vendor, SGSN_1, sqlExecutor, dateTime);
            insertRawRow(SGEH_ERR_RAW, causeCode_38, subCauseCode_12, IMSI_2,
                    APN_1, TAC_1, BSC_1, Vendor, SGSN_1, sqlExecutor, dateTime);

            insertRawRow(LTE_ERR_RAW, causeCode_7, subCauseCode_42, IMSI_2,
                    APN_1, TAC_1, BSC_1, Vendor, SGSN_1, sqlExecutor, dateTime);
            insertRawRow(LTE_ERR_RAW, causeCode_38, subCauseCode_12, IMSI_1,
                    APN_1, TAC_1, BSC_1, Vendor, SGSN_1, sqlExecutor, dateTime);

            insertRow(SGEH_ERR_APN, causeCode_7, subCauseCode_12, 0,
                    noErrorsForSgehCauseCode_7, APN_1, sqlExecutor, dateTime);
            insertRow(SGEH_ERR_APN, causeCode_38, subCauseCode_42, 0,
                    noErrorsForSgehCauseCode_38, APN_1, sqlExecutor, dateTime);

            insertRow(LTE_ERR_APN, causeCode_7, subCauseCode_42, 0,
                    noErrorsForLteCauseCode_7, APN_1, sqlExecutor, dateTime);
            insertRow(LTE_ERR_APN, causeCode_38, subCauseCode_12, 0,
                    noErrorsForLteCauseCode_38, APN_1, sqlExecutor, dateTime);

            insertRow(LTE_ERR_APN, causeCode_38, subCauseCode_522, 0,
                    noErrorsForLteCauseCode_38, APN_1, sqlExecutor, dateTime);
            insertRow(LTE_ERR_APN, causeCode_38, subCauseCode_522, 0,
                    noErrorsForLteCauseCode_38, APN_1, sqlExecutor, dateTime);

            insertRowController(SGEH_ERR_Controller, causeCode_7,
                    subCauseCode_12, 0, noErrorsForSgehCauseCode_7, BSC_1,
                    Vendor, sqlExecutor, dateTime);
            insertRowController(SGEH_ERR_Controller, causeCode_38,
                    subCauseCode_42, 0, noErrorsForSgehCauseCode_38, BSC_1,
                    Vendor, sqlExecutor, dateTime);

            insertRowController(LTE_ERR_Controller, causeCode_7,
                    subCauseCode_42, 0, noErrorsForLteCauseCode_7, BSC_1,
                    Vendor, sqlExecutor, dateTime);
            insertRowController(LTE_ERR_Controller, causeCode_38,
                    subCauseCode_12, 0, noErrorsForLteCauseCode_38, BSC_1,
                    Vendor, sqlExecutor, dateTime);

            insertRowController(LTE_ERR_Controller, causeCode_38,
                    subCauseCode_522, 0, noErrorsForLteCauseCode_38, BSC_1,
                    Vendor, sqlExecutor, dateTime);
            insertRowController(LTE_ERR_Controller, causeCode_38,
                    subCauseCode_522, 0, noErrorsForLteCauseCode_38, BSC_1,
                    Vendor, sqlExecutor, dateTime);

            insertRowSgsn(SGEH_ERR_SGSN, causeCode_7, subCauseCode_12, 0,
                    noErrorsForSgehCauseCode_7, SGSN_1, sqlExecutor, dateTime);
            insertRowSgsn(SGEH_ERR_SGSN, causeCode_38, subCauseCode_42, 0,
                    noErrorsForSgehCauseCode_38, SGSN_1, sqlExecutor, dateTime);

            insertRowSgsn(LTE_ERR_SGSN, causeCode_7, subCauseCode_42, 0,
                    noErrorsForLteCauseCode_7, SGSN_1, sqlExecutor, dateTime);
            insertRowSgsn(LTE_ERR_SGSN, causeCode_38, subCauseCode_12, 0,
                    noErrorsForLteCauseCode_38, SGSN_1, sqlExecutor, dateTime);

            insertRowSgsn(LTE_ERR_SGSN, causeCode_38, subCauseCode_522, 0,
                    noErrorsForLteCauseCode_38, SGSN_1, sqlExecutor, dateTime);
            insertRowSgsn(LTE_ERR_SGSN, causeCode_38, subCauseCode_522, 0,
                    noErrorsForLteCauseCode_38, SGSN_1, sqlExecutor, dateTime);
        } finally {
            closeSQLExector(sqlExecutor);
        }
    }

    private void insertRow(final String table, final int causeCode,
            final int subCauseCode, final int numSuccesses,
            final int numErrors, final String testNode,
            final SQLExecutor sqlExecutor, final String dateTime)
            throws SQLException {
        sqlExecutor.executeUpdate("insert into " + table + " values("
                + causeCode + "," + subCauseCode + ","
                + causeProtTypeUsedInTable + "," + numSuccesses + ","
                + numErrors + ",'" + testNode + "','" + dateTime + "')");
    }

    private void insertRowController(final String table, final int causeCode,
            final int subCauseCode, final int numSuccesses,
            final int numErrors, final String testHeir3,
            final String testVendor, final SQLExecutor sqlExecutor,
            final String dateTime) throws SQLException {
        sqlExecutor.executeUpdate("insert into " + table + " values("
                + causeCode + "," + subCauseCode + ","
                + causeProtTypeUsedInTable + "," + numSuccesses + ","
                + numErrors + ",'" + testHeir3 + "','" + testVendor + "','"
                + dateTime + "')");
    }

    private void insertRowSgsn(final String table, final int causeCode,
            final int subCauseCode, final int numSuccesses,
            final int numErrors, final String testSgsn,
            final SQLExecutor sqlExecutor, final String dateTime)
            throws SQLException {
        sqlExecutor.executeUpdate("insert into " + table + " values("
                + causeCode + "," + subCauseCode + ","
                + causeProtTypeUsedInTable + "," + numSuccesses + ","
                + numErrors + ",'" + testSgsn + "','" + dateTime + "')");
    }

    private void insertRawRow(final String table, final int causeCode,
            final int subCauseCode, final int testImsi, final String testNode,
            final int testTac, final String testHeir3, final String testVendor,
            final String testSgsn, final SQLExecutor sqlExecutor,
            final String dateTime) throws SQLException, ParseException {

        final String localDateId = dateOnlyFormatter.format(dateOnlyFormatter
                .parse(dateTime));
        sqlExecutor.executeUpdate("insert into " + table + " values("
                + causeCode + "," + subCauseCode + ","
                + causeProtTypeUsedInTable + "," + testImsi + ",'" + testNode
                + "'," + testTac + ",'" + testHeir3 + "', '" + testVendor
                + "', '" + testSgsn + "', '" + dateTime + "','" + localDateId
                + "')");
    }

    private void createTemporaryTable(final String tempTableName)
            throws SQLException {
        SQLExecutor sqlExecutor = null;
        try {
            sqlExecutor = new SQLExecutor(connection);
            sqlExecutor
                    .executeUpdate("create local temporary table "
                            + tempTableName
                            + "(CAUSE_CODE smallint, SUBCAUSE_CODE smallint, CAUSE_PROT_TYPE tinyint, NO_OF_SUCCESSES int, "
                            + "NO_OF_ERRORS int, APN varchar(128), DATETIME_ID timestamp)");

        } finally {
            closeSQLExector(sqlExecutor);
        }
    }

    private void createTemporaryTableController(final String tempTableName)
            throws SQLException {
        SQLExecutor sqlExecutor = null;
        try {
            sqlExecutor = new SQLExecutor(connection);
            sqlExecutor
                    .executeUpdate("create local temporary table "
                            + tempTableName
                            + "(CAUSE_CODE smallint, SUBCAUSE_CODE smallint, CAUSE_PROT_TYPE tinyint, NO_OF_SUCCESSES int, "
                            + "NO_OF_ERRORS int, HIERARCHY_3 varchar(128), VENDOR varchar(128), DATETIME_ID timestamp)");

        } finally {
            closeSQLExector(sqlExecutor);
        }
    }

    private void createTemporaryTableSgsn(final String tempTableName)
            throws SQLException {
        SQLExecutor sqlExecutor = null;
        try {
            sqlExecutor = new SQLExecutor(connection);
            sqlExecutor
                    .executeUpdate("create local temporary table "
                            + tempTableName
                            + "(CAUSE_CODE smallint, SUBCAUSE_CODE smallint, CAUSE_PROT_TYPE tinyint, NO_OF_SUCCESSES int, "
                            + "NO_OF_ERRORS int, EVENT_SOURCE_NAME varchar(128), DATETIME_ID timestamp)");

        } finally {
            closeSQLExector(sqlExecutor);
        }
    }

    private void createRawTemporaryTable(final String tempTableName)
            throws SQLException {
        SQLExecutor sqlExecutor = null;
        try {
            sqlExecutor = new SQLExecutor(connection);
            sqlExecutor
                    .executeUpdate("create local temporary table "
                            + tempTableName
                            + "(CAUSE_CODE smallint, SUBCAUSE_CODE smallint, CAUSE_PROT_TYPE tinyint, IMSI int, APN varchar(128), TAC int, HIERARCHY_3 varchar(128), VENDOR varchar(128), EVENT_SOURCE_NAME varchar(128),"
                            + "DATETIME_ID timestamp, LOCAL_DATE_ID date)");
        } finally {
            closeSQLExector(sqlExecutor);
        }
    }

    private void createTopologyTemporaryTable(final String tempTableName)
            throws Exception {
        final Collection<String> columns = new ArrayList<String>();
        columns.add(CAUSE_CODE_COLUMN);
        columns.add(SUBCAUSE_CODE_COLUMN);
        columns.add(ADVICE_COLUMN);
        createTemporaryTable(tempTableName, columns);
    }

    private void createTemporaryCCTables() throws Exception {
        final List<String> columnsForTable = new ArrayList<String>();
        columnsForTable.add(CAUSE_CODE_COLUMN);
        columnsForTable.add(CAUSE_PROT_TYPE_COLUMN);
        columnsForTable.add(CAUSE_CODE_DESC_COLUMN);
        columnsForTable.add(CAUSE_CODE_HELP_COLUMN);
        new SQLCommand(connection).createTemporaryTable(
                TEMP_DIM_E_SGEH_CAUSECODE, columnsForTable);
        new SQLCommand(connection).createTemporaryTable(
                TEMP_DIM_E_LTE_CAUSECODE, columnsForTable);
    }

    private void createTemporarySCCTables() throws Exception {
        final List<String> columnsForTable = new ArrayList<String>();
        columnsForTable.add(SUBCAUSE_CODE_COLUMN);
        columnsForTable.add(SUB_CAUSE_CODE_DESC_COLUMN);
        columnsForTable.add(SUB_CAUSE_CODE_HELP_COLUMN);
        new SQLCommand(connection).createTemporaryTable(
                TEMP_DIM_E_SGEH_SUBCAUSECODE, columnsForTable);
        new SQLCommand(connection).createTemporaryTable(
                TEMP_DIM_E_LTE_SUBCAUSECODE, columnsForTable);
    }

    private void createTemporaryCPTTables() throws Exception {
        final List<String> columnsForTable = new ArrayList<String>();
        columnsForTable.add(CAUSE_PROT_TYPE_COLUMN);
        columnsForTable.add(CAUSE_PROT_TYPE_DESC_COLUMN);
        new SQLCommand(connection).createTemporaryTable(
                TEMP_DIM_E_SGEH_CAUSE_PROT_TYPE, columnsForTable);
        new SQLCommand(connection).createTemporaryTable(
                TEMP_DIM_E_LTE_CAUSE_PROT_TYPE, columnsForTable);
    }

    private void insertRowIntoCCTable(final String table, final int causeCode,
            final int causeProtoType, final String ccDesc, final String ccHelp)
            throws SQLException {
        final Map<String, Object> valuesForTable = new HashMap<String, Object>();
        valuesForTable.put(CAUSE_CODE_COLUMN, causeCode);
        valuesForTable.put(CAUSE_PROT_TYPE_COLUMN, causeProtoType);
        valuesForTable.put(CAUSE_CODE_DESC_COLUMN, ccDesc);
        valuesForTable.put(CAUSE_CODE_HELP_COLUMN, ccHelp);
        new SQLCommand(connection).insertRow(table, valuesForTable);
    }

    private void insertRowIntoSCCTable(final String table,
            final int subCauseCode, final String sccDesc, final String whatNext)
            throws SQLException {
        final Map<String, Object> valuesForTable = new HashMap<String, Object>();
        valuesForTable.put(SUBCAUSE_CODE_COLUMN, subCauseCode);
        valuesForTable.put(SUB_CAUSE_CODE_DESC_COLUMN, sccDesc);
        valuesForTable.put(SUB_CAUSE_CODE_HELP_COLUMN, whatNext);
        new SQLCommand(connection).insertRow(table, valuesForTable);
    }

    private void insertRowIntoCPTTable(final String table,
            final int causeProtoType, final String cptDesc) throws SQLException {
        final Map<String, Object> valuesForTable = new HashMap<String, Object>();
        valuesForTable.put(CAUSE_PROT_TYPE_COLUMN, causeProtoType);
        valuesForTable.put(CAUSE_PROT_TYPE_DESC_COLUMN, cptDesc);
        new SQLCommand(connection).insertRow(table, valuesForTable);
    }

    private void populateCCTables() throws SQLException {
        for (final int causeCode : causeCodeMapping.keySet()) {
            insertRowIntoCCTable(TEMP_DIM_E_SGEH_CAUSECODE, causeCode,
                    causeProtTypeUsedInTable, causeCodeMapping.get(causeCode),
                    causeCodeHelpMapping.get(causeCode));
        }

        for (final int causeCode : causeCodeMappingLTE.keySet()) {
            insertRowIntoCCTable(TEMP_DIM_E_LTE_CAUSECODE, causeCode,
                    causeProtTypeUsedInTable,
                    causeCodeMappingLTE.get(causeCode),
                    causeCodeHelpMappingLTE.get(causeCode));
        }
    }

    private void populateSCCTables() throws SQLException {
        for (final int subcauseCode : subCauseCodeMapping.keySet()) {
            insertRowIntoSCCTable(TEMP_DIM_E_SGEH_SUBCAUSECODE, subcauseCode,
                    subCauseCodeMapping.get(subcauseCode),
                    subCauseCodeHelpMapping.get(subcauseCode));
        }

        for (final int subcauseCode : subCauseCodeMappingLTE.keySet()) {
            insertRowIntoSCCTable(TEMP_DIM_E_LTE_SUBCAUSECODE, subcauseCode,
                    subCauseCodeMappingLTE.get(subcauseCode),
                    subCauseCodeHelpMappingLTE.get(subcauseCode));
        }
    }

    private void populateCPTTables() throws SQLException {
        insertRowIntoCPTTable(TEMP_DIM_E_SGEH_CAUSE_PROT_TYPE,
                causeProtTypeUsedInTable, DESC_FOR_CAUSE_PROT_TYPE_1_IN_2G);

        insertRowIntoCPTTable(TEMP_DIM_E_LTE_CAUSE_PROT_TYPE,
                causeProtTypeUsedInTable, DESC_FOR_CAUSE_PROT_TYPE_1_IN_4G);
    }

    private void createTemporaryTables() throws Exception {
        createTemporaryCCTables();
        populateCCTables();

        createTemporarySCCTables();
        populateSCCTables();

        createTemporaryCPTTables();
        populateCPTTables();
    }
}
