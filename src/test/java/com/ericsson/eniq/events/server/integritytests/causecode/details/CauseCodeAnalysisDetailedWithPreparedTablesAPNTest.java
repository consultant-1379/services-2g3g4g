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
package com.ericsson.eniq.events.server.integritytests.causecode.details;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.common.EventIDConstants.*;
import static com.ericsson.eniq.events.server.integritytests.stubs.ReplaceTablesWithTempTablesTemplateUtils.*;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.*;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.*;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.sql.SQLException;
import java.util.*;

import javax.ws.rs.core.MultivaluedMap;

import org.junit.BeforeClass;
import org.junit.Test;

import com.ericsson.eniq.events.server.resources.CauseCodeAnalysisResource;
import com.ericsson.eniq.events.server.resources.TestsWithTemporaryTablesBaseTestCase;
import com.ericsson.eniq.events.server.test.populator.RawTablesPopulator;
import com.ericsson.eniq.events.server.test.queryresults.CauseCodeEventAnalysisDetailedResult;
import com.ericsson.eniq.events.server.test.stubs.DummyUriInfoImpl;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;
import com.sun.jersey.core.util.MultivaluedMapImpl;

public class CauseCodeAnalysisDetailedWithPreparedTablesAPNTest extends TestsWithTemporaryTablesBaseTestCase<CauseCodeEventAnalysisDetailedResult> {

    private final CauseCodeAnalysisResource causeCodeAnalysisResource = new CauseCodeAnalysisResource();

    private final static int CAUSE_CODE_VALUE = 3;

    private final static int SUBCAUSE_CODE_VALUE = 10;

    private final static int CAUSE_PROT_TYPE_VALUE = 0;

    private final static String EXPECTED_LTE_CAUSECODE_AND_SUB_CAUSECODE_HELP = "text";

    private final static String EXPECTED_SGEH_CAUSECODE_HELP = "help";

    private final static String EXPECTED_SGEH_SUB_CAUSECODE_HELP = "dummy text";

    private final static String EXPECTED_VALID_MANUFACTURER = "Apple";

    private final static String EXPECTED_VALID_MARKETING_NAME = "iPad 2 A1396";

    private final static String EXPECTED_UNKNOWN_MANUFACTURER = "Manufacturer Unknown";

    private final static String EXPECTED_UNKNOWN_MARKETING_NAME = "Model Unknown";

    private final static Map<Integer, String> sgehCauseCodeMapping = new HashMap<Integer, String>();

    private final static Map<Integer, String> lteCauseCodeMapping = new HashMap<Integer, String>();

    private final static Map<Integer, String> sgehSubCauseCodeMapping = new HashMap<Integer, String>();

    private final static Map<Integer, String> lteSubCauseCodeMapping = new HashMap<Integer, String>();

    @BeforeClass
    static public void setupBeforeClass() {

        sgehCauseCodeMapping.put(CAUSE_CODE_VALUE, "ILLEGAL_MS");
        lteCauseCodeMapping.put(CAUSE_CODE_VALUE, "This cause is sent when the PDN connection is released due to LTE generated reasons.");
        sgehSubCauseCodeMapping.put(SUBCAUSE_CODE_VALUE, "Auth failed");
        lteSubCauseCodeMapping.put(SUBCAUSE_CODE_VALUE, "Auth failed");
    }

    @Test
    public void testGetDetailedDataForAPN1WeekLteAndSgeh() throws Exception {
        createAndPopulateRawTables(DateTimeUtilities.getDateTimeMinus48Hours());

        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();

        getQueryParams(map, TYPE_APN);
        map.putSingle(TIME_QUERY_PARAM, ONE_WEEK);
        map.putSingle(APN_PARAM, SAMPLE_APN);

        DummyUriInfoImpl.setUriInfo(map, causeCodeAnalysisResource);

        final String json = causeCodeAnalysisResource.getData();
        final List<CauseCodeEventAnalysisDetailedResult> summaryResult = getTranslator().translateResult(json,
                CauseCodeEventAnalysisDetailedResult.class);

        assertThat(summaryResult.size(), is(2));
        assertThat(summaryResult.get(0).getCauseCodeDesc(), is(sgehCauseCodeMapping.get(CAUSE_CODE_VALUE)));
        assertThat(summaryResult.get(0).getSubcauseCodeDesc(), is(sgehSubCauseCodeMapping.get(SUBCAUSE_CODE_VALUE)));
        assertThat(summaryResult.get(1).getCauseCodeDesc(), is(lteCauseCodeMapping.get(CAUSE_CODE_VALUE)));
        assertThat(summaryResult.get(1).getSubcauseCodeDesc(), is(lteSubCauseCodeMapping.get(SUBCAUSE_CODE_VALUE)));
        assertThat(summaryResult.get(0).getCauseCodeHelp(), is(EXPECTED_SGEH_CAUSECODE_HELP));
        assertThat(summaryResult.get(0).getSubcauseCodeHelp(), is(EXPECTED_SGEH_SUB_CAUSECODE_HELP));
        assertThat(summaryResult.get(1).getCauseCodeHelp(), is(EXPECTED_LTE_CAUSECODE_AND_SUB_CAUSECODE_HELP));
        assertThat(summaryResult.get(1).getSubcauseCodeHelp(), is(EXPECTED_LTE_CAUSECODE_AND_SUB_CAUSECODE_HELP));
        assertThat(summaryResult.get(0).getPdpGgsnName_1(), is(PDP_GGSN_NAME_1_VALUE));
        assertThat(summaryResult.get(0).getPdpGgsnName_2(), is(PDP_GGSN_NAME_2_VALUE));
    }

    @Test
    public void testGetDetailedDataForAPN1DayLteAndSgeh() throws Exception {
        createAndPopulateRawTables(DateTimeUtilities.getDateTime(Calendar.HOUR, -12));
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();

        getQueryParams(map, TYPE_APN);
        map.putSingle(TIME_QUERY_PARAM, ONE_DAY);
        map.putSingle(APN_PARAM, SAMPLE_APN);

        DummyUriInfoImpl.setUriInfo(map, causeCodeAnalysisResource);

        final String json = causeCodeAnalysisResource.getData();
        final List<CauseCodeEventAnalysisDetailedResult> summaryResult = getTranslator().translateResult(json,
                CauseCodeEventAnalysisDetailedResult.class);
        assertThat(summaryResult.size(), is(2));
        assertThat(summaryResult.get(0).getCauseCodeDesc(), is(sgehCauseCodeMapping.get(CAUSE_CODE_VALUE)));
        assertThat(summaryResult.get(0).getSubcauseCodeDesc(), is(sgehSubCauseCodeMapping.get(SUBCAUSE_CODE_VALUE)));
        assertThat(summaryResult.get(1).getCauseCodeDesc(), is(lteCauseCodeMapping.get(CAUSE_CODE_VALUE)));
        assertThat(summaryResult.get(1).getSubcauseCodeDesc(), is(lteSubCauseCodeMapping.get(SUBCAUSE_CODE_VALUE)));
        assertThat(summaryResult.get(0).getCauseCodeHelp(), is(EXPECTED_SGEH_CAUSECODE_HELP));
        assertThat(summaryResult.get(0).getSubcauseCodeHelp(), is(EXPECTED_SGEH_SUB_CAUSECODE_HELP));
        assertThat(summaryResult.get(1).getCauseCodeHelp(), is(EXPECTED_LTE_CAUSECODE_AND_SUB_CAUSECODE_HELP));
        assertThat(summaryResult.get(1).getSubcauseCodeHelp(), is(EXPECTED_LTE_CAUSECODE_AND_SUB_CAUSECODE_HELP));
        assertThat(summaryResult.get(0).getPdpGgsnName_1(), is(PDP_GGSN_NAME_1_VALUE));
        assertThat(summaryResult.get(0).getPdpGgsnName_2(), is(PDP_GGSN_NAME_2_VALUE));
    }

    @Test
    public void testGetDetailedDataForAPN1HourLteAndSgeh() throws Exception {
        createAndPopulateRawTables(DateTimeUtilities.getDateTime(Calendar.MINUTE, -15));
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();

        getQueryParams(map, TYPE_APN);
        map.putSingle(TIME_QUERY_PARAM, ONE_HOUR);
        map.putSingle(APN_PARAM, SAMPLE_APN);

        DummyUriInfoImpl.setUriInfo(map, causeCodeAnalysisResource);

        final String json = causeCodeAnalysisResource.getData();
        final List<CauseCodeEventAnalysisDetailedResult> summaryResult = getTranslator().translateResult(json,
                CauseCodeEventAnalysisDetailedResult.class);
        assertThat(summaryResult.size(), is(2));
        assertThat(summaryResult.get(0).getCauseCodeDesc(), is(sgehCauseCodeMapping.get(CAUSE_CODE_VALUE)));
        assertThat(summaryResult.get(0).getSubcauseCodeDesc(), is(sgehSubCauseCodeMapping.get(SUBCAUSE_CODE_VALUE)));
        assertThat(summaryResult.get(1).getCauseCodeDesc(), is(lteCauseCodeMapping.get(CAUSE_CODE_VALUE)));
        assertThat(summaryResult.get(1).getSubcauseCodeDesc(), is(lteSubCauseCodeMapping.get(SUBCAUSE_CODE_VALUE)));
        assertThat(summaryResult.get(0).getCauseCodeHelp(), is(EXPECTED_SGEH_CAUSECODE_HELP));
        assertThat(summaryResult.get(0).getSubcauseCodeHelp(), is(EXPECTED_SGEH_SUB_CAUSECODE_HELP));
        assertThat(summaryResult.get(1).getCauseCodeHelp(), is(EXPECTED_LTE_CAUSECODE_AND_SUB_CAUSECODE_HELP));
        assertThat(summaryResult.get(1).getSubcauseCodeHelp(), is(EXPECTED_LTE_CAUSECODE_AND_SUB_CAUSECODE_HELP));
        assertThat(summaryResult.get(0).getPdpGgsnName_1(), is(PDP_GGSN_NAME_1_VALUE));
        assertThat(summaryResult.get(0).getPdpGgsnName_2(), is(PDP_GGSN_NAME_2_VALUE));
    }

    @Test
    public void testGetDetailedDataForAPNGroup1HourLteAndSgeh() throws Exception {
        createAndPopulateRawTables(DateTimeUtilities.getDateTime(Calendar.MINUTE, -15));
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();

        getQueryParams(map, TYPE_APN);
        map.putSingle(TIME_QUERY_PARAM, ONE_HOUR);
        map.putSingle(GROUP_NAME_PARAM, SAMPLE_APN_GROUP);

        DummyUriInfoImpl.setUriInfo(map, causeCodeAnalysisResource);

        final String json = causeCodeAnalysisResource.getData();
        final List<CauseCodeEventAnalysisDetailedResult> summaryResult = getTranslator().translateResult(json,
                CauseCodeEventAnalysisDetailedResult.class);
        assertThat(summaryResult.size(), is(2));
        assertThat(summaryResult.get(0).getCauseCodeDesc(), is(sgehCauseCodeMapping.get(CAUSE_CODE_VALUE)));
        assertThat(summaryResult.get(0).getSubcauseCodeDesc(), is(sgehSubCauseCodeMapping.get(SUBCAUSE_CODE_VALUE)));
        assertThat(summaryResult.get(1).getCauseCodeDesc(), is(lteCauseCodeMapping.get(CAUSE_CODE_VALUE)));
        assertThat(summaryResult.get(1).getSubcauseCodeDesc(), is(lteSubCauseCodeMapping.get(SUBCAUSE_CODE_VALUE)));
        assertThat(summaryResult.get(0).getCauseCodeHelp(), is(EXPECTED_SGEH_CAUSECODE_HELP));
        assertThat(summaryResult.get(0).getSubcauseCodeHelp(), is(EXPECTED_SGEH_SUB_CAUSECODE_HELP));
        assertThat(summaryResult.get(1).getCauseCodeHelp(), is(EXPECTED_LTE_CAUSECODE_AND_SUB_CAUSECODE_HELP));
        assertThat(summaryResult.get(1).getSubcauseCodeHelp(), is(EXPECTED_LTE_CAUSECODE_AND_SUB_CAUSECODE_HELP));
        assertThat(summaryResult.get(0).getPdpGgsnName_1(), is(PDP_GGSN_NAME_1_VALUE));
        assertThat(summaryResult.get(0).getPdpGgsnName_2(), is(PDP_GGSN_NAME_2_VALUE));
    }

    @Test
    public void testGetDetailedDataForAPNGroup1DayLteAndSgeh() throws Exception {
        createAndPopulateRawTables(DateTimeUtilities.getDateTime(Calendar.HOUR, -12));
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();

        getQueryParams(map, TYPE_APN);
        map.putSingle(TIME_QUERY_PARAM, ONE_DAY);
        map.putSingle(GROUP_NAME_PARAM, SAMPLE_APN_GROUP);

        DummyUriInfoImpl.setUriInfo(map, causeCodeAnalysisResource);

        final String json = causeCodeAnalysisResource.getData();
        final List<CauseCodeEventAnalysisDetailedResult> summaryResult = getTranslator().translateResult(json,
                CauseCodeEventAnalysisDetailedResult.class);
        assertThat(summaryResult.size(), is(2));
        assertThat(summaryResult.get(0).getCauseCodeDesc(), is(sgehCauseCodeMapping.get(CAUSE_CODE_VALUE)));
        assertThat(summaryResult.get(0).getSubcauseCodeDesc(), is(sgehSubCauseCodeMapping.get(SUBCAUSE_CODE_VALUE)));
        assertThat(summaryResult.get(1).getCauseCodeDesc(), is(lteCauseCodeMapping.get(CAUSE_CODE_VALUE)));
        assertThat(summaryResult.get(1).getSubcauseCodeDesc(), is(lteSubCauseCodeMapping.get(SUBCAUSE_CODE_VALUE)));
        assertThat(summaryResult.get(0).getCauseCodeHelp(), is(EXPECTED_SGEH_CAUSECODE_HELP));
        assertThat(summaryResult.get(0).getSubcauseCodeHelp(), is(EXPECTED_SGEH_SUB_CAUSECODE_HELP));
        assertThat(summaryResult.get(1).getCauseCodeHelp(), is(EXPECTED_LTE_CAUSECODE_AND_SUB_CAUSECODE_HELP));
        assertThat(summaryResult.get(1).getSubcauseCodeHelp(), is(EXPECTED_LTE_CAUSECODE_AND_SUB_CAUSECODE_HELP));
        assertThat(summaryResult.get(0).getPdpGgsnName_1(), is(PDP_GGSN_NAME_1_VALUE));
        assertThat(summaryResult.get(0).getPdpGgsnName_2(), is(PDP_GGSN_NAME_2_VALUE));
    }

    @Test
    public void testGetDetailedDataForAPNGroup1WeekLteAndSgeh() throws Exception {
        createAndPopulateRawTables(DateTimeUtilities.getDateTimeMinus48Hours());
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();

        getQueryParams(map, TYPE_APN);
        map.putSingle(TIME_QUERY_PARAM, ONE_WEEK);
        map.putSingle(GROUP_NAME_PARAM, SAMPLE_APN_GROUP);

        DummyUriInfoImpl.setUriInfo(map, causeCodeAnalysisResource);

        final String json = causeCodeAnalysisResource.getData();
        final List<CauseCodeEventAnalysisDetailedResult> summaryResult = getTranslator().translateResult(json,
                CauseCodeEventAnalysisDetailedResult.class);
        assertThat(summaryResult.size(), is(2));
        assertThat(summaryResult.get(0).getCauseCodeDesc(), is(sgehCauseCodeMapping.get(CAUSE_CODE_VALUE)));
        assertThat(summaryResult.get(0).getSubcauseCodeDesc(), is(sgehSubCauseCodeMapping.get(SUBCAUSE_CODE_VALUE)));
        assertThat(summaryResult.get(1).getCauseCodeDesc(), is(lteCauseCodeMapping.get(CAUSE_CODE_VALUE)));
        assertThat(summaryResult.get(1).getSubcauseCodeDesc(), is(lteSubCauseCodeMapping.get(SUBCAUSE_CODE_VALUE)));
        assertThat(summaryResult.get(0).getCauseCodeHelp(), is(EXPECTED_SGEH_CAUSECODE_HELP));
        assertThat(summaryResult.get(0).getSubcauseCodeHelp(), is(EXPECTED_SGEH_SUB_CAUSECODE_HELP));
        assertThat(summaryResult.get(1).getCauseCodeHelp(), is(EXPECTED_LTE_CAUSECODE_AND_SUB_CAUSECODE_HELP));
        assertThat(summaryResult.get(1).getSubcauseCodeHelp(), is(EXPECTED_LTE_CAUSECODE_AND_SUB_CAUSECODE_HELP));
        assertThat(summaryResult.get(0).getPdpGgsnName_1(), is(PDP_GGSN_NAME_1_VALUE));
        assertThat(summaryResult.get(0).getPdpGgsnName_2(), is(PDP_GGSN_NAME_2_VALUE));
    }

    @Test
    public void testGetDetailedDataForAPNWeekNineHourOffset() throws Exception {
        createAndPopulateRawTables(DateTimeUtilities.getDateTimeMinusDay(4));
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();

        getQueryParams(map, TYPE_APN);
        map.putSingle(TIME_QUERY_PARAM, ONE_WEEK);
        map.putSingle(GROUP_NAME_PARAM, SAMPLE_APN_GROUP);

        DummyUriInfoImpl.setUriInfo(map, causeCodeAnalysisResource);

        final String json = causeCodeAnalysisResource.getData();
        final List<CauseCodeEventAnalysisDetailedResult> summaryResult = getTranslator().translateResult(json,
                CauseCodeEventAnalysisDetailedResult.class);
        assertThat(summaryResult.size(), is(2));
        assertThat(summaryResult.get(0).getCauseCodeDesc(), is(sgehCauseCodeMapping.get(CAUSE_CODE_VALUE)));
        assertThat(summaryResult.get(0).getSubcauseCodeDesc(), is(sgehSubCauseCodeMapping.get(SUBCAUSE_CODE_VALUE)));
        assertThat(summaryResult.get(1).getCauseCodeDesc(), is(lteCauseCodeMapping.get(CAUSE_CODE_VALUE)));
        assertThat(summaryResult.get(1).getSubcauseCodeDesc(), is(lteSubCauseCodeMapping.get(SUBCAUSE_CODE_VALUE)));
        assertThat(summaryResult.get(0).getCauseCodeHelp(), is(EXPECTED_SGEH_CAUSECODE_HELP));
        assertThat(summaryResult.get(0).getSubcauseCodeHelp(), is(EXPECTED_SGEH_SUB_CAUSECODE_HELP));
        assertThat(summaryResult.get(1).getCauseCodeHelp(), is(EXPECTED_LTE_CAUSECODE_AND_SUB_CAUSECODE_HELP));
        assertThat(summaryResult.get(1).getSubcauseCodeHelp(), is(EXPECTED_LTE_CAUSECODE_AND_SUB_CAUSECODE_HELP));
        assertThat(summaryResult.get(0).getPdpGgsnName_1(), is(PDP_GGSN_NAME_1_VALUE));
        assertThat(summaryResult.get(0).getPdpGgsnName_2(), is(PDP_GGSN_NAME_2_VALUE));
    }

    @Test
    public void testGetDetailedDataForAPNToVerifyValidManufacturerAndModelvalue() throws Exception {
        createAndPopulateRawTables(DateTimeUtilities.getDateTime(Calendar.MINUTE, -15), SAMPLE_TAC_2);
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();

        getQueryParams(map, TYPE_APN);
        map.putSingle(TIME_QUERY_PARAM, ONE_HOUR);
        map.putSingle(APN_PARAM, SAMPLE_APN);

        DummyUriInfoImpl.setUriInfo(map, causeCodeAnalysisResource);

        final String json = causeCodeAnalysisResource.getData();
        final List<CauseCodeEventAnalysisDetailedResult> summaryResult = getTranslator().translateResult(json,
                CauseCodeEventAnalysisDetailedResult.class);
        assertThat(summaryResult.size(), is(2));
        assertThat(summaryResult.get(0).getManufacturer(), is(EXPECTED_VALID_MANUFACTURER));
        assertThat(summaryResult.get(0).getMarketingName(), is(EXPECTED_VALID_MARKETING_NAME));
    }

    @Test
    public void testGetDetailedDataForAPNToVerifyUnknownManufacturerAndUnknownModelvalue() throws Exception {
        createAndPopulateRawTables(DateTimeUtilities.getDateTime(Calendar.MINUTE, -15), SONY_ERICSSON_TAC);
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();

        getQueryParams(map, TYPE_APN);
        map.putSingle(TIME_QUERY_PARAM, ONE_HOUR);
        map.putSingle(APN_PARAM, SAMPLE_APN);

        DummyUriInfoImpl.setUriInfo(map, causeCodeAnalysisResource);

        final String json = causeCodeAnalysisResource.getData();
        final List<CauseCodeEventAnalysisDetailedResult> summaryResult = getTranslator().translateResult(json,
                CauseCodeEventAnalysisDetailedResult.class);
        assertThat(summaryResult.size(), is(2));
        assertThat(summaryResult.get(0).getManufacturer(), is(EXPECTED_UNKNOWN_MANUFACTURER));
        assertThat(summaryResult.get(0).getMarketingName(), is(EXPECTED_UNKNOWN_MARKETING_NAME));
    }

    @Override
    public void onSetUp() {
        attachDependencies(causeCodeAnalysisResource);
        try {
            createAndPopulateLookupTables();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void getQueryParams(final MultivaluedMap<String, String> map, String typeParamVal) {
        map.putSingle(TYPE_PARAM, typeParamVal);
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(MAX_ROWS, DEFAULT_MAX_ROWS);
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(CAUSE_CODE_PARAM, Integer.toString(CAUSE_CODE_VALUE));
        map.putSingle(SUB_CAUSE_CODE_PARAM, Integer.toString(SUBCAUSE_CODE_VALUE));
        map.putSingle(CAUSE_PROT_TYPE, Integer.toString(CAUSE_PROT_TYPE_VALUE));
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
    }

    private void createAndPopulateRawTables(final String timestamp) {
        createAndPopulateRawTables(timestamp, -1);
    }

    private void createAndPopulateRawTables(final String timestamp, final int tac) {
        try {
            final RawTablesPopulator rawTablesPopulator = new RawTablesPopulator();
            final Map<String, Object> valuesToInsertInLTETable = new HashMap<String, Object>();
            valuesToInsertInLTETable.put(EVENT_ID, ATTACH_IN_4G);
            valuesToInsertInLTETable.put(RAT, RAT_FOR_LTE);
            valuesToInsertInLTETable.put(APN, SAMPLE_APN);
            valuesToInsertInLTETable.put(CAUSE_CODE_COLUMN, CAUSE_CODE_VALUE);
            valuesToInsertInLTETable.put(CAUSE_PROT_TYPE_COLUMN, CAUSE_PROT_TYPE_VALUE);
            valuesToInsertInLTETable.put(SUBCAUSE_CODE_COLUMN, SUBCAUSE_CODE_VALUE);
            rawTablesPopulator.createAndPopulateRawLteErrTable(valuesToInsertInLTETable, timestamp, connection);

            final Map<String, Object> valuesToInsertInSgehTable = new HashMap<String, Object>();
            valuesToInsertInSgehTable.put(EVENT_ID, ATTACH_IN_2G_AND_3G);
            valuesToInsertInSgehTable.put(RAT, RAT_FOR_GSM);
            if (tac != -1) {
                valuesToInsertInSgehTable.put(TAC, tac);
            }
            valuesToInsertInSgehTable.put(APN, SAMPLE_APN);
            valuesToInsertInSgehTable.put(CAUSE_CODE_COLUMN, CAUSE_CODE_VALUE);
            valuesToInsertInSgehTable.put(CAUSE_PROT_TYPE_COLUMN, CAUSE_PROT_TYPE_VALUE);
            valuesToInsertInSgehTable.put(SUBCAUSE_CODE_COLUMN, SUBCAUSE_CODE_VALUE);
            valuesToInsertInSgehTable.put(GGSN_NAME, GGSN_NAME_VALUE);
            valuesToInsertInSgehTable.put(PDP_GGSN_IPADDRESS_1,0);
            valuesToInsertInSgehTable.put(PDP_GGSN_IPADDRESS_2,0);
            valuesToInsertInSgehTable.put(PDP_GGSN_NAME_1,PDP_GGSN_NAME_1_VALUE);
            valuesToInsertInSgehTable.put(PDP_GGSN_NAME_2,PDP_GGSN_NAME_2_VALUE);
            rawTablesPopulator.createAndPopulateRawSgehErrTable(valuesToInsertInSgehTable, timestamp, connection);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void createAndPopulateLookupTables() {
        final List<String> lookupTables = new ArrayList<String>();
        lookupTables.add(TEMP_DIM_E_SGEH_CAUSE_PROT_TYPE);
        lookupTables.add(TEMP_DIM_E_SGEH_CAUSECODE);
        lookupTables.add(TEMP_DIM_E_SGEH_SUBCAUSECODE);
        lookupTables.add(TEMP_DIM_E_LTE_CAUSE_PROT_TYPE);
        lookupTables.add(TEMP_DIM_E_LTE_CAUSECODE);
        lookupTables.add(TEMP_DIM_E_LTE_SUBCAUSECODE);
        lookupTables.add(TEMP_DIM_E_SGEH_TAC);
        lookupTables.add(TEMP_DIM_E_SGEH_GGSN);
        for (final String lookupTableRequired : lookupTables) {
            try {
                createAndPopulateLookupTable(lookupTableRequired);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        addTableNameToReplace(DIM_E_SGEH_TAC, TEMP_DIM_E_SGEH_TAC);
        final Collection<String> columnsForGroup = new ArrayList<String>();
        columnsForGroup.add(GROUP_NAME);
        columnsForGroup.add(APN);
        try {
            createTemporaryTable(TEMP_GROUP_TYPE_E_APN, columnsForGroup);
        } catch (Exception e) {
            e.printStackTrace();
        }
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(APN, SAMPLE_APN);
        values.put(GROUP_NAME, SAMPLE_APN_GROUP);
        try {
            insertRow(TEMP_GROUP_TYPE_E_APN, values);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
