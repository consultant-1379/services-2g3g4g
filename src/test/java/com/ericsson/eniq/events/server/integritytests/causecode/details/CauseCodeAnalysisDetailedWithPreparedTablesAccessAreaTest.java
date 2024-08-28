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

import org.junit.Test;

import com.ericsson.eniq.events.server.resources.CauseCodeAnalysisResource;
import com.ericsson.eniq.events.server.resources.TestsWithTemporaryTablesBaseTestCase;
import com.ericsson.eniq.events.server.test.populator.RawTablesPopulator;
import com.ericsson.eniq.events.server.test.queryresults.CauseCodeEventAnalysisDetailedResult;
import com.ericsson.eniq.events.server.test.stubs.DummyUriInfoImpl;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;
import com.sun.jersey.core.util.MultivaluedMapImpl;

public class CauseCodeAnalysisDetailedWithPreparedTablesAccessAreaTest extends
        TestsWithTemporaryTablesBaseTestCase<CauseCodeEventAnalysisDetailedResult> {

    private final CauseCodeAnalysisResource causeCodeAnalysisResource = new CauseCodeAnalysisResource();

    private final static int CAUSE_CODE_VALUE = 3;

    private final static int SUBCAUSE_CODE_VALUE = 10;

    private final static int CAUSE_PROT_TYPE_VALUE = 0;

    private final static String RAT_VALUE = "2";

    private final static String EXPECTED_LTE_CAUSECODE_AND_SUB_CAUSECODE_HELP = "text";

    private final static String EXPECTED_VALID_MANUFACTURER = "Apple";

    private final static String EXPECTED_VALID_MARKETING_NAME = "iPad 2 A1396";

    private final static String EXPECTED_UNKNOWN_MANUFACTURER = "Manufacturer Unknown";

    private final static String EXPECTED_UNKNOWN_MARKETING_NAME = "Model Unknown";

    @Test
    public void testGetDetailedDataForAccessArea1WeekLteAndSgeh() throws Exception {
        createAndPopulateRawTables(DateTimeUtilities.getDateTimeMinus48Hours());

        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        getQueryParams(map, TYPE_CELL);
        map.putSingle(RAT, RAT_VALUE);
        map.putSingle(CELL_PARAM, SAMPLE_CELL);
        map.putSingle(TIME_QUERY_PARAM, ONE_WEEK);
        map.putSingle(VENDOR_PARAM, ERICSSON);
        map.putSingle(BSC_PARAM, SAMPLE_HIERARCHY_3);

        DummyUriInfoImpl.setUriInfo(map, causeCodeAnalysisResource);

        final String json = causeCodeAnalysisResource.getData();
        final List<CauseCodeEventAnalysisDetailedResult> summaryResult = getTranslator().translateResult(json,
                CauseCodeEventAnalysisDetailedResult.class);
        assertThat(summaryResult.size(), is(1));
        assertThat(summaryResult.get(0).getCauseCodeHelp(), is(EXPECTED_LTE_CAUSECODE_AND_SUB_CAUSECODE_HELP));
        assertThat(summaryResult.get(0).getSubcauseCodeHelp(), is(EXPECTED_LTE_CAUSECODE_AND_SUB_CAUSECODE_HELP));

    }

    @Test
    public void testGetDetailedDataForAccessArea1DayLteAndSgeh() throws Exception {
        createAndPopulateRawTables(DateTimeUtilities.getDateTime(Calendar.HOUR, -12));
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        getQueryParams(map, TYPE_CELL);
        map.putSingle(RAT, RAT_VALUE);
        map.putSingle(CELL_PARAM, SAMPLE_CELL);
        map.putSingle(TIME_QUERY_PARAM, ONE_DAY);
        map.putSingle(VENDOR_PARAM, ERICSSON);
        map.putSingle(BSC_PARAM, SAMPLE_HIERARCHY_3);

        DummyUriInfoImpl.setUriInfo(map, causeCodeAnalysisResource);

        final String json = causeCodeAnalysisResource.getData();
        final List<CauseCodeEventAnalysisDetailedResult> summaryResult = getTranslator().translateResult(json,
                CauseCodeEventAnalysisDetailedResult.class);
        assertThat(summaryResult.size(), is(1));
        assertThat(summaryResult.get(0).getCauseCodeHelp(), is(EXPECTED_LTE_CAUSECODE_AND_SUB_CAUSECODE_HELP));
        assertThat(summaryResult.get(0).getSubcauseCodeHelp(), is(EXPECTED_LTE_CAUSECODE_AND_SUB_CAUSECODE_HELP));

    }

    @Test
    public void testGetDetailedDataForAccessArea1HourLteAndSgeh() throws Exception {
        createAndPopulateRawTables(DateTimeUtilities.getDateTime(Calendar.MINUTE, -15));
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        getQueryParams(map, TYPE_CELL);
        map.putSingle(RAT, RAT_VALUE);
        map.putSingle(CELL_PARAM, SAMPLE_CELL);
        map.putSingle(TIME_QUERY_PARAM, ONE_HOUR);
        map.putSingle(VENDOR_PARAM, ERICSSON);
        map.putSingle(BSC_PARAM, SAMPLE_HIERARCHY_3);

        DummyUriInfoImpl.setUriInfo(map, causeCodeAnalysisResource);

        final String json = causeCodeAnalysisResource.getData();
        final List<CauseCodeEventAnalysisDetailedResult> summaryResult = getTranslator().translateResult(json,
                CauseCodeEventAnalysisDetailedResult.class);
        assertThat(summaryResult.size(), is(1));
        assertThat(summaryResult.get(0).getCauseCodeHelp(), is(EXPECTED_LTE_CAUSECODE_AND_SUB_CAUSECODE_HELP));
        assertThat(summaryResult.get(0).getSubcauseCodeHelp(), is(EXPECTED_LTE_CAUSECODE_AND_SUB_CAUSECODE_HELP));
    }

    @Test
    public void testGetDetailedDataForAccessAreaGroup1HourLteAndSgeh() throws Exception {
        createAndPopulateRawTables(DateTimeUtilities.getDateTime(Calendar.MINUTE, -15));
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        getQueryParams(map, TYPE_CELL);
        map.putSingle(TIME_QUERY_PARAM, ONE_HOUR);
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(GROUP_NAME_PARAM, SAMPLE_CELL_GROUP);

        DummyUriInfoImpl.setUriInfo(map, causeCodeAnalysisResource);

        final String json = causeCodeAnalysisResource.getData();
        final List<CauseCodeEventAnalysisDetailedResult> summaryResult = getTranslator().translateResult(json,
                CauseCodeEventAnalysisDetailedResult.class);
        assertThat(summaryResult.size(), is(1));
        assertThat(summaryResult.get(0).getCauseCodeHelp(), is(EXPECTED_LTE_CAUSECODE_AND_SUB_CAUSECODE_HELP));
        assertThat(summaryResult.get(0).getSubcauseCodeHelp(), is(EXPECTED_LTE_CAUSECODE_AND_SUB_CAUSECODE_HELP));
    }

    @Test
    public void testGetDetailedDataForAccessAreaGroup1DayLteAndSgeh() throws Exception {
        createAndPopulateRawTables(DateTimeUtilities.getDateTime(Calendar.HOUR, -12));
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        getQueryParams(map, TYPE_CELL);
        map.putSingle(TIME_QUERY_PARAM, ONE_DAY);
        map.putSingle(GROUP_NAME_PARAM, SAMPLE_CELL_GROUP);

        DummyUriInfoImpl.setUriInfo(map, causeCodeAnalysisResource);

        final String json = causeCodeAnalysisResource.getData();
        final List<CauseCodeEventAnalysisDetailedResult> summaryResult = getTranslator().translateResult(json,
                CauseCodeEventAnalysisDetailedResult.class);
        assertThat(summaryResult.size(), is(1));
        assertThat(summaryResult.get(0).getCauseCodeHelp(), is(EXPECTED_LTE_CAUSECODE_AND_SUB_CAUSECODE_HELP));
        assertThat(summaryResult.get(0).getSubcauseCodeHelp(), is(EXPECTED_LTE_CAUSECODE_AND_SUB_CAUSECODE_HELP));
    }

    @Test
    public void testGetDetailedDataForAccessAreaGroup1WeekLteAndSgeh() throws Exception {
        createAndPopulateRawTables(DateTimeUtilities.getDateTimeMinus48Hours());
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        getQueryParams(map, TYPE_CELL);
        map.putSingle(TIME_QUERY_PARAM, ONE_WEEK);
        map.putSingle(GROUP_NAME_PARAM, SAMPLE_CELL_GROUP);

        DummyUriInfoImpl.setUriInfo(map, causeCodeAnalysisResource);

        final String json = causeCodeAnalysisResource.getData();
        final List<CauseCodeEventAnalysisDetailedResult> summaryResult = getTranslator().translateResult(json,
                CauseCodeEventAnalysisDetailedResult.class);
        assertThat(summaryResult.size(), is(1));
        assertThat(summaryResult.get(0).getCauseCodeHelp(), is(EXPECTED_LTE_CAUSECODE_AND_SUB_CAUSECODE_HELP));
        assertThat(summaryResult.get(0).getSubcauseCodeHelp(), is(EXPECTED_LTE_CAUSECODE_AND_SUB_CAUSECODE_HELP));
    }

    @Test
    public void testGetDetailedDataToVerifyValidManufacturerAndModelvalue() throws Exception {
        createAndPopulateRawTables(DateTimeUtilities.getDateTime(Calendar.MINUTE, -30), SAMPLE_TAC_2);
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        getQueryParams(map, TYPE_CELL);
        map.putSingle(RAT, RAT_VALUE);
        map.putSingle(CELL_PARAM, SAMPLE_CELL);
        map.putSingle(TIME_QUERY_PARAM, ONE_HOUR);
        map.putSingle(VENDOR_PARAM, ERICSSON);
        map.putSingle(BSC_PARAM, SAMPLE_HIERARCHY_3);

        DummyUriInfoImpl.setUriInfo(map, causeCodeAnalysisResource);

        final String json = causeCodeAnalysisResource.getData();
        final List<CauseCodeEventAnalysisDetailedResult> summaryResult = getTranslator().translateResult(json,
                CauseCodeEventAnalysisDetailedResult.class);
        assertThat(summaryResult.size(), is(1));
        assertThat(summaryResult.get(0).getManufacturer(), is(EXPECTED_VALID_MANUFACTURER));
        assertThat(summaryResult.get(0).getMarketingName(), is(EXPECTED_VALID_MARKETING_NAME));
    }

    @Test
    public void testGetDetailedDataToVerifyUnknownManufacturerAndUnknownModelvalue() throws Exception {
        createAndPopulateRawTables(DateTimeUtilities.getDateTime(Calendar.MINUTE, -30), SONY_ERICSSON_TAC);
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        getQueryParams(map, TYPE_CELL);
        map.putSingle(RAT, RAT_VALUE);
        map.putSingle(CELL_PARAM, SAMPLE_CELL);
        map.putSingle(TIME_QUERY_PARAM, ONE_HOUR);
        map.putSingle(VENDOR_PARAM, ERICSSON);
        map.putSingle(BSC_PARAM, SAMPLE_HIERARCHY_3);

        DummyUriInfoImpl.setUriInfo(map, causeCodeAnalysisResource);

        final String json = causeCodeAnalysisResource.getData();
        final List<CauseCodeEventAnalysisDetailedResult> summaryResult = getTranslator().translateResult(json,
                CauseCodeEventAnalysisDetailedResult.class);
        assertThat(summaryResult.size(), is(1));
        assertThat(summaryResult.get(0).getManufacturer(), is(EXPECTED_UNKNOWN_MANUFACTURER));
        assertThat(summaryResult.get(0).getMarketingName(), is(EXPECTED_UNKNOWN_MARKETING_NAME));
    }

    @Override
    public void onSetUp() {
        attachDependencies(causeCodeAnalysisResource);
        createAndPopulateLookupTables();
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
            if (tac != -1) {
                valuesToInsertInLTETable.put(TAC, tac);
            }
            valuesToInsertInLTETable.put(APN, SAMPLE_APN);
            valuesToInsertInLTETable.put(HIERARCHY_3, SAMPLE_HIERARCHY_3);
            valuesToInsertInLTETable.put(HIERARCHY_1, SAMPLE_CELL);
            valuesToInsertInLTETable.put(VENDOR, ERICSSON);
            valuesToInsertInLTETable.put(CAUSE_CODE_COLUMN, CAUSE_CODE_VALUE);
            valuesToInsertInLTETable.put(CAUSE_PROT_TYPE_COLUMN, CAUSE_PROT_TYPE_VALUE);
            valuesToInsertInLTETable.put(SUBCAUSE_CODE_COLUMN, SUBCAUSE_CODE_VALUE);
            rawTablesPopulator.createAndPopulateRawLteErrTable(valuesToInsertInLTETable, timestamp, connection);

            final Map<String, Object> valuesToInsertInSgehTable = new HashMap<String, Object>();
            valuesToInsertInSgehTable.put(EVENT_ID, ATTACH_IN_2G_AND_3G);
            valuesToInsertInSgehTable.put(RAT, RAT_FOR_GSM);
            valuesToInsertInSgehTable.put(APN, SAMPLE_APN);
            valuesToInsertInSgehTable.put(CAUSE_CODE_COLUMN, CAUSE_CODE_VALUE);
            valuesToInsertInSgehTable.put(CAUSE_PROT_TYPE_COLUMN, CAUSE_PROT_TYPE_VALUE);
            valuesToInsertInSgehTable.put(SUBCAUSE_CODE_COLUMN, SUBCAUSE_CODE_VALUE);
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
        columnsForGroup.add(HIERARCHY_3);
        columnsForGroup.add(HIERARCHY_1);
        try {
            createTemporaryTable(TEMP_GROUP_TYPE_E_RAT_VEND_HIER321, columnsForGroup);
        } catch (Exception e) {
            e.printStackTrace();
        }

        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(HIERARCHY_1, SAMPLE_CELL);
        values.put(HIERARCHY_3, SAMPLE_HIERARCHY_3);
        values.put(GROUP_NAME, SAMPLE_CELL_GROUP);
        try {
            insertRow(TEMP_GROUP_TYPE_E_RAT_VEND_HIER321, values);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
