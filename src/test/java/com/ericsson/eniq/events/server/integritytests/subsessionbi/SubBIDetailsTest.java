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
package com.ericsson.eniq.events.server.integritytests.subsessionbi;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.*;
import static org.junit.Assert.assertEquals;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.ws.rs.core.MultivaluedMap;

import org.junit.Test;

import com.ericsson.eniq.events.server.resources.SubsessionBIResource;
import com.ericsson.eniq.events.server.resources.TestsWithTemporaryTablesBaseTestCase;
import com.ericsson.eniq.events.server.test.queryresults.SubBIDetailsResult;
import com.ericsson.eniq.events.server.test.stubs.DummyUriInfoImpl;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;
import com.sun.jersey.core.util.MultivaluedMapImpl;

/**
 * @since 2011
 * 
 */
public class SubBIDetailsTest extends TestsWithTemporaryTablesBaseTestCase<SubBIDetailsResult> {

    private static final Map<String, String> rawTableColumns = new HashMap<String, String>();
    private static final Map<String, Object> rawTableValues = new HashMap<String, Object>();

    private static final Map<String, String> sgehImsiTableColumns = new HashMap<String, String>();
    private static final Map<String, Object> sgehIimsiTableValues = new HashMap<String, Object>();

    private static final Map<String, String> lteImsiTableColumns = new HashMap<String, String>();
    private static final Map<String, Object> lteImsiTableValues = new HashMap<String, Object>();

    private static final Map<String, String> groupColumns = new HashMap<String, String>();
    private static final Map<String, Object> groupTableValues = new HashMap<String, Object>();

    private static final Map<String, Object> SGEHHier321Columns = new HashMap<String, Object>();
    private static final Map<String, Object> SGEHHier321Values = new HashMap<String, Object>();

    private static final Map<String, Object> LTEHier321Columns = new HashMap<String, Object>();
    private static final Map<String, Object> LTEHier321Values = new HashMap<String, Object>();

    private static final Map<String, String> imsiMsisdnColumns = new HashMap<String, String>();
    private static final Map<String, Object> imsiMsisdnTableValues = new HashMap<String, Object>();

    private SubsessionBIResource subsessionBIResource;

    private final List<String> tempDataTables = new ArrayList<String>();

    private static final String TEST_TIME = "60";

    private static final String TEST_GROUP_NAME = "VIP";

    private static final long TEST_MSISDN = 312030411110004L;

    private static final long TEST_MSISDN_FOR_INVALID_IMSI_MCC_MNC = 312030411110052L;

    private static final String TEST_TZ_OFFSET = "+0000";

    private static final String TEST_MAX_ROWS = "50";

    private static final String TEST_TRAC_SGEH = "102";

    private static final String TEST_TRAC_LTE = "103";

    private static final String TEST_VENDOR = "Ericsson";

    private final static long TEST_IMSI_SGEH = 312030410000004L;

    private final static long TEST_IMSI_FOR_INVALID_IMSI_MCC_MNC_SGEH = 312030410000052L;

    private final static String TEST_PTMSI = "908";

    private final static String TEST_EVENT_SOURCE_NAME = "MME2";

    private final static String TEST_HIERARCHY_3 = "BSC56";

    private final static String TEST_HIERARCHY_1 = "CELL11116";

    private final static String TEST_MCC = "460";

    private final static String TEST_MNC = "00";

    private final static String TEST_INVALID_IMSI_MCC = "312";

    private final static String TEST_INVALID_IMSI_MNC = "99";

    private final static int TEST_LAC = 112;

    private final static int TEST_RAC = 1;

    private final static long TEST_IMSI_LTE = 312030419990004L;

    private final static long TEST_IMSI_FOR_INVALID_IMSI_MCC_MNC_LTE = 312030419990053L;

    private static String SGEH_FIRST_SEEN_RAW = "";
    private static String LTE_FIRST_SEEN_RAW = "";
    private static String SGEH_LAST_SEEN_RAW = "";
    private static String LTE_LAST_SEEN_RAW = "";

    private static String SGEH_FIRST_SEEN_IMSI_RAW = "";
    private static String LTE_FIRST_SEEN_IMSI_RAW = "";
    private static String SGEH_LAST_SEEN_IMSI_RAW = "";
    private static String LTE_LAST_SEEN_IMSI_RAW = "";

    private static long TEST_HIER321_ID = 314530410000004L;
    private static long TEST_HIER3_ID = 314530412200004L;

    @Override
    public void onSetUp() throws Exception {
        super.onSetUp();
        subsessionBIResource = new SubsessionBIResource();

        createTempTables();
        populateTemporaryTables();

        attachDependencies(subsessionBIResource);
    }

    private void createTempTables() throws Exception {
        for (final String tempTable : getTempTables()) {
            createTemporaryTableWithColumnTypes(tempTable, getRawTableColumns());
        }

        createTemporaryTableWithColumnTypes(TEMP_EVENT_E_SGEH_IMSI_SUC_RAW, getSgehImsiTableColumns());
        createTemporaryTableWithColumnTypes(TEMP_EVENT_E_LTE_IMSI_SUC_RAW, getLteImsiTableColumns());

        createTemporaryTableWithColumnTypes(TEMP_GROUP_TYPE_E_IMSI, getGroupColumns());
        createTemporaryTableWithColumnTypes(TEMP_DIM_E_IMSI_MSISDN, getImsiMsisdnColumns());
        createTemporaryTableWithColumnTypes(TEMP_DIM_E_SGEH_HIER321, getSGEHHier321Columns());
        createTemporaryTableWithColumnTypes(TEMP_DIM_E_LTE_HIER321, getLTEHier321Columns());
    }

    private List<String> getTempTables() {
        tempDataTables.add(TEMP_EVENT_E_SGEH_ERR_RAW);
        tempDataTables.add(TEMP_EVENT_E_SGEH_SUC_RAW);
        tempDataTables.add(TEMP_EVENT_E_LTE_ERR_RAW);
        tempDataTables.add(TEMP_EVENT_E_LTE_SUC_RAW);
        return tempDataTables;
    }

    private Map<String, String> getRawTableColumns() {
        rawTableColumns.put("DATETIME_ID", "timestamp");
        rawTableColumns.put("EVENT_TIME", "timestamp");
        rawTableColumns.put("MSISDN", "unsigned bigint");
        rawTableColumns.put("IMSI", "unsigned bigint");
        rawTableColumns.put("PTMSI", "unsigned int");
        rawTableColumns.put("ROAMING", "bit");
        rawTableColumns.put("HIERARCHY_1", "varchar(128)");
        rawTableColumns.put("HIERARCHY_3", "varchar(128)");
        rawTableColumns.put("VENDOR", "varchar(20)");
        rawTableColumns.put("RAT", "tinyint");
        rawTableColumns.put("MCC", "varchar(3)");
        rawTableColumns.put("MNC", "varchar(3)");
        rawTableColumns.put("LAC", "unsigned int");
        rawTableColumns.put("RAC", "tinyint");
        rawTableColumns.put("EVENT_SOURCE_NAME", "varchar(128)");
        rawTableColumns.put("IMSI_MCC", "varchar(3)");
        rawTableColumns.put("IMSI_MNC", "varchar(3)");
        rawTableColumns.put("TRAC", "unsigned int");
        return rawTableColumns;
    }

    private Map<String, Object> getSGEHHier321Columns() {
        SGEHHier321Columns.put("HIERARCHY_1", "varchar(128)");
        SGEHHier321Columns.put("HIERARCHY_3", "varchar(128)");
        SGEHHier321Columns.put("HIER321_ID", "unsigned bigint");
        SGEHHier321Columns.put("MCC", "varchar(3)");
        SGEHHier321Columns.put("MNC", "varchar(3)");
        SGEHHier321Columns.put("VENDOR", "varchar(20)");
        SGEHHier321Columns.put("RAC", "tinyint");
        SGEHHier321Columns.put("LAC", "unsigned int");
        return SGEHHier321Columns;
    }

    private Map<String, Object> getLTEHier321Columns() {
        LTEHier321Columns.put("HIERARCHY_1", "varchar(128)");
        LTEHier321Columns.put("HIERARCHY_3", "varchar(128)");
        LTEHier321Columns.put("HIER321_ID", "unsigned bigint");
        LTEHier321Columns.put("MCC", "varchar(3)");
        LTEHier321Columns.put("MNC", "varchar(3)");
        LTEHier321Columns.put("VENDOR", "varchar(20)");
        return LTEHier321Columns;
    }

    private Map<String, String> getImsiMsisdnColumns() {
        imsiMsisdnColumns.put("IMSI", "unsigned bigint");
        imsiMsisdnColumns.put("MSISDN", "unsigned bigint");
        imsiMsisdnColumns.put("TIMESTAMP_ID", "timestamp");
        imsiMsisdnColumns.put("MODIFIED", "timestamp");
        return imsiMsisdnColumns;
    }

    private Map<String, String> getSgehImsiTableColumns() {
        sgehImsiTableColumns.put("DATETIME_ID", "timestamp");
        sgehImsiTableColumns.put("IMSI", "unsigned bigint");
        sgehImsiTableColumns.put("PTMSI", "unsigned int");
        sgehImsiTableColumns.put("ROAMING", "bit");
        sgehImsiTableColumns.put("HIER321_ID", "unsigned bigint");
        sgehImsiTableColumns.put("HIER3_ID", "unsigned bigint");
        sgehImsiTableColumns.put("RAT", "tinyint");
        sgehImsiTableColumns.put("EVENT_SOURCE_NAME", "varchar(128)");
        sgehImsiTableColumns.put("IMSI_MCC", "varchar(3)");
        sgehImsiTableColumns.put("IMSI_MNC", "varchar(3)");
        sgehImsiTableColumns.put("TRAC", "unsigned int");
        return sgehImsiTableColumns;
    }

    private Map<String, String> getLteImsiTableColumns() {
        lteImsiTableColumns.put("DATETIME_ID", "timestamp");
        lteImsiTableColumns.put("IMSI", "unsigned bigint");
        lteImsiTableColumns.put("ROAMING", "bit");
        lteImsiTableColumns.put("HIER321_ID", "unsigned bigint");
        lteImsiTableColumns.put("HIER3_ID", "unsigned bigint");
        lteImsiTableColumns.put("RAT", "tinyint");
        lteImsiTableColumns.put("EVENT_SOURCE_NAME", "varchar(128)");
        lteImsiTableColumns.put("IMSI_MCC", "varchar(3)");
        lteImsiTableColumns.put("IMSI_MNC", "varchar(3)");
        lteImsiTableColumns.put("TRAC", "unsigned int");
        return lteImsiTableColumns;
    }

    private Map<String, String> getGroupColumns() {
        groupColumns.put("IMSI", "unsigned bigint");
        groupColumns.put("GROUP_NAME", "varchar(64)");
        return groupColumns;
    }

    private void populateTemporaryTables() throws SQLException {
        populateRawTables();
        populateIMSISucRawTables();
        populateDIMTables();
    }

    private void populateRawTables() throws SQLException {
        SGEH_LAST_SEEN_RAW = DateTimeUtilities.getDateTime(Calendar.MINUTE, -24);
        SGEH_FIRST_SEEN_RAW = DateTimeUtilities.getDateTime(Calendar.MINUTE, -35);

        rawTableValues.put("DATETIME_ID", DateTimeUtilities.getDateTimeMinus25Minutes());
        rawTableValues.put("EVENT_TIME", DateTimeUtilities.getDateTimeMinus25Minutes());
        rawTableValues.put("MSISDN", TEST_MSISDN);
        rawTableValues.put("PTMSI", TEST_PTMSI);
        rawTableValues.put("ROAMING", 1);
        rawTableValues.put("HIERARCHY_1", TEST_HIERARCHY_1);
        rawTableValues.put("HIERARCHY_3", TEST_HIERARCHY_3);
        rawTableValues.put("VENDOR", TEST_VENDOR);
        rawTableValues.put("MCC", TEST_MCC);
        rawTableValues.put("MNC", TEST_MNC);
        rawTableValues.put("LAC", TEST_LAC);
        rawTableValues.put("RAC", TEST_RAC);
        rawTableValues.put("EVENT_SOURCE_NAME", TEST_EVENT_SOURCE_NAME);
        rawTableValues.put("IMSI_MCC", TEST_MCC);
        rawTableValues.put("IMSI_MNC", TEST_MNC);

        rawTableValues.put("TRAC", TEST_TRAC_SGEH);
        rawTableValues.put("IMSI", TEST_IMSI_SGEH);
        rawTableValues.put("RAT", "1");

        rawTableValues.put("DATETIME_ID", SGEH_FIRST_SEEN_RAW);
        rawTableValues.put("EVENT_TIME", SGEH_FIRST_SEEN_RAW);
        insertRow(TEMP_EVENT_E_SGEH_ERR_RAW, rawTableValues);

        rawTableValues.put("DATETIME_ID", DateTimeUtilities.getDateTime(Calendar.MINUTE, -27));
        rawTableValues.put("EVENT_TIME", DateTimeUtilities.getDateTime(Calendar.MINUTE, -27));
        insertRow(TEMP_EVENT_E_SGEH_ERR_RAW, rawTableValues);

        rawTableValues.put("IMSI_MCC", TEST_INVALID_IMSI_MCC);
        rawTableValues.put("IMSI_MNC", TEST_INVALID_IMSI_MNC);
        rawTableValues.put("MSISDN", TEST_MSISDN_FOR_INVALID_IMSI_MCC_MNC);
        rawTableValues.put("IMSI", TEST_IMSI_FOR_INVALID_IMSI_MCC_MNC_SGEH);
        insertRow(TEMP_EVENT_E_SGEH_ERR_RAW, rawTableValues);

        rawTableValues.put("DATETIME_ID", SGEH_FIRST_SEEN_RAW);
        rawTableValues.put("EVENT_TIME", SGEH_FIRST_SEEN_RAW);
        insertRow(TEMP_EVENT_E_SGEH_ERR_RAW, rawTableValues);

        rawTableValues.put("IMSI_MCC", TEST_MCC);
        rawTableValues.put("IMSI_MNC", TEST_MNC);
        rawTableValues.put("MSISDN", TEST_MSISDN);
        rawTableValues.put("IMSI", TEST_IMSI_SGEH);

        rawTableValues.put("DATETIME_ID", SGEH_LAST_SEEN_RAW);
        rawTableValues.put("EVENT_TIME", SGEH_LAST_SEEN_RAW);
        insertRow(TEMP_EVENT_E_SGEH_SUC_RAW, rawTableValues);

        rawTableValues.put("IMSI_MNC", TEST_MNC);
        rawTableValues.put("MSISDN", TEST_MSISDN);
        rawTableValues.put("TRAC", TEST_TRAC_LTE);
        rawTableValues.put("IMSI", TEST_IMSI_LTE);
        rawTableValues.put("PTMSI", "000");
        rawTableValues.put("RAT", "2");
        LTE_LAST_SEEN_RAW = DateTimeUtilities.getDateTime(Calendar.MINUTE, -25);
        LTE_FIRST_SEEN_RAW = DateTimeUtilities.getDateTime(Calendar.MINUTE, -36);

        rawTableValues.put("DATETIME_ID", LTE_FIRST_SEEN_RAW);
        rawTableValues.put("EVENT_TIME", LTE_FIRST_SEEN_RAW);
        insertRow(TEMP_EVENT_E_LTE_ERR_RAW, rawTableValues);

        rawTableValues.put("DATETIME_ID", DateTimeUtilities.getDateTime(Calendar.MINUTE, -28));
        rawTableValues.put("EVENT_TIME", DateTimeUtilities.getDateTime(Calendar.MINUTE, -28));
        insertRow(TEMP_EVENT_E_LTE_ERR_RAW, rawTableValues);

        rawTableValues.put("IMSI_MCC", TEST_INVALID_IMSI_MCC);
        rawTableValues.put("IMSI_MNC", TEST_INVALID_IMSI_MNC);
        rawTableValues.put("MSISDN", TEST_MSISDN_FOR_INVALID_IMSI_MCC_MNC);
        rawTableValues.put("IMSI", TEST_IMSI_FOR_INVALID_IMSI_MCC_MNC_LTE);
        insertRow(TEMP_EVENT_E_LTE_ERR_RAW, rawTableValues);

        rawTableValues.put("DATETIME_ID", LTE_FIRST_SEEN_RAW);
        rawTableValues.put("EVENT_TIME", LTE_FIRST_SEEN_RAW);
        insertRow(TEMP_EVENT_E_LTE_ERR_RAW, rawTableValues);

        rawTableValues.put("IMSI_MCC", TEST_MCC);
        rawTableValues.put("IMSI_MNC", TEST_MNC);
        rawTableValues.put("MSISDN", TEST_MSISDN);
        rawTableValues.put("IMSI", TEST_IMSI_LTE);

        rawTableValues.put("DATETIME_ID", LTE_LAST_SEEN_RAW);
        rawTableValues.put("EVENT_TIME", LTE_LAST_SEEN_RAW);
        insertRow(TEMP_EVENT_E_LTE_SUC_RAW, rawTableValues);
    }

    private void populateIMSISucRawTables() throws SQLException {
        SGEH_LAST_SEEN_IMSI_RAW = DateTimeUtilities.getDateTime(Calendar.MINUTE, -24);
        SGEH_FIRST_SEEN_IMSI_RAW = DateTimeUtilities.getDateTime(Calendar.MINUTE, -34);

        sgehIimsiTableValues.put("DATETIME_ID", DateTimeUtilities.getDateTimeMinus25Minutes());
        sgehIimsiTableValues.put("ROAMING", 1);
        sgehIimsiTableValues.put("HIER321_ID", TEST_HIER321_ID);
        sgehIimsiTableValues.put("HIER3_ID", TEST_HIER3_ID);
        sgehIimsiTableValues.put("EVENT_SOURCE_NAME", TEST_EVENT_SOURCE_NAME);
        sgehIimsiTableValues.put("IMSI_MCC", TEST_MCC);
        sgehIimsiTableValues.put("IMSI_MNC", TEST_MNC);
        sgehIimsiTableValues.put("TRAC", TEST_TRAC_SGEH);
        sgehIimsiTableValues.put("IMSI", TEST_IMSI_SGEH);
        sgehIimsiTableValues.put("PTMSI", TEST_PTMSI);
        sgehIimsiTableValues.put("RAT", "1");

        sgehIimsiTableValues.put("DATETIME_ID", SGEH_FIRST_SEEN_IMSI_RAW);
        insertRow(TEMP_EVENT_E_SGEH_IMSI_SUC_RAW, sgehIimsiTableValues);

        sgehIimsiTableValues.put("DATETIME_ID", DateTimeUtilities.getDateTime(Calendar.MINUTE, -27));
        insertRow(TEMP_EVENT_E_SGEH_IMSI_SUC_RAW, sgehIimsiTableValues);

        sgehIimsiTableValues.put("DATETIME_ID", SGEH_LAST_SEEN_IMSI_RAW);
        insertRow(TEMP_EVENT_E_SGEH_IMSI_SUC_RAW, sgehIimsiTableValues);

        sgehIimsiTableValues.put("IMSI_MCC", TEST_INVALID_IMSI_MCC);
        sgehIimsiTableValues.put("IMSI_MNC", TEST_INVALID_IMSI_MNC);
        sgehIimsiTableValues.put("IMSI", TEST_IMSI_FOR_INVALID_IMSI_MCC_MNC_SGEH);

        sgehIimsiTableValues.put("DATETIME_ID", SGEH_FIRST_SEEN_IMSI_RAW);
        insertRow(TEMP_EVENT_E_SGEH_IMSI_SUC_RAW, sgehIimsiTableValues);

        sgehIimsiTableValues.put("DATETIME_ID", DateTimeUtilities.getDateTime(Calendar.MINUTE, -27));
        insertRow(TEMP_EVENT_E_SGEH_IMSI_SUC_RAW, sgehIimsiTableValues);

        sgehIimsiTableValues.put("DATETIME_ID", SGEH_LAST_SEEN_IMSI_RAW);
        insertRow(TEMP_EVENT_E_SGEH_IMSI_SUC_RAW, sgehIimsiTableValues);

        lteImsiTableValues.put("DATETIME_ID", DateTimeUtilities.getDateTimeMinus25Minutes());
        lteImsiTableValues.put("ROAMING", 1);
        lteImsiTableValues.put("HIER321_ID", TEST_HIER321_ID);
        lteImsiTableValues.put("HIER3_ID", TEST_HIER3_ID);
        lteImsiTableValues.put("EVENT_SOURCE_NAME", TEST_EVENT_SOURCE_NAME);
        lteImsiTableValues.put("IMSI_MCC", TEST_MCC);
        lteImsiTableValues.put("IMSI_MNC", TEST_MNC);
        lteImsiTableValues.put("TRAC", TEST_TRAC_LTE);
        lteImsiTableValues.put("IMSI", TEST_IMSI_LTE);
        lteImsiTableValues.put("RAT", "2");
        LTE_LAST_SEEN_IMSI_RAW = DateTimeUtilities.getDateTime(Calendar.MINUTE, -26);
        LTE_FIRST_SEEN_IMSI_RAW = DateTimeUtilities.getDateTime(Calendar.MINUTE, -33);

        lteImsiTableValues.put("DATETIME_ID", LTE_FIRST_SEEN_IMSI_RAW);
        insertRow(TEMP_EVENT_E_LTE_IMSI_SUC_RAW, lteImsiTableValues);

        lteImsiTableValues.put("DATETIME_ID", DateTimeUtilities.getDateTime(Calendar.MINUTE, -28));
        insertRow(TEMP_EVENT_E_LTE_IMSI_SUC_RAW, lteImsiTableValues);

        lteImsiTableValues.put("DATETIME_ID", LTE_LAST_SEEN_IMSI_RAW);
        insertRow(TEMP_EVENT_E_LTE_IMSI_SUC_RAW, lteImsiTableValues);

        lteImsiTableValues.put("IMSI_MCC", TEST_INVALID_IMSI_MCC);
        lteImsiTableValues.put("IMSI_MNC", TEST_INVALID_IMSI_MNC);
        lteImsiTableValues.put("IMSI", TEST_IMSI_FOR_INVALID_IMSI_MCC_MNC_LTE);

        lteImsiTableValues.put("DATETIME_ID", LTE_FIRST_SEEN_IMSI_RAW);
        insertRow(TEMP_EVENT_E_LTE_IMSI_SUC_RAW, lteImsiTableValues);

        lteImsiTableValues.put("DATETIME_ID", DateTimeUtilities.getDateTime(Calendar.MINUTE, -28));
        insertRow(TEMP_EVENT_E_LTE_IMSI_SUC_RAW, lteImsiTableValues);

        lteImsiTableValues.put("DATETIME_ID", LTE_LAST_SEEN_IMSI_RAW);
        insertRow(TEMP_EVENT_E_LTE_IMSI_SUC_RAW, lteImsiTableValues);

    }

    private void populateDIMTables() throws SQLException {
        groupTableValues.put("GROUP_NAME", TEST_GROUP_NAME);
        groupTableValues.put("IMSI", TEST_IMSI_SGEH);
        insertRow(TEMP_GROUP_TYPE_E_IMSI, groupTableValues);
        groupTableValues.put("IMSI", TEST_IMSI_LTE);
        insertRow(TEMP_GROUP_TYPE_E_IMSI, groupTableValues);

        groupTableValues.put("IMSI", TEST_IMSI_FOR_INVALID_IMSI_MCC_MNC_SGEH);
        insertRow(TEMP_GROUP_TYPE_E_IMSI, groupTableValues);
        groupTableValues.put("IMSI", TEST_IMSI_FOR_INVALID_IMSI_MCC_MNC_LTE);
        insertRow(TEMP_GROUP_TYPE_E_IMSI, groupTableValues);

        SGEHHier321Values.put("HIERARCHY_1", TEST_HIERARCHY_1);
        SGEHHier321Values.put("HIERARCHY_3", TEST_HIERARCHY_3);
        SGEHHier321Values.put("HIER321_ID", TEST_HIER321_ID);
        SGEHHier321Values.put("MCC", TEST_MCC);
        SGEHHier321Values.put("MNC", TEST_MNC);
        SGEHHier321Values.put("VENDOR", TEST_VENDOR);
        SGEHHier321Values.put("RAC", TEST_RAC);
        SGEHHier321Values.put("LAC", TEST_LAC);
        insertRow(TEMP_DIM_E_SGEH_HIER321, SGEHHier321Values);

        LTEHier321Values.put("HIERARCHY_1", TEST_HIERARCHY_1);
        LTEHier321Values.put("HIERARCHY_3", TEST_HIERARCHY_3);
        LTEHier321Values.put("HIER321_ID", TEST_HIER321_ID);
        LTEHier321Values.put("MCC", TEST_MCC);
        LTEHier321Values.put("MNC", TEST_MNC);
        LTEHier321Values.put("VENDOR", TEST_VENDOR);
        insertRow(TEMP_DIM_E_LTE_HIER321, LTEHier321Values);

        imsiMsisdnTableValues.put("MSISDN", TEST_MSISDN);
        imsiMsisdnTableValues.put("IMSI", TEST_IMSI_SGEH);
        imsiMsisdnTableValues.put("TIMESTAMP_ID", SGEH_FIRST_SEEN_RAW);
        imsiMsisdnTableValues.put("MODIFIED", SGEH_FIRST_SEEN_RAW);
        insertRow(TEMP_DIM_E_IMSI_MSISDN, imsiMsisdnTableValues);

        imsiMsisdnTableValues.put("MSISDN", TEST_MSISDN_FOR_INVALID_IMSI_MCC_MNC);
        imsiMsisdnTableValues.put("IMSI", TEST_IMSI_FOR_INVALID_IMSI_MCC_MNC_SGEH);
        insertRow(TEMP_DIM_E_IMSI_MSISDN, imsiMsisdnTableValues);

        imsiMsisdnTableValues.put("MSISDN", TEST_MSISDN);
        imsiMsisdnTableValues.put("TIMESTAMP_ID", LTE_FIRST_SEEN_RAW);
        imsiMsisdnTableValues.put("MODIFIED", LTE_FIRST_SEEN_RAW);
        imsiMsisdnTableValues.put("IMSI", TEST_IMSI_LTE);
        insertRow(TEMP_DIM_E_IMSI_MSISDN, imsiMsisdnTableValues);

        imsiMsisdnTableValues.put("MSISDN", TEST_MSISDN_FOR_INVALID_IMSI_MCC_MNC);
        imsiMsisdnTableValues.put("IMSI", TEST_IMSI_FOR_INVALID_IMSI_MCC_MNC_LTE);
        insertRow(TEMP_DIM_E_IMSI_MSISDN, imsiMsisdnTableValues);

    }

    /**
     * Test method for {@link com.ericsson.eniq.events.server.resources.SubsessionBIResource#getSubBITerminalData()} .
     */
    @Test
    public void testGetSubBISubscriberDetailsDataSGEH_SucRawEnabled() throws Exception {
        jndiProperties.useSucRawJNDIProperty();

        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(IMSI_PARAM, Long.toString(TEST_IMSI_SGEH));
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String result = subsessionBIResource.getSubBISubscriberDetailsData();

        SubBIDetailsResult.isTypeIMSI = true;
        validateResultsSGEH_SucRawEnabled(getTranslator().translateResult(result, SubBIDetailsResult.class).get(0));
        jndiProperties.setUpJNDIPropertiesForTest();
    }

    @Test
    public void testGetSubBISubscriberDetailsDataLTE_SucRawEnabled() throws Exception {
        jndiProperties.useSucRawJNDIProperty();

        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(IMSI_PARAM, Long.toString(TEST_IMSI_LTE));
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String result = subsessionBIResource.getSubBISubscriberDetailsData();

        SubBIDetailsResult.isTypeIMSI = true;
        validateResultsLTE_SucRawEnabled(getTranslator().translateResult(result, SubBIDetailsResult.class).get(0));
        jndiProperties.setUpJNDIPropertiesForTest();
    }

    @Test
    public void testGetSubBISubscriberDetailsDataPTMSI_SucRawEnabled() throws Exception {
        jndiProperties.useSucRawJNDIProperty();

        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(TYPE_PARAM, TYPE_PTMSI);
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(PTMSI_PARAM, TEST_PTMSI);
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String result = subsessionBIResource.getSubBISubscriberDetailsData();

        SubBIDetailsResult.isTypeIMSI = false;
        validateResultsPTMSI_SucRawEnabled(getTranslator().translateResult(result, SubBIDetailsResult.class).get(0));
        jndiProperties.setUpJNDIPropertiesForTest();
    }

    @Test
    public void testGetSubBISubscriberDetailsDataSGEH_SucRawDisabled() throws Exception {
        jndiProperties.useSucRawJNDIProperty();
        jndiProperties.disableSucRawJNDIProperty();

        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(IMSI_PARAM, Long.toString(TEST_IMSI_SGEH));
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String result = subsessionBIResource.getSubBISubscriberDetailsData();

        SubBIDetailsResult.isTypeIMSI = true;
        validateResultsSGEH_SucRawDisabled(getTranslator().translateResult(result, SubBIDetailsResult.class).get(0));
        jndiProperties.setUpJNDIPropertiesForTest();
    }

    @Test
    public void testGetSubBISubscriberDetailsDataSGEH_SucRawDisabled_Invalid_IMSI_Mcc_Mnc() throws Exception {
        jndiProperties.useSucRawJNDIProperty();
        jndiProperties.disableSucRawJNDIProperty();

        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(IMSI_PARAM, Long.toString(TEST_IMSI_FOR_INVALID_IMSI_MCC_MNC_SGEH));
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String result = subsessionBIResource.getSubBISubscriberDetailsData();

        SubBIDetailsResult.isTypeIMSI = true;
        validateResultsSGEH_SucRawDisabled_Invalid_IMSI_Mcc_Mnc(getTranslator().translateResult(result, SubBIDetailsResult.class).get(0));
        jndiProperties.setUpJNDIPropertiesForTest();
    }

    @Test
    public void testGetSubBISubscriberDetailsDataLTE_SucRawDisabled() throws Exception {
        jndiProperties.useSucRawJNDIProperty();
        jndiProperties.disableSucRawJNDIProperty();

        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(IMSI_PARAM, Long.toString(TEST_IMSI_LTE));
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String result = subsessionBIResource.getSubBISubscriberDetailsData();

        SubBIDetailsResult.isTypeIMSI = true;
        validateResultsLTE_SucRawDisabled(getTranslator().translateResult(result, SubBIDetailsResult.class).get(0));
        jndiProperties.setUpJNDIPropertiesForTest();
    }

    @Test
    public void testGetSubBISubscriberDetailsDataPTMSI_SucRawDisabled() throws Exception {
        jndiProperties.useSucRawJNDIProperty();
        jndiProperties.disableSucRawJNDIProperty();

        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(TYPE_PARAM, TYPE_PTMSI);
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(TIME_QUERY_PARAM, TEST_TIME);
        map.putSingle(PTMSI_PARAM, TEST_PTMSI);
        map.putSingle(TZ_OFFSET, TEST_TZ_OFFSET);
        map.putSingle(MAX_ROWS, TEST_MAX_ROWS);
        DummyUriInfoImpl.setUriInfo(map, subsessionBIResource);
        final String result = subsessionBIResource.getSubBISubscriberDetailsData();

        SubBIDetailsResult.isTypeIMSI = false;
        validateResultsPTMSI_SucRawDisabled(getTranslator().translateResult(result, SubBIDetailsResult.class).get(0));
        jndiProperties.setUpJNDIPropertiesForTest();
    }

    private void validateResultsSGEH_SucRawEnabled(final SubBIDetailsResult result) {
        final SubBIDetailsResult expected = new SubBIDetailsResult();
        expected.setVipStatus("1");
        expected.setMsisdn(Long.toString(TEST_MSISDN));
        expected.setHomeCountry("China");
        expected.setMobileNetworkOperator("China Mobile");
        expected.setRoamingStatus(ROAMING_STATUS_AWAY);
        expected.setLastCellLocation(TEST_HIERARCHY_1 + "-" + TEST_HIERARCHY_3 + "-" + TEST_VENDOR + "-1");
        expected.setLastRoutingArea(TEST_MCC + "-" + TEST_MNC + "-" + TEST_LAC + "-" + TEST_RAC);
        expected.setLastObservedSGSN("MME2");
        expected.setFirstObserved(SGEH_FIRST_SEEN_RAW + ".0");
        expected.setLastObserved(SGEH_LAST_SEEN_RAW + ".0");
        expected.setLastObservedPTMSI(TEST_PTMSI);

        assertEquals(expected, result);
    }

    private void validateResultsLTE_SucRawEnabled(final SubBIDetailsResult result) {

        final SubBIDetailsResult expected = new SubBIDetailsResult();
        expected.setVipStatus("1");
        expected.setMsisdn(Long.toString(TEST_MSISDN));
        expected.setHomeCountry("China");
        expected.setMobileNetworkOperator("China Mobile");
        expected.setRoamingStatus(ROAMING_STATUS_AWAY);
        expected.setLastCellLocation(TEST_HIERARCHY_1 + "-" + TEST_HIERARCHY_3 + "-" + TEST_VENDOR + "-2");
        expected.setLastRoutingArea(TEST_MCC + "-" + TEST_MNC + "-" + TEST_TRAC_LTE);
        expected.setLastObservedSGSN("MME2");
        expected.setFirstObserved(LTE_FIRST_SEEN_RAW + ".0");
        expected.setLastObserved(LTE_LAST_SEEN_RAW + ".0");
        expected.setLastObservedPTMSI("");

        assertEquals(expected, result);
    }

    private void validateResultsPTMSI_SucRawEnabled(final SubBIDetailsResult result) {
        final SubBIDetailsResult expected = new SubBIDetailsResult();
        expected.setVipStatus(null);
        expected.setHomeCountry("China");
        expected.setMobileNetworkOperator("China Mobile");
        expected.setRoamingStatus(ROAMING_STATUS_AWAY);
        expected.setLastCellLocation(TEST_HIERARCHY_1 + "-" + TEST_HIERARCHY_3 + "-" + TEST_VENDOR + "-1");
        expected.setLastRoutingArea(TEST_MCC + "-" + TEST_MNC + "-" + TEST_LAC + "-" + TEST_RAC);
        expected.setLastObservedSGSN("MME2");
        expected.setFirstObserved(SGEH_FIRST_SEEN_RAW + ".0");
        expected.setLastObserved(SGEH_LAST_SEEN_RAW + ".0");
        expected.setLastObservedPTMSI(TEST_PTMSI);

        assertEquals(expected, result);
    }

    private void validateResultsSGEH_SucRawDisabled(final SubBIDetailsResult result) {
        final SubBIDetailsResult expected = new SubBIDetailsResult();
        expected.setVipStatus("1");
        expected.setMsisdn(Long.toString(TEST_MSISDN));
        expected.setHomeCountry("China");
        expected.setMobileNetworkOperator("China Mobile");
        expected.setRoamingStatus(ROAMING_STATUS_AWAY);
        expected.setLastCellLocation(TEST_HIERARCHY_1 + "-" + TEST_HIERARCHY_3 + "-" + TEST_VENDOR + "-1");
        expected.setLastRoutingArea(TEST_MCC + "-" + TEST_MNC + "-" + TEST_LAC + "-" + TEST_RAC);
        expected.setLastObservedSGSN("MME2");
        expected.setFirstObserved(SGEH_FIRST_SEEN_RAW + ".0");
        expected.setLastObserved(SGEH_LAST_SEEN_IMSI_RAW + ".0");
        expected.setLastObservedPTMSI(TEST_PTMSI);

        assertEquals(expected, result);
    }

    private void validateResultsSGEH_SucRawDisabled_Invalid_IMSI_Mcc_Mnc(final SubBIDetailsResult result) {
        final SubBIDetailsResult expected = new SubBIDetailsResult();
        expected.setVipStatus("1");
        expected.setMsisdn(Long.toString(TEST_MSISDN_FOR_INVALID_IMSI_MCC_MNC));
        expected.setHomeCountry("");
        expected.setMobileNetworkOperator("");
        expected.setRoamingStatus(ROAMING_STATUS_AWAY);
        expected.setLastCellLocation(TEST_HIERARCHY_1 + "-" + TEST_HIERARCHY_3 + "-" + TEST_VENDOR + "-1");
        expected.setLastRoutingArea(TEST_MCC + "-" + TEST_MNC + "-" + TEST_LAC + "-" + TEST_RAC);
        expected.setLastObservedSGSN("MME2");
        expected.setFirstObserved(SGEH_FIRST_SEEN_RAW + ".0");
        expected.setLastObserved(SGEH_LAST_SEEN_IMSI_RAW + ".0");
        expected.setLastObservedPTMSI(TEST_PTMSI);

        assertEquals(expected, result);
    }

    private void validateResultsLTE_SucRawDisabled(final SubBIDetailsResult result) {

        final SubBIDetailsResult expected = new SubBIDetailsResult();
        expected.setVipStatus("1");
        expected.setMsisdn(Long.toString(TEST_MSISDN));
        expected.setHomeCountry("China");
        expected.setMobileNetworkOperator("China Mobile");
        expected.setRoamingStatus(ROAMING_STATUS_AWAY);
        expected.setLastCellLocation(TEST_HIERARCHY_1 + "-" + TEST_HIERARCHY_3 + "-" + TEST_VENDOR + "-2");
        expected.setLastRoutingArea(TEST_MCC + "-" + TEST_MNC + "-" + TEST_TRAC_LTE);
        expected.setLastObservedSGSN("MME2");
        expected.setFirstObserved(LTE_FIRST_SEEN_RAW + ".0");
        expected.setLastObserved(LTE_LAST_SEEN_IMSI_RAW + ".0");
        expected.setLastObservedPTMSI("");

        assertEquals(expected, result);
    }

    private void validateResultsPTMSI_SucRawDisabled(final SubBIDetailsResult result) {
        final SubBIDetailsResult expected = new SubBIDetailsResult();
        expected.setVipStatus(null);
        expected.setHomeCountry("China");
        expected.setMobileNetworkOperator("China Mobile");
        expected.setRoamingStatus(ROAMING_STATUS_AWAY);
        expected.setLastCellLocation(TEST_HIERARCHY_1 + "-" + TEST_HIERARCHY_3 + "-" + TEST_VENDOR + "-1");
        expected.setLastRoutingArea(TEST_MCC + "-" + TEST_MNC + "-" + TEST_LAC + "-" + TEST_RAC);
        expected.setLastObservedSGSN("MME2");
        expected.setFirstObserved(SGEH_FIRST_SEEN_RAW + ".0");
        expected.setLastObserved(SGEH_LAST_SEEN_IMSI_RAW + ".0");
        expected.setLastObservedPTMSI(TEST_PTMSI);

        assertEquals(expected, result);
    }
}
