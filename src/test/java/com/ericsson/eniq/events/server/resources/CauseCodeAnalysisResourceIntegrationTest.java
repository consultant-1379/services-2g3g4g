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

package com.ericsson.eniq.events.server.resources;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.common.MessageConstants.*;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.*;
import javax.ws.rs.core.MultivaluedMap;
import org.junit.Test;
import com.sun.jersey.core.util.MultivaluedMapImpl;

public class CauseCodeAnalysisResourceIntegrationTest extends DataServiceBaseTestCase {

    private MultivaluedMap<String, String> map;

    private CauseCodeAnalysisResource causeCodeAnalysisResource;

    private final String time = "120";

    private final String SGSN = "SGSN1";

    private final String BSC = "BSC191";

    private final String VENDOR = "ERICSSON";

    private final String APN = "blackberry.net";

    private final String APN_GROUP = "apn_tester";

    private final String CELL = "CELL38000";

    private final String CAUSE_CODE = "65";

    private final String SUB_CAUSE_CODE = "0";

    private final String CAUSE_CODE_VALUE = "30";

    private final String SUB_CAUSE_CODE_VALUE = "101";

    private final String CAUSE_PROT_VALUE = "0";

    private final String TIME_FROM_QUERY_VALUE = "1015";

    private final String TIME_TO_QUERY_VALUE = "1030";

    private final String DATE_TO_AND_FROM_QUERY_VALUE = "11062010";

    private static final String DISPLAY_TYPE = GRID_PARAM;

    private static final String MAX_ROWS = "maxRows";

    private static final String MAX_ROWS_VALUE = "50";

    @Override
    public void onSetUp() {
        causeCodeAnalysisResource = new CauseCodeAnalysisResource();
        attachDependencies(causeCodeAnalysisResource);
        map = new MultivaluedMapImpl();
    }

    @Test
    public void testGetCauseCodeAnalysisDataByApnGroupAgg() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        map.putSingle(TYPE_PARAM, TYPE_APN);
        map.putSingle(TIME_QUERY_PARAM, "10080");
        map.putSingle(GROUP_NAME_PARAM, APN_GROUP);
        map.putSingle(TZ_OFFSET, "+0100");

        assertJSONSucceeds(causeCodeAnalysisResource.getData("requestID", map));
    }

    @Test
    public void testGetCauseCodeAnalysisDataByApnGroupRaw() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        map.putSingle(TYPE_PARAM, TYPE_APN);
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(GROUP_NAME_PARAM, APN_GROUP);
        map.putSingle(TZ_OFFSET, "+0100");

        assertJSONSucceeds(causeCodeAnalysisResource.getData("requestID", map));
    }

    @Test
    public void testGetCauseCodeAnalysisDetail() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        map.putSingle(TYPE_PARAM, TYPE_APN);
        map.putSingle(TIME_QUERY_PARAM, ONE_WEEK);
        map.putSingle(APN_PARAM, APN);
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(CAUSE_CODE_PARAM, CAUSE_CODE);
        map.putSingle(SUB_CAUSE_CODE_PARAM, SUB_CAUSE_CODE);
        map.putSingle(CAUSE_PROT_TYPE, "0");
        map.putSingle(TZ_OFFSET, "+0100");
        assertJSONSucceeds(causeCodeAnalysisResource.getData("requestID", map));
    }

    @Test
    public void testGetCauseCodeAnalysisDetailSGSN() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        map.putSingle(TYPE_PARAM, TYPE_SGSN);
        map.putSingle(TIME_QUERY_PARAM, ONE_WEEK);
        map.putSingle(SGSN_PARAM, SGSN);
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(CAUSE_CODE_PARAM, CAUSE_CODE);
        map.putSingle(SUB_CAUSE_CODE_PARAM, SUB_CAUSE_CODE);
        map.putSingle(CAUSE_PROT_TYPE, "0");
        map.putSingle(TZ_OFFSET, "+0100");
        assertJSONSucceeds(causeCodeAnalysisResource.getData("requestID", map));
    }

    @Test
    public void testGetCauseCodeAnalysisDetailBSC() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        map.putSingle(TYPE_PARAM, TYPE_BSC);
        map.putSingle(TIME_QUERY_PARAM, "120");
        map.putSingle(BSC_PARAM, "BSC200");
        map.putSingle(VENDOR_PARAM, VENDOR);
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(CAUSE_CODE_PARAM, "30");
        map.putSingle(SUB_CAUSE_CODE_PARAM, "101");
        map.putSingle(CAUSE_PROT_TYPE, "0");
        map.putSingle(RAT_PARAM, "1");
        map.putSingle(TZ_OFFSET, "+0100");

        assertJSONSucceeds(causeCodeAnalysisResource.getData("requestID", map));
    }

    @Test
    public void testGetCauseCodeAnalysisDetailCell() throws Exception {
        map.clear();
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        map.putSingle(TYPE_PARAM, TYPE_CELL);
        map.putSingle(TIME_QUERY_PARAM, time);
        map.putSingle(VENDOR_PARAM, VENDOR);
        map.putSingle(CELL_PARAM, CELL);
        map.putSingle(BSC_PARAM, BSC);
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(CAUSE_CODE_PARAM, "30");
        map.putSingle(SUB_CAUSE_CODE_PARAM, "101");
        map.putSingle(CAUSE_PROT_TYPE, "0");
        map.putSingle(RAT_PARAM, "1");
        map.putSingle(TZ_OFFSET, "+0100");

        assertJSONSucceeds(causeCodeAnalysisResource.getData("requestID", map));
    }

    @Test
    public void testNoSuchDisplayType() throws Exception {
        final String invalidDisplayType = "error";

        map.clear();
        map.putSingle(DISPLAY_PARAM, invalidDisplayType);
        map.putSingle(TYPE_PARAM, TYPE_APN);
        map.putSingle(TIME_QUERY_PARAM, time);
        map.putSingle(APN_PARAM, APN);
        map.putSingle(TZ_OFFSET, "+0100");

        final String result = causeCodeAnalysisResource.getData("requestID", map);
        assertJSONErrorResult(result);
        assertResultContains(result, E_NO_SUCH_DISPLAY_TYPE);
    }

    @Test
    public void testCauseCodeGroupDrilldown() throws Exception {
        map.clear();
        map.putSingle(TYPE_PARAM, TYPE_BSC);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(CAUSE_CODE_PARAM, "30");
        map.putSingle(SUB_CAUSE_CODE_PARAM, "101");

        map.putSingle(GROUP_NAME_PARAM, "bscgroup-1");

        map.putSingle(TIME_FROM_QUERY_PARAM, "1015");
        map.putSingle(TIME_TO_QUERY_PARAM, "1030");
        map.putSingle(DATE_FROM_QUERY_PARAM, "11062010");
        map.putSingle(DATE_TO_QUERY_PARAM, "11062010");
        map.putSingle(CAUSE_PROT_TYPE, "0");
        map.putSingle(TZ_OFFSET, "+0100");

        assertJSONSucceeds(causeCodeAnalysisResource.getData("requestID", map));
    }

    @Test
    public void testCauseCodeGroupDrilldownSGSNMME() throws Exception {
        map.clear();
        map.putSingle(TYPE_PARAM, TYPE_SGSN);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(CAUSE_CODE_PARAM, CAUSE_CODE_VALUE);
        map.putSingle(SUB_CAUSE_CODE_PARAM, SUB_CAUSE_CODE_VALUE);
        map.putSingle(GROUP_NAME_PARAM, SAMPLE_SGSN_GROUP);
        map.putSingle(TIME_FROM_QUERY_PARAM, TIME_FROM_QUERY_VALUE);
        map.putSingle(TIME_TO_QUERY_PARAM, TIME_TO_QUERY_VALUE);
        map.putSingle(DATE_FROM_QUERY_PARAM, DATE_TO_AND_FROM_QUERY_VALUE);
        map.putSingle(DATE_TO_QUERY_PARAM, DATE_TO_AND_FROM_QUERY_VALUE);
        map.putSingle(CAUSE_PROT_TYPE, CAUSE_PROT_VALUE);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);

        assertJSONSucceeds(causeCodeAnalysisResource.getData("requestID", map));
    }

    @Test
    public void testCauseCodeGroupDrilldownAccessArea() throws Exception {
        map.clear();
        map.putSingle(TYPE_PARAM, TYPE_CELL);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(KEY_PARAM, KEY_TYPE_ERR);
        map.putSingle(CAUSE_CODE_PARAM, CAUSE_CODE_VALUE);
        map.putSingle(SUB_CAUSE_CODE_PARAM, SUB_CAUSE_CODE_VALUE);
        map.putSingle(GROUP_NAME_PARAM, SAMPLE_CELL_GROUP);
        map.putSingle(TIME_FROM_QUERY_PARAM, TIME_FROM_QUERY_VALUE);
        map.putSingle(TIME_TO_QUERY_PARAM, TIME_TO_QUERY_VALUE);
        map.putSingle(DATE_FROM_QUERY_PARAM, DATE_TO_AND_FROM_QUERY_VALUE);
        map.putSingle(DATE_TO_QUERY_PARAM, DATE_TO_AND_FROM_QUERY_VALUE);
        map.putSingle(CAUSE_PROT_TYPE, CAUSE_PROT_VALUE);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);

        assertJSONSucceeds(causeCodeAnalysisResource.getData("requestID", map));
    }

    @Test
    public void testCheckValidBSC() throws Exception {
        map.clear();
        map.putSingle(TYPE_PARAM, TYPE_BSC);
        map.putSingle(NODE_PARAM, "dfasdaf");
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(TIME_QUERY_PARAM, "30");
        map.putSingle(TZ_OFFSET, "+0100");
        final String result = causeCodeAnalysisResource.getData("requestID", map);
        assertJSONErrorResult(result);
    }

    @Test
    public void testGetTzOffsetForCSV() {
        String tzOffsetQuery = causeCodeAnalysisResource.getTzOffsetForCSV("00530");
        assertEquals("TZ Offset for CSV Queries", "0330", tzOffsetQuery);
    }
}
