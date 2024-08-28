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
package com.ericsson.eniq.events.server.integritytests.dtput;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.integritytests.stubs.ReplaceTablesWithTempTablesTemplateUtils.*;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.*;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.*;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.sql.SQLException;
import java.util.*;

import javax.ws.rs.core.MultivaluedMap;

import org.junit.Test;

import com.ericsson.eniq.events.server.resources.TestsWithTemporaryTablesBaseTestCase;
import com.ericsson.eniq.events.server.resources.dtput.DatavolRankingResource;
import com.ericsson.eniq.events.server.test.queryresults.DatavolRankingResult;
import com.ericsson.eniq.events.server.test.stubs.DummyUriInfoImpl;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;
import com.sun.jersey.core.util.MultivaluedMapImpl;

public class DatavolRankingResourceWithPreparedTablesTest extends TestsWithTemporaryTablesBaseTestCase<DatavolRankingResult> {
    private MultivaluedMap<String, String> map;

    private DatavolRankingResource datavolRankingResource;

    private static final String DISPLAY_TYPE = GRID_PARAM;

    private static final String MAX_ROWS_VALUE = "500";

    private static final long MEGABYTE = 1024L * 1024L;

    private static final String APN1 = "apn1";

    private static final int KILOBYTEUNIT = 1024;

    private static final String APN2 = "apn2";

    private static final String APN3 = "apn3";

    private static final String APN_GROUP_NAME1 = "apnGroup1";

    private static final String APN_GROUP_NAME2 = "apnGroup2";

    private static final String APN_GROUP_NAME3 = "apnGroup3";

    private static final long IMSI1 = 12300000000L;

    private static final long IMSI2 = 22900000000L;

    private static final long IMSI3 = 29110000000L;

    private static final String IMSI_GROUP_NAME1 = "imsiGroup1";

    private static final String IMSI_GROUP_NAME2 = "imsiGroup2";

    private static final String IMSI_GROUP_NAME3 = "imsiGroup3";

    private static final long TAC1 = 30000000L;

    private static final long TAC2 = 40000000L;

    private static final long TAC3 = 50000000L;

    private static final String TAC_GROUP_NAME1 = "tacGroup1";

    private static final String TAC_GROUP_NAME2 = "tacGroup2";

    private static final String TAC_GROUP_NAME3 = "tacGroup3";

    private static final String EVENT_SOURCE_NAME1 = "ggsn1";

    private static final String EVENT_SOURCE_NAME2 = "ggsn2";

    private static final String EVENT_SOURCE_NAME3 = "ggsn3";

    private static final String MANUFACTURER1 = "Manufacturer1";

    private static final String MANUFACTURER2 = "Manufacturer2";

    private static final String MANUFACTURER3 = "Manufacturer3";

    private static final String MARKETING_NAME1 = "Marketing_Name1";

    private static final String MARKETING_NAME2 = "Marketing_Name2";

    private static final String MARKETING_NAME3 = "Marketing_Name3";

    private static final String BAND1 = "BAND1";

    private static final String BAND2 = "BAND2";

    private static final String BAND3 = "BAND3";

    private static final String TEST_APN_01 = APN1;

    private static final long TEST_IMSI_01 = IMSI1;

    private static final String TEST_IMSI_GROUP_NAME_01 = IMSI_GROUP_NAME1;

    private static final long TEST_TAC_01 = TAC1;

    private static final String TEST_EVENT_SOURCE_NAME_01 = EVENT_SOURCE_NAME1;

    private static final String LAST_SEEN_DATE = DateTimeUtilities.getDateTime(DATE_TIME_FORMAT, Calendar.DATE, -10);

    private static final double TEST_DL_01 = 1000L;

    private static final double TEST_UL_01 = 3000L;

    private static final String TEST_DATETIME_ID_01 = DateTimeUtilities.getDateTime(DATE_TIME_FORMAT, Calendar.MINUTE, -45);

    private static final String TEST_DATETIME_ID_DAY_01 = DateTimeUtilities.getDateTime(DATE_TIME_FORMAT, Calendar.DATE, -1);

    private static final String TEST_APN_02 = APN1;

    private static final long TEST_IMSI_02 = IMSI1;

    private static final String TEST_IMSI_GROUP_NAME_02 = IMSI_GROUP_NAME1;

    private static final long TEST_TAC_02 = TAC1;

    private static final String TEST_EVENT_SOURCE_NAME_02 = EVENT_SOURCE_NAME1;

    private static final double TEST_DL_02 = 2000L;

    private static final double TEST_UL_02 = 4000L;

    private static final String TEST_DATETIME_ID_02 = DateTimeUtilities.getDateTime(DATE_TIME_FORMAT, Calendar.MINUTE, -46);

    private static final String TEST_DATETIME_ID_DAY_02 = DateTimeUtilities.getDateTime(DATE_TIME_FORMAT, Calendar.DATE, -2);

    private static final String TEST_APN_03 = APN2;

    private static final long TEST_IMSI_03 = IMSI2;

    private static final String TEST_IMSI_GROUP_NAME_03 = IMSI_GROUP_NAME2;

    private static final long TEST_TAC_03 = TAC2;

    private static final String TEST_EVENT_SOURCE_NAME_03 = EVENT_SOURCE_NAME2;

    private static final double TEST_DL_03 = 2400L;

    private static final double TEST_UL_03 = 4200L;

    private static final String TEST_DATETIME_ID_03 = DateTimeUtilities.getDateTime(DATE_TIME_FORMAT, Calendar.MINUTE, -47);

    private static final String TEST_DATETIME_ID_DAY_03 = DateTimeUtilities.getDateTime(DATE_TIME_FORMAT, Calendar.DATE, -3);

    private static final String TEST_APN_04 = APN2;

    private static final long TEST_IMSI_04 = IMSI2;

    private static final String TEST_IMSI_GROUP_NAME_04 = IMSI_GROUP_NAME2;

    private static final long TEST_TAC_04 = TAC2;

    private static final String TEST_EVENT_SOURCE_NAME_04 = EVENT_SOURCE_NAME2;

    private static final double TEST_DL_04 = 2200L;

    private static final double TEST_UL_04 = 3200L;

    private static final String TEST_DATETIME_ID_04 = DateTimeUtilities.getDateTime(DATE_TIME_FORMAT, Calendar.MINUTE, -48);

    private static final String TEST_DATETIME_ID_DAY_04 = DateTimeUtilities.getDateTime(DATE_TIME_FORMAT, Calendar.DATE, -4);

    private static final String TEST_APN_05 = APN3;

    private static final long TEST_IMSI_05 = IMSI3;

    private static final String TEST_IMSI_GROUP_NAME_05 = IMSI_GROUP_NAME3;

    private static final long TEST_TAC_05 = TAC3;

    private static final String TEST_EVENT_SOURCE_NAME_05 = EVENT_SOURCE_NAME3;

    private static final double TEST_DL_05 = 2400L;

    private static final double TEST_UL_05 = 4200L;

    private static final String TEST_DATETIME_ID_05 = DateTimeUtilities.getDateTime(DATE_TIME_FORMAT, Calendar.MINUTE, -49);

    private static final String TEST_DATETIME_ID_DAY_05 = DateTimeUtilities.getDateTime(DATE_TIME_FORMAT, Calendar.DATE, -5);

    private final static int EXPECTED_TOTAL_RESULTS = 3;

    private final static double EXPECTED_TOTAL_DL_APN1 = TEST_DL_01 + TEST_DL_02;

    private final static double EXPECTED_TOTAL_UL_APN1 = TEST_UL_01 + TEST_UL_02;

    private final static double EXPECTED_TOTAL_DATAVOLUME_APN1 = EXPECTED_TOTAL_DL_APN1 + EXPECTED_TOTAL_UL_APN1;

    private final static double EXPECTED_TOTAL_DL_APN2 = TEST_DL_03 + TEST_DL_04;

    private final static double EXPECTED_TOTAL_UL_APN2 = TEST_UL_03 + TEST_UL_04;

    private final static double EXPECTED_TOTAL_DATAVOLUME_APN2 = EXPECTED_TOTAL_DL_APN2 + EXPECTED_TOTAL_UL_APN2;

    private final static double EXPECTED_TOTAL_DL_APN3 = TEST_DL_05;

    private final static double EXPECTED_TOTAL_UL_APN3 = TEST_UL_05;

    private final static double EXPECTED_TOTAL_DATAVOLUME_APN3 = EXPECTED_TOTAL_DL_APN3 + EXPECTED_TOTAL_UL_APN3;

    private final static double EXPECTED_TOTAL_DL_APN_GROUP1 = TEST_DL_01 + TEST_DL_02;

    private final static double EXPECTED_TOTAL_UL_APN_GROUP1 = TEST_UL_01 + TEST_UL_02;

    private final static double EXPECTED_TOTAL_DATAVOLUME_APN_GROUP1 = EXPECTED_TOTAL_DL_APN_GROUP1 + EXPECTED_TOTAL_UL_APN_GROUP1;

    private final static double EXPECTED_TOTAL_DL_APN_GROUP2 = TEST_DL_03 + TEST_DL_04;

    private final static double EXPECTED_TOTAL_UL_APN_GROUP2 = TEST_UL_03 + TEST_UL_04;

    private final static double EXPECTED_TOTAL_DATAVOLUME_APN_GROUP2 = EXPECTED_TOTAL_DL_APN_GROUP2 + EXPECTED_TOTAL_UL_APN_GROUP2;

    private final static double EXPECTED_TOTAL_DL_APN_GROUP3 = TEST_DL_05;

    private final static double EXPECTED_TOTAL_UL_APN_GROUP3 = TEST_UL_05;

    private final static double EXPECTED_TOTAL_DATAVOLUME_APN_GROUP3 = EXPECTED_TOTAL_DL_APN_GROUP3 + EXPECTED_TOTAL_UL_APN_GROUP3;

    private final static double EXPECTED_TOTAL_DL_IMSI1 = (TEST_DL_01 + TEST_DL_02) * KILOBYTEUNIT;

    private final static double EXPECTED_TOTAL_UL_IMSI1 = (TEST_UL_01 + TEST_UL_02) * KILOBYTEUNIT;

    private final static double EXPECTED_TOTAL_DATAVOLUME_IMSI1 = EXPECTED_TOTAL_DL_IMSI1 + EXPECTED_TOTAL_UL_IMSI1;

    private final static double EXPECTED_TOTAL_DL_IMSI2 = (TEST_DL_03 + TEST_DL_04) * KILOBYTEUNIT;

    private final static double EXPECTED_TOTAL_UL_IMSI2 = (TEST_UL_03 + TEST_UL_04) * KILOBYTEUNIT;

    private final static double EXPECTED_TOTAL_DATAVOLUME_IMSI2 = EXPECTED_TOTAL_DL_IMSI2 + EXPECTED_TOTAL_UL_IMSI2;

    private final static double EXPECTED_TOTAL_DL_IMSI3 = TEST_DL_05 * KILOBYTEUNIT;

    private final static double EXPECTED_TOTAL_UL_IMSI3 = TEST_UL_05 * KILOBYTEUNIT;

    private final static double EXPECTED_TOTAL_DATAVOLUME_IMSI3 = EXPECTED_TOTAL_DL_IMSI3 + EXPECTED_TOTAL_UL_IMSI3;

    private final static double EXPECTED_TOTAL_DL_IMSI_GROUP1 = (TEST_DL_01 + TEST_DL_02) * KILOBYTEUNIT;

    private final static double EXPECTED_TOTAL_UL_IMSI_GROUP1 = (TEST_UL_01 + TEST_UL_02) * KILOBYTEUNIT;

    private final static double EXPECTED_TOTAL_DATAVOLUME_IMSI_GROUP1 = EXPECTED_TOTAL_DL_IMSI_GROUP1 + EXPECTED_TOTAL_UL_IMSI_GROUP1;

    private final static double EXPECTED_TOTAL_DL_IMSI_GROUP2 = (TEST_DL_03 + TEST_DL_04) * KILOBYTEUNIT;

    private final static double EXPECTED_TOTAL_UL_IMSI_GROUP2 = (TEST_UL_03 + TEST_UL_04) * KILOBYTEUNIT;

    private final static double EXPECTED_TOTAL_DATAVOLUME_IMSI_GROUP2 = EXPECTED_TOTAL_DL_IMSI_GROUP2 + EXPECTED_TOTAL_UL_IMSI_GROUP2;

    private final static double EXPECTED_TOTAL_DL_IMSI_GROUP3 = TEST_DL_05 * KILOBYTEUNIT;

    private final static double EXPECTED_TOTAL_UL_IMSI_GROUP3 = TEST_UL_05 * KILOBYTEUNIT;

    private final static double EXPECTED_TOTAL_DATAVOLUME_IMSI_GROUP3 = EXPECTED_TOTAL_DL_IMSI_GROUP3 + EXPECTED_TOTAL_UL_IMSI_GROUP3;

    private final static double EXPECTED_TOTAL_DL_TAC1 = TEST_DL_01 + TEST_DL_02;

    private final static double EXPECTED_TOTAL_UL_TAC1 = TEST_UL_01 + TEST_UL_02;

    private final static double EXPECTED_TOTAL_DATAVOLUME_TAC1 = EXPECTED_TOTAL_DL_TAC1 + EXPECTED_TOTAL_UL_TAC1;

    private final static double EXPECTED_TOTAL_DL_TAC2 = TEST_DL_03 + TEST_DL_04;

    private final static double EXPECTED_TOTAL_UL_TAC2 = TEST_UL_03 + TEST_UL_04;

    private final static double EXPECTED_TOTAL_DATAVOLUME_TAC2 = EXPECTED_TOTAL_DL_TAC2 + EXPECTED_TOTAL_UL_TAC2;

    private final static double EXPECTED_TOTAL_DL_TAC3 = TEST_DL_05;

    private final static double EXPECTED_TOTAL_UL_TAC3 = TEST_UL_05;

    private final static double EXPECTED_TOTAL_DATAVOLUME_TAC3 = EXPECTED_TOTAL_DL_TAC3 + EXPECTED_TOTAL_UL_TAC3;

    private final static double EXPECTED_TOTAL_DL_TAC_GROUP1 = TEST_DL_01 + TEST_DL_02;

    private final static double EXPECTED_TOTAL_UL_TAC_GROUP1 = TEST_UL_01 + TEST_UL_02;

    private final static double EXPECTED_TOTAL_DATAVOLUME_TAC_GROUP1 = EXPECTED_TOTAL_DL_TAC_GROUP1 + EXPECTED_TOTAL_UL_TAC_GROUP1;

    private final static double EXPECTED_TOTAL_DL_TAC_GROUP2 = TEST_DL_03 + TEST_DL_04;

    private final static double EXPECTED_TOTAL_UL_TAC_GROUP2 = TEST_UL_03 + TEST_UL_04;

    private final static double EXPECTED_TOTAL_DATAVOLUME_TAC_GROUP2 = EXPECTED_TOTAL_DL_TAC_GROUP2 + EXPECTED_TOTAL_UL_TAC_GROUP2;

    private final static double EXPECTED_TOTAL_DL_TAC_GROUP3 = TEST_DL_05;

    private final static double EXPECTED_TOTAL_UL_TAC_GROUP3 = TEST_UL_05;

    private final static double EXPECTED_TOTAL_DATAVOLUME_TAC_GROUP3 = EXPECTED_TOTAL_DL_TAC_GROUP3 + EXPECTED_TOTAL_UL_TAC_GROUP3;

    private final static double EXPECTED_TOTAL_DL_EVENT_SOURCE_NAME1 = TEST_DL_01 + TEST_DL_02;

    private final static double EXPECTED_TOTAL_UL_EVENT_SOURCE_NAME1 = TEST_UL_01 + TEST_UL_02;

    private final static double EXPECTED_TOTAL_DATAVOLUME_EVENT_SOURCE_NAME1 = EXPECTED_TOTAL_DL_EVENT_SOURCE_NAME1
            + EXPECTED_TOTAL_UL_EVENT_SOURCE_NAME1;

    private final static double EXPECTED_TOTAL_DL_EVENT_SOURCE_NAME2 = TEST_DL_03 + TEST_DL_04;

    private final static double EXPECTED_TOTAL_UL_EVENT_SOURCE_NAME2 = TEST_UL_03 + TEST_UL_04;

    private final static double EXPECTED_TOTAL_DATAVOLUME_EVENT_SOURCE_NAME2 = EXPECTED_TOTAL_DL_EVENT_SOURCE_NAME2
            + EXPECTED_TOTAL_UL_EVENT_SOURCE_NAME2;

    private final static double EXPECTED_TOTAL_DL_EVENT_SOURCE_NAME3 = TEST_DL_05;

    private final static double EXPECTED_TOTAL_UL_EVENT_SOURCE_NAME3 = TEST_UL_05;

    private final static double EXPECTED_TOTAL_DATAVOLUME_EVENT_SOURCE_NAME3 = EXPECTED_TOTAL_DL_EVENT_SOURCE_NAME3
            + EXPECTED_TOTAL_UL_EVENT_SOURCE_NAME3;

    private final static int EXPECTED_APN1_RANK = 2;
    private final static int EXPECTED_APN2_RANK = 1;
    private final static int EXPECTED_APN3_RANK = 3;

    private final static int EXPECTED_APN_GROUP1_RANK = 2;
    private final static int EXPECTED_APN_GROUP2_RANK = 1;
    private final static int EXPECTED_APN_GROUP3_RANK = 3;

    private final static int EXPECTED_IMSI1_RANK = 2;
    private final static int EXPECTED_IMSI2_RANK = 1;
    private final static int EXPECTED_IMSI3_RANK = 3;

    private final static int EXPECTED_IMSI_GROUP1_RANK = 2;
    private final static int EXPECTED_IMSI_GROUP2_RANK = 1;
    private final static int EXPECTED_IMSI_GROUP3_RANK = 3;

    private final static int EXPECTED_TAC1_RANK = 2;
    private final static int EXPECTED_TAC2_RANK = 1;
    private final static int EXPECTED_TAC3_RANK = 3;

    private final static int EXPECTED_TAC_GROUP1_RANK = 2;
    private final static int EXPECTED_TAC_GROUP2_RANK = 1;
    private final static int EXPECTED_TAC_GROUP3_RANK = 3;

    private final static int EXPECTED_EVENT_SOURCE_NAME1_RANK = 2;
    private final static int EXPECTED_EVENT_SOURCE_NAME2_RANK = 1;
    private final static int EXPECTED_EVENT_SOURCE_NAME3_RANK = 3;

    private static final String EXPECTED_TAC1_MANUFACTURER = MANUFACTURER1;

    private static final String EXPECTED_TAC2_MANUFACTURER = MANUFACTURER2;

    private static final String EXPECTED_TAC3_MANUFACTURER = MANUFACTURER3;

    private static final String EXPECTED_TAC1_MARKETING_NAME = MARKETING_NAME1;

    private static final String EXPECTED_TAC2_MARKETING_NAME = MARKETING_NAME2;

    private static final String EXPECTED_TAC3_MARKETING_NAME = MARKETING_NAME3;

    private static final String PATH = "NETWORK/DATAVOL_RANKING_ANALYSIS";

    private static final String PATH_GROUP = DATAVOL_GROUP_RANKING_ANALYSIS;

    Map<String, Object> tableValues = new HashMap<String, Object>();

    @Override
    public void setUpForTest() throws Exception {
        super.setUpForTest();
        datavolRankingResource = new DatavolRankingResource();

        createAndPopulateLookupTable(TEMP_DIM_E_SGEH_APN);
        createAndPopulateLookupTable(TEMP_DIM_E_SGEH_TAC);

        createGroupTypeETables(APN, TEMP_GROUP_TYPE_E_APN);
        createGroupTypeETables(IMSI, TEMP_GROUP_TYPE_E_IMSI);
        addTableNameToReplace(DIM_E_SGEH_TAC, TEMP_DIM_E_SGEH_TAC);

        createRawTables();
        createAggregationTables(APN, TEMP_EVENT_E_DVTP_DT_APN_15MIN, TEMP_EVENT_E_DVTP_DT_APN_DAY);
        createAggregationTables(IMSI, TEMP_EVENT_E_DVTP_DT_IMSI_RANK_15MIN, TEMP_EVENT_E_DVTP_DT_IMSI_RANK_DAY);
        createAggregationTables(GROUP_NAME, TEMP_EVENT_E_DVTP_DT_IMSI_GROUP_RANK_15MIN, TEMP_EVENT_E_DVTP_DT_IMSI_GROUP_RANK_DAY);
        createAggregationTables(TAC, TEMP_EVENT_E_DVTP_DT_TERM_15MIN, TEMP_EVENT_E_DVTP_DT_TERM_DAY);
        createAggregationTables(EVENT_SOURCE_NAME, TEMP_EVENT_E_DVTP_DT_GGSN_15MIN, TEMP_EVENT_E_DVTP_DT_GGSN_DAY);

        populateTemporaryTables();
        attachDependencies(datavolRankingResource);
        datavolRankingResource.setTechPackCXCMappingService(techPackCXCMappingService);
        map = new MultivaluedMapImpl();
        DummyUriInfoImpl.setUriInfo(map, datavolRankingResource);
    }

    @Test
    public void testDrillDownByAPNRaw1Hour() throws Exception {
        map = getRequestParameters(TYPE_APN, DISPLAY_TYPE, ONE_HOUR, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        validateResults(datavolRankingResource.getDatavolRankingResults(CANCEL_REQUEST_NOT_SUPPORTED, map, PATH));
    }

    @Test
    public void testDrillDownByAPNAggregation1Day() throws Exception {
        map = getRequestParameters(TYPE_APN, DISPLAY_TYPE, ONE_DAY, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        validateResults(datavolRankingResource.getDatavolRankingResults(CANCEL_REQUEST_NOT_SUPPORTED, map, PATH));
    }

    @Test
    public void testDrillDownByAPNAggregation1Week() throws Exception {
        map = getRequestParameters(TYPE_APN, DISPLAY_TYPE, ONE_WEEK, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        validateResults(datavolRankingResource.getDatavolRankingResults(CANCEL_REQUEST_NOT_SUPPORTED, map, PATH));
    }

    @Test
    public void testDrillDownByAPNCustomTime() throws Exception {
        map = getRequestParameters(TYPE_APN, DISPLAY_TYPE, THREE_DAY, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        validateResults(datavolRankingResource.getDatavolRankingResults(CANCEL_REQUEST_NOT_SUPPORTED, map, PATH));
    }

    @Test
    public void testDrillDownByAPNGroupRaw1Hour() throws Exception {
        map = getRequestParameters(TYPE_APN, DISPLAY_TYPE, ONE_HOUR, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        validateResults(datavolRankingResource.getDatavolRankingResults(CANCEL_REQUEST_NOT_SUPPORTED, map, PATH_GROUP));
    }

    @Test
    public void testDrillDownByAPNGroupAggregation1Day() throws Exception {
        map = getRequestParameters(TYPE_APN, DISPLAY_TYPE, ONE_DAY, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        validateResults(datavolRankingResource.getDatavolRankingResults(CANCEL_REQUEST_NOT_SUPPORTED, map, PATH_GROUP));
    }

    @Test
    public void testDrillDownByAPNGroupAggregation1Week() throws Exception {
        map = getRequestParameters(TYPE_APN, DISPLAY_TYPE, ONE_WEEK, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        validateResults(datavolRankingResource.getDatavolRankingResults(CANCEL_REQUEST_NOT_SUPPORTED, map, PATH_GROUP));
    }

    @Test
    public void testDrillDownByAPNGroupCustomTime() throws Exception {
        map = getRequestParameters(TYPE_APN, DISPLAY_TYPE, THREE_DAY, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        validateResults(datavolRankingResource.getDatavolRankingResults(CANCEL_REQUEST_NOT_SUPPORTED, map, PATH_GROUP));
    }

    @Test
    public void testDrillDownByIMSIRaw1Hour() throws Exception {
        map = getRequestParameters(TYPE_IMSI, DISPLAY_TYPE, ONE_HOUR, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        validateResults(datavolRankingResource.getDatavolRankingResults(CANCEL_REQUEST_NOT_SUPPORTED, map, PATH));
    }

    @Test
    public void testDrillDownByIMSIAggregation1Day() throws Exception {
        map = getRequestParameters(TYPE_IMSI, DISPLAY_TYPE, ONE_DAY, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        validateResults(datavolRankingResource.getDatavolRankingResults(CANCEL_REQUEST_NOT_SUPPORTED, map, PATH));
    }

    @Test
    public void testDrillDownByIMSIAggregation1Week() throws Exception {
        map = getRequestParameters(TYPE_IMSI, DISPLAY_TYPE, ONE_WEEK, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        validateResults(datavolRankingResource.getDatavolRankingResults(CANCEL_REQUEST_NOT_SUPPORTED, map, PATH));
    }

    @Test
    public void testDrillDownByIMSICustomTime() throws Exception {
        map = getRequestParameters(TYPE_IMSI, DISPLAY_TYPE, THREE_DAY, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        validateResults(datavolRankingResource.getDatavolRankingResults(CANCEL_REQUEST_NOT_SUPPORTED, map, PATH));
    }

    @Test
    public void testDrillDownByIMSIGroupRaw1Hour() throws Exception {
        map = getRequestParameters(TYPE_IMSI, DISPLAY_TYPE, ONE_HOUR, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        validateResults(datavolRankingResource.getDatavolRankingResults(CANCEL_REQUEST_NOT_SUPPORTED, map, PATH_GROUP));
    }

    @Test
    public void testDrillDownByIMSIGroupAggregation1Day() throws Exception {
        map = getRequestParameters(TYPE_IMSI, DISPLAY_TYPE, ONE_DAY, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        validateResults(datavolRankingResource.getDatavolRankingResults(CANCEL_REQUEST_NOT_SUPPORTED, map, PATH_GROUP));
    }

    @Test
    public void testDrillDownByIMSIGroupAggregation1Week() throws Exception {
        map = getRequestParameters(TYPE_IMSI, DISPLAY_TYPE, ONE_WEEK, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        validateResults(datavolRankingResource.getDatavolRankingResults(CANCEL_REQUEST_NOT_SUPPORTED, map, PATH_GROUP));
    }

    @Test
    public void testDrillDownByIMSIGroupCustomTime() throws Exception {
        map = getRequestParameters(TYPE_IMSI, DISPLAY_TYPE, THREE_DAY, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        validateResults(datavolRankingResource.getDatavolRankingResults(CANCEL_REQUEST_NOT_SUPPORTED, map, PATH_GROUP));
    }

    @Test
    public void testDrillDownByTACRaw1Hour() throws Exception {
        map = getRequestParameters(TYPE_TAC, DISPLAY_TYPE, ONE_HOUR, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        validateResults(datavolRankingResource.getDatavolRankingResults(CANCEL_REQUEST_NOT_SUPPORTED, map, PATH));
    }

    @Test
    public void testDrillDownByTACAggregation1Day() throws Exception {
        map = getRequestParameters(TYPE_TAC, DISPLAY_TYPE, ONE_DAY, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        validateResults(datavolRankingResource.getDatavolRankingResults(CANCEL_REQUEST_NOT_SUPPORTED, map, PATH));
    }

    @Test
    public void testDrillDownByTACAggregation1Week() throws Exception {
        map = getRequestParameters(TYPE_TAC, DISPLAY_TYPE, ONE_WEEK, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        validateResults(datavolRankingResource.getDatavolRankingResults(CANCEL_REQUEST_NOT_SUPPORTED, map, PATH));
    }

    @Test
    public void testDrillDownByTACCustomTime() throws Exception {
        map = getRequestParameters(TYPE_TAC, DISPLAY_TYPE, THREE_DAY, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        validateResults(datavolRankingResource.getDatavolRankingResults(CANCEL_REQUEST_NOT_SUPPORTED, map, PATH));
    }

    @Test
    public void testDrillDownByTACGroupRaw1Hour() throws Exception {
        map = getRequestParameters(TYPE_TAC, DISPLAY_TYPE, ONE_HOUR, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        validateResults(datavolRankingResource.getDatavolRankingResults(CANCEL_REQUEST_NOT_SUPPORTED, map, PATH_GROUP));
    }

    @Test
    public void testDrillDownByTACGroupAggregation1Day() throws Exception {
        map = getRequestParameters(TYPE_TAC, DISPLAY_TYPE, ONE_DAY, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        validateResults(datavolRankingResource.getDatavolRankingResults(CANCEL_REQUEST_NOT_SUPPORTED, map, PATH_GROUP));
    }

    @Test
    public void testDrillDownByTACGroupAggregation1Week() throws Exception {
        map = getRequestParameters(TYPE_TAC, DISPLAY_TYPE, ONE_WEEK, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        validateResults(datavolRankingResource.getDatavolRankingResults(CANCEL_REQUEST_NOT_SUPPORTED, map, PATH_GROUP));
    }

    @Test
    public void testDrillDownByTACGroupCustomTime() throws Exception {
        map = getRequestParameters(TYPE_TAC, DISPLAY_TYPE, THREE_DAY, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        validateResults(datavolRankingResource.getDatavolRankingResults(CANCEL_REQUEST_NOT_SUPPORTED, map, PATH_GROUP));
    }

    @Test
    public void testDrillDownByGGSNRaw1Hour() throws Exception {
        map = getRequestParameters(TYPE_GGSN, DISPLAY_TYPE, ONE_HOUR, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        validateResults(datavolRankingResource.getDatavolRankingResults(CANCEL_REQUEST_NOT_SUPPORTED, map, PATH));
    }

    @Test
    public void testDrillDownByGGSNAggregation1Day() throws Exception {
        map = getRequestParameters(TYPE_GGSN, DISPLAY_TYPE, ONE_DAY, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        validateResults(datavolRankingResource.getDatavolRankingResults(CANCEL_REQUEST_NOT_SUPPORTED, map, PATH));
    }

    @Test
    public void testDrillDownByGGSNAggregation1Week() throws Exception {
        map = getRequestParameters(TYPE_GGSN, DISPLAY_TYPE, ONE_WEEK, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        validateResults(datavolRankingResource.getDatavolRankingResults(CANCEL_REQUEST_NOT_SUPPORTED, map, PATH));
    }

    @Test
    public void testDrillDownByGGSNCustomTime() throws Exception {
        map = getRequestParameters(TYPE_GGSN, DISPLAY_TYPE, THREE_DAY, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        validateResults(datavolRankingResource.getDatavolRankingResults(CANCEL_REQUEST_NOT_SUPPORTED, map, PATH));
    }

    private void populateTemporaryTables() throws SQLException {
        populateDimTables();
        populateGroupTables();
        populateRawEventTables();
        populateAggregationEventTables();
    }

    private void validateResults(final String jsonResult) throws Exception {
        jsonAssertUtils.assertJSONSucceeds(jsonResult);
        final List<DatavolRankingResult> DatavolRankingResults = getTranslator().translateResult(jsonResult, DatavolRankingResult.class);
        assertThat(DatavolRankingResults.size(), is(EXPECTED_TOTAL_RESULTS));

        for (final DatavolRankingResult DatavolRankingResult : DatavolRankingResults) {
            validateResult(DatavolRankingResult);
        }
    }

    private void validateResult(final DatavolRankingResult DatavolRankingResult) {

        if (DatavolRankingResult.getAPN().equals(APN1)) {
            assertResult(DatavolRankingResult, EXPECTED_APN1_RANK, EXPECTED_TOTAL_DATAVOLUME_APN1, EXPECTED_TOTAL_DL_APN1, EXPECTED_TOTAL_UL_APN1);
        } else if (DatavolRankingResult.getAPN().equals(APN2)) {
            assertResult(DatavolRankingResult, EXPECTED_APN2_RANK, EXPECTED_TOTAL_DATAVOLUME_APN2, EXPECTED_TOTAL_DL_APN2, EXPECTED_TOTAL_UL_APN2);
        } else if (DatavolRankingResult.getAPN().equals(APN3)) {
            assertResult(DatavolRankingResult, EXPECTED_APN3_RANK, EXPECTED_TOTAL_DATAVOLUME_APN3, EXPECTED_TOTAL_DL_APN3, EXPECTED_TOTAL_UL_APN3);
        } else if (DatavolRankingResult.getAPNGroup().equals(APN_GROUP_NAME1)) {
            assertResult(DatavolRankingResult, EXPECTED_APN_GROUP1_RANK, EXPECTED_TOTAL_DATAVOLUME_APN_GROUP1, EXPECTED_TOTAL_DL_APN_GROUP1,
                    EXPECTED_TOTAL_UL_APN_GROUP1);
        } else if (DatavolRankingResult.getAPNGroup().equals(APN_GROUP_NAME2)) {
            assertResult(DatavolRankingResult, EXPECTED_APN_GROUP2_RANK, EXPECTED_TOTAL_DATAVOLUME_APN_GROUP2, EXPECTED_TOTAL_DL_APN_GROUP2,
                    EXPECTED_TOTAL_UL_APN_GROUP2);
        } else if (DatavolRankingResult.getAPNGroup().equals(APN_GROUP_NAME3)) {
            assertResult(DatavolRankingResult, EXPECTED_APN_GROUP3_RANK, EXPECTED_TOTAL_DATAVOLUME_APN_GROUP3, EXPECTED_TOTAL_DL_APN_GROUP3,
                    EXPECTED_TOTAL_UL_APN_GROUP3);
        } else if (DatavolRankingResult.getIMSI().equals(String.valueOf(IMSI1))) {
            assertResult(DatavolRankingResult, EXPECTED_IMSI1_RANK, EXPECTED_TOTAL_DATAVOLUME_IMSI1, EXPECTED_TOTAL_DL_IMSI1, EXPECTED_TOTAL_UL_IMSI1);
        } else if (DatavolRankingResult.getIMSI().equals(String.valueOf(IMSI2))) {
            assertResult(DatavolRankingResult, EXPECTED_IMSI2_RANK, EXPECTED_TOTAL_DATAVOLUME_IMSI2, EXPECTED_TOTAL_DL_IMSI2, EXPECTED_TOTAL_UL_IMSI2);
        } else if (DatavolRankingResult.getIMSI().equals(String.valueOf(IMSI3))) {
            assertResult(DatavolRankingResult, EXPECTED_IMSI3_RANK, EXPECTED_TOTAL_DATAVOLUME_IMSI3, EXPECTED_TOTAL_DL_IMSI3, EXPECTED_TOTAL_UL_IMSI3);
        } else if (DatavolRankingResult.getIMSIGroup().equals(IMSI_GROUP_NAME1)) {
            assertResult(DatavolRankingResult, EXPECTED_IMSI_GROUP1_RANK, EXPECTED_TOTAL_DATAVOLUME_IMSI_GROUP1, EXPECTED_TOTAL_DL_IMSI_GROUP1,
                    EXPECTED_TOTAL_UL_IMSI_GROUP1);
        } else if (DatavolRankingResult.getIMSIGroup().equals(IMSI_GROUP_NAME2)) {
            assertResult(DatavolRankingResult, EXPECTED_IMSI_GROUP2_RANK, EXPECTED_TOTAL_DATAVOLUME_IMSI_GROUP2, EXPECTED_TOTAL_DL_IMSI_GROUP2,
                    EXPECTED_TOTAL_UL_IMSI_GROUP2);
        } else if (DatavolRankingResult.getIMSIGroup().equals(IMSI_GROUP_NAME3)) {
            assertResult(DatavolRankingResult, EXPECTED_IMSI_GROUP3_RANK, EXPECTED_TOTAL_DATAVOLUME_IMSI_GROUP3, EXPECTED_TOTAL_DL_IMSI_GROUP3,
                    EXPECTED_TOTAL_UL_IMSI_GROUP3);
        } else if (DatavolRankingResult.getTAC().equals(String.valueOf(TAC1))) {
            assertResult(DatavolRankingResult, EXPECTED_TAC1_RANK, EXPECTED_TAC1_MANUFACTURER, EXPECTED_TAC1_MARKETING_NAME,
                    EXPECTED_TOTAL_DATAVOLUME_TAC1, EXPECTED_TOTAL_DL_TAC1, EXPECTED_TOTAL_UL_TAC1);
        } else if (DatavolRankingResult.getTAC().equals(String.valueOf(TAC2))) {
            assertResult(DatavolRankingResult, EXPECTED_TAC2_RANK, EXPECTED_TAC2_MANUFACTURER, EXPECTED_TAC2_MARKETING_NAME,
                    EXPECTED_TOTAL_DATAVOLUME_TAC2, EXPECTED_TOTAL_DL_TAC2, EXPECTED_TOTAL_UL_TAC2);
        } else if (DatavolRankingResult.getTAC().equals(String.valueOf(TAC3))) {
            assertResult(DatavolRankingResult, EXPECTED_TAC3_RANK, EXPECTED_TAC3_MANUFACTURER, EXPECTED_TAC3_MARKETING_NAME,
                    EXPECTED_TOTAL_DATAVOLUME_TAC3, EXPECTED_TOTAL_DL_TAC3, EXPECTED_TOTAL_UL_TAC3);
        } else if (DatavolRankingResult.getTACGroup().equals(TAC_GROUP_NAME1)) {
            assertResult(DatavolRankingResult, EXPECTED_TAC_GROUP1_RANK, EXPECTED_TOTAL_DATAVOLUME_TAC_GROUP1, EXPECTED_TOTAL_DL_TAC_GROUP1,
                    EXPECTED_TOTAL_UL_TAC_GROUP1);
        } else if (DatavolRankingResult.getTACGroup().equals(TAC_GROUP_NAME2)) {
            assertResult(DatavolRankingResult, EXPECTED_TAC_GROUP2_RANK, EXPECTED_TOTAL_DATAVOLUME_TAC_GROUP2, EXPECTED_TOTAL_DL_TAC_GROUP2,
                    EXPECTED_TOTAL_UL_TAC_GROUP2);
        } else if (DatavolRankingResult.getTACGroup().equals(TAC_GROUP_NAME3)) {
            assertResult(DatavolRankingResult, EXPECTED_TAC_GROUP3_RANK, EXPECTED_TOTAL_DATAVOLUME_TAC_GROUP3, EXPECTED_TOTAL_DL_TAC_GROUP3,
                    EXPECTED_TOTAL_UL_TAC_GROUP3);
        } else if (DatavolRankingResult.getEventSourceName().equals(EVENT_SOURCE_NAME1)) {
            assertResult(DatavolRankingResult, EXPECTED_EVENT_SOURCE_NAME1_RANK, EXPECTED_TOTAL_DATAVOLUME_EVENT_SOURCE_NAME1,
                    EXPECTED_TOTAL_DL_EVENT_SOURCE_NAME1, EXPECTED_TOTAL_UL_EVENT_SOURCE_NAME1);
        } else if (DatavolRankingResult.getEventSourceName().equals(EVENT_SOURCE_NAME2)) {
            assertResult(DatavolRankingResult, EXPECTED_EVENT_SOURCE_NAME2_RANK, EXPECTED_TOTAL_DATAVOLUME_EVENT_SOURCE_NAME2,
                    EXPECTED_TOTAL_DL_EVENT_SOURCE_NAME2, EXPECTED_TOTAL_UL_EVENT_SOURCE_NAME2);
        } else if (DatavolRankingResult.getEventSourceName().equals(EVENT_SOURCE_NAME3)) {
            assertResult(DatavolRankingResult, EXPECTED_EVENT_SOURCE_NAME3_RANK, EXPECTED_TOTAL_DATAVOLUME_EVENT_SOURCE_NAME3,
                    EXPECTED_TOTAL_DL_EVENT_SOURCE_NAME3, EXPECTED_TOTAL_UL_EVENT_SOURCE_NAME3);
        }
    }

    private void createGroupTypeETables(String type, String table) throws Exception {
        final Collection<String> groupTypeEColumns = new ArrayList<String>();
        groupTypeEColumns.add(type);
        groupTypeEColumns.add(GROUP_NAME);
        createTemporaryTable(table, groupTypeEColumns);
    }

    private void createRawTables() throws Exception {
        final Collection<String> dtRawColumns = new ArrayList<String>();
        dtRawColumns.add(DATAVOL_DL);
        dtRawColumns.add(DATAVOL_UL);
        dtRawColumns.add(APN);
        dtRawColumns.add(TAC);
        dtRawColumns.add(IMSI);
        dtRawColumns.add(EVENT_SOURCE_NAME);
        dtRawColumns.add(DATETIME_ID);
        createTemporaryTable(TEMP_EVENT_E_DVTP_DT_RAW, dtRawColumns);
    }

    private void createAggregationTables(String firstColumn, String table15Min, String tableDay) throws Exception {
        final Collection<String> dtAggregationColumns = new ArrayList<String>();

        dtAggregationColumns.add(firstColumn);
        dtAggregationColumns.add(DATAVOL_DL);
        dtAggregationColumns.add(DATAVOL_UL);
        dtAggregationColumns.add(DATETIME_ID);
        createTemporaryTable(table15Min, dtAggregationColumns);
        createTemporaryTable(tableDay, dtAggregationColumns);
        dtAggregationColumns.clear();
    }

    private void populateDimTables() throws SQLException {
        populateAPNDIMTable(APN1, LAST_SEEN_DATE);
        populateAPNDIMTable(APN2, LAST_SEEN_DATE);
        populateAPNDIMTable(APN3, LAST_SEEN_DATE);

        populateTACDIMTable(TAC1, MANUFACTURER1, MARKETING_NAME1, BAND1);
        populateTACDIMTable(TAC2, MANUFACTURER2, MARKETING_NAME2, BAND2);
        populateTACDIMTable(TAC3, MANUFACTURER3, MARKETING_NAME3, BAND3);
    }

    private void populateAPNDIMTable(String apn, String lastSeenDate) throws SQLException {
        tableValues.put(APN, apn);
        tableValues.put(LAST_SEEN, lastSeenDate);
        insertRow(TEMP_DIM_E_SGEH_APN, tableValues);
        tableValues.clear();
    }

    private void populateTACDIMTable(long tac, String manufacturer, String marketingName, String band) throws SQLException {
        tableValues.put(TAC, tac);
        tableValues.put(MANUFACTURER, manufacturer);
        tableValues.put(MARKETING_NAME, marketingName);
        tableValues.put(BAND, band);
        insertRow(TEMP_DIM_E_SGEH_TAC, tableValues);
        tableValues.clear();
    }

    private void populateGroupTables() throws SQLException {
        populateGroupTypeETable(APN, APN1, APN_GROUP_NAME1, TEMP_GROUP_TYPE_E_APN);
        populateGroupTypeETable(APN, APN2, APN_GROUP_NAME2, TEMP_GROUP_TYPE_E_APN);
        populateGroupTypeETable(APN, APN3, APN_GROUP_NAME3, TEMP_GROUP_TYPE_E_APN);

        populateGroupTypeETable(IMSI, IMSI1, IMSI_GROUP_NAME1, TEMP_GROUP_TYPE_E_IMSI);
        populateGroupTypeETable(IMSI, IMSI2, IMSI_GROUP_NAME2, TEMP_GROUP_TYPE_E_IMSI);
        populateGroupTypeETable(IMSI, IMSI3, IMSI_GROUP_NAME3, TEMP_GROUP_TYPE_E_IMSI);

        populateGroupTypeETable(TAC, TAC1, TAC_GROUP_NAME1, TEMP_GROUP_TYPE_E_TAC);
        populateGroupTypeETable(TAC, TAC2, TAC_GROUP_NAME2, TEMP_GROUP_TYPE_E_TAC);
        populateGroupTypeETable(TAC, TAC3, TAC_GROUP_NAME3, TEMP_GROUP_TYPE_E_TAC);
    }

    private void populateGroupTypeETable(String type, String value, String groupName, String tableGroup) throws SQLException {
        tableValues.put(type, value);
        tableValues.put(GROUP_NAME, groupName);
        insertRow(tableGroup, tableValues);
        tableValues.clear();
    }

    private void populateGroupTypeETable(String type, long value, String groupName, String tableGroup) throws SQLException {
        tableValues.put(type, value);
        tableValues.put(GROUP_NAME, groupName);
        insertRow(tableGroup, tableValues);
        tableValues.clear();
    }

    private void populateRawEventTables() throws SQLException {
        populateRawEventTable(TEST_APN_01, TEST_TAC_01, TEST_IMSI_01, TEST_EVENT_SOURCE_NAME_01, TEST_DL_01, TEST_UL_01, TEST_DATETIME_ID_01);
        populateRawEventTable(TEST_APN_02, TEST_TAC_02, TEST_IMSI_02, TEST_EVENT_SOURCE_NAME_02, TEST_DL_02, TEST_UL_02, TEST_DATETIME_ID_02);
        populateRawEventTable(TEST_APN_03, TEST_TAC_03, TEST_IMSI_03, TEST_EVENT_SOURCE_NAME_03, TEST_DL_03, TEST_UL_03, TEST_DATETIME_ID_03);
        populateRawEventTable(TEST_APN_04, TEST_TAC_04, TEST_IMSI_04, TEST_EVENT_SOURCE_NAME_04, TEST_DL_04, TEST_UL_04, TEST_DATETIME_ID_04);
        populateRawEventTable(TEST_APN_05, TEST_TAC_05, TEST_IMSI_05, TEST_EVENT_SOURCE_NAME_05, TEST_DL_05, TEST_UL_05, TEST_DATETIME_ID_05);
    }

    private void populateRawEventTable(String apn, long tac, long imsi, String eventSourceName, double dl, double ul, String date)
            throws SQLException {
        tableValues.put(APN, apn);
        tableValues.put(TAC, tac);
        tableValues.put(IMSI, imsi);
        tableValues.put(EVENT_SOURCE_NAME, eventSourceName);
        tableValues.put(DATAVOL_DL, dl * MEGABYTE);
        tableValues.put(DATAVOL_UL, ul * MEGABYTE);
        tableValues.put(DATETIME_ID, date);
        insertRow(TEMP_EVENT_E_DVTP_DT_RAW, tableValues);
        tableValues.clear();
    }

    private void populateAggregationEventTables() throws SQLException {
        populateAggregationEventTable(APN, TEST_APN_01, TEST_DL_01, TEST_UL_01, TEST_DATETIME_ID_01, TEMP_EVENT_E_DVTP_DT_APN_15MIN);
        populateAggregationEventTable(APN, TEST_APN_02, TEST_DL_02, TEST_UL_02, TEST_DATETIME_ID_02, TEMP_EVENT_E_DVTP_DT_APN_15MIN);
        populateAggregationEventTable(APN, TEST_APN_03, TEST_DL_03, TEST_UL_03, TEST_DATETIME_ID_03, TEMP_EVENT_E_DVTP_DT_APN_15MIN);
        populateAggregationEventTable(APN, TEST_APN_04, TEST_DL_04, TEST_UL_04, TEST_DATETIME_ID_04, TEMP_EVENT_E_DVTP_DT_APN_15MIN);
        populateAggregationEventTable(APN, TEST_APN_05, TEST_DL_05, TEST_UL_05, TEST_DATETIME_ID_05, TEMP_EVENT_E_DVTP_DT_APN_15MIN);

        populateAggregationEventTable(APN, TEST_APN_01, TEST_DL_01, TEST_UL_01, TEST_DATETIME_ID_DAY_01, TEMP_EVENT_E_DVTP_DT_APN_DAY);
        populateAggregationEventTable(APN, TEST_APN_02, TEST_DL_02, TEST_UL_02, TEST_DATETIME_ID_DAY_02, TEMP_EVENT_E_DVTP_DT_APN_DAY);
        populateAggregationEventTable(APN, TEST_APN_03, TEST_DL_03, TEST_UL_03, TEST_DATETIME_ID_DAY_03, TEMP_EVENT_E_DVTP_DT_APN_DAY);
        populateAggregationEventTable(APN, TEST_APN_04, TEST_DL_04, TEST_UL_04, TEST_DATETIME_ID_DAY_04, TEMP_EVENT_E_DVTP_DT_APN_DAY);
        populateAggregationEventTable(APN, TEST_APN_05, TEST_DL_05, TEST_UL_05, TEST_DATETIME_ID_DAY_05, TEMP_EVENT_E_DVTP_DT_APN_DAY);

        populateAggregationEventTable(IMSI, TEST_IMSI_01, TEST_DL_01, TEST_UL_01, TEST_DATETIME_ID_01, TEMP_EVENT_E_DVTP_DT_IMSI_RANK_15MIN);
        populateAggregationEventTable(IMSI, TEST_IMSI_02, TEST_DL_02, TEST_UL_02, TEST_DATETIME_ID_02, TEMP_EVENT_E_DVTP_DT_IMSI_RANK_15MIN);
        populateAggregationEventTable(IMSI, TEST_IMSI_03, TEST_DL_03, TEST_UL_03, TEST_DATETIME_ID_03, TEMP_EVENT_E_DVTP_DT_IMSI_RANK_15MIN);
        populateAggregationEventTable(IMSI, TEST_IMSI_04, TEST_DL_04, TEST_UL_04, TEST_DATETIME_ID_04, TEMP_EVENT_E_DVTP_DT_IMSI_RANK_15MIN);
        populateAggregationEventTable(IMSI, TEST_IMSI_05, TEST_DL_05, TEST_UL_05, TEST_DATETIME_ID_05, TEMP_EVENT_E_DVTP_DT_IMSI_RANK_15MIN);

        populateAggregationEventTable(IMSI, TEST_IMSI_01, TEST_DL_01, TEST_UL_01, TEST_DATETIME_ID_DAY_01, TEMP_EVENT_E_DVTP_DT_IMSI_RANK_DAY);
        populateAggregationEventTable(IMSI, TEST_IMSI_02, TEST_DL_02, TEST_UL_02, TEST_DATETIME_ID_DAY_02, TEMP_EVENT_E_DVTP_DT_IMSI_RANK_DAY);
        populateAggregationEventTable(IMSI, TEST_IMSI_03, TEST_DL_03, TEST_UL_03, TEST_DATETIME_ID_DAY_03, TEMP_EVENT_E_DVTP_DT_IMSI_RANK_DAY);
        populateAggregationEventTable(IMSI, TEST_IMSI_04, TEST_DL_04, TEST_UL_04, TEST_DATETIME_ID_DAY_04, TEMP_EVENT_E_DVTP_DT_IMSI_RANK_DAY);
        populateAggregationEventTable(IMSI, TEST_IMSI_05, TEST_DL_05, TEST_UL_05, TEST_DATETIME_ID_DAY_05, TEMP_EVENT_E_DVTP_DT_IMSI_RANK_DAY);

        populateAggregationEventTable(GROUP_NAME, TEST_IMSI_GROUP_NAME_01, TEST_DL_01, TEST_UL_01, TEST_DATETIME_ID_01,
                TEMP_EVENT_E_DVTP_DT_IMSI_GROUP_RANK_15MIN);
        populateAggregationEventTable(GROUP_NAME, TEST_IMSI_GROUP_NAME_02, TEST_DL_02, TEST_UL_02, TEST_DATETIME_ID_02,
                TEMP_EVENT_E_DVTP_DT_IMSI_GROUP_RANK_15MIN);
        populateAggregationEventTable(GROUP_NAME, TEST_IMSI_GROUP_NAME_03, TEST_DL_03, TEST_UL_03, TEST_DATETIME_ID_03,
                TEMP_EVENT_E_DVTP_DT_IMSI_GROUP_RANK_15MIN);
        populateAggregationEventTable(GROUP_NAME, TEST_IMSI_GROUP_NAME_04, TEST_DL_04, TEST_UL_04, TEST_DATETIME_ID_04,
                TEMP_EVENT_E_DVTP_DT_IMSI_GROUP_RANK_15MIN);
        populateAggregationEventTable(GROUP_NAME, TEST_IMSI_GROUP_NAME_05, TEST_DL_05, TEST_UL_05, TEST_DATETIME_ID_05,
                TEMP_EVENT_E_DVTP_DT_IMSI_GROUP_RANK_15MIN);

        populateAggregationEventTable(GROUP_NAME, TEST_IMSI_GROUP_NAME_01, TEST_DL_01, TEST_UL_01, TEST_DATETIME_ID_DAY_01,
                TEMP_EVENT_E_DVTP_DT_IMSI_GROUP_RANK_DAY);
        populateAggregationEventTable(GROUP_NAME, TEST_IMSI_GROUP_NAME_02, TEST_DL_02, TEST_UL_02, TEST_DATETIME_ID_DAY_02,
                TEMP_EVENT_E_DVTP_DT_IMSI_GROUP_RANK_DAY);
        populateAggregationEventTable(GROUP_NAME, TEST_IMSI_GROUP_NAME_03, TEST_DL_03, TEST_UL_03, TEST_DATETIME_ID_DAY_03,
                TEMP_EVENT_E_DVTP_DT_IMSI_GROUP_RANK_DAY);
        populateAggregationEventTable(GROUP_NAME, TEST_IMSI_GROUP_NAME_04, TEST_DL_04, TEST_UL_04, TEST_DATETIME_ID_DAY_04,
                TEMP_EVENT_E_DVTP_DT_IMSI_GROUP_RANK_DAY);
        populateAggregationEventTable(GROUP_NAME, TEST_IMSI_GROUP_NAME_05, TEST_DL_05, TEST_UL_05, TEST_DATETIME_ID_DAY_05,
                TEMP_EVENT_E_DVTP_DT_IMSI_GROUP_RANK_DAY);

        populateAggregationEventTable(TAC, TEST_TAC_01, TEST_DL_01, TEST_UL_01, TEST_DATETIME_ID_01, TEMP_EVENT_E_DVTP_DT_TERM_15MIN);
        populateAggregationEventTable(TAC, TEST_TAC_02, TEST_DL_02, TEST_UL_02, TEST_DATETIME_ID_02, TEMP_EVENT_E_DVTP_DT_TERM_15MIN);
        populateAggregationEventTable(TAC, TEST_TAC_03, TEST_DL_03, TEST_UL_03, TEST_DATETIME_ID_03, TEMP_EVENT_E_DVTP_DT_TERM_15MIN);
        populateAggregationEventTable(TAC, TEST_TAC_04, TEST_DL_04, TEST_UL_04, TEST_DATETIME_ID_04, TEMP_EVENT_E_DVTP_DT_TERM_15MIN);
        populateAggregationEventTable(TAC, TEST_TAC_05, TEST_DL_05, TEST_UL_05, TEST_DATETIME_ID_05, TEMP_EVENT_E_DVTP_DT_TERM_15MIN);

        populateAggregationEventTable(TAC, TEST_TAC_01, TEST_DL_01, TEST_UL_01, TEST_DATETIME_ID_DAY_01, TEMP_EVENT_E_DVTP_DT_TERM_DAY);
        populateAggregationEventTable(TAC, TEST_TAC_02, TEST_DL_02, TEST_UL_02, TEST_DATETIME_ID_DAY_02, TEMP_EVENT_E_DVTP_DT_TERM_DAY);
        populateAggregationEventTable(TAC, TEST_TAC_03, TEST_DL_03, TEST_UL_03, TEST_DATETIME_ID_DAY_03, TEMP_EVENT_E_DVTP_DT_TERM_DAY);
        populateAggregationEventTable(TAC, TEST_TAC_04, TEST_DL_04, TEST_UL_04, TEST_DATETIME_ID_DAY_04, TEMP_EVENT_E_DVTP_DT_TERM_DAY);
        populateAggregationEventTable(TAC, TEST_TAC_05, TEST_DL_05, TEST_UL_05, TEST_DATETIME_ID_DAY_05, TEMP_EVENT_E_DVTP_DT_TERM_DAY);

        populateAggregationEventTable(EVENT_SOURCE_NAME, TEST_EVENT_SOURCE_NAME_01, TEST_DL_01, TEST_UL_01, TEST_DATETIME_ID_01,
                TEMP_EVENT_E_DVTP_DT_GGSN_15MIN);
        populateAggregationEventTable(EVENT_SOURCE_NAME, TEST_EVENT_SOURCE_NAME_02, TEST_DL_02, TEST_UL_02, TEST_DATETIME_ID_02,
                TEMP_EVENT_E_DVTP_DT_GGSN_15MIN);
        populateAggregationEventTable(EVENT_SOURCE_NAME, TEST_EVENT_SOURCE_NAME_03, TEST_DL_03, TEST_UL_03, TEST_DATETIME_ID_03,
                TEMP_EVENT_E_DVTP_DT_GGSN_15MIN);
        populateAggregationEventTable(EVENT_SOURCE_NAME, TEST_EVENT_SOURCE_NAME_04, TEST_DL_04, TEST_UL_04, TEST_DATETIME_ID_04,
                TEMP_EVENT_E_DVTP_DT_GGSN_15MIN);
        populateAggregationEventTable(EVENT_SOURCE_NAME, TEST_EVENT_SOURCE_NAME_05, TEST_DL_05, TEST_UL_05, TEST_DATETIME_ID_05,
                TEMP_EVENT_E_DVTP_DT_GGSN_15MIN);

        populateAggregationEventTable(EVENT_SOURCE_NAME, TEST_EVENT_SOURCE_NAME_01, TEST_DL_01, TEST_UL_01, TEST_DATETIME_ID_DAY_01,
                TEMP_EVENT_E_DVTP_DT_GGSN_DAY);
        populateAggregationEventTable(EVENT_SOURCE_NAME, TEST_EVENT_SOURCE_NAME_02, TEST_DL_02, TEST_UL_02, TEST_DATETIME_ID_DAY_02,
                TEMP_EVENT_E_DVTP_DT_GGSN_DAY);
        populateAggregationEventTable(EVENT_SOURCE_NAME, TEST_EVENT_SOURCE_NAME_03, TEST_DL_03, TEST_UL_03, TEST_DATETIME_ID_DAY_03,
                TEMP_EVENT_E_DVTP_DT_GGSN_DAY);
        populateAggregationEventTable(EVENT_SOURCE_NAME, TEST_EVENT_SOURCE_NAME_04, TEST_DL_04, TEST_UL_04, TEST_DATETIME_ID_DAY_04,
                TEMP_EVENT_E_DVTP_DT_GGSN_DAY);
        populateAggregationEventTable(EVENT_SOURCE_NAME, TEST_EVENT_SOURCE_NAME_05, TEST_DL_05, TEST_UL_05, TEST_DATETIME_ID_DAY_05,
                TEMP_EVENT_E_DVTP_DT_GGSN_DAY);
    }

    private void populateAggregationEventTable(String firstColumn, String value, double dl, double ul, String date, String aggregationTable)
            throws SQLException {
        tableValues.put(firstColumn, value);
        tableValues.put(DATAVOL_DL, dl * MEGABYTE);
        tableValues.put(DATAVOL_UL, ul * MEGABYTE);
        tableValues.put(DATETIME_ID, date);
        insertRow(aggregationTable, tableValues);
        tableValues.clear();
    }

    private void populateAggregationEventTable(String firstColumn, long value, double dl, double ul, String date, String aggregationTable)
            throws SQLException {
        tableValues.put(firstColumn, value);
        tableValues.put(DATAVOL_DL, dl * MEGABYTE);
        tableValues.put(DATAVOL_UL, ul * MEGABYTE);
        tableValues.put(DATETIME_ID, date);
        insertRow(aggregationTable, tableValues);
        tableValues.clear();
    }

    private MultivaluedMap<String, String> getRequestParameters(String type, String display, String time, String timeZoneOffset) {
        map.clear();
        map.putSingle(TYPE_PARAM, type);
        map.putSingle(DISPLAY_PARAM, display);
        map.putSingle(TIME_QUERY_PARAM, time);
        map.putSingle(TZ_OFFSET, timeZoneOffset);
        return map;
    }

    private void assertResult(final DatavolRankingResult DatavolRankingResult, int expectedRank, double expectedTotalDataVolume,
                              double expectedTotalDl, double expectedTotalUl) {
        assertThat(DatavolRankingResult.getRank(), is(expectedRank));
        assertThat(DatavolRankingResult.getTotalDataVol(), is(expectedTotalDataVolume));
        assertThat(DatavolRankingResult.getDLDataVol(), is(expectedTotalDl));
        assertThat(DatavolRankingResult.getULDataVol(), is(expectedTotalUl));
    }

    private void assertResult(final DatavolRankingResult DatavolRankingResult, int expectedRank, String expectedManufacturer,
                              String expectedMarketingName, double expectedTotalDataVolume, double expectedTotalDl, double expectedTotalUl) {
        assertThat(DatavolRankingResult.getRank(), is(expectedRank));
        assertThat(DatavolRankingResult.getManufacturer(), is(expectedManufacturer));
        assertThat(DatavolRankingResult.getModel(), is(expectedMarketingName));
        assertThat(DatavolRankingResult.getTotalDataVolTac(), is(expectedTotalDataVolume));
        assertThat(DatavolRankingResult.getDLDataVolTac(), is(expectedTotalDl));
        assertThat(DatavolRankingResult.getULDataVolTac(), is(expectedTotalUl));
    }
}