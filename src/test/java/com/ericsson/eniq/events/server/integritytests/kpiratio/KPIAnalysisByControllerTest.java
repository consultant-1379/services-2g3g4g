package com.ericsson.eniq.events.server.integritytests.kpiratio;

import com.ericsson.eniq.events.server.resources.KPIRatioResource;
import com.ericsson.eniq.events.server.resources.TestsWithTemporaryTablesBaseTestCase;
import com.ericsson.eniq.events.server.test.queryresults.KPIAnalysisByControllerResult;
import com.ericsson.eniq.events.server.test.stubs.DummyUriInfoImpl;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;
import com.sun.jersey.core.util.MultivaluedMapImpl;
import org.junit.Test;

import java.net.URISyntaxException;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.common.EventIDConstants.ISRAU_IN_2G_AND_3G;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.*;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.*;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.TEMP_EVENT_E_SGEH_SUC_RAW;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * Created by eeikbe on 03/07/2014.
 */
public class KPIAnalysisByControllerTest extends TestsWithTemporaryTablesBaseTestCase<KPIAnalysisByControllerResult> {

    private KPIRatioResource kpiRatioResource;

    private final SimpleDateFormat dateOnlyFormatter = new SimpleDateFormat(DATE_FORMAT);

    @Override
    public void onSetUp() throws Exception {
        super.onSetUp();
        kpiRatioResource = new KPIRatioResource();
        attachDependencies(kpiRatioResource);
    }

    @Test
    public void testTypeAPNDrilltypeSGSN() throws Exception {
        final String timestamp = DateTimeUtilities.getDateTimeMinusDay(7);
        final String localDateId = dateOnlyFormatter.format(dateOnlyFormatter.parse(timestamp));
        createAndPopulateRawTablesForSuccessRatioQuery(timestamp, localDateId);
        final String result = runQueryDrill(SAMPLE_BSC, RAT_FOR_3G, SAMPLE_SGSN, ISRAU_IN_2G_AND_3G, ONE_WEEK);
        validateResultForDrillRatioQuery(result);
    }

    private void validateResultForDrillRatioQuery(final String json) throws Exception {
        final List<KPIAnalysisByControllerResult> results = getTranslator().translateResult(json, KPIAnalysisByControllerResult.class);
        assertThat(results.size(), is(1));
        final KPIAnalysisByControllerResult result = results.get(0);
        assertThat(result.getRat(), is(1));
        assertThat(result.getApn(), is(SAMPLE_APN));
        assertThat(result.getSGSN(), is(SAMPLE_SGSN));
        assertThat(result.getVendor(), is(ERICSSON));
        assertThat(result.getController(), is(SAMPLE_BSC));
        assertThat(result.getRATDesc(), is("3G"));
        assertThat(result.getEvendID(), is(ISRAU_IN_2G_AND_3G));
        assertThat(result.getEventDesc(), is(ISRAU));
        assertThat(result.getNoErrors(), is(1));
        assertThat(result.getNoSuccesses(), is(1));
        assertThat(result.getOccurrences(), is(2));
        assertThat(result.getSuccessRatio(), is(50.00));
    }


    private String runQueryDrill(final String bsc, final int rat, final String sgsnOrMME, final int eventId, final String time) throws URISyntaxException {
        final MultivaluedMapImpl map = new MultivaluedMapImpl();
        map.putSingle(TYPE_PARAM, TYPE_APN);
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(TIME_QUERY_PARAM, time);
        map.putSingle(APN_PARAM, SAMPLE_APN);
        map.putSingle(SGSN_PARAM, sgsnOrMME);
        map.putSingle(EVENT_ID_PARAM, eventId);
        map.putSingle(RAT_PARAM, rat);
        map.putSingle(TZ_OFFSET, "+0100");
        map.putSingle(MAX_ROWS, "500");
        map.putSingle(BSC_PARAM, bsc);
        map.putSingle(VENDOR_PARAM, ERICSSON);
        DummyUriInfoImpl.setUriInfo(map, kpiRatioResource);
        final String result = kpiRatioResource.getData();
        System.out.println(result);
        return result;
    }


    private void createAndPopulateRawTablesForSuccessRatioQuery(final String timestamp, final String localDateId) throws Exception {
        final Map<String, Object> columnsAndValuesForRawTable = new HashMap<String, Object>();
        columnsAndValuesForRawTable.put(APN, SAMPLE_APN);
        columnsAndValuesForRawTable.put(EVENT_SOURCE_NAME, SAMPLE_SGSN);
        columnsAndValuesForRawTable.put(VENDOR_PARAM_UPPER_CASE, ERICSSON);
        columnsAndValuesForRawTable.put(IMSI, SAMPLE_IMSI);
        columnsAndValuesForRawTable.put(HIERARCHY_3, "BSC1");
        columnsAndValuesForRawTable.put(HIERARCHY_1, "yyy");
        columnsAndValuesForRawTable.put(EVENT_ID, ISRAU_IN_2G_AND_3G);
        columnsAndValuesForRawTable.put(RAT, 1);
        columnsAndValuesForRawTable.put(TAC, SAMPLE_TAC);
        columnsAndValuesForRawTable.put(DEACTIVATION_TRIGGER, 0);
        columnsAndValuesForRawTable.put(DATETIME_ID, timestamp);
        columnsAndValuesForRawTable.put(LOCAL_DATE_ID, localDateId);
        columnsAndValuesForRawTable.put(IMSI, "1234567");
        createTemporaryTable(TEMP_EVENT_E_LTE_ERR_RAW, columnsAndValuesForRawTable.keySet());
        createTemporaryTable(TEMP_EVENT_E_LTE_SUC_RAW, columnsAndValuesForRawTable.keySet());
        createTemporaryTable(TEMP_EVENT_E_SGEH_ERR_RAW, columnsAndValuesForRawTable.keySet());
        createTemporaryTable(TEMP_EVENT_E_SGEH_SUC_RAW, columnsAndValuesForRawTable.keySet());
        insertRow(TEMP_EVENT_E_LTE_ERR_RAW, columnsAndValuesForRawTable);
        insertRow(TEMP_EVENT_E_LTE_SUC_RAW, columnsAndValuesForRawTable);
        insertRow(TEMP_EVENT_E_SGEH_ERR_RAW, columnsAndValuesForRawTable);
        insertRow(TEMP_EVENT_E_SGEH_SUC_RAW, columnsAndValuesForRawTable);
    }

}
