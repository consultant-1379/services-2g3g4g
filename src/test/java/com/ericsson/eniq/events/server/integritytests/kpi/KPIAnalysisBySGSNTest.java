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

package com.ericsson.eniq.events.server.integritytests.kpi;

import static com.ericsson.eniq.events.server.common.ApplicationConfigConstants.*;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.*;

import java.net.URISyntaxException;

import javax.ws.rs.core.MultivaluedMap;

import org.junit.Test;

import com.sun.jersey.core.util.MultivaluedMapImpl;

public class KPIAnalysisBySGSNTest extends BaseKPIAnalysisTest {

    @Override
    public void onSetUp() throws Exception {
        super.onSetUp();
        createTemporaryTables(tempSGSNAggTables);
        addConnectionToInterceptedDbConnectionManager(tempSGSNAggTables);
    }

    @Test
    public void testGetKPIData_30Minutes() throws Exception {
        jndiProperties.setUpDataTieringJNDIProperty();
        populateTemporaryTablesWithEvents(THIRTY_MINUTES, SGSN);
        final String json = getData(THIRTY_MINUTES);
        validateResults(json, THIRTY_MINUTES);
        jndiProperties.setUpJNDIPropertiesForTest();
    }

    @Test
    public void testGetKPIData_3Hours() throws Exception {
        populateTemporaryTablesWithEvents(THREE_HOURS, SGSN);
        final String json = getData(THREE_HOURS);
        validateResults(json, THREE_HOURS);
    }

    @Test
    public void testGetKPIData_OneWeek() throws Exception {
        populateTemporaryTablesWithEvents_OneWeek(SGSN);
        final String json = getData(ONE_WEEK);
        validateResults(json, ONE_WEEK);
    }

    private String getData(final String time) throws URISyntaxException {
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(TIME_QUERY_PARAM, time);
        map.putSingle(TYPE_PARAM, SGSN);
        map.putSingle(NODE_PARAM, SAMPLE_MME);
        map.putSingle(DISPLAY_PARAM, CHART_PARAM);
        map.putSingle(TZ_OFFSET, PLUS_THREE_HOURS);
        map.putSingle(MAX_ROWS, Integer.toString(DEFAULT_MAXIMUM_JSON_RESULT_SIZE));
        return runGetDataQuery(kpiResource, map);
    }

}
