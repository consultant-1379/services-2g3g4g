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
package com.ericsson.eniq.events.server.resources.dtput;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.common.RequestParametersUtilities.*;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.*;

import javax.ws.rs.core.MultivaluedMap;

import org.junit.Test;

import com.ericsson.eniq.events.server.resources.DataServiceBaseTestCase;
import com.sun.jersey.core.util.MultivaluedMapImpl;

public class DataVolumeRoamingAnalysisResourceIntegrationTest extends DataServiceBaseTestCase {
    private MultivaluedMap<String, String> map;

    private DatavolRoamingAnalysisResource datavolRoamingAnalysisResource;

    @Override
    public void onSetUp() {
        datavolRoamingAnalysisResource = new DatavolRoamingAnalysisResource();
        datavolRoamingAnalysisResource.setTechPackCXCMappingService(techPackCXCMappingService);
        attachDependencies(datavolRoamingAnalysisResource);
        map = new MultivaluedMapImpl();
    }

    @Test
    public void testGetRoamingDataByCountry_15min() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForNetwork(CHART_PARAM, FIFTEEN_MINUTES_TEST,
                TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        final String result = datavolRoamingAnalysisResource.getRoamingResults(CANCEL_REQUEST_NOT_SUPPORTED, TYPE_ROAMING_COUNTRY, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetRoamingDataByCountry_30min() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForNetwork(GRID_PARAM, THIRTY_MINUTES, TIME_ZONE_OFFSET_OF_MINUS_EIGHT_HOUR);
        final String result = datavolRoamingAnalysisResource.getRoamingResults(CANCEL_REQUEST_NOT_SUPPORTED, TYPE_ROAMING_COUNTRY, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetRoamingDataByCountry_1hour() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForNetwork(CHART_PARAM, ONE_HOUR, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        final String result = datavolRoamingAnalysisResource.getRoamingResults(CANCEL_REQUEST_NOT_SUPPORTED, TYPE_ROAMING_COUNTRY, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetRoamingDataByCountry_2hours() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForNetwork(GRID_PARAM, TWO_HOURS, TIME_ZONE_OFFSET_OF_MINUS_EIGHT_HOUR);
        final String result = datavolRoamingAnalysisResource.getRoamingResults(CANCEL_REQUEST_NOT_SUPPORTED, TYPE_ROAMING_COUNTRY, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetRoamingDataByCountry_6hours() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForNetwork(GRID_PARAM, SIX_HOURS, TIME_ZONE_OFFSET_OF_MINUS_EIGHT_HOUR);
        final String result = datavolRoamingAnalysisResource.getRoamingResults(CANCEL_REQUEST_NOT_SUPPORTED, TYPE_ROAMING_COUNTRY, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetRoamingDataByCountry_12hours() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForNetwork(GRID_PARAM, TWELVE_HOURS, TIME_ZONE_OFFSET_OF_MINUS_EIGHT_HOUR);
        final String result = datavolRoamingAnalysisResource.getRoamingResults(CANCEL_REQUEST_NOT_SUPPORTED, TYPE_ROAMING_COUNTRY, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetRoamingDataByCountry_1day() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForNetwork(GRID_PARAM, ONE_DAY, TIME_ZONE_OFFSET_OF_MINUS_EIGHT_HOUR);
        final String result = datavolRoamingAnalysisResource.getRoamingResults(CANCEL_REQUEST_NOT_SUPPORTED, TYPE_ROAMING_COUNTRY, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetRoamingDataByCountry_1week() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForNetwork(GRID_PARAM, ONE_WEEK, TIME_ZONE_OFFSET_OF_MINUS_EIGHT_HOUR);
        final String result = datavolRoamingAnalysisResource.getRoamingResults(CANCEL_REQUEST_NOT_SUPPORTED, TYPE_ROAMING_COUNTRY, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetRoamingDataByOperator_15min() throws Exception {

        final MultivaluedMap<String, String> map = getRequestParametersForNetwork(CHART_PARAM, FIFTEEN_MINUTES_TEST,
                TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        final String result = datavolRoamingAnalysisResource.getRoamingResults(CANCEL_REQUEST_NOT_SUPPORTED, TYPE_ROAMING_OPERATOR, map);

        assertJSONSucceeds(result);
    }

    @Test
    public void testGetRoamingDataByOperator_30min() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForNetwork(GRID_PARAM, THIRTY_MINUTES, TIME_ZONE_OFFSET_OF_MINUS_EIGHT_HOUR);
        final String result = datavolRoamingAnalysisResource.getRoamingResults(CANCEL_REQUEST_NOT_SUPPORTED, TYPE_ROAMING_OPERATOR, map);

        assertJSONSucceeds(result);
    }

    @Test
    public void testGetRoamingDataByOperator_1hour() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForNetwork(CHART_PARAM, ONE_HOUR, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        final String result = datavolRoamingAnalysisResource.getRoamingResults(CANCEL_REQUEST_NOT_SUPPORTED, TYPE_ROAMING_OPERATOR, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetRoamingDataByOperator_2hours() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForNetwork(GRID_PARAM, TWO_HOURS, TIME_ZONE_OFFSET_OF_MINUS_EIGHT_HOUR);
        final String result = datavolRoamingAnalysisResource.getRoamingResults(CANCEL_REQUEST_NOT_SUPPORTED, TYPE_ROAMING_OPERATOR, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetRoamingDataByOperator_6hours() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForNetwork(GRID_PARAM, SIX_HOURS, TIME_ZONE_OFFSET_OF_MINUS_EIGHT_HOUR);
        final String result = datavolRoamingAnalysisResource.getRoamingResults(CANCEL_REQUEST_NOT_SUPPORTED, TYPE_ROAMING_OPERATOR, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetRoamingDataByOperator_12hours() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForNetwork(GRID_PARAM, TWELVE_HOURS, TIME_ZONE_OFFSET_OF_MINUS_EIGHT_HOUR);
        final String result = datavolRoamingAnalysisResource.getRoamingResults(CANCEL_REQUEST_NOT_SUPPORTED, TYPE_ROAMING_OPERATOR, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetRoamingDataByOperator_1day() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForNetwork(GRID_PARAM, ONE_DAY, TIME_ZONE_OFFSET_OF_MINUS_EIGHT_HOUR);
        final String result = datavolRoamingAnalysisResource.getRoamingResults(CANCEL_REQUEST_NOT_SUPPORTED, TYPE_ROAMING_OPERATOR, map);
        assertJSONSucceeds(result);
    }

    @Test
    public void testGetRoamingDataByOperator_1week() throws Exception {
        final MultivaluedMap<String, String> map = getRequestParametersForNetwork(GRID_PARAM, ONE_WEEK, TIME_ZONE_OFFSET_OF_MINUS_EIGHT_HOUR);
        final String result = datavolRoamingAnalysisResource.getRoamingResults(CANCEL_REQUEST_NOT_SUPPORTED, TYPE_ROAMING_OPERATOR, map);
        assertJSONSucceeds(result);
    }

}
