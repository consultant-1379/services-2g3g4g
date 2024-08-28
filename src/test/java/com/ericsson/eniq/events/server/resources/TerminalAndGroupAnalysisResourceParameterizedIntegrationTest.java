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
package com.ericsson.eniq.events.server.resources;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;

import javax.annotation.Resource;
import javax.ws.rs.core.MultivaluedMap;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;

import org.junit.Test;
import org.junit.runner.RunWith;

import com.ericsson.eniq.events.server.resources.automation.ResourceBaseTest;
import com.ericsson.eniq.events.server.resources.automation.dataproviders.TerminalAnalysisTestDataProvider;
import com.ericsson.eniq.events.server.resources.automation.dataproviders.TerminalGroupAnalysisTestDataProvider;
import com.ericsson.eniq.events.server.test.stubs.DummyUriInfoImpl;

@RunWith(JUnitParamsRunner.class)
public class TerminalAndGroupAnalysisResourceParameterizedIntegrationTest extends ResourceBaseTest {

    private static final String ABSOLUTE_PATH = "absoluteURI";

    @Resource(name = "terminalAndGroupAnalysisResource")
    private TerminalAndGroupAnalysisResource terminalAndGroupAnalysisResource;

    @Test
    @Parameters(source = TerminalAnalysisTestDataProvider.class)
    public void testGetMostPopularData_Terminal(final MultivaluedMap<String, String> requestParameters) throws Exception {
        terminalAndGroupAnalysisResource.setUriInfo(new DummyUriInfoImpl(requestParameters, ABSOLUTE_PATH, TERMINAL_ANALYSIS + PATH_SEPARATOR
                + MOST_POPULAR));
        final String result = terminalAndGroupAnalysisResource.getMostPopularData();
        jsonAssertUtils.assertJSONSucceeds(result);
    }

    @Test
    @Parameters(source = TerminalGroupAnalysisTestDataProvider.class)
    public void testGetMostPopularData_Group(final MultivaluedMap<String, String> requestParameters) throws Exception {
        terminalAndGroupAnalysisResource.setUriInfo(new DummyUriInfoImpl(requestParameters, ABSOLUTE_PATH, TERMINAL_GROUP_ANALYSIS + PATH_SEPARATOR
                + MOST_POPULAR));
        final String result = terminalAndGroupAnalysisResource.getMostPopularData();
        jsonAssertUtils.assertJSONSucceeds(result);
    }

    @Test
    @Parameters(source = TerminalGroupAnalysisTestDataProvider.class)
    public void testGetMostPopularEventSummary_Group(final MultivaluedMap<String, String> requestParameters) throws Exception {
        terminalAndGroupAnalysisResource.setUriInfo(new DummyUriInfoImpl(requestParameters, ABSOLUTE_PATH, TERMINAL_GROUP_ANALYSIS + PATH_SEPARATOR
                + MOST_POPULAR_EVENT_SUMMARY));
        final String result = terminalAndGroupAnalysisResource.getMostPopularEventSummaryData();
        jsonAssertUtils.assertJSONSucceeds(result);
    }

    @Test
    @Parameters(source = TerminalAnalysisTestDataProvider.class)
    public void testGetMostPopularEventSummary_Terminal(final MultivaluedMap<String, String> requestParameters) throws Exception {
        terminalAndGroupAnalysisResource.setUriInfo(new DummyUriInfoImpl(requestParameters, ABSOLUTE_PATH, TERMINAL_ANALYSIS + PATH_SEPARATOR
                + MOST_POPULAR_EVENT_SUMMARY));
        final String result = terminalAndGroupAnalysisResource.getMostPopularEventSummaryData();
        jsonAssertUtils.assertJSONSucceeds(result);
    }

    @Test
    @Parameters(source = TerminalAnalysisTestDataProvider.class)
    public void testGetMostAttachedFailures_Terminal(final MultivaluedMap<String, String> requestParameters) throws Exception {
        terminalAndGroupAnalysisResource.setUriInfo(new DummyUriInfoImpl(requestParameters, ABSOLUTE_PATH, TERMINAL_ANALYSIS + PATH_SEPARATOR
                + MOST_ATTACHED_FAILURES));
        final String result = terminalAndGroupAnalysisResource.getMostAttachedFailuresData();
        jsonAssertUtils.assertJSONSucceeds(result);
    }

    @Test
    @Parameters(source = TerminalGroupAnalysisTestDataProvider.class)
    public void testGetMostAttachedFailures_Group(final MultivaluedMap<String, String> requestParameters) throws Exception {
        terminalAndGroupAnalysisResource.setUriInfo(new DummyUriInfoImpl(requestParameters, ABSOLUTE_PATH, TERMINAL_GROUP_ANALYSIS + PATH_SEPARATOR
                + MOST_ATTACHED_FAILURES));
        final String result = terminalAndGroupAnalysisResource.getMostAttachedFailuresData();
        jsonAssertUtils.assertJSONSucceeds(result);
    }

    @Test
    @Parameters(source = TerminalGroupAnalysisTestDataProvider.class)
    public void testGetMostPDPSessionSetupFailures_Group(final MultivaluedMap<String, String> requestParameters) throws Exception {
        terminalAndGroupAnalysisResource.setUriInfo(new DummyUriInfoImpl(requestParameters, ABSOLUTE_PATH, TERMINAL_GROUP_ANALYSIS + PATH_SEPARATOR
                + MOST_PDP_SESSION_SETUP_FAILURES));
        final String result = terminalAndGroupAnalysisResource.getMostPDPSessionSetupFailuresData();
        jsonAssertUtils.assertJSONSucceeds(result);
    }

    @Test
    @Parameters(source = TerminalAnalysisTestDataProvider.class)
    public void testGetMostPDPSessionSetupFailures_Terminal(final MultivaluedMap<String, String> requestParameters) throws Exception {
        terminalAndGroupAnalysisResource.setUriInfo(new DummyUriInfoImpl(requestParameters, ABSOLUTE_PATH, TERMINAL_ANALYSIS + PATH_SEPARATOR
                + MOST_PDP_SESSION_SETUP_FAILURES));
        final String result = terminalAndGroupAnalysisResource.getMostPDPSessionSetupFailuresData();
        jsonAssertUtils.assertJSONSucceeds(result);
    }

    @Test
    @Parameters(source = TerminalGroupAnalysisTestDataProvider.class)
    public void testGetMostMobilityIssues_Group(final MultivaluedMap<String, String> requestParameters) throws Exception {
        terminalAndGroupAnalysisResource.setUriInfo(new DummyUriInfoImpl(requestParameters, ABSOLUTE_PATH, TERMINAL_GROUP_ANALYSIS + PATH_SEPARATOR
                + MOST_MOBILITY_ISSUES));
        final String result = terminalAndGroupAnalysisResource.getMostMobilityIssuesData();
        jsonAssertUtils.assertJSONSucceeds(result);
    }

    @Test
    @Parameters(source = TerminalAnalysisTestDataProvider.class)
    public void testGetMostMobilityIssues_Terminal(final MultivaluedMap<String, String> requestParameters) throws Exception {
        terminalAndGroupAnalysisResource.setUriInfo(new DummyUriInfoImpl(requestParameters, ABSOLUTE_PATH, TERMINAL_ANALYSIS + PATH_SEPARATOR
                + MOST_MOBILITY_ISSUES));
        final String result = terminalAndGroupAnalysisResource.getMostMobilityIssuesData();
        jsonAssertUtils.assertJSONSucceeds(result);
    }

    @Test
    @Parameters(source = TerminalGroupAnalysisTestDataProvider.class)
    public void testGetHighestDatavol_Group(final MultivaluedMap<String, String> requestParameters) throws Exception {
        terminalAndGroupAnalysisResource.setUriInfo(new DummyUriInfoImpl(requestParameters, ABSOLUTE_PATH, TERMINAL_GROUP_ANALYSIS + PATH_SEPARATOR
                + HIGHEST_DATAVOL));
        final String result = terminalAndGroupAnalysisResource.getHighestDatavolData();
        jsonAssertUtils.assertJSONSucceeds(result);
    }

    @Test
    @Parameters(source = TerminalGroupAnalysisTestDataProvider.class)
    public void testGetHighestDatavol_Terminal(final MultivaluedMap<String, String> requestParameters) throws Exception {
        terminalAndGroupAnalysisResource.setUriInfo(new DummyUriInfoImpl(requestParameters, ABSOLUTE_PATH, TERMINAL_ANALYSIS + PATH_SEPARATOR
                + HIGHEST_DATAVOL));
        final String result = terminalAndGroupAnalysisResource.getHighestDatavolData();
        jsonAssertUtils.assertJSONSucceeds(result);
    }

}
