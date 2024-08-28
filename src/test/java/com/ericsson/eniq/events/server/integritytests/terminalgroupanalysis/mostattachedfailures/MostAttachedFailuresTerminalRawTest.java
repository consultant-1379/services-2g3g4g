/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.integritytests.terminalgroupanalysis.mostattachedfailures;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.*;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.sql.SQLException;
import java.util.List;

import javax.ws.rs.core.MultivaluedMap;

import org.junit.Test;

import com.ericsson.eniq.events.server.resources.TerminalAndGroupAnalysisResource;
import com.ericsson.eniq.events.server.resources.TestsWithTemporaryTablesBaseTestCase;
import com.ericsson.eniq.events.server.test.queryresults.MostAttachedFailuresTerminalResult;
import com.ericsson.eniq.events.server.test.stubs.DummyUriInfoImpl;
import com.sun.jersey.core.util.MultivaluedMapImpl;

/**
 * @author eemecoy
 * 
 */
public class MostAttachedFailuresTerminalRawTest extends TestsWithTemporaryTablesBaseTestCase<MostAttachedFailuresTerminalResult> {

    private TerminalAndGroupAnalysisResource terminalAndGroupAnalysisResource;

    private MostAttachedFailuresTablesPopulator populator;

    /*
     * (non-Javadoc)
     * 
     * @see com.ericsson.eniq.events.server.resources.TestsWithTemporaryTablesBaseTestCase#onSetUp()
     */
    @Override
    public void onSetUp() throws Exception {
        super.onSetUp();
    }

    @Test
    public void testGetTerminalMostAttachedFailuresData_FiveMinutes() throws Exception {
        setupTerminalAndGroupAnalysisResource();
        populator.populateTemporaryTablesForLast3Minutes();
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(TIME_QUERY_PARAM, FIVE_MINUTES);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        final String uriPath = TERMINAL_ANALYSIS + "/" + MOST_ATTACHED_FAILURES;
        DummyUriInfoImpl.setUriInfo(map, terminalAndGroupAnalysisResource, SAMPLE_BASE_URI, uriPath);

        final String result = terminalAndGroupAnalysisResource.getMostAttachedFailuresData();
        System.out.println(result);
        validateResult(result);
    }

    @Test
    public void testGetTerminalMostAttachedFailuresData_30Minutes() throws Exception {
        setupTerminalAndGroupAnalysisResource();
        populator.populateTemporaryTablesForLast30Minutes();
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(TIME_QUERY_PARAM, THIRTY_MINUTES);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        final String uriPath = TERMINAL_ANALYSIS + "/" + MOST_ATTACHED_FAILURES;
        DummyUriInfoImpl.setUriInfo(map, terminalAndGroupAnalysisResource, SAMPLE_BASE_URI, uriPath);

        final String result = terminalAndGroupAnalysisResource.getMostAttachedFailuresData();
        System.out.println(result);
        validateResult(result);
    }

    @Test
    public void testGetTerminalMostAttachedFailuresData_6hours() throws Exception {
        setupTerminalAndGroupAnalysisResource();
        populator.populateTemporaryTablesForLast6hours();
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(TIME_QUERY_PARAM, SIX_HOURS);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        final String uriPath = TERMINAL_ANALYSIS + "/" + MOST_ATTACHED_FAILURES;
        DummyUriInfoImpl.setUriInfo(map, terminalAndGroupAnalysisResource, SAMPLE_BASE_URI, uriPath);

        final String result = terminalAndGroupAnalysisResource.getMostAttachedFailuresData();
        System.out.println(result);
        final List<MostAttachedFailuresTerminalResult> results = getTranslator().translateResult(result, MostAttachedFailuresTerminalResult.class);
        assertThat(results.size(), is(2));
        final MostAttachedFailuresTerminalResult worstTac = results.get(0);
        assertThat(worstTac.getTac(), is(MostAttachedFailuresTablesPopulator.WORST_TAC));
        assertThat(worstTac.getManufacturer(), is(MostAttachedFailuresTablesPopulator.MANFACTURER_FOR_WORST_TAC));
        assertThat(worstTac.getMarketingName(), is(MostAttachedFailuresTablesPopulator.MARKETING_NAME_FOR_WORST_TAC));
        assertThat(worstTac.getNoErrors(), is(8));

        final MostAttachedFailuresTerminalResult secondWorstTac = results.get(1);
        assertThat(secondWorstTac.getTac(), is(MostAttachedFailuresTablesPopulator.SECOND_WORST_TAC));
        assertThat(secondWorstTac.getManufacturer(), is(MostAttachedFailuresTablesPopulator.MANFACTURER_FOR_SECOND_WORST_TAC));
        assertThat(secondWorstTac.getMarketingName(), is(MostAttachedFailuresTablesPopulator.MARKETING_NAME_FOR_SECOND_WORST_TAC));
        assertThat(secondWorstTac.getNoErrors(), is(2));
    }

    @Test
    public void testGetTerminalMostAttachedFailuresData_1Week() throws Exception {
        setupTerminalAndGroupAnalysisResource();
        populator.populateTemporaryTablesForLast48Hours();
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(TIME_QUERY_PARAM, ONE_WEEK);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        final String uriPath = TERMINAL_ANALYSIS + "/" + MOST_ATTACHED_FAILURES;
        DummyUriInfoImpl.setUriInfo(map, terminalAndGroupAnalysisResource, SAMPLE_BASE_URI, uriPath);

        final String result = terminalAndGroupAnalysisResource.getMostAttachedFailuresData();
        System.out.println(result);
        final List<MostAttachedFailuresTerminalResult> results = getTranslator().translateResult(result, MostAttachedFailuresTerminalResult.class);
        assertThat(results.size(), is(2));
        final MostAttachedFailuresTerminalResult worstTac = results.get(0);
        assertThat(worstTac.getTac(), is(MostAttachedFailuresTablesPopulator.WORST_TAC));
        assertThat(worstTac.getManufacturer(), is(MostAttachedFailuresTablesPopulator.MANFACTURER_FOR_WORST_TAC));
        assertThat(worstTac.getMarketingName(), is(MostAttachedFailuresTablesPopulator.MARKETING_NAME_FOR_WORST_TAC));
        assertThat(worstTac.getNoErrors(), is(10));

        final MostAttachedFailuresTerminalResult secondWorstTac = results.get(1);
        assertThat(secondWorstTac.getTac(), is(MostAttachedFailuresTablesPopulator.SECOND_WORST_TAC));
        assertThat(secondWorstTac.getManufacturer(), is(MostAttachedFailuresTablesPopulator.MANFACTURER_FOR_SECOND_WORST_TAC));
        assertThat(secondWorstTac.getMarketingName(), is(MostAttachedFailuresTablesPopulator.MARKETING_NAME_FOR_SECOND_WORST_TAC));
        assertThat(secondWorstTac.getNoErrors(), is(3));
    }

    @Test
    public void testGetTerminalMostAttachedFailuresDataForUnknownTac_FiveMinutes() throws Exception {
        setupTerminalAndGroupAnalysisResource();
        populator.populateTemporaryTablesForLast3MinutesWithUnknownTac();
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(TIME_QUERY_PARAM, FIVE_MINUTES);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        final String uriPath = TERMINAL_ANALYSIS + "/" + MOST_ATTACHED_FAILURES;
        DummyUriInfoImpl.setUriInfo(map, terminalAndGroupAnalysisResource, SAMPLE_BASE_URI, uriPath);

        final String result = terminalAndGroupAnalysisResource.getMostAttachedFailuresData();
        System.out.println(result);
        final List<MostAttachedFailuresTerminalResult> results = getTranslator().translateResult(result, MostAttachedFailuresTerminalResult.class);
        assertThat(results.size(), is(1));
        final MostAttachedFailuresTerminalResult worstTac = results.get(0);
        assertThat(worstTac.getTac(), is(MostAttachedFailuresTablesPopulator.UNKNOWN_TAC));
        assertThat(Integer.parseInt(worstTac.getManufacturer()), is(MostAttachedFailuresTablesPopulator.UNKNOWN_TAC));
        assertThat(Integer.parseInt(worstTac.getMarketingName()), is(MostAttachedFailuresTablesPopulator.UNKNOWN_TAC));
        assertThat(worstTac.getNoErrors(), is(2));
    }

    @Test
    public void testGetTerminalMostAttachedFailuresDataForUnknownTac_30Minutes() throws Exception {
        setupTerminalAndGroupAnalysisResource();
        populator.populateTemporaryTablesForLast30MinutesWithUnknownTac();
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(TIME_QUERY_PARAM, THIRTY_MINUTES);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        final String uriPath = TERMINAL_ANALYSIS + "/" + MOST_ATTACHED_FAILURES;
        DummyUriInfoImpl.setUriInfo(map, terminalAndGroupAnalysisResource, SAMPLE_BASE_URI, uriPath);

        final String result = terminalAndGroupAnalysisResource.getMostAttachedFailuresData();
        System.out.println(result);
        final List<MostAttachedFailuresTerminalResult> results = getTranslator().translateResult(result, MostAttachedFailuresTerminalResult.class);
        assertThat(results.size(), is(1));
        final MostAttachedFailuresTerminalResult worstTac = results.get(0);
        assertThat(worstTac.getTac(), is(MostAttachedFailuresTablesPopulator.UNKNOWN_TAC));
        assertThat(Integer.parseInt(worstTac.getManufacturer()), is(MostAttachedFailuresTablesPopulator.UNKNOWN_TAC));
        assertThat(Integer.parseInt(worstTac.getMarketingName()), is(MostAttachedFailuresTablesPopulator.UNKNOWN_TAC));
        assertThat(worstTac.getNoErrors(), is(2));
    }

    @Test
    public void testGetTerminalMostAttachedFailuresDataForUnknownTac_6hours() throws Exception {
        setupTerminalAndGroupAnalysisResource();
        populator.populateTemporaryTablesForLast6hoursWithUnknownTac();
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(TIME_QUERY_PARAM, SIX_HOURS);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        final String uriPath = TERMINAL_ANALYSIS + "/" + MOST_ATTACHED_FAILURES;
        DummyUriInfoImpl.setUriInfo(map, terminalAndGroupAnalysisResource, SAMPLE_BASE_URI, uriPath);

        final String result = terminalAndGroupAnalysisResource.getMostAttachedFailuresData();
        System.out.println(result);
        final List<MostAttachedFailuresTerminalResult> results = getTranslator().translateResult(result, MostAttachedFailuresTerminalResult.class);
        assertThat(results.size(), is(1));
        final MostAttachedFailuresTerminalResult worstTac = results.get(0);
        assertThat(worstTac.getTac(), is(MostAttachedFailuresTablesPopulator.UNKNOWN_TAC));
        assertThat(Integer.parseInt(worstTac.getManufacturer()), is(MostAttachedFailuresTablesPopulator.UNKNOWN_TAC));
        assertThat(Integer.parseInt(worstTac.getMarketingName()), is(MostAttachedFailuresTablesPopulator.UNKNOWN_TAC));
        assertThat(worstTac.getNoErrors(), is(8));
    }

    @Test
    public void testGetTerminalMostAttachedFailuresDataForUnknownTac_1Week() throws Exception {
        setupTerminalAndGroupAnalysisResource();
        populator.populateTemporaryTablesForLast48HoursWithUnknownTac();
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(TIME_QUERY_PARAM, ONE_WEEK);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        final String uriPath = TERMINAL_ANALYSIS + "/" + MOST_ATTACHED_FAILURES;
        DummyUriInfoImpl.setUriInfo(map, terminalAndGroupAnalysisResource, SAMPLE_BASE_URI, uriPath);

        final String result = terminalAndGroupAnalysisResource.getMostAttachedFailuresData();
        System.out.println(result);
        final List<MostAttachedFailuresTerminalResult> results = getTranslator().translateResult(result, MostAttachedFailuresTerminalResult.class);
        assertThat(results.size(), is(1));
        final MostAttachedFailuresTerminalResult worstTac = results.get(0);
        assertThat(worstTac.getTac(), is(MostAttachedFailuresTablesPopulator.UNKNOWN_TAC));
        assertThat(Integer.parseInt(worstTac.getManufacturer()), is(MostAttachedFailuresTablesPopulator.UNKNOWN_TAC));
        assertThat(Integer.parseInt(worstTac.getMarketingName()), is(MostAttachedFailuresTablesPopulator.UNKNOWN_TAC));
        assertThat(worstTac.getNoErrors(), is(10));
    }

    private void setupTerminalAndGroupAnalysisResource() throws Exception, SQLException {
        terminalAndGroupAnalysisResource = new TerminalAndGroupAnalysisResource();
        attachDependencies(terminalAndGroupAnalysisResource);
        terminalAndGroupAnalysisResource.setTechPackCXCMapping(techPackCXCMappingService);
        populator = new MostAttachedFailuresTablesPopulator(connection);
        populator.createTemporaryTables();
        populator.populateTemporaryGroupTable();
    }

    private void validateResult(final String result) throws Exception {
        final List<MostAttachedFailuresTerminalResult> results = getTranslator().translateResult(result, MostAttachedFailuresTerminalResult.class);
        assertThat(results.size(), is(2));
        final MostAttachedFailuresTerminalResult worstTac = results.get(0);
        assertThat(worstTac.getTac(), is(MostAttachedFailuresTablesPopulator.WORST_TAC));
        assertThat(worstTac.getManufacturer(), is(MostAttachedFailuresTablesPopulator.MANFACTURER_FOR_WORST_TAC));
        assertThat(worstTac.getMarketingName(), is(MostAttachedFailuresTablesPopulator.MARKETING_NAME_FOR_WORST_TAC));
        assertThat(worstTac.getNoErrors(), is(2));

    }
}
