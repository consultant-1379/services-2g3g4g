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
import java.util.*;
import javax.ws.rs.core.MultivaluedMap;
import org.junit.Test;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.*;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import com.ericsson.eniq.events.server.resources.*;
import com.ericsson.eniq.events.server.test.queryresults.CauseCodeTableCCSummaryResult;
import com.ericsson.eniq.events.server.test.stubs.DummyUriInfoImpl;
import com.sun.jersey.core.util.MultivaluedMapImpl;

public class CauseCodeTablesCCResourceTest extends 
TestsWithTemporaryTablesBaseTestCase<CauseCodeTableCCSummaryResult> {

    private CauseCodeTablesCCResource objToTest;

    private MultivaluedMap<String, String> map;

    private static final String DISPLAY_TYPE = GRID_PARAM;

    private static final String MAX_ROWS = "maxRows";

    private static final String MAX_ROWS_VALUE = "50";
    
    private final static String CAUSE_CODE_VALUE_FOR_SGEH = "Success";
    private final static String CAUSE_CODE_VALUE_FOR_LTE = "This cause is sent when the PDN connection is released due to LTE generated reasons.";
    private final static String CAUSE_CODE_HELP_FOR_SGEH = "help";
    private final static String CAUSE_CODE_HELP_FOR_LTE = "text";
    
    

    @Override
    public void onSetUp() throws Exception {
        objToTest = new CauseCodeTablesCCResource();
        attachDependencies(objToTest);
        map = new MultivaluedMapImpl();
        createAndPopulateLookupTables();
    }

    @Test
    public void testCauseCodeTablesData() throws Exception {
        map.putSingle(DISPLAY_PARAM, DISPLAY_TYPE);
        map.putSingle(MAX_ROWS, MAX_ROWS_VALUE);
        map.putSingle(TZ_OFFSET, "+0000");
        DummyUriInfoImpl.setUriInfo(map, objToTest);
        final String json = objToTest.getData();
        final List<CauseCodeTableCCSummaryResult> summaryResult = getTranslator()
                .translateResult(json, CauseCodeTableCCSummaryResult.class);

        assertThat(summaryResult.size(), is(17));
        assertThat(summaryResult.get(0).getCauseCode(), is(CAUSE_CODE_VALUE_FOR_SGEH));
        assertThat(summaryResult.get(0).getCauseCodeHelp(), is(CAUSE_CODE_HELP_FOR_SGEH));
        
        assertThat(summaryResult.get(5).getCauseCode(), is(CAUSE_CODE_VALUE_FOR_LTE));
        assertThat(summaryResult.get(5).getCauseCodeHelp(), is(CAUSE_CODE_HELP_FOR_LTE));
        
    }

    private void createAndPopulateLookupTables(){

        final List<String> lookupTables = new ArrayList<String>();
        lookupTables.add(TEMP_DIM_E_SGEH_CAUSE_PROT_TYPE);
        lookupTables.add(TEMP_DIM_E_SGEH_CAUSECODE);
        lookupTables.add(TEMP_DIM_E_LTE_CAUSE_PROT_TYPE);
        lookupTables.add(TEMP_DIM_E_LTE_CAUSECODE);
        for (final String lookupTableRequired : lookupTables) {
            try {
                createAndPopulateLookupTable(lookupTableRequired);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
