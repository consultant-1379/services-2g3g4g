/*
 * -----------------------------------------------------------------------
 *     Copyright (C) 2012 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.resources.automation.dataproviders;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.test.automation.util.CombinationUtils.*;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.*;

import com.ericsson.eniq.events.server.test.automation.util.CombinationGenerator;
import com.ericsson.eniq.events.server.test.automation.util.CombinationGeneratorImpl;

/**
 * Desrible RoamingDrillByCountryDataProvider
 * 
 * @author ezhelao
 * @since 02/2012
 */
public class RoamingDrillByCountryDataProvider {
    public static Object[] provideTestDataForRoamingDrill() {
        final CombinationGenerator<String> combinationGenerator = new CombinationGeneratorImpl.Builder<String>().add(DISPLAY_PARAM, CHART_PARAM)
                .add(TIME_QUERY_PARAM, THIRTY_MINUTES, ONE_DAY, ONE_WEEK).add(TZ_OFFSET, TEST_VALUE_TIMEZONE_OFFSET)
                .add(MAX_ROWS, DEFAULT_MAX_ROWS, NO_MAX_ROWS).add(MCC_PARAM, "310").build();
        return convertToArrayOfMultivaluedMap(combinationGenerator.getAllCombinations());
    }

}
