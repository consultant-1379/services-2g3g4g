/*------------------------------------------------------------------------------
 *******************************************************************************
 * COPYRIGHT Ericsson 2015
 *
 * The copyright to the computer program(s) herein is the property of
 * Ericsson Inc. The programs may be used and/or copied only with written
 * permission from Ericsson Inc. or in accordance with the terms and
 * conditions stipulated in the agreement/contract under which the
 * program(s) have been supplied.
 *******************************************************************************
 *----------------------------------------------------------------------------*/

package com.ericsson.eniq.events.server.resources.automation.dataproviders;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.test.automation.util.CombinationUtils.*;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.*;

import com.ericsson.eniq.events.server.test.automation.util.CombinationGenerator;
import com.ericsson.eniq.events.server.test.automation.util.CombinationGeneratorImpl;

public class CauseCodeAnalysisTestDataProvider {

    private static final String TEST_CELL_NODE = "LTECELL1,ERBS1,ERICSSON,LTE,X";

    private static final String CAUSE_CODE = "65";
    private static final String TIME_FROM = "1500";

    private static final String TIME_TO = "1600";

    private static final String DATE_FROM = "11052010";

    private static final String DATE_TO = "12052010";

    public static Object[] provideTestDataForAPN() {
        final CombinationGenerator<String> combinationGenerator = new CombinationGeneratorImpl.Builder<String>().add(DISPLAY_PARAM, GRID_PARAM)
                .add(TYPE_PARAM, TYPE_APN).add(NODE_PARAM, SAMPLE_APN).add(TIME_QUERY_PARAM, THIRTY_MINUTES, ONE_DAY, ONE_WEEK)
                .add(TZ_OFFSET, TEST_VALUE_TIMEZONE_OFFSET).add(MAX_ROWS, DEFAULT_MAX_ROWS, NO_MAX_ROWS).build();
        return convertToArrayOfMultivaluedMap(combinationGenerator.getAllCombinations());
    }

    public static Object[] provideTestDataForSGSN() {
        final CombinationGenerator<String> combinationGenerator = new CombinationGeneratorImpl.Builder<String>().add(DISPLAY_PARAM, GRID_PARAM)
                .add(TYPE_PARAM, TYPE_SGSN).add(NODE_PARAM, SAMPLE_SGSN).add(TIME_QUERY_PARAM, THIRTY_MINUTES, ONE_DAY, ONE_WEEK)
                .add(TZ_OFFSET, TEST_VALUE_TIMEZONE_OFFSET).add(MAX_ROWS, DEFAULT_MAX_ROWS, NO_MAX_ROWS).build();
        return convertToArrayOfMultivaluedMap(combinationGenerator.getAllCombinations());
    }

    public static Object[] provideTestDataForBSC() {
        final CombinationGenerator<String> combinationGenerator = new CombinationGeneratorImpl.Builder<String>().add(DISPLAY_PARAM, GRID_PARAM)
                .add(TYPE_PARAM, TYPE_BSC).add(NODE_PARAM, TEST_BSC1_NODE).add(TIME_QUERY_PARAM, THIRTY_MINUTES, ONE_DAY, ONE_WEEK)
                .add(TZ_OFFSET, TEST_VALUE_TIMEZONE_OFFSET).add(MAX_ROWS, DEFAULT_MAX_ROWS, NO_MAX_ROWS).build();
        return convertToArrayOfMultivaluedMap(combinationGenerator.getAllCombinations());
    }

    public static Object[] provideTestDataForCell() {
        final CombinationGenerator<String> combinationGenerator = new CombinationGeneratorImpl.Builder<String>().add(DISPLAY_PARAM, GRID_PARAM)
                .add(TYPE_PARAM, TYPE_CELL).add(NODE_PARAM, TEST_CELL_NODE).add(TIME_QUERY_PARAM, THIRTY_MINUTES, ONE_DAY, ONE_WEEK)
                .add(TZ_OFFSET, TEST_VALUE_TIMEZONE_OFFSET).add(MAX_ROWS, DEFAULT_MAX_ROWS, NO_MAX_ROWS).build();
        return convertToArrayOfMultivaluedMap(combinationGenerator.getAllCombinations());
    }

    public static Object[] provideTestDataForNoType() {
        final CombinationGenerator<String> combinationGenerator = new CombinationGeneratorImpl.Builder<String>().add(DISPLAY_PARAM, GRID_PARAM)
                .add(CAUSE_CODE_PARAM, CAUSE_CODE).add(CAUSE_PROT_TYPE).add(CAUSE_PROT_TYPE_HEADER)
                .add(TIME_QUERY_PARAM, THIRTY_MINUTES, ONE_DAY, ONE_WEEK).add(TZ_OFFSET, TEST_VALUE_TIMEZONE_OFFSET)
                .add(MAX_ROWS, DEFAULT_MAX_ROWS, NO_MAX_ROWS).build();
        return convertToArrayOfMultivaluedMap(combinationGenerator.getAllCombinations());
    }

    public static Object[] provideTestDataForTimeAndDatesFromAndTo() {
        final CombinationGenerator<String> combinationGenerator = new CombinationGeneratorImpl.Builder<String>().add(DISPLAY_PARAM, GRID_PARAM)
                .add(TIME_FROM_QUERY_PARAM, TIME_FROM).add(DATE_FROM_QUERY_PARAM, DATE_FROM).add(TIME_TO_QUERY_PARAM, TIME_TO)
                .add(DATE_TO_QUERY_PARAM, DATE_TO).add(TZ_OFFSET, TEST_VALUE_TIMEZONE_OFFSET).add(CAUSE_PROT_TYPE).add(CAUSE_PROT_TYPE_HEADER)
                .add(MAX_ROWS, DEFAULT_MAX_ROWS).build();
        return convertToArrayOfMultivaluedMap(combinationGenerator.getAllCombinations());
    }
}
