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
package com.ericsson.eniq.events.server.integritytests.terminalgroupanalysis.mostpopulareventsummary;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.*;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.*;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;

import com.ericsson.eniq.events.server.test.sql.SQLCommand;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;

/**
 * Populate temporary tables with data required for longer time ranges - ie add data to the aggregation tables, plus the matching data in the raw
 * tables for joins on the raw tables
 * 
 */
public class TablesPopulatorForOneWeekQuery {

    private static final int noSuccessesForExclusiveTacsInSgeh15MinTable = 2;
    private static final int noSuccessesForExclusiveTacsInLte15MinTable = 2;
    private static final int noErrorsForExclusiveTacsInLte15MinTable = 3;
    private static final int noSuccessesForExclusiveTacsInSgehDayTable = 2;
    private static final int noSuccessesForExclusiveTacsInLteDayTable = 2;
    private static final int noErrorsForExclusiveTacsInLteDayTable = 3;
    public static final String MOST_POPULAR_TAC_GROUP = "mostPopularTacGroup";
    public final static String SECOND_MOST_POPULAR_TAC_GROUP = "secondMostPopularTacGroup";
    public static final String THIRD_MOST_POPULAR_TAC_GROUP = "thirdMostPopularTacGroup";

    static final int IMSI1 = 123456789;

    public static final int MOST_POPULAR_TAC = 102000;
    public static final int SECOND_MOST_POPULAR_TAC = 100100;
    public static final int THIRD_MOST_POPULAR_TAC = 789789;
    public static final int UNKNOWN_TAC = 65535;

    public static final String MANUFACTURER_FOR_MOST_POPULAR_TAC = "RIM";
    public static final String MARKETING_NAME_FOR_MOST_POPULAR_TAC = "RAP40GW";
    public static final String MANUFACTURER_FOR_SECOND_MOST_POPULAR_TAC = "Mitsubishi";
    public static final String MARKETING_NAME_FOR_SECOND_MOST_POPULAR_TAC = "G410";

    /* Values for DAY Tables */

    public final static int noErrorsForMostPopularTacInSgehDayTable = 4;
    public final static int noOfTotalErrSubscribersForMostPopularTacInSgehDayTable = 2;
    public final static int noSuccessesForMostPopularTacInSgehDayTable = 3;

    public final static int noErrorsForMostPopularTacInLTEDayTable = 1;
    public final static int noOfTotalErrSubscribersForMostPopularTacInLTEDayTable = 1;
    public final static int noSuccessesForMostPopularTacInLTEDayTable = 1;

    public final static int noSuccessesForSecondMostPopularTacInSgehDayTable = 0;

    public final static int noSuccessesForSecondMostPopularTacInLTEDayTable = 6;

    public final static int noErrorsForThirdMostPopularTacInLTEDayTable = 8;
    public final static int noOfTotalErrSubscribersForThirdMostPopularTacInLTEDayTable = 2;

    /* Values for 15 MIN Tables */

    public final static int noErrorsForMostPopularTacInSgeh15MinTable = 5;
    public final static int noSuccessesForMostPopularTacInSgeh15MinTable = 2;

    public final static int noErrorsForMostPopularTacInLTE15MinTable = 4;
    public final static int noSuccessesForMostPopularTacInLTE15MinTable = 0;

    public final static int noSuccessesForSecondMostPopularTacInSgeh15MinTable = 4;

    public final static int noSuccessesForSecondMostPopularTacInLTE15MinTable = 22;

    public final static int noErrorsForThirdMostPopularTacInLTE15MinTable = 4;

    /* Valuse for IMSI Suc Raw */

    public final static int noOfSuccessesForExclusiveTacInSGEHIMSISucRawFor30Minutes = 22;
    public final static int noOfSuccessesForExclusiveTacInLTEIMSISucRawFor30Minutes = 11;
    public final static int noOfSuccessesForExclusiveTacInSGEHIMSISucRawForOneWeek = 3;
    public final static int noOfSuccessesForExclusiveTacInLTEIMSISucRawForOneWeek = 9;

    private final Connection connection;

    public TablesPopulatorForOneWeekQuery(final Connection connection) {
        this.connection = connection;
    }

    public void createTemporaryTables() throws Exception {
        final List<String> columnsForTacAggregationTable = new ArrayList<String>();
        columnsForTacAggregationTable.add(NO_OF_SUCCESSES);
        columnsForTacAggregationTable.add(NO_OF_ERRORS);
        columnsForTacAggregationTable.add(TAC);
        columnsForTacAggregationTable.add(DATETIME_ID);

        final SQLCommand sqlCommand = new SQLCommand(connection);

        sqlCommand.createTemporaryTable(TEMP_EVENT_E_SGEH_MANUF_TAC_ERR_DAY, columnsForTacAggregationTable);
        sqlCommand.createTemporaryTable(TEMP_EVENT_E_SGEH_MANUF_TAC_SUC_DAY, columnsForTacAggregationTable);
        sqlCommand.createTemporaryTable(TEMP_EVENT_E_LTE_MANUF_TAC_ERR_DAY, columnsForTacAggregationTable);
        sqlCommand.createTemporaryTable(TEMP_EVENT_E_LTE_MANUF_TAC_SUC_DAY, columnsForTacAggregationTable);

        sqlCommand.createTemporaryTable(TEMP_EVENT_E_SGEH_MANUF_TAC_ERR_15MIN, columnsForTacAggregationTable);
        sqlCommand.createTemporaryTable(TEMP_EVENT_E_SGEH_MANUF_TAC_SUC_15MIN, columnsForTacAggregationTable);
        sqlCommand.createTemporaryTable(TEMP_EVENT_E_LTE_MANUF_TAC_ERR_15MIN, columnsForTacAggregationTable);
        sqlCommand.createTemporaryTable(TEMP_EVENT_E_LTE_MANUF_TAC_SUC_15MIN, columnsForTacAggregationTable);

        final List<String> columnsForRawTable = new ArrayList<String>();
        columnsForRawTable.add(IMSI);
        columnsForRawTable.add(DATETIME_ID);
        columnsForRawTable.add(LOCAL_DATE_ID);
        columnsForRawTable.add(TAC);
        sqlCommand.createTemporaryTable(TEMP_EVENT_E_SGEH_ERR_RAW, columnsForRawTable);
        sqlCommand.createTemporaryTable(TEMP_EVENT_E_LTE_ERR_RAW, columnsForRawTable);
        sqlCommand.createTemporaryTable(TEMP_EVENT_E_SGEH_SUC_RAW, columnsForRawTable);
        sqlCommand.createTemporaryTable(TEMP_EVENT_E_LTE_SUC_RAW, columnsForRawTable);

        final List<String> columnsForIMSISucRawTable = new ArrayList<String>();
        columnsForIMSISucRawTable.add(NO_OF_SUCCESSES);
        columnsForIMSISucRawTable.add(IMSI);
        columnsForIMSISucRawTable.add(TAC);
        columnsForIMSISucRawTable.add(DATETIME_ID);
        sqlCommand.createTemporaryTable(TEMP_EVENT_E_LTE_IMSI_SUC_RAW, columnsForIMSISucRawTable);
        sqlCommand.createTemporaryTable(TEMP_EVENT_E_SGEH_IMSI_SUC_RAW, columnsForIMSISucRawTable);
    }

    public void populateTemporaryTables() throws SQLException {
        String timeStamp = DateTimeUtilities.getDateTimeMinus48Hours();
        insertRowIntoAggTable(TEMP_EVENT_E_SGEH_MANUF_TAC_ERR_DAY, MOST_POPULAR_TAC, noErrorsForMostPopularTacInSgehDayTable, 0, timeStamp);
        insertRowIntoAggTable(TEMP_EVENT_E_SGEH_MANUF_TAC_SUC_DAY, MOST_POPULAR_TAC, 0, noSuccessesForMostPopularTacInSgehDayTable, timeStamp);
        insertRowIntoAggTable(TEMP_EVENT_E_LTE_MANUF_TAC_ERR_DAY, MOST_POPULAR_TAC, noErrorsForMostPopularTacInLTEDayTable, 0, timeStamp);
        insertRowIntoAggTable(TEMP_EVENT_E_LTE_MANUF_TAC_SUC_DAY, MOST_POPULAR_TAC, 0, noSuccessesForMostPopularTacInLTEDayTable, timeStamp);
        insertRowIntoAggTable(TEMP_EVENT_E_SGEH_MANUF_TAC_SUC_DAY, SECOND_MOST_POPULAR_TAC, 0, noSuccessesForSecondMostPopularTacInSgehDayTable,
                timeStamp);
        insertRowIntoAggTable(TEMP_EVENT_E_LTE_MANUF_TAC_SUC_DAY, SECOND_MOST_POPULAR_TAC, 0, noSuccessesForSecondMostPopularTacInLTEDayTable,
                timeStamp);
        insertRowIntoAggTable(TEMP_EVENT_E_LTE_MANUF_TAC_ERR_DAY, THIRD_MOST_POPULAR_TAC, noErrorsForThirdMostPopularTacInLTEDayTable, 0, timeStamp);

        insertRowIntoAggTable(TEMP_EVENT_E_LTE_MANUF_TAC_ERR_DAY, SAMPLE_EXCLUSIVE_TAC, noErrorsForExclusiveTacsInLteDayTable, 0, timeStamp);
        insertRowIntoAggTable(TEMP_EVENT_E_LTE_MANUF_TAC_SUC_DAY, SAMPLE_EXCLUSIVE_TAC, 0, noSuccessesForExclusiveTacsInLteDayTable, timeStamp);
        insertRowIntoAggTable(TEMP_EVENT_E_SGEH_MANUF_TAC_ERR_DAY, SAMPLE_EXCLUSIVE_TAC, noSuccessesForExclusiveTacsInSgehDayTable, 0, timeStamp);

        timeStamp = DateTimeUtilities.getDateTimeMinus30Minutes();

        insertRowIntoAggTable(TEMP_EVENT_E_SGEH_MANUF_TAC_ERR_15MIN, MOST_POPULAR_TAC, noErrorsForMostPopularTacInSgeh15MinTable, 0, timeStamp);
        insertRowIntoAggTable(TEMP_EVENT_E_SGEH_MANUF_TAC_SUC_15MIN, MOST_POPULAR_TAC, 0, noSuccessesForMostPopularTacInSgeh15MinTable, timeStamp);
        insertRowIntoAggTable(TEMP_EVENT_E_LTE_MANUF_TAC_ERR_15MIN, MOST_POPULAR_TAC, noErrorsForMostPopularTacInLTE15MinTable, 0, timeStamp);
        insertRowIntoAggTable(TEMP_EVENT_E_LTE_MANUF_TAC_SUC_15MIN, MOST_POPULAR_TAC, 0, noSuccessesForMostPopularTacInLTE15MinTable, timeStamp);
        insertRowIntoAggTable(TEMP_EVENT_E_SGEH_MANUF_TAC_SUC_15MIN, SECOND_MOST_POPULAR_TAC, 0, noSuccessesForSecondMostPopularTacInSgeh15MinTable,
                timeStamp);
        insertRowIntoAggTable(TEMP_EVENT_E_LTE_MANUF_TAC_SUC_15MIN, SECOND_MOST_POPULAR_TAC, 0, noSuccessesForSecondMostPopularTacInLTE15MinTable,
                timeStamp);
        insertRowIntoAggTable(TEMP_EVENT_E_LTE_MANUF_TAC_ERR_15MIN, THIRD_MOST_POPULAR_TAC, noErrorsForThirdMostPopularTacInLTE15MinTable, 0,
                timeStamp);
        insertRowIntoAggTable(TEMP_EVENT_E_LTE_MANUF_TAC_ERR_15MIN, SAMPLE_EXCLUSIVE_TAC, noErrorsForExclusiveTacsInLte15MinTable, 0, timeStamp);
        insertRowIntoAggTable(TEMP_EVENT_E_LTE_MANUF_TAC_SUC_15MIN, SAMPLE_EXCLUSIVE_TAC, 0, noSuccessesForExclusiveTacsInLte15MinTable, timeStamp);
        insertRowIntoAggTable(TEMP_EVENT_E_SGEH_MANUF_TAC_ERR_15MIN, SAMPLE_EXCLUSIVE_TAC, noSuccessesForExclusiveTacsInSgeh15MinTable, 0, timeStamp);
        timeStamp = DateTimeUtilities.getDateTimeMinus48Hours();

        insertRowIntoRawTable(TEMP_EVENT_E_SGEH_ERR_RAW, MOST_POPULAR_TAC, timeStamp);
        insertRowIntoRawTable(TEMP_EVENT_E_LTE_ERR_RAW, MOST_POPULAR_TAC, timeStamp);
        insertRowIntoRawTable(TEMP_EVENT_E_LTE_ERR_RAW, THIRD_MOST_POPULAR_TAC, timeStamp);
        insertRowIntoRawTable(TEMP_EVENT_E_LTE_ERR_RAW, THIRD_MOST_POPULAR_TAC, timeStamp);
        insertRowIntoRawTable(TEMP_EVENT_E_LTE_ERR_RAW, THIRD_MOST_POPULAR_TAC, timeStamp);
        insertRowIntoRawTable(TEMP_EVENT_E_LTE_ERR_RAW, THIRD_MOST_POPULAR_TAC, timeStamp);
        insertRowIntoRawTable(TEMP_EVENT_E_SGEH_ERR_RAW, SAMPLE_EXCLUSIVE_TAC, timeStamp);
        insertRowIntoRawTable(TEMP_EVENT_E_LTE_ERR_RAW, SAMPLE_EXCLUSIVE_TAC, timeStamp);
        insertRowIntoRawTable(TEMP_EVENT_E_LTE_SUC_RAW, SAMPLE_EXCLUSIVE_TAC, timeStamp);

        insertRowIntoIMSISucRawTable(TEMP_EVENT_E_LTE_IMSI_SUC_RAW, SAMPLE_EXCLUSIVE_TAC, noOfSuccessesForExclusiveTacInLTEIMSISucRawForOneWeek,
                timeStamp);
        insertRowIntoIMSISucRawTable(TEMP_EVENT_E_SGEH_IMSI_SUC_RAW, SAMPLE_EXCLUSIVE_TAC, noOfSuccessesForExclusiveTacInSGEHIMSISucRawForOneWeek,
                timeStamp);

        timeStamp = DateTimeUtilities.getDateTimeMinus30Minutes();

        insertRowIntoRawTable(TEMP_EVENT_E_SGEH_ERR_RAW, MOST_POPULAR_TAC, timeStamp);
        insertRowIntoRawTable(TEMP_EVENT_E_LTE_ERR_RAW, SAMPLE_EXCLUSIVE_TAC, timeStamp);
        insertRowIntoRawTable(TEMP_EVENT_E_LTE_ERR_RAW, THIRD_MOST_POPULAR_TAC, timeStamp);
        insertRowIntoRawTable(TEMP_EVENT_E_LTE_ERR_RAW, THIRD_MOST_POPULAR_TAC, timeStamp);
        insertRowIntoRawTable(TEMP_EVENT_E_LTE_ERR_RAW, THIRD_MOST_POPULAR_TAC, timeStamp);
        insertRowIntoRawTable(TEMP_EVENT_E_LTE_ERR_RAW, THIRD_MOST_POPULAR_TAC, timeStamp);
        insertRowIntoRawTable(TEMP_EVENT_E_SGEH_ERR_RAW, SAMPLE_EXCLUSIVE_TAC, timeStamp);
        insertRowIntoRawTable(TEMP_EVENT_E_LTE_ERR_RAW, SAMPLE_EXCLUSIVE_TAC, timeStamp);
        insertRowIntoRawTable(TEMP_EVENT_E_LTE_SUC_RAW, SAMPLE_EXCLUSIVE_TAC, timeStamp);

        insertRowIntoIMSISucRawTable(TEMP_EVENT_E_LTE_IMSI_SUC_RAW, SAMPLE_EXCLUSIVE_TAC, noOfSuccessesForExclusiveTacInLTEIMSISucRawFor30Minutes,
                timeStamp);
        insertRowIntoIMSISucRawTable(TEMP_EVENT_E_SGEH_IMSI_SUC_RAW, SAMPLE_EXCLUSIVE_TAC, noOfSuccessesForExclusiveTacInSGEHIMSISucRawFor30Minutes,
                timeStamp);

        populateTACGroupTable(MOST_POPULAR_TAC_GROUP, MOST_POPULAR_TAC);
        populateTACGroupTable(SECOND_MOST_POPULAR_TAC_GROUP, SECOND_MOST_POPULAR_TAC);
        populateTACGroupTable(THIRD_MOST_POPULAR_TAC_GROUP, THIRD_MOST_POPULAR_TAC);
    }

    public void populateTemporaryTablesForUnknownTAC() throws SQLException {
        final String timeStamp = DateTimeUtilities.getDateTimeMinus48Hours();
        populateTACGroupTable(EXCLUSIVE_TAC_GROUP, UNKNOWN_TAC);
        insertRowIntoIMSISucRawTable(TEMP_EVENT_E_SGEH_IMSI_SUC_RAW, UNKNOWN_TAC, 15, timeStamp);
        insertRowIntoRawTable(TEMP_EVENT_E_SGEH_SUC_RAW, UNKNOWN_TAC, timeStamp);
        insertRowIntoRawTable(TEMP_EVENT_E_SGEH_SUC_RAW, UNKNOWN_TAC, timeStamp);
        insertRowIntoRawTable(TEMP_EVENT_E_SGEH_SUC_RAW, UNKNOWN_TAC, timeStamp);
        insertRowIntoRawTable(TEMP_EVENT_E_SGEH_SUC_RAW, UNKNOWN_TAC, timeStamp);
        insertRowIntoRawTable(TEMP_EVENT_E_SGEH_SUC_RAW, UNKNOWN_TAC, timeStamp);
        insertRowIntoRawTable(TEMP_EVENT_E_SGEH_ERR_RAW, UNKNOWN_TAC, timeStamp);
    }

    private void populateTACGroupTable(final String tacGroup, final int tac) throws SQLException {
        final Map<String, Object> valuesForTacGroupTable = new HashMap<String, Object>();
        valuesForTacGroupTable.put(GROUP_NAME, tacGroup);
        valuesForTacGroupTable.put(TAC, tac);
        new SQLCommand(connection).insertRow(TEMP_GROUP_TYPE_E_TAC, valuesForTacGroupTable);
    }

    private void insertRowIntoRawTable(final String table, final int tac, final String timeStamp) throws SQLException {
        final Map<String, Object> valuesForRawTable = new HashMap<String, Object>();
        valuesForRawTable.put(TAC, tac);
        valuesForRawTable.put(IMSI, IMSI1);
        valuesForRawTable.put(DATETIME_ID, timeStamp);
        valuesForRawTable.put(LOCAL_DATE_ID, timeStamp.substring(0, 10));
        new SQLCommand(connection).insertRow(table, valuesForRawTable);
    }

    private void insertRowIntoAggTable(final String table, final int tac, final int noErrors, final int noSuccesses, final String timeStamp)
            throws SQLException {
        final Map<String, Object> valuesForAggTable = new HashMap<String, Object>();
        valuesForAggTable.put(NO_OF_ERRORS, noErrors);
        valuesForAggTable.put(NO_OF_SUCCESSES, noSuccesses);
        valuesForAggTable.put(TAC, tac);
        valuesForAggTable.put(DATETIME_ID, timeStamp);
        new SQLCommand(connection).insertRow(table, valuesForAggTable);
    }

    private void insertRowIntoIMSISucRawTable(final String table, final int tac, final int noSuccesses, final String timeStamp) throws SQLException {
        final Map<String, Object> valuesForIMSISucRawTable = new HashMap<String, Object>();
        valuesForIMSISucRawTable.put(NO_OF_SUCCESSES, noSuccesses);
        valuesForIMSISucRawTable.put(IMSI, IMSI1);
        valuesForIMSISucRawTable.put(TAC, tac);
        valuesForIMSISucRawTable.put(DATETIME_ID, timeStamp);
        new SQLCommand(connection).insertRow(table, valuesForIMSISucRawTable);
    }
}
