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

import com.ericsson.eniq.events.server.test.sql.SQLCommand;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.*;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.*;

public class RawTablesPopulatorForTerminalGroupAnalysis {

    public static final String MARKETING_NAME_FOR_MOST_POPULAR_TAC = "Zoarmon";

    public static final String MANUFACTURER_FOR_MOST_POPULAR_TAC = "Intel";

    public static final int MOST_POPULAR_TAC = 102200;

    public static final String MOST_POPULAR_TAC_GROUP = "mostPopularTacGroup";

    private static final int IMSI1 = 123456789;

    public static final int SECOND_MOST_POPULAR_TAC = 106900;

    public static final String MARKETING_NAME_FOR_SECOND_MOST_POPULAR_TAC = "R100";

    public static final String MANUFACTURER_FOR_SECOND_MOST_POPULAR_TAC = "Firefly Mobile";

    public static final String SECOND_MOST_POPULAR_TAC_GROUP = "secondMostPopularTacGroup";

    private static final int IMSI2 = 654654;

    public static final String THIRD_MOST_POPULAR_TAC_GROUP = "thirdMostPopularTacGroup";

    public static final int THIRD_MOST_POPULAR_TAC = 789789;

    public static final int UNKNOWN_TAC = 65535;

    public final static int noSuccessesForMostPopularTac = 2;
    public final static int noSuccessesForSecondMostPopularTac = 1;
    public final static int noSuccessesForThirdMostPopularTac = 1;

    public final static int noSuccessesForMostPopularTacInSGEHIMSISucRaw = 6;
    public final static int noSuccessesForMostPopularTacInLTEIMSISucRaw = 4;

    private final Connection connection;

    public RawTablesPopulatorForTerminalGroupAnalysis(final Connection connection) {
        this.connection = connection;
    }

    public void createTemporaryTables() throws Exception {
        final List<String> columnsForRawTable = new ArrayList<String>();
        columnsForRawTable.add(IMSI);
        columnsForRawTable.add(DATETIME_ID);
        columnsForRawTable.add(TAC);
        final List<String> columnsForTacAggregationTable = new ArrayList<String>();
        columnsForTacAggregationTable.add(NO_OF_SUCCESSES);
        columnsForTacAggregationTable.add(NO_OF_ERRORS);
        columnsForTacAggregationTable.add(TAC);
        columnsForTacAggregationTable.add(DATETIME_ID);
        final List<String> columnsForIMSISucRawTable = new ArrayList<String>();
        columnsForIMSISucRawTable.add(NO_OF_SUCCESSES);
        columnsForIMSISucRawTable.add(IMSI);
        columnsForIMSISucRawTable.add(TAC);
        columnsForIMSISucRawTable.add(DATETIME_ID);
        final SQLCommand sqlCommand = new SQLCommand(connection);
        sqlCommand.createTemporaryTable(TEMP_EVENT_E_SGEH_ERR_RAW, columnsForRawTable);
        sqlCommand.createTemporaryTable(TEMP_EVENT_E_SGEH_SUC_RAW, columnsForRawTable);
        sqlCommand.createTemporaryTable(TEMP_EVENT_E_LTE_ERR_RAW, columnsForRawTable);
        sqlCommand.createTemporaryTable(TEMP_EVENT_E_LTE_SUC_RAW, columnsForRawTable);
        sqlCommand.createTemporaryTable(TEMP_EVENT_E_SGEH_MANUF_TAC_SUC_15MIN, columnsForTacAggregationTable);
        sqlCommand.createTemporaryTable(TEMP_EVENT_E_LTE_MANUF_TAC_SUC_15MIN, columnsForTacAggregationTable);
        sqlCommand.createTemporaryTable(TEMP_EVENT_E_SGEH_IMSI_SUC_RAW, columnsForIMSISucRawTable);
        sqlCommand.createTemporaryTable(TEMP_EVENT_E_LTE_IMSI_SUC_RAW, columnsForIMSISucRawTable);
    }

    public void populateTemporaryTables(final String timeStamp) throws SQLException {
        insertRowIntoRawTable(TEMP_EVENT_E_SGEH_ERR_RAW, MOST_POPULAR_TAC, IMSI1, timeStamp);
        insertRowIntoRawTable(TEMP_EVENT_E_SGEH_SUC_RAW, MOST_POPULAR_TAC, IMSI1, timeStamp);
        insertRowIntoRawTable(TEMP_EVENT_E_LTE_ERR_RAW, MOST_POPULAR_TAC, IMSI2, timeStamp);
        insertRowIntoRawTable(TEMP_EVENT_E_LTE_SUC_RAW, MOST_POPULAR_TAC, IMSI2, timeStamp);
        populateTACGroupTable(MOST_POPULAR_TAC_GROUP, MOST_POPULAR_TAC);

        insertRowIntoRawTable(TEMP_EVENT_E_SGEH_SUC_RAW, SECOND_MOST_POPULAR_TAC, IMSI1, timeStamp);
        insertRowIntoRawTable(TEMP_EVENT_E_LTE_SUC_RAW, SECOND_MOST_POPULAR_TAC, IMSI1, timeStamp);
        insertRowIntoRawTable(TEMP_EVENT_E_LTE_SUC_RAW, SECOND_MOST_POPULAR_TAC, IMSI1, timeStamp);
        populateTACGroupTable(SECOND_MOST_POPULAR_TAC_GROUP, SECOND_MOST_POPULAR_TAC);

        insertRowIntoRawTable(TEMP_EVENT_E_SGEH_SUC_RAW, THIRD_MOST_POPULAR_TAC, IMSI1, timeStamp);
        insertRowIntoRawTable(TEMP_EVENT_E_SGEH_ERR_RAW, THIRD_MOST_POPULAR_TAC, IMSI1, timeStamp);
        populateTACGroupTable(THIRD_MOST_POPULAR_TAC_GROUP, THIRD_MOST_POPULAR_TAC);

        insertRowIntoRawTable(TEMP_EVENT_E_LTE_ERR_RAW, SAMPLE_EXCLUSIVE_TAC, IMSI2, timeStamp);
        insertRowIntoRawTable(TEMP_EVENT_E_SGEH_SUC_RAW, SAMPLE_EXCLUSIVE_TAC, IMSI2, timeStamp);

        insertRowIntoAggTable(TEMP_EVENT_E_SGEH_MANUF_TAC_SUC_15MIN, MOST_POPULAR_TAC, 0, noSuccessesForMostPopularTac, timeStamp);

        insertRowIntoAggTable(TEMP_EVENT_E_SGEH_MANUF_TAC_SUC_15MIN, SECOND_MOST_POPULAR_TAC, 0, noSuccessesForSecondMostPopularTac, timeStamp);
        insertRowIntoAggTable(TEMP_EVENT_E_LTE_MANUF_TAC_SUC_15MIN, SECOND_MOST_POPULAR_TAC, 0, noSuccessesForMostPopularTac, timeStamp);

        insertRowIntoAggTable(TEMP_EVENT_E_SGEH_MANUF_TAC_SUC_15MIN, THIRD_MOST_POPULAR_TAC, 0, noSuccessesForThirdMostPopularTac, timeStamp);

        insertRowIntoIMSISucRawTable(TEMP_EVENT_E_SGEH_IMSI_SUC_RAW, IMSI2, SAMPLE_EXCLUSIVE_TAC, noSuccessesForMostPopularTacInSGEHIMSISucRaw,
                timeStamp);
        insertRowIntoIMSISucRawTable(TEMP_EVENT_E_LTE_IMSI_SUC_RAW, IMSI2, SAMPLE_EXCLUSIVE_TAC, noSuccessesForMostPopularTacInLTEIMSISucRaw,
                timeStamp);

        insertRowIntoAggTable(TEMP_EVENT_E_SGEH_MANUF_TAC_SUC_15MIN, SAMPLE_EXCLUSIVE_TAC, 0, noSuccessesForMostPopularTacInSGEHIMSISucRaw, timeStamp);
        insertRowIntoAggTable(TEMP_EVENT_E_LTE_MANUF_TAC_SUC_15MIN, SAMPLE_EXCLUSIVE_TAC, 0, noSuccessesForMostPopularTacInLTEIMSISucRaw, timeStamp);

    }

    public void populateTemporaryTablesForUnknownTAC(final String timeStamp) throws SQLException {
        populateTACGroupTable(EXCLUSIVE_TAC_GROUP, UNKNOWN_TAC);
        insertRowIntoIMSISucRawTable(TEMP_EVENT_E_SGEH_IMSI_SUC_RAW, IMSI1, UNKNOWN_TAC, 15, timeStamp);
        insertRowIntoRawTable(TEMP_EVENT_E_SGEH_SUC_RAW, UNKNOWN_TAC, IMSI1, timeStamp);
        insertRowIntoRawTable(TEMP_EVENT_E_SGEH_SUC_RAW, UNKNOWN_TAC, IMSI1, timeStamp);
        insertRowIntoRawTable(TEMP_EVENT_E_SGEH_SUC_RAW, UNKNOWN_TAC, IMSI1, timeStamp);
        insertRowIntoRawTable(TEMP_EVENT_E_SGEH_SUC_RAW, UNKNOWN_TAC, IMSI1, timeStamp);
        insertRowIntoRawTable(TEMP_EVENT_E_SGEH_SUC_RAW, UNKNOWN_TAC, IMSI1, timeStamp);
        insertRowIntoRawTable(TEMP_EVENT_E_SGEH_ERR_RAW, UNKNOWN_TAC, IMSI1, timeStamp);
    }

    private void populateTACGroupTable(final String tacGroup, final int tac) throws SQLException {
        final Map<String, Object> valuesForTacGroupTable = new HashMap<String, Object>();
        valuesForTacGroupTable.put(GROUP_NAME, tacGroup);
        valuesForTacGroupTable.put(TAC, tac);
        new SQLCommand(connection).insertRow(TEMP_GROUP_TYPE_E_TAC, valuesForTacGroupTable);
    }

    private void insertRowIntoRawTable(final String table, final int tac, final int imsi, final String timeStamp) throws SQLException {
        final Map<String, Object> valuesForRawTable = new HashMap<String, Object>();
        valuesForRawTable.put(TAC, tac);
        valuesForRawTable.put(IMSI, imsi);
        valuesForRawTable.put(DATETIME_ID, timeStamp);
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

    private void insertRowIntoIMSISucRawTable(final String table, final int imsi, final int tac, final int noSuccesses, final String timeStamp)
            throws SQLException {
        final Map<String, Object> valuesForIMSISucRawTable = new HashMap<String, Object>();
        valuesForIMSISucRawTable.put(NO_OF_SUCCESSES, noSuccesses);
        valuesForIMSISucRawTable.put(IMSI, imsi);
        valuesForIMSISucRawTable.put(TAC, tac);
        valuesForIMSISucRawTable.put(DATETIME_ID, timeStamp);
        new SQLCommand(connection).insertRow(table, valuesForIMSISucRawTable);
    }
}