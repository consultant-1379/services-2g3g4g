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
package com.ericsson.eniq.events.server.integritytests.terminalgroupanalysis.mostattachedfailures;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.common.EventIDConstants.*;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.*;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.*;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;

import com.ericsson.eniq.events.server.resources.TerminalAndGroupAnalysisResource;
import com.ericsson.eniq.events.server.test.sql.SQLCommand;
import com.ericsson.eniq.events.server.test.util.DateTimeUtilities;

public class MostAttachedFailuresTablesPopulator {

    static final int WORST_TAC = 102900;
    static final int UNKNOWN_TAC = 99999999;

    static final String MARKETING_NAME_FOR_WORST_TAC = "Ferry";

    static final String MANFACTURER_FOR_WORST_TAC = "Quanta Computer";

    static final int IMSI1 = 123456789;

    static final String WORST_TAC_GROUP = "sampleTacGroup";

    static final String SECOND_WORST_TAC_GROUP = "secondWorstTacGroup";

    static final int SECOND_WORST_TAC = 107800;

    static final String MARKETING_NAME_FOR_SECOND_WORST_TAC = "0247910";

    static final String MANFACTURER_FOR_SECOND_WORST_TAC = "Garmin International";

    protected TerminalAndGroupAnalysisResource terminalAndGroupAnalysisResource;

    final static int noErrorsForWorstTacInDayTables = 5;

    final static int noErrorsForSecondWorstTacInDayTables = 3;

    final static int noErrorsForWorstTacIn15minTables = 4;

    final static int noErrorsForSecondWorstTacIn15minTables = 2;

    private final Connection connection;

    public MostAttachedFailuresTablesPopulator(final Connection connection) {
        this.connection = connection;
    }

    protected void populateTemporaryTablesForLast3Minutes() throws SQLException {
        final String timeStamp = DateTimeUtilities.getDateTimeMinus3Minutes();
        populateTemporaryTables(timeStamp);
    }

    protected void populateTemporaryTablesForLast30Minutes() throws SQLException {
        final String timeStamp = DateTimeUtilities.getDateTimeMinus30Minutes();
        populateTemporaryTables(timeStamp);
    }

    protected void populateTemporaryTablesForLast6hours() throws SQLException {
        final String timeStamp = DateTimeUtilities.getDateTimeMinusHours(6);
        populateTemporaryTables(timeStamp);
    }

    protected void populateTemporaryTablesForLast48Hours() throws SQLException {
        final String timeStamp = DateTimeUtilities.getDateTimeMinus48Hours();
        populateTemporaryTables(timeStamp);
    }

    protected void populateTemporaryTablesForLast3MinutesWithUnknownTac() throws SQLException {
        final String timeStamp = DateTimeUtilities.getDateTimeMinus3Minutes();
        populateTemporaryTablesWithUnknownTac(timeStamp);
    }

    protected void populateTemporaryTablesForLast30MinutesWithUnknownTac() throws SQLException {
        final String timeStamp = DateTimeUtilities.getDateTimeMinus30Minutes();
        populateTemporaryTablesWithUnknownTac(timeStamp);
    }

    protected void populateTemporaryTablesForLast6hoursWithUnknownTac() throws SQLException {
        final String timeStamp = DateTimeUtilities.getDateTimeMinusHours(6);
        populateTemporaryTablesWithUnknownTac(timeStamp);
    }

    protected void populateTemporaryTablesForLast48HoursWithUnknownTac() throws SQLException {
        final String timeStamp = DateTimeUtilities.getDateTimeMinus48Hours();
        populateTemporaryTablesWithUnknownTac(timeStamp);
    }

    protected void populateTemporaryGroupTable() throws SQLException {
        populateTACGroupTable(WORST_TAC_GROUP, WORST_TAC);
        populateTACGroupTable(WORST_TAC_GROUP, SAMPLE_EXCLUSIVE_TAC);
        populateTACGroupTable(SECOND_WORST_TAC_GROUP, SECOND_WORST_TAC);
    }

    private void populateTemporaryTables(final String timeStamp) throws SQLException {
        insertRowIntoAggTable(TEMP_EVENT_E_SGEH_MANUF_TAC_EVENTID_ERR_DAY, WORST_TAC, noErrorsForWorstTacInDayTables, timeStamp, ATTACH_IN_2G_AND_3G);
        insertRowIntoAggTable(TEMP_EVENT_E_LTE_MANUF_TAC_EVENTID_ERR_DAY, WORST_TAC, noErrorsForWorstTacInDayTables, timeStamp, ATTACH_IN_4G);
        insertRowIntoAggTable(TEMP_EVENT_E_SGEH_MANUF_TAC_EVENTID_ERR_15MIN, WORST_TAC, noErrorsForWorstTacIn15minTables, timeStamp,
                ATTACH_IN_2G_AND_3G);
        insertRowIntoAggTable(TEMP_EVENT_E_LTE_MANUF_TAC_EVENTID_ERR_15MIN, WORST_TAC, noErrorsForWorstTacIn15minTables, timeStamp, ATTACH_IN_4G);
        insertRowIntoRawTable(TEMP_EVENT_E_SGEH_ERR_RAW, WORST_TAC, timeStamp, ATTACH_IN_2G_AND_3G);
        insertRowIntoRawTable(TEMP_EVENT_E_LTE_ERR_RAW, WORST_TAC, timeStamp, ATTACH_IN_4G);

        insertRowIntoAggTable(TEMP_EVENT_E_SGEH_MANUF_TAC_EVENTID_ERR_DAY, SECOND_WORST_TAC, noErrorsForSecondWorstTacInDayTables, timeStamp,
                ATTACH_IN_2G_AND_3G);
        insertRowIntoAggTable(TEMP_EVENT_E_SGEH_MANUF_TAC_EVENTID_ERR_15MIN, SECOND_WORST_TAC, noErrorsForSecondWorstTacIn15minTables, timeStamp,
                ATTACH_IN_2G_AND_3G);
        insertRowIntoRawTable(TEMP_EVENT_E_SGEH_ERR_RAW, SECOND_WORST_TAC, timeStamp, ATTACH_IN_2G_AND_3G);

        insertRowIntoRawTable(TEMP_EVENT_E_LTE_ERR_RAW, SAMPLE_EXCLUSIVE_TAC, timeStamp, ATTACH_IN_4G);

        insertRowIntoAggTable(TEMP_EVENT_E_LTE_MANUF_TAC_EVENTID_ERR_DAY, SAMPLE_EXCLUSIVE_TAC_2, 1, timeStamp, ATTACH_IN_4G);
    }

    private void populateTemporaryTablesWithUnknownTac(final String timeStamp) throws SQLException {
        insertRowIntoAggTable(TEMP_EVENT_E_SGEH_MANUF_TAC_EVENTID_ERR_DAY, UNKNOWN_TAC, noErrorsForWorstTacInDayTables, timeStamp,
                ATTACH_IN_2G_AND_3G);
        insertRowIntoAggTable(TEMP_EVENT_E_LTE_MANUF_TAC_EVENTID_ERR_DAY, UNKNOWN_TAC, noErrorsForWorstTacInDayTables, timeStamp, ATTACH_IN_4G);
        insertRowIntoAggTable(TEMP_EVENT_E_SGEH_MANUF_TAC_EVENTID_ERR_15MIN, UNKNOWN_TAC, noErrorsForWorstTacIn15minTables, timeStamp,
                ATTACH_IN_2G_AND_3G);
        insertRowIntoAggTable(TEMP_EVENT_E_LTE_MANUF_TAC_EVENTID_ERR_15MIN, UNKNOWN_TAC, noErrorsForWorstTacIn15minTables, timeStamp, ATTACH_IN_4G);
        insertRowIntoRawTable(TEMP_EVENT_E_SGEH_ERR_RAW, UNKNOWN_TAC, timeStamp, ATTACH_IN_2G_AND_3G);
        insertRowIntoRawTable(TEMP_EVENT_E_LTE_ERR_RAW, UNKNOWN_TAC, timeStamp, ATTACH_IN_4G);
    }

    void populateTACGroupTable(final String tacGroup, final int tac) throws SQLException {
        final Map<String, Object> valuesForTacGroupTable = new HashMap<String, Object>();
        valuesForTacGroupTable.put(GROUP_NAME, tacGroup);
        valuesForTacGroupTable.put(TAC, tac);
        new SQLCommand(connection).insertRow(TEMP_GROUP_TYPE_E_TAC, valuesForTacGroupTable);
    }

    private void insertRowIntoRawTable(final String table, final int tac, final String timeStamp, final int eventId) throws SQLException {
        final Map<String, Object> valuesForRawTable = new HashMap<String, Object>();
        valuesForRawTable.put(EVENT_ID, eventId);
        valuesForRawTable.put(TAC, tac);
        valuesForRawTable.put(IMSI, IMSI1);
        valuesForRawTable.put(DATETIME_ID, timeStamp);
        valuesForRawTable.put(LOCAL_DATE_ID, timeStamp.substring(0, 10));
        new SQLCommand(connection).insertRow(table, valuesForRawTable);
    }

    private void insertRowIntoAggTable(final String table, final int tac, final int noErrors, final String timeStamp, final int eventId)
            throws SQLException {
        final Map<String, Object> valuesForAggTable = new HashMap<String, Object>();
        valuesForAggTable.put(NO_OF_ERRORS, noErrors);
        valuesForAggTable.put(EVENT_ID, eventId);
        valuesForAggTable.put(TAC, tac);
        valuesForAggTable.put(DATETIME_ID, timeStamp);
        new SQLCommand(connection).insertRow(table, valuesForAggTable);
    }

    protected void createTemporaryTables() throws Exception {
        final List<String> columnsForTacAggregationTable = new ArrayList<String>();
        columnsForTacAggregationTable.add(NO_OF_ERRORS);
        columnsForTacAggregationTable.add(EVENT_ID);
        columnsForTacAggregationTable.add(TAC);
        columnsForTacAggregationTable.add(DATETIME_ID);
        new SQLCommand(connection).createTemporaryTable(TEMP_EVENT_E_SGEH_MANUF_TAC_EVENTID_ERR_DAY, columnsForTacAggregationTable);
        new SQLCommand(connection).createTemporaryTable(TEMP_EVENT_E_LTE_MANUF_TAC_EVENTID_ERR_DAY, columnsForTacAggregationTable);
        new SQLCommand(connection).createTemporaryTable(TEMP_EVENT_E_SGEH_MANUF_TAC_EVENTID_ERR_15MIN, columnsForTacAggregationTable);
        new SQLCommand(connection).createTemporaryTable(TEMP_EVENT_E_LTE_MANUF_TAC_EVENTID_ERR_15MIN, columnsForTacAggregationTable);
        final List<String> columnsForRawTable = new ArrayList<String>();
        columnsForRawTable.add(IMSI);
        columnsForRawTable.add(DATETIME_ID);
        columnsForRawTable.add(EVENT_ID);
        columnsForRawTable.add(TAC);
        columnsForRawTable.add(LOCAL_DATE_ID);
        new SQLCommand(connection).createTemporaryTable(TEMP_EVENT_E_SGEH_ERR_RAW, columnsForRawTable);
        new SQLCommand(connection).createTemporaryTable(TEMP_EVENT_E_LTE_ERR_RAW, columnsForRawTable);
    }

}
