/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.integritytests.qosstatistics;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.*;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.*;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

/**
 * @author edivkir
 * @since 2011
 *
 */
public class QOSStatisticsResourceTerminalGroupWithPreparedRawTablesTest extends BaseQOSStatisticsResourceWithPreparedTablesTest {

    private static final String TEMP_GROUP_TYPE_E_TAC = "#GROUP_TYPE_E_TAC";

    private Map<String, Object> valuesForGroupTable;

    /*
     * (non-Javadoc)
     * 
     * @see com.ericsson.eniq.events.server.resources.testswithtemporarytables.qosstatistics.BaseQOSStatisticsResourceWithPreparedTables#onSetUp()
     */
    @Override
    public void onSetUp() throws Exception {
        super.onSetUp();

        valuesForGroupTable = new HashMap<String, Object>();
        valuesForGroupTable.put(GROUP_NAME, "DG_GroupNameTAC_249");
        valuesForGroupTable.put(TAC, SOME_TAC);
    }

    @Test
    public void testGetSummary_FiveMinutes_TACGroup() throws Exception {
        aggTables.add(TEMP_EVENT_E_LTE_MANUF_TAC_SUC_15MIN);
        insertRow(TEMP_GROUP_TYPE_E_TAC, valuesForGroupTable);
        getQCIGroupSummaryForFiveMinutes(TYPE_TAC, "DG_GroupNameTAC_249");
    }

    @Test
    public void testGetSummaryWithDataTiering_30Minutes_TACGroup() throws Exception {
        jndiProperties.setUpDataTieringJNDIProperty();
        aggTables.add(TEMP_EVENT_E_LTE_MANUF_TAC_SUC_15MIN);
        insertRow(TEMP_GROUP_TYPE_E_TAC, valuesForGroupTable);
        getQCIGroupSummary(TYPE_TAC, "DG_GroupNameTAC_249", THIRTY_MINUTES);
        jndiProperties.setUpJNDIPropertiesForTest();
    }

    @Test
    public void testGetSummary_OneWeek_TACGroup() throws Exception {
        insertRow(TEMP_GROUP_TYPE_E_TAC, valuesForGroupTable);
        aggTables.add(TEMP_EVENT_E_LTE_MANUF_TAC_SUC_DAY);
        aggTables.add(TEMP_EVENT_E_LTE_MANUF_TAC_ERR_DAY);
        getQCIGroupSummary(TYPE_TAC, "DG_GroupNameTAC_249", ONE_WEEK);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.ericsson.eniq.events.server.resources.testswithtemporarytables.ranking.accessarea.BaseQOSStatisticsResourceWithPreparedTables#
     * getTableSpecificValues()
     */
    @Override
    protected Map<String, Object> getTableSpecificColumnsAndValues() {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(TAC, SOME_TAC);
        return values;
    }
}
