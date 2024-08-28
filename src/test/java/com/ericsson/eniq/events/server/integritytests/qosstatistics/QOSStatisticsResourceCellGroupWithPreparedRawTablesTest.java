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
 * @author eemecoy
 *
 */
public class QOSStatisticsResourceCellGroupWithPreparedRawTablesTest extends BaseQOSStatisticsResourceWithPreparedTablesTest {

    private static final String GROUP_TYPE_E_CELL = "#GROUP_TYPE_E_RAT_VEND_HIER321";

    private static final String HIERARCHY_1 = "HIERARCHY_1";

    private static final String RAT = "RAT";

    private static final String HIERARCHY_3 = "HIERARCHY_3";

    private static final String VENDOR = "VENDOR";

    private Map<String, String> cellGroupTableColumns;

    private Map<String, Object> valuesForGroupTable;

    /*
     * (non-Javadoc)
     * 
     * @see com.ericsson.eniq.events.server.resources.testswithtemporarytables.qosstatistics.BaseQOSStatisticsResourceWithPreparedTables#onSetUp()
     */
    @Override
    public void onSetUp() throws Exception {
        super.onSetUp();
        cellGroupTableColumns = new HashMap<String, String>();
        cellGroupTableColumns.put(GROUP_NAME, VARCHAR_64);
        cellGroupTableColumns.put(HIERARCHY_3, VARCHAR_128);
        cellGroupTableColumns.put(HIERARCHY_1, VARCHAR_128);
        cellGroupTableColumns.put(RAT, TINYINT);
        cellGroupTableColumns.put(VENDOR, VARCHAR_20);
        valuesForGroupTable = new HashMap<String, Object>();
        valuesForGroupTable.put(GROUP_NAME, SAMPLE_ECELL_GROUP);
        valuesForGroupTable.put(HIERARCHY_3, ERBS1);
        valuesForGroupTable.put(HIERARCHY_1, LTECELL1);
        valuesForGroupTable.put(RAT, 2);
        valuesForGroupTable.put(VENDOR, ERICSSON);
    }

    @Test
    public void testGetSummary_FiveMinutes_AccessCellGroup() throws Exception {
        aggTables.add(TEMP_EVENT_E_LTE_VEND_HIER321_SUC_15MIN);
        createTemporaryTableWithColumnTypes(GROUP_TYPE_E_CELL, cellGroupTableColumns);
        insertRow(GROUP_TYPE_E_CELL, valuesForGroupTable);
        getQCIGroupSummaryForFiveMinutes(TYPE_CELL, SAMPLE_ECELL_GROUP);

    }

    @Test
    public void testGetSummaryWithDataTiering_30Minutes_AccessCellGroup() throws Exception {
        jndiProperties.setUpDataTieringJNDIProperty();
        aggTables.add(TEMP_EVENT_E_LTE_VEND_HIER321_SUC_15MIN);
        createTemporaryTableWithColumnTypes(GROUP_TYPE_E_CELL, cellGroupTableColumns);
        insertRow(GROUP_TYPE_E_CELL, valuesForGroupTable);
        getQCIGroupSummary(TYPE_CELL, SAMPLE_ECELL_GROUP, THIRTY_MINUTES);
        jndiProperties.setUpJNDIPropertiesForTest();
    }

    @Test
    public void testGetSummary_OneWeek_AccessCellGroup() throws Exception {
        createTemporaryTableWithColumnTypes(GROUP_TYPE_E_CELL, cellGroupTableColumns);
        insertRow(GROUP_TYPE_E_CELL, valuesForGroupTable);
        aggTables.add(TEMP_EVENT_E_LTE_VEND_HIER321_SUC_DAY);
        aggTables.add(TEMP_EVENT_E_LTE_VEND_HIER321_ERR_DAY);
        getQCIGroupSummary(TYPE_CELL, SAMPLE_ECELL_GROUP, ONE_WEEK);
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
        values.put(RAT, RAT_INTEGER_VALUE_FOR_4G);
        values.put(VENDOR_PARAM_UPPER_CASE, ERICSSON);
        values.put(HIERARCHY_3, ERBS1);
        values.put(HIERARCHY_1, LTECELL1);
        return values;
    }
}
