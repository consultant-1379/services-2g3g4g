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
public class QOSStatisticsResourceControllerGroupWithPreparedRawTablesTest extends BaseQOSStatisticsResourceWithPreparedTablesTest {

    private Map<String, String> groupTableColumns;

    private Map<String, Object> valuesForGroupTable;

    @Override
    public void onSetUp() throws Exception {
        super.onSetUp();
        groupTableColumns = new HashMap<String, String>();
        groupTableColumns.put(GROUP_NAME, VARCHAR_64);
        groupTableColumns.put(HIERARCHY_3, VARCHAR_64);

        valuesForGroupTable = new HashMap<String, Object>();
        valuesForGroupTable.put(GROUP_NAME, SAMPLE_BSC_GROUP);
        valuesForGroupTable.put(HIERARCHY_3, SAMPLE_HIERARCHY_3);

    }

    @SuppressWarnings("unchecked")
    @Test
    public void testGetSummary_OneWeek_Controller_Group() throws Exception {
        createTemporaryTableWithColumnTypes(TEMP_GROUP_TYPE_E_RAT_VEND_HIER3, groupTableColumns);
        insertRow(TEMP_GROUP_TYPE_E_RAT_VEND_HIER3, valuesForGroupTable);

        aggTables.add(TEMP_EVENT_E_LTE_VEND_HIER3_ERR_DAY);
        aggTables.add(TEMP_EVENT_E_LTE_VEND_HIER3_SUC_DAY);
        getQCIGroupSummary(TYPE_BSC, SAMPLE_BSC_GROUP, ONE_WEEK);
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
        values.put(HIERARCHY_3, SAMPLE_HIERARCHY_3);
        return values;
    }

}
