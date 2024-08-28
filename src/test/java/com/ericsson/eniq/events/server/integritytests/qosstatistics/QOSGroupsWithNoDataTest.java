/**
 * -----------------------------------------------------------------------
 *     Copyright (C) 2011 LM Ericsson Limited.  All rights reserved.
 * -----------------------------------------------------------------------
 */
package com.ericsson.eniq.events.server.integritytests.qosstatistics;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.*;
import static com.ericsson.eniq.events.server.test.temptables.TempTableNames.*;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.sql.SQLException;
import java.util.*;

import javax.ws.rs.core.MultivaluedMap;

import org.junit.Test;

import com.ericsson.eniq.events.server.resources.QOSStatisticsResource;
import com.ericsson.eniq.events.server.resources.TestsWithTemporaryTablesBaseTestCase;
import com.ericsson.eniq.events.server.test.queryresults.QOSStatisticsSummaryResult;
import com.sun.jersey.core.util.MultivaluedMapImpl;

/**
 * @author eemecoy
 *
 */
public class QOSGroupsWithNoDataTest extends TestsWithTemporaryTablesBaseTestCase<QOSStatisticsSummaryResult> {

    private QOSStatisticsResource qosStatisticsResource;

    /*
     * (non-Javadoc)
     * 
     * @see com.ericsson.eniq.events.server.resources.TestsWithTemporaryTablesBaseTestCase#onSetUp()
     */
    @Override
    public void onSetUp() throws Exception {
        super.onSetUp();
        qosStatisticsResource = new QOSStatisticsResource();
        attachDependencies(qosStatisticsResource);
        insertDataIntoTacGroupTable();
        createAggregationTables();
        createTemporaryTable(TEMP_EVENT_E_LTE_ERR_RAW, getColumnsForRawTables());
    }

    @Test
    public void testGroupQueryReturnsRowsWith0ErrorsWhenNoData() throws Exception {
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(DISPLAY_PARAM, GRID);
        map.putSingle(TIME_QUERY_PARAM, TWO_WEEKS);
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(GROUP_NAME_PARAM, SAMPLE_TAC_GROUP);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, "10");
        final String json = qosStatisticsResource.getData(SAMPLE_REQUEST_ID, map);
        System.out.println(json);
        jsonAssertUtils.assertJSONSucceeds(json);
        final List<QOSStatisticsSummaryResult> qciSummaryResult = getTranslator().translateResult(json, QOSStatisticsSummaryResult.class);
        validateResult(qciSummaryResult);
    }

    private void validateResult(final List<QOSStatisticsSummaryResult> results) {
        assertThat(results.size(), is(10));
        for (final QOSStatisticsSummaryResult result : results) {
            assertThat(result.getNoErrors(), is(0));
            assertThat(result.getNoSuccesses(), is(0));
            assertThat(result.getImpactedSubscribers(), is(0));
        }
    }

    private void createAggregationTables() throws Exception {
        final Collection<String> columns = getColumnsForAggregationTables();
        createTemporaryTable(TEMP_EVENT_E_LTE_MANUF_TAC_ERR_DAY, columns);
        createTemporaryTable(TEMP_EVENT_E_LTE_MANUF_TAC_SUC_DAY, columns);
    }

    private Collection<String> getColumnsForRawTables() {
        final Collection<String> columns = new ArrayList<String>();
        columns.add(TAC);
        columns.add(IMSI);
        columns.add(DATETIME_ID);
        columns.add(LOCAL_DATE_ID);
        columns.addAll(QCIColumns.getQCIColumns());
        return columns;
    }

    private Collection<String> getColumnsForAggregationTables() {
        final Collection<String> columns = new ArrayList<String>();
        columns.add(TAC);
        columns.add(DATETIME_ID);
        columns.addAll(QCIColumns.getQCIColumns());
        return columns;
    }

    private void insertDataIntoTacGroupTable() throws SQLException {
        final Map<String, Object> values = new HashMap<String, Object>();
        values.put(TAC, SAMPLE_TAC);
        values.put(GROUP_NAME, SAMPLE_TAC_GROUP);
        insertRow(TEMP_GROUP_TYPE_E_TAC, values);

    }

}
