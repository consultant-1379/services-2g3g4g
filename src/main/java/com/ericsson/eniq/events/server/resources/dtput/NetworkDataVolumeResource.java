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
package com.ericsson.eniq.events.server.resources.dtput;

import static com.ericsson.eniq.events.server.common.ApplicationConfigConstants.*;
import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.common.MessageConstants.*;
import static com.ericsson.eniq.events.server.common.tablesandviews.TableKeys.*;
import static com.ericsson.eniq.events.server.common.utils.JSONUtilsConstants.*;
import static com.ericsson.eniq.events.server.utils.DateTimeUtils.*;

import java.util.*;

import javax.ejb.*;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MultivaluedMap;

import org.apache.commons.lang.StringUtils;

import com.ericsson.eniq.events.server.common.EventDataSourceType;
import com.ericsson.eniq.events.server.common.TechPackData;
import com.ericsson.eniq.events.server.common.tablesandviews.AggregationTableInfo;
import com.ericsson.eniq.events.server.common.tablesandviews.TechPackTables;
import com.ericsson.eniq.events.server.services.impl.TechPackCXCMappingService;
import com.ericsson.eniq.events.server.utils.DateTimeRange;
import com.ericsson.eniq.events.server.utils.FormattedDateTimeRange;
import com.ericsson.eniq.events.server.utils.json.JSONUtils;

@Stateless
@LocalBean
@SuppressWarnings("PMD.CyclomaticComplexity")
public class NetworkDataVolumeResource extends DvtpBaseResource {

    private static final String NO_TYPE = "NO_TYPE";

    private static final String IS_NODE_TYPE = "isTypeNode";

    @EJB
    private TechPackCXCMappingService techPackCXCMappingService;

    public NetworkDataVolumeResource() {
        aggregationViews = new HashMap<String, AggregationTableInfo>();
        aggregationViews.put(TYPE_APN, new AggregationTableInfo(TYPE_APN, EventDataSourceType.AGGREGATED_15MIN, EventDataSourceType.AGGREGATED_DAY));
        aggregationViews.put(TYPE_TAC, new AggregationTableInfo(TERM, EventDataSourceType.AGGREGATED_15MIN, EventDataSourceType.AGGREGATED_DAY));
        aggregationViews.put(TYPE_MSISDN, new AggregationTableInfo(IMSI_RANK, EventDataSourceType.AGGREGATED_15MIN,
                EventDataSourceType.AGGREGATED_DAY));
        aggregationViews
                .put(TYPE_IMSI, new AggregationTableInfo(IMSI_RANK, EventDataSourceType.AGGREGATED_15MIN, EventDataSourceType.AGGREGATED_DAY));
        aggregationViews
                .put(NO_TYPE, new AggregationTableInfo(GSN_NETWORK, EventDataSourceType.AGGREGATED_15MIN, EventDataSourceType.AGGREGATED_DAY));
    }

    public void setTechPackCXCMappingService(final TechPackCXCMappingService techPackCXCMappingService) {
        this.techPackCXCMappingService = techPackCXCMappingService;
    }

    @Override
    protected boolean isValidValue(final MultivaluedMap<String, String> requestParameters) {
        if (requestParameters.containsKey(NODE_PARAM) || requestParameters.containsKey(GROUP_NAME_PARAM)) {
            if (!queryUtils.checkValidValue(requestParameters)) {
                return false;
            }
        }
        return true;
    }

    /**
     * 
     * @param requestParameters
     * @return
     */
    private boolean isTypeRequired(final MultivaluedMap<String, String> requestParameters) {
        if (requestParameters.containsKey(TYPE_PARAM)) {
            if (!requestParameters.getFirst(TYPE_PARAM).equals(TYPE_APN) && !requestParameters.getFirst(TYPE_PARAM).equals(TYPE_IMSI)
                    && !requestParameters.getFirst(TYPE_PARAM).equals(TYPE_TAC) && !requestParameters.getFirst(TYPE_PARAM).equals(TYPE_MSISDN)) {
                return false;
            }
        }
        return true;
    }

    @Override
    protected String getData(final String requestId, final MultivaluedMap<String, String> requestParameters) throws WebApplicationException {
        final List<String> errors = checkParameters(requestParameters);
        if (!errors.isEmpty()) {
            return getErrorResponse(E_INVALID_OR_MISSING_PARAMS, errors);
        }

        checkAndCreateINFOAuditLogEntryForURI(requestParameters);

        final String displayType = requestParameters.getFirst(DISPLAY_PARAM);
        if (displayType.equals(CHART_PARAM) || displayType.equals(GRID_PARAM)) {

            return getNetworkDataVolumeResults(requestId, requestParameters);
        }
        return getNoSuchDisplayErrorResponse(displayType);
    }

    /**
     * Gets the network datavol results.
     * 
     * @param requestId the request id
     * @param requestParameters the request parameters
     * @param path the path
     * @return the network datavol results
     */
    public String getNetworkDataVolumeResults(final String requestId, final MultivaluedMap<String, String> requestParameters) {
        for (final String techPackName : TechPackData.completeDVTPTechPackList) {
            String techPack = techPackName;
            if (techPackName.startsWith(EVENT_E_DVTP_TPNAME)) {
                techPack = EVENT_E_DVTP_TPNAME;
            }
            final List<String> cxcLicensesForTechPack = techPackCXCMappingService.getTechPackCXCNumbers(techPack);
            if (cxcLicensesForTechPack.isEmpty()) {
                return JSONUtils.createJSONErrorResult("TechPack " + techPack + " has not been installed.");
            }
        }

        final FormattedDateTimeRange dateTimeRange = getAndCheckFormattedDateTimeRangeForDailyAggregation(requestParameters,
                TechPackData.completeDVTPTechPackList);

        final Map<String, Object> templateParameters = new HashMap<String, Object>();

        final String timerange = queryUtils.getEventDataSourceType(dateTimeRange).getValue();
        String type = requestParameters.getFirst(TYPE_PARAM);

        final String nodeName = requestParameters.getFirst(NODE_PARAM);
        final String groupName = requestParameters.getFirst(GROUP_NAME_PARAM);
        if (groupName != null) {
            templateParameters.put(GROUP_NAME_PARAM, groupName);
            updateTemplateParametersWithGroupDefinition(templateParameters, requestParameters);
        }
        final String tzOffset = requestParameters.getFirst(TZ_OFFSET);
        final String chartTitle = getChartTitle(requestParameters, type, nodeName, groupName);

        if (requestParameters.containsKey(TYPE_PARAM)) {
            templateParameters.put(TYPE_PARAM, type);
            templateParameters.put(IS_NODE_TYPE, true);
        } else {
            type = NO_TYPE;
            templateParameters.put(IS_NODE_TYPE, false);
        }
        if (!isTypeRequired(requestParameters)) {
            return JSONUtils.jsonErrorTypeMsg();
        }
        if (!isValidValue(requestParameters)) {
            return JSONUtils.jsonErrorInputMsg();
        }

        String template = null;

        template = getTemplate(NETWORK_DATAVOL_ANALYSIS, requestParameters, null);

        final TechPackTables techPackTables = getTechPackTablesOrViews(dateTimeRange, timerange, type);
        if (techPackTables.shouldReportErrorAboutEmptyRawTables()) {
            return JSONUtils.JSONEmptySuccessResult();
        }

        final long rangeInMin = dateTimeRange.getRangeInMinutes();

        updateTemplateParameters(requestParameters, templateParameters, timerange, techPackTables, rangeInMin);

        FormattedDateTimeRange newDateTimeRange = getDateTimeRangeOfChartAndSummaryGrid(dateTimeRange, timerange,
                TechPackData.completeDVTPTechPackList);

        if (rangeInMin <= MINUTES_IN_2_HOURS) {
            newDateTimeRange = DateTimeRange.getFormattedDateTimeRange(roundToLastFifthMinute(newDateTimeRange.getStartDateTime()),
                    roundToLastFifthMinute(newDateTimeRange.getEndDateTime()), 0, 0, 0);
        }

        final StringBuffer startTime = new StringBuffer(QUOTE_SINGLE + newDateTimeRange.getStartDateTime() + QUOTE_SINGLE);
        templateParameters.put(START_TIME, startTime.toString());

        final String[] dataVolumeDateTimeList = DateTimeRange.getSamplingTimeList(newDateTimeRange,
                DateTimeRange.getChartInterval(newDateTimeRange, timerange));
        final StringBuffer endTime = new StringBuffer(QUOTE_SINGLE + newDateTimeRange.getEndDateTime() + QUOTE_SINGLE);
        templateParameters.put(END_TIME, endTime.toString());

        final int timeInterval = (requestParameters.get(DISPLAY_PARAM).get(0).equalsIgnoreCase(CHART_PARAM)) ? getDVTPIntervalForChart(rangeInMin)
                : getDVTPIntervalForGrid(rangeInMin);
        if (isMediaTypeApplicationCSV()) {
            templateParameters.put(CSV_PARAM, new Boolean(true));
            templateParameters.put(TZ_OFFSET, tzOffset);
        }
        final String query = templateUtils.getQueryFromTemplate(template, templateParameters);

        if (StringUtils.isBlank(query)) {
            return JSONUtils.JSONBuildFailureError();
        }

        checkAndCreateFineAuditLogEntryForQuery(requestParameters, query, newDateTimeRange);

        if (isMediaTypeApplicationCSV()) {
            streamDataAsCSV(requestParameters, requestParameters.getFirst(TZ_OFFSET), null, query, newDateTimeRange);
            return null;
        }

        return getDataVolume(requestId, query, requestParameters, newDateTimeRange, dataVolumeDateTimeList, tzOffset,
                NETWORK_DATA_VOLUME_X_AXIS_VALUE, NETWORK_DATA_VOLUME_SECOND_Y_AXIS_VALUE, NETWORK_DATA_VOLUME_X_AXIS_VALUE, chartTitle, timeInterval);
    }

    private String getChartTitle(final MultivaluedMap<String, String> requestParameters, final String type, final String nodeName,
                                 final String groupName) {
        String chartTitle = "";
        if (type == null) {
            chartTitle = DVTP_CHART_TITLE_PREFIX + NETWORK;
        } else if (groupName != null) {
            chartTitle = DVTP_CHART_TITLE_PREFIX + type + SINGLE_SPACE + GROUP + HYPHEN + groupName;
        } else if (nodeName != null) {
            chartTitle = DVTP_CHART_TITLE_PREFIX + type + HYPHEN + nodeName;
        } else if (type.equalsIgnoreCase(IMSI)) {
            chartTitle = DVTP_CHART_TITLE_PREFIX + type + HYPHEN + requestParameters.getFirst(IMSI_PARAM);
        } else if (type.equalsIgnoreCase(TYPE_MSISDN)) {
            chartTitle = DVTP_CHART_TITLE_PREFIX + type + HYPHEN + requestParameters.getFirst(MSISDN_PARAM);
        } else if (type.equalsIgnoreCase(TYPE_TAC)) {
            chartTitle = DVTP_CHART_TITLE_PREFIX + type + HYPHEN + requestParameters.getFirst(TAC_PARAM);
        }
        return chartTitle;
    }

    /**
     * add various template parameters.
     * 
     * @param requestParameters
     * @param templateParameters
     * @param timerange
     * @param techPackTables
     * @param rangeInMin
     */
    private void updateTemplateParameters(final MultivaluedMap<String, String> requestParameters, final Map<String, Object> templateParameters,
                                          final String timerange, final TechPackTables techPackTables, final long rangeInMin) {
        templateParameters.put(TIMERANGE_PARAM, timerange);
        if (requestParameters.get(DISPLAY_PARAM).get(0).equalsIgnoreCase(CHART_PARAM)) {
            templateParameters.put(INTERVAL_PARAM, getDVTPIntervalForChart(rangeInMin));
        } else {
            templateParameters.put(INTERVAL_PARAM, getDVTPIntervalForGrid(rangeInMin));
        }
        templateParameters.put(COUNT_PARAM, getCountValue(requestParameters, MAXIMUM_POSSIBLE_GRID_DATA_ROW_COUNTS));
        templateParameters.put(TECH_PACK_TABLES, techPackTables);
        templateParameters.put(USE_AGGREGATION_TABLES, shouldQueryUseAggregationTables(timerange));
    }

    /**
     * Gets the tech pack tables or views.
     * 
     * @param dateTimeRange the date time range
     * @param timerange the timerange
     * @param type the type
     * @return the tech pack tables or views
     */
    TechPackTables getTechPackTablesOrViews(final FormattedDateTimeRange dateTimeRange, final String timerange, final String type) {
        TechPackTables tables = null;
        if (shouldQueryUseAggregationTables(timerange)) {
            tables = getDTPutAggregationTables(type, timerange);
            tables.addTechPack(getDTPutRawTables(type, dateTimeRange).getTechPacks().get(0));
        } else {
            tables = getDTPutRawTables(type, dateTimeRange);
        }
        return tables;
    }

    /**
     * 
     * @param timerange
     * @return
     */
    boolean shouldQueryUseAggregationTables(final String timerange) {
        if (timerange.equals(FIFTEEN_MINUTES) || timerange.equals(DAY)) {
            return true;
        }
        return false;
    }

    @Override
    protected List<String> checkParameters(final MultivaluedMap<String, String> requestParameters) {
        final List<String> errors = new ArrayList<String>();

        if (!requestParameters.containsKey(DISPLAY_PARAM)) {
            errors.add(DISPLAY_PARAM);
        }
        return errors;
    }

    /**
     * @param requestId corresponds to this request for cancelling later
     * @param query the sql query
     * @param requestParameters the UI request parameters
     * @param newDateTimeRange the formatted input time range
     * @param eventVolumeDateTimeList the event volume time list
     * @param tzOffset the time zone offset
     * @param xAxis the X axis of event volume chart
     * @param secondYAxis the second Y axis of event volume chart
     * @param timeColumn the time column of event volume chart
     * @param chartTitle
     * @param timeInterval interval between two time tick on xaxis
     * @return event-volume data
     */
    private String getDataVolume(final String requestId, final String query, final MultivaluedMap<String, String> requestParameters,
                                 final FormattedDateTimeRange newDateTimeRange, final String[] dataVolumeDateTimeList, final String tzOffset,
                                 final String xAxis, final String secondYAxis, final String timeColumn, final String chartTitle,
                                 final int timeInterval) {
        return this.dataService.getChartWithTitleAndTimeIntervalData(requestId, query,
                this.queryUtils.mapRequestParameters(requestParameters, newDateTimeRange), dataVolumeDateTimeList, xAxis, secondYAxis, timeColumn,
                tzOffset, getLoadBalancingPolicy(requestParameters), chartTitle, timeInterval);
    }
}