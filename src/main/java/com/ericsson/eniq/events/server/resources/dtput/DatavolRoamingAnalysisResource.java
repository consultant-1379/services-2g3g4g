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

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.common.MessageConstants.*;
import static com.ericsson.eniq.events.server.utils.DateTimeUtils.*;

import java.util.*;

import javax.ejb.*;
import javax.ws.rs.*;
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
public class DatavolRoamingAnalysisResource extends DvtpBaseResource {
    @EJB
    private TechPackCXCMappingService techPackCXCMappingService;

    public DatavolRoamingAnalysisResource() {
        aggregationViews = new HashMap<String, AggregationTableInfo>();
        aggregationViews.put(TYPE_NETWORK, new AggregationTableInfo(TYPE_NETWORK, EventDataSourceType.AGGREGATED_15MIN,
                EventDataSourceType.AGGREGATED_DAY));
    }

    @Override
    protected String getData(final String requestId, final MultivaluedMap<String, String> requestParameters) throws WebApplicationException {
        throw new UnsupportedOperationException();
    }

    @Path(ROAMING_COUNTRY)
    @GET
    public String getRoamingCountryData() throws WebApplicationException {
        final String requestId = httpHeaders.getRequestHeaders().getFirst(REQUEST_ID);
        return getRoamingResults(requestId, TYPE_ROAMING_COUNTRY, getDecodedQueryParameters());
    }

    /**
     * Roaming by operator
     * 
     * @return JSON encoded results
     * @throws WebApplicationException
     */
    @Path(ROAMING_OPERATOR)
    @GET
    public String getRoamingOperatorData() throws WebApplicationException {
        final String requestId = httpHeaders.getRequestHeaders().getFirst(REQUEST_ID);
        return getRoamingResults(requestId, TYPE_ROAMING_OPERATOR, getDecodedQueryParameters());
    }

    /**
     * 
     * Get Roaming Results: (Both grid and chart display types as user can for example change time when in grid version of chart - so valid that
     * "grid" can be in call the UI can handle chart style data in the grid).
     * 
     * 
     * @param requestId corresponds to this request for cancelling later
     * @param roamingObject - roaming object [county,operator]
     * @param requestParameters - URL query parameters
     * @return JSON encoded string
     * @throws WebApplicationException
     */
    public String getRoamingResults(final String requestId, final String roamingObject, final MultivaluedMap<String, String> requestParameters)
            throws WebApplicationException {

        final List<String> errors = checkParameters(requestParameters);
        if (!errors.isEmpty()) {
            return getErrorResponse(E_INVALID_OR_MISSING_PARAMS, errors);
        }

        final String displayType = requestParameters.getFirst(DISPLAY_PARAM);
        if (displayType.equals(CHART_PARAM) || displayType.equals(GRID_PARAM)) {
            return getChartResults(requestId, roamingObject, requestParameters);
        }
        return getNoSuchDisplayErrorResponse(displayType);
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
     * Gets the JSON chart results for network roaming.
     * 
     * @param requestId corresponds to this request for cancelling later
     * @param requestParameters - URL query parameters
     * @param dateTimeRange - formatted date time range
     * @return JSON encoded results
     * @throws WebApplicationException
     */
    private String getChartResults(final String requestId, final String roamingObject, final MultivaluedMap<String, String> requestParameters)
            throws WebApplicationException {
        final String drillType = null;

        for (final String techPackName : TechPackData.completeDVTPTechPackList) {
            String techPack = techPackName;
            if (techPackName.startsWith("EVENT_E_DVTP")) {
                techPack = "EVENT_E_DVTP";
            }
            final List<String> cxcLicensesForTechPack = techPackCXCMappingService.getTechPackCXCNumbers(techPack);
            if (cxcLicensesForTechPack.isEmpty()) {
                return JSONUtils.createJSONErrorResult("TechPack " + techPack + " has not been installed.");
            }
        }

        final FormattedDateTimeRange dateTimeRange = getAndCheckFormattedDateTimeRangeForDailyAggregation(requestParameters,
                TechPackData.completeDVTPTechPackList);
        final Map<String, Object> templateParameters = new HashMap<String, Object>();

        templateParameters.put(ROAMING_OBJECT, roamingObject);
        final String timerange = queryUtils.getEventDataSourceType(dateTimeRange).getValue();
        templateParameters.put(TIMERANGE_PARAM, timerange);

        final long rangeInMin = dateTimeRange.getRangeInMinutes();
        FormattedDateTimeRange newDateTimeRange = getDateTimeRangeOfChartAndSummaryGrid(dateTimeRange, timerange,
                TechPackData.completeDVTPTechPackList);
        if (rangeInMin <= MINUTES_IN_2_HOURS) {
            newDateTimeRange = DateTimeRange.getFormattedDateTimeRange(roundToLastFifthMinute(newDateTimeRange.getStartDateTime()),
                    roundToLastFifthMinute(newDateTimeRange.getEndDateTime()), 0, 0, 0);
        }

        final TechPackTables techPackTables = getTechPackTablesOrViews(dateTimeRange, timerange, TYPE_NETWORK);
        if (techPackTables.shouldReportErrorAboutEmptyRawTables()) {
            return JSONUtils.JSONEmptySuccessResult();
        }
        templateParameters.put(TECH_PACK_TABLES, techPackTables);
        templateParameters.put(USE_AGGREGATION_TABLES, shouldQueryUseAggregationTables(timerange));

        final String query = templateUtils.getQueryFromTemplate(getTemplate(DATAVOL_ROAMING_ANALYSIS, requestParameters, drillType),
                templateParameters);

        if (StringUtils.isBlank(query)) {
            return JSONUtils.JSONBuildFailureError();
        }

        if (isMediaTypeApplicationCSV()) {
            streamDataAsCSV(requestParameters, requestParameters.getFirst(TZ_OFFSET), null, query, newDateTimeRange);
            return null;
        }
        return this.dataService.getChartData(requestId, query, this.queryUtils.mapRequestParameters(requestParameters, newDateTimeRange),
                ROAMING_X_AXIS_VALUE, ROAMING_SECOND_Y_AXIS_VALUE, null, requestParameters.getFirst(TZ_OFFSET));
    }

    public void setTechPackCXCMappingService(final TechPackCXCMappingService techPackCXCMappingService) {
        this.techPackCXCMappingService = techPackCXCMappingService;
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
        if (shouldQueryUseAggregationTables(timerange)) {
            final TechPackTables ret = getDTPutAggregationTables(type, timerange, TechPackData.completeDVTPTechPackList);
            ret.addTechPack(getDTPutRawTables(type, dateTimeRange).getTechPacks().get(0));
            return ret;
        }
        return getDTPutRawTables(type, dateTimeRange, TechPackData.completeDVTPTechPackList);
    }

    /**
     * taken directly from the velocity template.
     * 
     * @param timerange the timerange
     * @return true, if should query use aggregation tables
     */
    boolean shouldQueryUseAggregationTables(final String timerange) {
        if (timerange.equals(FIFTEEN_MINUTES) || timerange.equals(DAY)) {
            return true;
        }
        return false;
    }

    @Override
    protected boolean isValidValue(final MultivaluedMap<String, String> requestParameters) {
        throw new UnsupportedOperationException();
    }
}