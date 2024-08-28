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

import java.util.List;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MultivaluedMap;

import com.ericsson.eniq.events.server.common.EventDataSourceType;
import com.ericsson.eniq.events.server.resources.BaseResource;
import com.ericsson.eniq.events.server.utils.DateTimeRange;
import com.ericsson.eniq.events.server.utils.FormattedDateTimeRange;

public abstract class DvtpBaseResource extends BaseResource {
    /**
     * @param isCSV isCSV param is true when user click on export to CSV
     */
    @Override
    protected FormattedDateTimeRange getFormattedDateTimeRange(final MultivaluedMap<String, String> requestParameters, final List<String> techPacks) {
        boolean isCSV = false;
        if (isMediaTypeApplicationCSV()) {
            isCSV = true;
        }

        final FormattedDateTimeRange dateTimeRange = DateTimeRange.getFormattedDateTimeRange(requestParameters.getFirst(KEY_PARAM),
                requestParameters.getFirst(TIME_QUERY_PARAM), requestParameters.getFirst(TIME_FROM_QUERY_PARAM),
                requestParameters.getFirst(TIME_TO_QUERY_PARAM), requestParameters.getFirst(DATE_FROM_QUERY_PARAM),
                requestParameters.getFirst(DATE_TO_QUERY_PARAM), requestParameters.getFirst(DATA_TIME_FROM_QUERY_PARAM),
                requestParameters.getFirst(DATA_TIME_TO_QUERY_PARAM), requestParameters.getFirst(TZ_OFFSET),
                applicationConfigManager.getDvtpTimeDelayFifteenMinuteData(), applicationConfigManager.getDvtpTimeDelayThirtyMinuteData(),
                applicationConfigManager.getDvtpTimeDelayDayData(), isCSV);
        return dateTimeRange;
    }

    @Override
    protected FormattedDateTimeRange getAndCheckFormattedDateTimeRangeForDailyAggregation(final MultivaluedMap<String, String> requestParameters,
                                                                                          final List<String> techPacks) {
        FormattedDateTimeRange timerange = getFormattedDateTimeRange(requestParameters, techPacks);
        if (requestParameters.containsKey(TIME_QUERY_PARAM)
                && queryUtils.getEventDataSourceType(timerange).equals(EventDataSourceType.AGGREGATED_DAY.getValue())) {
            timerange = DateTimeRange.getDailyAggregationTimeRangebyLocalTime(requestParameters.getFirst(TIME_QUERY_PARAM),
                    applicationConfigManager.getDvtpTimeDelayFifteenMinuteData(), applicationConfigManager.getDvtpTimeDelayThirtyMinuteData(),
                    applicationConfigManager.getDvtpTimeDelayDayData());

        }
        return timerange;
    }

    @Override
    public FormattedDateTimeRange getDateTimeRangeOfChartAndSummaryGrid(final FormattedDateTimeRange dateTimeRange, final String viewName,
                                                                        final List<String> techPacks) throws WebApplicationException {
        FormattedDateTimeRange newDateTimeRange = null;
        if (viewName.equals(EventDataSourceType.AGGREGATED_15MIN.getValue())) {
            newDateTimeRange = DateTimeRange.getFormattedDateTimeRange(
                    DateTimeRange.formattedDateTimeAgainst15MinsTable(dateTimeRange.getStartDateTime(), dateTimeRange.getMinutesOfStartDateTime()),
                    DateTimeRange.formattedDateTimeAgainst15MinsTable(dateTimeRange.getEndDateTime(), dateTimeRange.getMinutesOfEndDateTime()),
                    applicationConfigManager.getDvtpTimeDelayFifteenMinuteData(), applicationConfigManager.getDvtpTimeDelayThirtyMinuteData(),
                    applicationConfigManager.getDvtpTimeDelayDayData());
        } else if (viewName.equals(EventDataSourceType.AGGREGATED_DAY.getValue())) {
            newDateTimeRange = DateTimeRange.getFormattedDateTimeRange(
                    DateTimeRange.formattedDateTimeAgainstDayTable(dateTimeRange.getStartDateTime(), dateTimeRange.getMinutesOfStartDateTime()),
                    DateTimeRange.formattedDateTimeAgainstDayTable(dateTimeRange.getEndDateTime(), dateTimeRange.getMinutesOfEndDateTime()), 0, 0, 0);
        } else {
            newDateTimeRange = dateTimeRange;
        }
        return newDateTimeRange;
    }
}
