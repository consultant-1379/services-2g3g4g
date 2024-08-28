/*------------------------------------------------------------------------------
 *******************************************************************************
 * COPYRIGHT Ericsson 2013
 *
 * The copyright to the computer program(s) herein is the property of
 * Ericsson Inc. The programs may be used and/or copied only with written
 * permission from Ericsson Inc. or in accordance with the terms and
 * conditions stipulated in the agreement/contract under which the
 * program(s) have been supplied.
 *******************************************************************************
 *----------------------------------------------------------------------------*/
package com.ericsson.eniq.events.server.common;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;

import javax.ws.rs.core.MultivaluedMap;

import com.sun.jersey.core.util.MultivaluedMapImpl;

public final class RequestParametersUtilities {
    public static MultivaluedMap<String, String> getRequestParametersForAPN(final String node, final String display, final String time,
                                                                            final String offset) {
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(TYPE_PARAM, TYPE_APN);
        map.putSingle(NODE_PARAM, node);
        map.putSingle(DISPLAY_PARAM, display);
        map.putSingle(TIME_QUERY_PARAM, time);
        map.putSingle(TZ_OFFSET, offset);
        return map;
    }

    public static MultivaluedMap<String, String> getRequestParametersForIMSI(final String imsi, final String display, final String time,
                                                                             final String offset) {
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        map.putSingle(IMSI_PARAM, imsi);
        map.putSingle(DISPLAY_PARAM, display);
        map.putSingle(TIME_QUERY_PARAM, time);
        map.putSingle(TZ_OFFSET, offset);
        return map;
    }

    public static MultivaluedMap<String, String> getRequestParametersForMSISDN(final String msisdn, final String display, final String time,
                                                                               final String offset) {
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(TYPE_PARAM, TYPE_MSISDN);
        map.putSingle(MSISDN_PARAM, msisdn);
        map.putSingle(DISPLAY_PARAM, display);
        map.putSingle(TIME_QUERY_PARAM, time);
        map.putSingle(TZ_OFFSET, offset);
        return map;
    }

    public static MultivaluedMap<String, String> getRequestParametersForTAC(final String tac, final String display, final String time,
                                                                            final String offset) {
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(TYPE_PARAM, TYPE_TAC);
        map.putSingle(TAC_PARAM, tac);
        map.putSingle(DISPLAY_PARAM, display);
        map.putSingle(TIME_QUERY_PARAM, time);
        map.putSingle(TZ_OFFSET, offset);
        return map;
    }

    public static MultivaluedMap<String, String> getRequestParametersForNetwork(final String display, final String time, final String offset) {
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(DISPLAY_PARAM, display);
        map.putSingle(TIME_QUERY_PARAM, time);
        map.putSingle(TZ_OFFSET, offset);
        return map;
    }

    public static MultivaluedMap<String, String> getRequestParametersForGroup(final String type, final String group_name, final String display,
                                                                              final String time, final String offset) {
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(TYPE_PARAM, type);
        map.putSingle(GROUP_NAME_PARAM, group_name);
        map.putSingle(DISPLAY_PARAM, display);
        map.putSingle(TIME_QUERY_PARAM, time);
        map.putSingle(TZ_OFFSET, offset);
        return map;
    }

    public static double getThroughput(final double datavolume, final double duration, final String view) {
        if (view.equalsIgnoreCase(RAW)) {
            return Math.floor(((datavolume * 8.00) / (duration / 1000.00)) * 100) / 100;
        }
        return Math.floor(((datavolume * 8.00) / duration) * 100) / 100;

    }

}
