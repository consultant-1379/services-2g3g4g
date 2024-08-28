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
package eniq.events.server.serviceprovider.impl.eventanalysis;

import static com.ericsson.eniq.events.server.common.ApplicationConstants.*;
import static com.ericsson.eniq.events.server.test.common.ApplicationTestConstants.*;
import static org.junit.Assert.*;

import javax.ws.rs.core.MultivaluedMap;

import org.junit.Before;
import org.junit.Test;

import com.ericsson.eniq.events.server.resources.BaseDataIntegrityTest;
import com.ericsson.eniq.events.server.serviceprovider.impl.eventanalysis.EventAnalysisService;
import com.ericsson.eniq.events.server.test.queryresults.EventAnalysisImsiGroupSummaryResult;
import com.ericsson.eniq.events.server.utils.RequestParametersWrapper;
import com.sun.jersey.core.util.MultivaluedMapImpl;

public class EventAnalysisServiceTest extends BaseDataIntegrityTest<EventAnalysisImsiGroupSummaryResult> {
    private final EventAnalysisService service = new EventAnalysisService();

    @Before
    public void onSetUp() throws Exception {
        attachDependencies(service);
    }

    @Test
    public void shouldUseSuccessRawWillReturnFalseWhenSucRawDisabled() throws Exception {
        jndiProperties.setupDataTieringAndDisableSucRawJNDIProperties();
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(TIME_QUERY_PARAM, ONE_WEEK);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        map.putSingle(GROUP_NAME_PARAM, SAMPLE_IMSI_GROUP);
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, "20");
        final RequestParametersWrapper requestParametersWrapper = new RequestParametersWrapper(map);
        assertFalse(service.shouldUseSuccessRaw(requestParametersWrapper));
    }

    @Test
    public void shouldUseSuccessRawWillReturnTrueWhenSucRawEnabled() throws Exception {
        jndiProperties.useSucRawJNDIProperty();
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(TIME_QUERY_PARAM, ONE_WEEK);
        map.putSingle(TYPE_PARAM, TYPE_IMSI);
        map.putSingle(GROUP_NAME_PARAM, SAMPLE_IMSI_GROUP);
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, "20");
        final RequestParametersWrapper requestParametersWrapper = new RequestParametersWrapper(map);
        assertTrue(service.shouldUseSuccessRaw(requestParametersWrapper));
    }

    @Test
    public void shouldUseSuccessRawWillReturnTrueWhenSucRawDisabledAndTypeIsAPN() throws Exception {
        jndiProperties.setupDataTieringAndDisableSucRawJNDIProperties();
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(TIME_QUERY_PARAM, ONE_WEEK);
        map.putSingle(TYPE_PARAM, TYPE_APN);
        map.putSingle(GROUP_NAME_PARAM, SAMPLE_IMSI_GROUP);
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, "20");
        final RequestParametersWrapper requestParametersWrapper = new RequestParametersWrapper(map);
        assertTrue(service.shouldUseSuccessRaw(requestParametersWrapper));
    }

    @Test
    public void shouldUseSuccessRawWillReturnTrueWhenSucRawEnabledAndTypeIsAPN() throws Exception {
        jndiProperties.setupDataTieringAndDisableSucRawJNDIProperties();
        final MultivaluedMap<String, String> map = new MultivaluedMapImpl();
        map.putSingle(TIME_QUERY_PARAM, ONE_WEEK);
        map.putSingle(TYPE_PARAM, TYPE_APN);
        map.putSingle(GROUP_NAME_PARAM, SAMPLE_IMSI_GROUP);
        map.putSingle(DISPLAY_PARAM, GRID_PARAM);
        map.putSingle(KEY_PARAM, KEY_TYPE_SUM);
        map.putSingle(TZ_OFFSET, TIME_ZONE_OFFSET_OF_PLUS_ONE_HOUR);
        map.putSingle(MAX_ROWS, "20");
        final RequestParametersWrapper requestParametersWrapper = new RequestParametersWrapper(map);
        assertTrue(service.shouldUseSuccessRaw(requestParametersWrapper));
    }
}
