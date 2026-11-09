/***************************** BEGIN LICENSE BLOCK ***************************

 The contents of this file are subject to the Mozilla Public License, v. 2.0.
 If a copy of the MPL was not distributed with this file, You can obtain one
 at http://mozilla.org/MPL/2.0/.

 Software distributed under the License is distributed on an "AS IS" basis,
 WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
 for the specific language governing rights and limitations under the License.

 Copyright (C) 2023 Georobotix. All Rights Reserved.

 ******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.datastore.postgis;

import org.junit.After;
import org.junit.Test;
import org.sensorhub.api.command.*;
import org.sensorhub.api.common.BigId;
import org.sensorhub.api.datastore.DataStoreException;
import org.sensorhub.api.datastore.command.CommandStatusFilter;
import org.sensorhub.api.feature.FeatureId;
import org.sensorhub.impl.datastore.AbstractTestCommandStore;
import org.sensorhub.impl.datastore.postgis.store.command.PostgisCommandStatusStore;
import org.sensorhub.impl.datastore.postgis.store.command.PostgisCommandStoreImpl;
import org.sensorhub.impl.datastore.postgis.store.command.PostgisCommandStreamStoreImpl;
import org.vast.data.DataRecordImpl;
import org.vast.data.TextEncodingImpl;
import org.vast.swe.SWEHelper;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.*;

public class TestPostgisCommandStore extends AbstractTestCommandStore<PostgisCommandStoreImpl> {
    protected static String COMMAND_DATASTORE_NAME = "test_command";

    private static String DB_NAME = "gis";

    private final String url = "localhost:5432";
    private final String login = "postgres";
    private final String password = "postgres";

    protected  PostgisCommandStoreImpl postgisCommandStore;
    //    @Override
    protected PostgisCommandStoreImpl initStore() throws Exception {
        postgisCommandStore = new PostgisCommandStoreImpl(url, DB_NAME, login, password, COMMAND_DATASTORE_NAME, DATABASE_NUM, IdProviderType.UID_HASH, false);
        return postgisCommandStore;
    }


    @Test
    @Override
    public void testGetDatastoreName() throws Exception
    {
        assertEquals(COMMAND_DATASTORE_NAME, cmdStore.getDatastoreName());
        forceReadBackFromStorage();
        assertEquals(COMMAND_DATASTORE_NAME, cmdStore.getDatastoreName());
    }

    protected void forceReadBackFromStorage() {

    }


    @After
    public void cleanup() throws Exception {
        postgisCommandStore.drop();
        ((PostgisCommandStreamStoreImpl)postgisCommandStore.getCommandStreams()).drop();
        ((PostgisCommandStatusStore)postgisCommandStore.getStatusReports()).drop();
    }

    @Test
    @Override
    public void testAddAndCheckMapKeys() throws Exception
    {
        var csKey = addSimpleCommandStream(bigId(10), "out1");
        addCommands(csKey.getInternalID(), Instant.parse("2000-01-01T00:00:00Z"), 10);
        checkMapKeySet(cmdStore.keySet());

        forceReadBackFromStorage();
        checkMapKeySet(cmdStore.keySet());

        csKey = addSimpleCommandStream(bigId(10), "out2");
        addCommands(csKey.getInternalID(), Instant.parse("-4700-01-01T00:00:00Z").plusSeconds(1), 11);
        addCommands(csKey.getInternalID(), Instant.parse("3000-01-01T00:00:00Z").minus(10, ChronoUnit.DAYS), 11);
        checkMapKeySet(cmdStore.keySet());

        csKey = addSimpleCommandStream(bigId(456), "output");
        addCommands(csKey.getInternalID(), Instant.parse("1950-01-01T00:00:00.5648712Z"), 56);
        checkMapKeySet(cmdStore.keySet());

        forceReadBackFromStorage();
        checkMapKeySet(cmdStore.keySet());
    }

    private void checkCommandStatusEqual(ICommandStatus o1, ICommandStatus o2) {
        assertEquals(o1.getCommandID(), o2.getCommandID());
        assertEquals(o1.getExecutionTime(), o2.getExecutionTime());
        assertEquals(o1.getMessage(), o2.getMessage());
        assertEquals(o1.getProgress(), o2.getProgress());
        assertTrue(o1.getReportTime().getNano() - o2.getReportTime().getNano() < 1000);
        assertEquals(o1.getStatusCode(), o2.getStatusCode());
    }

    @Override
    protected void checkSelectedEntries(Stream<Map.Entry<BigId, ICommandStatus>> resultStream, Map<BigId, ICommandStatus> expectedResults, CommandStatusFilter filter)
    {
        System.out.println("Select status with " + filter);

        var resultMap = resultStream
                .peek(e -> System.out.println(e.getKey() + ": " + e.getValue()))
                //.peek(e -> System.out.println(Arrays.toString((double[])e.getValue().getResult().getUnderlyingObject())))
                .collect(Collectors.toMap(e->e.getKey(), e->e.getValue()));
        System.out.println(resultMap.size() + " entries selected");

        resultMap.forEach((k, v) -> {
            assertTrue("Result set contains extra key "+k, expectedResults.containsKey(k));
            checkCommandStatusEqual(expectedResults.get(k), v);
            assertEquals("Invalid scope", DATABASE_NUM, k.getScope());
        });

        assertEquals(expectedResults.size(), resultMap.size());
        expectedResults.forEach((k, v) -> {
            assertTrue("Result set is missing key "+k, resultMap.containsKey(k));
        });
    }

    @Test
    public void testAddAndGetCommandStatus() throws DataStoreException {
        SWEHelper fac = new SWEHelper();
        var paramDesc = fac.createRecord()
                .name("testParams")
                .addField("field1", fac.createText())
                .build();

        var resDesc = fac.createRecord()
                .name("testResult")
                .addField("resField2", fac.createText())
                .addField("resField3", fac.createText())
                .addField("resField4", fac.createText())
                .addField("resField5", fac.createText())
                .addField("resField9", fac.createText())
                .addField("resField10", fac.createText())
                .addField("resField6", fac.createText())
                .addField("resField1", fac.createText())
                .addField("resField7", fac.createText())
                .addField("resField8", fac.createText())
                .build();


        var csWithRes = new CommandStreamInfo.Builder()
                .withName("testCs1")
                .withRecordDescription(paramDesc)
                .withRecordEncoding(new TextEncodingImpl())
                .withResultDescription(resDesc)
                .withSystem(new FeatureId(bigId(2), "urn:system:test1"))
                .build();
        var csId = cmdStore.getCommandStreams().add(csWithRes);

        var cmd = new CommandData.Builder()
                .withCommandStream(csId.getInternalID())
                .withId(bigId(1))
                .withParams(paramDesc.createDataBlock())
                .build();

        var cmdId = cmdStore.add(cmd);
        var cmdRes = CommandResult.withData(resDesc.createDataBlock());
        var cmdStatus = new CommandStatus.Builder()
                .withCommand(cmdId)
                .withResult(cmdRes)
                .withStatusCode(ICommandStatus.CommandStatusCode.ACCEPTED)
                .build();

        var cmdStatusId = cmdStore.getStatusReports().add(cmdStatus);

        var status = cmdStore.getStatusReports().get(cmdStatusId);
        var statusRecs = status.getResult().getInlineRecords();
        assertNotNull(statusRecs);
        assertFalse(statusRecs.isEmpty());
    }

    @Test
    public void testGetWrongKey() throws Exception
    {
        testGetNumRecordsOneDataStream();
        assertNull(cmdStore.get(bigId(1100L)));
    }

    @Test
    public void testSelectCommandsByDataStreamIDAndPredicates() throws Exception {
        super.testSelectCommandsByDataStreamIDAndPredicates();
    }

    @Test
    public void testAdjudicationRecordsAreImmutableAndAppendOnly() throws Exception {
        SWEHelper fac = new SWEHelper();
        var paramDesc = fac.createRecord()
                .name("adjudicationControl")
                .addField("feedback", fac.createText())
                .build();
        var resDesc = fac.createRecord()
                .name("result")
                .addField("username", fac.createText())
                .addField("feedback", fac.createText())
                .build();

        var adjStream = new CommandStreamInfo.Builder()
                .withName("adjudicationControl")
                .withRecordDescription(paramDesc)
                .withRecordEncoding(new TextEncodingImpl())
                .withResultDescription(resDesc)
                .withResultEncoding(new TextEncodingImpl())
                .withSystem(new FeatureId(bigId(2), "urn:osh:system:lane:test-adj"))
                .build();
        var csKey = cmdStore.getCommandStreams().add(adjStream);

        var firstParams = paramDesc.createDataBlock();
        firstParams.setStringValue(0, "First immutable adjudication");
        var firstCmd = new CommandData.Builder()
                .withCommandStream(csKey.getInternalID())
                .withSender("operator1")
                .withIssueTime(Instant.parse("2026-11-09T12:00:00Z"))
                .withParams(firstParams)
                .build();
        var firstCmdId = cmdStore.add(firstCmd);

        var firstResBlock = resDesc.createDataBlock();
        firstResBlock.setStringValue(0, "operator1");
        firstResBlock.setStringValue(1, "First immutable adjudication");
        var firstStatus = new CommandStatus.Builder()
                .withCommand(firstCmdId)
                .withReportTime(Instant.parse("2026-11-09T12:00:01Z"))
                .withStatusCode(ICommandStatus.CommandStatusCode.COMPLETED)
                .withResult(CommandResult.withData(firstResBlock))
                .build();
        var firstStatusId = cmdStore.getStatusReports().add(firstStatus);

        // Appending a second adjudication record on the same stream succeeds
        var secondParams = paramDesc.createDataBlock();
        secondParams.setStringValue(0, "Second immutable adjudication");
        var secondCmd = new CommandData.Builder()
                .withCommandStream(csKey.getInternalID())
                .withSender("operator2")
                .withIssueTime(Instant.parse("2026-11-09T12:05:00Z"))
                .withParams(secondParams)
                .build();
        var secondCmdId = cmdStore.add(secondCmd);
        assertNotEquals(firstCmdId, secondCmdId);

        // In-place modification or deletion of an existing adjudication command or status is rejected
        assertThrows(UnsupportedOperationException.class, () -> cmdStore.put(firstCmdId, secondCmd));
        assertThrows(UnsupportedOperationException.class, () -> cmdStore.remove(firstCmdId));
        assertThrows(UnsupportedOperationException.class, () -> cmdStore.getStatusReports().put(firstStatusId, firstStatus));
        assertThrows(UnsupportedOperationException.class, () -> cmdStore.getStatusReports().remove(firstStatusId));

        // Original adjudication command and status remain intact
        var storedFirstCmd = cmdStore.get(firstCmdId);
        assertNotNull(storedFirstCmd);
        assertEquals("First immutable adjudication", storedFirstCmd.getParams().getStringValue(0));
        var storedFirstStatus = cmdStore.getStatusReports().get(firstStatusId);
        assertNotNull(storedFirstStatus);
        assertEquals("First immutable adjudication",
                storedFirstStatus.getResult().getInlineRecords().iterator().next().getStringValue(1));
    }
}
