/***************************** BEGIN LICENSE BLOCK ***************************

 The contents of this file are subject to the Mozilla Public License, v. 2.0.
 If a copy of the MPL was not distributed with this file, You can obtain one
 at http://mozilla.org/MPL/2.0/.

 Software distributed under the License is distributed on an "AS IS" basis,
 WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
 for the specific language governing rights and limitations under the License.

 Copyright (C) 2023 Georobotix. All Rights Reserved.

 ******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.datastore.postgis.store.command;

import org.postgresql.util.PGobject;
import org.sensorhub.api.command.*;
import org.sensorhub.api.common.BigId;
import org.sensorhub.api.common.BigIdLong;
import org.sensorhub.api.datastore.DataStoreException;
import org.sensorhub.api.datastore.command.*;
import org.sensorhub.api.datastore.system.ISystemDescStore;
import org.sensorhub.impl.datastore.postgis.IdProviderType;
import org.sensorhub.impl.datastore.postgis.builder.ParameterizedQuery;
import org.sensorhub.impl.datastore.postgis.store.PostgisStore;
import org.sensorhub.impl.datastore.postgis.builder.IteratorResultSet;
import org.sensorhub.impl.datastore.postgis.builder.QueryBuilderCommandStatusStore;
import org.sensorhub.impl.datastore.postgis.utils.PostgisUtils;
import org.sensorhub.impl.datastore.postgis.utils.SerializerUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vast.util.TimeExtent;

import java.io.IOException;
import java.sql.*;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.sensorhub.api.datastore.command.ICommandStatusStore.CommandStatusField.*;


public class PostgisCommandStatusStore extends PostgisStore<QueryBuilderCommandStatusStore> implements ICommandStatusStore {
    private static final Logger logger = LoggerFactory.getLogger(PostgisCommandStoreImpl.class);
    PostgisCommandStoreImpl commandStore;

    public PostgisCommandStatusStore(PostgisCommandStoreImpl commandStore, String url, String dbName, String login, String password,
                                     int idScope, IdProviderType dsIdProviderType, boolean useBatch) {
        super(idScope, dsIdProviderType, new QueryBuilderCommandStatusStore(),useBatch);
        this.init(commandStore, url,dbName,login,password);
    }

    protected void init(PostgisCommandStoreImpl commandStore, String url, String dbName, String login, String password) {
        super.init(url, dbName, login, password, new String[]{
                queryBuilder.createTableQuery(),
                queryBuilder.createCommandReportTimeIndexQuery()
        });
        // PostgisStore.init runs creation scripts only for new tables. Existing
        // deployments also need the lookup index after an upgrade.
        try (Connection connection = connectionManager.getConnection()) {
            PostgisUtils.executeQueries(connection, new String[]{queryBuilder.createCommandReportTimeIndexQuery()});
        } catch (SQLException error) {
            throw new IllegalStateException("Cannot create command status lookup index", error);
        }
        this.commandStore = commandStore;
        queryBuilder.linkTo(this.commandStore);
        queryBuilder.linkTo(this.commandStore.getCommandStreams());
    }

    @Override
    public String getDatastoreName() {
        return queryBuilder.getStoreTableName();
    }

    @Override
    public Stream<Entry<BigId, ICommandStatus>> selectEntries(CommandStatusFilter filter, Set<CommandStatusField> fields) {
        Set<CommandStatusField> hashSet;

        if (fields != null) {
            hashSet = new HashSet<>(fields);
            hashSet.add(COMMAND_ID);
        } else {
            hashSet = null;
        }

        ParameterizedQuery query = queryBuilder.createParameterizedSelectEntriesQuery(filter, hashSet);
        if(logger.isDebugEnabled()) {
            logger.debug(query.sql());
        }
        IteratorResultSet<Entry<BigId, ICommandStatus>> iteratorResultSet =
                new IteratorResultSet<>(
                        query,
                        connectionManager,
                        filter.getLimit(),
                        (resultSet) -> resultSetToEntry(resultSet, fields),
                        (entry) -> (filter.getValuePredicate() == null || filter.getValuePredicate().test(entry.getValue())));
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iteratorResultSet, Spliterator.ORDERED), false);
    }

    private Entry<BigId, ICommandStatus> resultSetToEntry(ResultSet resultSet, Set<ICommandStatusStore.CommandStatusField> fields) {
        try {
            var cmdStatusBuilder = new CommandStatus.Builder();

            BigId id = BigId.fromLong(idScope, resultSet.getLong("id"));
            boolean noFields = fields != null && !fields.isEmpty();
            BigId commandId = null;
            if (!noFields || fields.contains(COMMAND_ID)) {
                long commandIdAsLong = resultSet.getLong(String.valueOf(COMMAND_ID));
                if (!resultSet.wasNull()) {
                    commandId = BigId.fromLong(idScope, commandIdAsLong);
                    cmdStatusBuilder.withCommand(commandId);
                }
            }

            int progress = resultSet.getInt("progress");
            if (progress >= -1) {
                cmdStatusBuilder.withProgress(progress);
            }

            // required
            Timestamp reportTime = resultSet.getTimestamp(String.valueOf(REPORT_TIME));
            if (!resultSet.wasNull()) {
                cmdStatusBuilder.withReportTime(reportTime.toInstant());
            }

            String statusCode = resultSet.getString(String.valueOf(STATUS_CODE));
            if (!resultSet.wasNull()) {
                cmdStatusBuilder.withStatusCode(ICommandStatus.CommandStatusCode.valueOf(statusCode));
            }

            if (!noFields || fields.contains(EXEC_TIME)) {
                PGobject pgRange = (PGobject) resultSet.getObject(String.valueOf(EXEC_TIME));
                if (!resultSet.wasNull()) {
                    Instant[] execTime = getInstantArray(pgRange);
                    cmdStatusBuilder.withExecutionTime(TimeExtent.period(execTime[0], execTime[1]));
                }
            }

            String message = resultSet.getString("message");
            if (!resultSet.wasNull() && message != null && !message.isEmpty()) {
                cmdStatusBuilder.withMessage(message);
            }

            String resultJson = resultSet.getString("result");
            if (!resultSet.wasNull() && resultJson != null && !resultJson.isBlank()) {
                ICommandResult cmdResult;
                BigId commandStreamId = commandId != null ? commandStore.getCommandStreamId(commandId) : null;
                if (commandStreamId != null) {
                    ICommandStreamInfo commandStreamInfo = commandStore.getCommandStreams().get(new CommandStreamKey(commandStreamId));
                    cmdResult = SerializerUtils.readICommandResultJson(resultJson, commandStreamInfo);
                } else {
                    cmdResult = SerializerUtils.readICommandResultJson(resultJson);
                }
                cmdStatusBuilder.withResult(cmdResult);
            }

            return Map.entry(id, cmdStatusBuilder.build());
        } catch (Exception ex) {
            throw new RuntimeException("Cannot parse resultSet to CommandStatus",ex);
        }
    }

    @Override
    public void commit() throws DataStoreException {}

    protected ICommandStreamInfo getContext(ICommandStatus status) {
        var cmdId = status.getCommandID();
        var cmdStreamId = cmdId != null ? commandStore.getCommandStreamId(cmdId) : null;
        if (cmdStreamId == null) {
            return null;
        }
        var cmdStreamKey = new CommandStreamKey(cmdStreamId);
        return commandStore.getCommandStreams().get(cmdStreamKey);
    }

    private PGobject serializeResultJson(ICommandStatus rec) throws SQLException, IOException {
        if (rec.getResult() == null) {
            return null;
        }
        ICommandResult result = rec.getResult();
        String objectAsJson;
        if (result.getInlineRecords() != null && !result.getInlineRecords().isEmpty()) {
            ICommandStreamInfo csInfo = getContext(rec);
            objectAsJson = csInfo != null
                    ? SerializerUtils.writeICommandResultJson(result, csInfo)
                    : SerializerUtils.writeICommandResultJson(result);
        } else {
            objectAsJson = SerializerUtils.writeICommandResultJson(result);
        }
        PGobject jsonObject = new PGobject();
        jsonObject.setType("json");
        jsonObject.setValue(objectAsJson);
        return jsonObject;
    }

    protected PGobject createPGObjectExecTime(TimeExtent timeExtent) throws SQLException {
        PGobject range = new PGobject();
        range.setType("tsrange");  // type PostgreSQL
        String startRangeValue;
        String endRangeValue;

        if (timeExtent == null) {
            startRangeValue = "-infinity";
            endRangeValue = "infinity";
        } else if (timeExtent.beginsNow()) {
            startRangeValue = "-infinity";
            endRangeValue = PostgisUtils.writeInstantToString(timeExtent.end(), false);
        } else if (timeExtent.endsNow()) {
            endRangeValue = "infinity";
            startRangeValue = PostgisUtils.writeInstantToString(timeExtent.begin(), false);
        } else {
            startRangeValue = PostgisUtils.writeInstantToString(timeExtent.begin(), false);
            endRangeValue = PostgisUtils.writeInstantToString(timeExtent.end(), false);
        }
        range.setValue("[\"" + startRangeValue + "\",\"" + endRangeValue + "\"]");
        return range;
    }

    @Override
    public BigId add(ICommandStatus rec) {
        final PGobject jsonObject;
        try {
            jsonObject = serializeResultJson(rec);
        } catch (Exception e) {
            throw new RuntimeException("Cannot insert command", e);
        }
        try (Connection connection1 = this.connectionManager.getConnection()) {
            try (PreparedStatement preparedStatement = connection1.prepareStatement(queryBuilder.insertCommandQuery(), Statement.RETURN_GENERATED_KEYS)) {
                // command ID
                preparedStatement.setLong(1, rec.getCommandID().getIdAsLong());

                // progress
                preparedStatement.setInt(2, rec.getProgress());

                // report time
                preparedStatement.setTimestamp(3, Timestamp.from(rec.getReportTime()));

                // status code
                preparedStatement.setString(4, String.valueOf(rec.getStatusCode()));

                // exec time
                if (rec.getExecutionTime() != null) {
                    preparedStatement.setObject(5, createPGObjectExecTime(rec.getExecutionTime()));
                } else {
                    preparedStatement.setNull(5, Types.OTHER);
                }

                // message
                if (rec.getMessage() != null) {
                    preparedStatement.setString(6,  rec.getMessage());
                } else {
                    preparedStatement.setNull(6, Types.VARCHAR);
                }

                // command result
                if (jsonObject != null) {
                    preparedStatement.setObject(7, jsonObject);
                } else {
                    preparedStatement.setNull(7, Types.OTHER);
                }

                int rows = preparedStatement.executeUpdate();
                if (rows > 0) {
                    try (ResultSet rs = preparedStatement.getGeneratedKeys()) {
                        long generatedKey = 0;
                        if (rs.next()) {
                            generatedKey = rs.getLong(1);
                        }
                        return BigId.fromLong(idScope, generatedKey);
                    }
                } else {
                    throw new RuntimeException("Cannot insert command ");
                }
            } catch (Exception e) {
                throw new RuntimeException("Cannot insert command", e);
            }
        } catch (Exception e) {
            throw new RuntimeException("Cannot insert command", e);
        }
    }

    public static Instant[] getInstantArray(PGobject pgRange) {
        String rangeStr = pgRange.getValue();
        String inner = rangeStr.substring(1, rangeStr.length() - 1);
        String[] parts = inner.split(",", 2);
        String startStr = parts[0].trim().replaceAll("\"","").replace(" ", "T");
        String endStr = parts[0].trim().replaceAll("\"","").replace(" ", "T");

        if (!startStr.endsWith("Z") &&  !endStr.endsWith("Z")) {
            startStr = startStr + "Z";
            endStr = endStr + "Z";
        }

        return new Instant[]{Instant.parse(startStr), Instant.parse(endStr)};
    }

    @Override
    public ICommandStatus get(Object o) {
        if (!(o instanceof BigId)) {
            throw new UnsupportedOperationException("Get operation is not supported with argument != BigId key, got=" + o.getClass());
        }
        BigId key = (BigId) o;
        CommandStatus.CommandStatusBuilder<?, ?> cmdStatusBuilder = null;
        long cmdId = 0L;
        String resultJson = null;
        try (Connection connection = this.connectionManager.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(queryBuilder.selectByIdQuery())) {
            preparedStatement.setLong(1, key.getIdAsLong());
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                if (resultSet.next()) {
                    cmdStatusBuilder = new CommandStatus.Builder();

                    // command ID
                    cmdId = resultSet.getLong(String.valueOf(COMMAND_ID));
                    cmdStatusBuilder.withCommand(new BigIdLong(commandStore.idScope, cmdId));

                    // progress
                    int progress = resultSet.getInt("progress");
                    cmdStatusBuilder.withProgress(!resultSet.wasNull() ? progress : -1);

                    // required
                    Timestamp reportTime = resultSet.getTimestamp(String.valueOf(REPORT_TIME));
                    if (!resultSet.wasNull()) {
                        cmdStatusBuilder.withReportTime(reportTime.toInstant());
                    }

                    String statusCode = resultSet.getString(String.valueOf(STATUS_CODE));
                    if (!resultSet.wasNull()) {
                        cmdStatusBuilder.withStatusCode(ICommandStatus.CommandStatusCode.valueOf(statusCode));
                    }

                    PGobject pgRange = (PGobject) resultSet.getObject(String.valueOf(EXEC_TIME));
                    if (!resultSet.wasNull()) {
                        Instant[] execTime = getInstantArray(pgRange);
                        cmdStatusBuilder.withExecutionTime(TimeExtent.period(execTime[0], execTime[1]));
                    }

                    String message = resultSet.getString("message");
                    if (!resultSet.wasNull() && message != null && !message.isEmpty()) {
                        cmdStatusBuilder.withMessage(message);
                    }

                    String rawResult = resultSet.getString("result");
                    if (!resultSet.wasNull()) {
                        resultJson = rawResult;
                    }
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        if (cmdStatusBuilder == null) {
            return null;
        }
        if (resultJson != null && !resultJson.isBlank()) {
            try {
                ICommandResult cmdResult;
                BigId cmdStreamId = commandStore.getCommandStreamId(new BigIdLong(commandStore.idScope, cmdId));
                if (cmdStreamId != null) {
                    ICommandStreamInfo commandStreamInfo = commandStore.getCommandStreams().get(new CommandStreamKey(cmdStreamId));
                    cmdResult = SerializerUtils.readICommandResultJson(resultJson, commandStreamInfo);
                } else {
                    cmdResult = SerializerUtils.readICommandResultJson(resultJson);
                }
                cmdStatusBuilder.withResult(cmdResult);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return cmdStatusBuilder.build();
    }

    @Override
    public ICommandStatus put(BigId key, ICommandStatus rec) {
        ICommandStatus oldCommand = this.get(key);
        if (oldCommand == null)
            throw new UnsupportedOperationException("put can only be used to update existing entries");
        if ((oldCommand.getCommandID() != null && commandStore.isImmutableCommandId(oldCommand.getCommandID()))
                || (rec != null && rec.getCommandID() != null && commandStore.isImmutableCommandId(rec.getCommandID()))) {
            throw new UnsupportedOperationException(
                    "Adjudication status records are immutable and cannot be modified; append a new adjudication record instead");
        }

        try {
            PGobject jsonObject = serializeResultJson(rec);
            try (Connection connection = this.connectionManager.getConnection();
                 PreparedStatement preparedStatement = connection.prepareStatement(queryBuilder.updateByIdQuery())) {
                // command ID
                preparedStatement.setLong(1, rec.getCommandID().getIdAsLong());

                // progress
                preparedStatement.setInt(2, rec.getProgress());

                // report time
                preparedStatement.setTimestamp(3, Timestamp.from(rec.getReportTime()));

                // status code
                preparedStatement.setString(4, String.valueOf(rec.getStatusCode()));

                // exec time
                if (rec.getExecutionTime() != null) {
                    preparedStatement.setObject(5, createPGObjectExecTime(rec.getExecutionTime()));
                } else {
                    preparedStatement.setNull(5, Types.OTHER);
                }

                // message
                if (rec.getMessage() != null) {
                    preparedStatement.setString(6,  rec.getMessage());
                } else {
                    preparedStatement.setNull(6, Types.VARCHAR);
                }

                // command result
                if (jsonObject != null) {
                    preparedStatement.setObject(7, jsonObject);
                } else {
                    preparedStatement.setNull(7, Types.OTHER);
                }

                int rows = preparedStatement.executeUpdate();
                if (rows > 0) {
                    return rec;
                } else {
                    throw new RuntimeException("Cannot update command ");
                }
            }
        } catch (SQLException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public ICommandStatus remove(Object o) {
        if (!(o instanceof BigId)) {
            throw new UnsupportedOperationException("Remove operation is not supported with argument != BigId key, got=" + o.getClass());
        }
        BigId key = (BigId) o;
        ICommandStatus data = this.get(o);
        if (data != null && data.getCommandID() != null && commandStore.isImmutableCommandId(data.getCommandID())) {
            throw new UnsupportedOperationException(
                    "Adjudication status records are immutable and cannot be deleted");
        }
        try (Connection connection = this.connectionManager.getConnection()) {
            try (PreparedStatement preparedStatement = connection.prepareStatement(queryBuilder.removeByIdQuery())) {
                preparedStatement.setLong(1, key.getIdAsLong());
                preparedStatement.executeUpdate();
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return data;
    }

    @Override
    public long removeEntries(CommandStatusFilter filter) {
        try (Stream<Entry<BigId, ICommandStatus>> matching = selectEntries(filter, Set.of(COMMAND_ID))) {
            if (matching.anyMatch(entry -> entry.getValue() != null
                    && entry.getValue().getCommandID() != null
                    && commandStore.isImmutableCommandId(entry.getValue().getCommandID()))) {
                throw new UnsupportedOperationException(
                        "Adjudication status records are immutable and cannot be deleted");
            }
        }
        return ICommandStatusStore.super.removeEntries(filter);
    }

    @Override
    public boolean containsValue(Object value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean containsKey(Object key) {
        return this.get(key) != null;
    }


    @Override
    public Set<Map.Entry<BigId, ICommandStatus>> entrySet() {
        CommandStatusFilter filter = new CommandStatusFilter.Builder().build();
        return this.selectEntries(filter, new HashSet<>()).collect(Collectors.toSet());
    }

    public Collection<ICommandStatus> values() {
        return this.entrySet().stream().map(Map.Entry::getValue).collect(Collectors.toSet());
    }

    public Set<BigId> keySet() {
        return this.entrySet().stream().map(Map.Entry::getKey).collect(Collectors.toSet());
    }

    public void linkTo(ISystemDescStore systemDescStore) {
        super.linkTo(systemDescStore);
    }
    public void linkTo(ICommandStreamStore commandStreamStore) { super.linkTo(commandStreamStore); }
    public void linkTo(ICommandStore commandStore) {
        super.linkTo(commandStore);
    }
}
