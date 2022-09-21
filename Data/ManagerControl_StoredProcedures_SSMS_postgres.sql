
CREATE OR REPLACE PROCEDURE mc.ack_manager_update_required
(
    _managerName text,
    INOUT _message text = ''
)
LANGUAGE plpgsql
AS $$
/****************************************************
**
**  Desc:
**      Acknowledges that a manager has seen that
**      ManagerUpdateRequired is True in the manager control DB
**
**      This SP will thus set ManagerUpdateRequired to False for this manager
**
**  Auth:   mem
**  Date:   01/16/2009 mem - Initial version
**          09/09/2009 mem - Added support for 'ManagerUpdateRequired' already being False
**          09/21/2022 mem - Ported to PostgreSQL
**
*****************************************************/
DECLARE
    _myRowCount int := 0;
    _mgrID int;
    _paramID int;
BEGIN
    _message := '';

    ---------------------------------------------------
    -- Confirm that the manager name is valid
    ---------------------------------------------------

    SELECT mgr_id INTO _mgrID
    FROM mc.t_mgrs
    WHERE (mgr_name = _managerName)
    --
    GET DIAGNOSTICS _myRowCount = ROW_COUNT;

    if _myRowCount <> 1 Then
        _myError := 52002;
        _message := 'Could not find entry for manager: ' || _managername;
        Return;
    End If;

    ---------------------------------------------------
    -- Update the 'ManagerUpdateRequired' entry for this manager
    ---------------------------------------------------

    UPDATE mc.t_param_value
    SET value = 'False'
    FROM mc.t_param_type
         INNER JOIN mc.t_param_value
           ON mc.t_param_type.param_id = mc.t_param_value.type_id
    WHERE (mc.t_param_type.param_name = 'ManagerUpdateRequired') AND
          (mc.t_param_value.mgr_id = _mgrID) AND
          value <> 'False'
    --
    GET DIAGNOSTICS _myRowCount = ROW_COUNT;

    If _myRowCount > 0 Then
        _message := 'Acknowledged that update is required';
    Else
        -- No rows were updated; may need to make a new entry for 'ManagerUpdateRequired' in the mc.t_param_value table
        _paramID := 0;

        SELECT param_id INTO _paramID
        FROM mc.t_param_type
        WHERE (param_name = 'ManagerUpdateRequired')
        --
        GET DIAGNOSTICS _myRowCount = ROW_COUNT;

        If _paramID > 0 Then
            If Exists (SELECT * FROM mc.t_param_value WHERE mgr_id = _mgrID AND type_id = _paramID) Then
                _message := 'ManagerUpdateRequired was already acknowledged in mc.t_param_value';
            Else
                INSERT INTO mc.t_param_value (mgr_id, type_id, value)
                VALUES (_mgrID, _paramID, 'False')

                _message := 'Acknowledged that update is required (added new entry to mc.t_param_value)';
            End If;
        End If;
    End If;

    ---------------------------------------------------
    -- Exit the procedure
    ---------------------------------------------------
Done:
    return _myError

END
$$;

COMMENT ON PROCEDURE mc.ack_manager_update_required IS 'AckManagerUpdateRequired';

CREATE OR REPLACE PROCEDURE mc.alter_entered_by_user
(
    _targetTableName text,
    _targetIDColumnName text,
    _targetID int,
    _newUser text,
    _applyTimeFilter int = 1,
    _entryTimeWindowSeconds int = 15,
    _entryDateColumnName text = 'Entered',
    _enteredByColumnName text = 'Entered_By',
    INOUT _message text = '',
    _infoOnly int = 0,
    _previewSql int = 0
)
LANGUAGE plpgsql
AS $$
/****************************************************
**
**  Desc:   Updates the Entered_By column for the specified row in the given table to be _newUser
**
**          If _applyTimeFilter is non-zero, then only matches entries made within the last
**            _entryTimeWindowSeconds seconds
**
**          Use _infoOnly = 1 to preview updates
**
**  Arguments:
**    _applyTimeFilter          If 1, then filters by the current date and time; if 0, looks for the most recent matching entry
**    _entryTimeWindowSeconds   Only used if _applyTimeFilter = 1
**
**  Auth:   mem
**  Date:   03/25/2008 mem - Initial version (Ticket: #644)
**          05/23/2008 mem - Expanded _entryDescription to varchar(512)
**          09/21/2022 mem - Ported to PostgreSQL
**
*****************************************************/
DECLARE
    _myRowCount int := 0;
    _entryDateStart timestamp;
    _entryDateEnd timestamp;
    _entryDescription text;
    _entryIndex int;
    _matchIndex int;
    _enteredBy text;
    _enteredByNew text;
    _currentTime timestamp;
    _s text;
    _entryFilterSql text;
    _paramDef text;
    _result int;
    _targetIDMatch int;
BEGIN
    _enteredByNew := '';

    _currentTime := CURRENT_TIMESTAMP;

    _entryFilterSql := '';

    ------------------------------------------------
    -- Validate the inputs
    ------------------------------------------------

    _newUser := Coalesce(_newUser, '');
    _applyTimeFilter := Coalesce(_applyTimeFilter, 0);
    _entryTimeWindowSeconds := Coalesce(_entryTimeWindowSeconds, 15);
    _message := '';
    _infoOnly := Coalesce(_infoOnly, 0);
    _previewSql := Coalesce(_previewSql, 0);

    If _targetTableName Is Null Or _targetIDColumnName Is Null Or _targetID Is Null Then
        _message := '_targetTableName and _targetIDColumnName and _targetID must be defined; unable to continue';
        _myError := 50201;
        Return;
    End If;

    If char_length(_newUser) = 0 Then
        _message := '_newUser is empty; unable to continue';
        _myError := 50202;
        Return;
    End If;

    _entryDescription := 'ID ' || _targetID::text || ' in table ' || _targetTableName || ' (column ' || _targetIDColumnName || ')';

    _s := '';
    _s := _s || '    SELECT _targetIDMatch = [' || _targetIDColumnName || '],';
    _s := _s ||        ' _enteredBy = [' || _enteredByColumnName || ']';
    _s := _s || ' FROM [' || _targetTableName || ']';
    _s := _s || ' WHERE [' || _targetIDColumnName || '] = ' || _targetID::text;

    If _applyTimeFilter <> 0 And Coalesce(_entryTimeWindowSeconds, 0) >= 1 Then
        ------------------------------------------------
        -- Filter using the current date/time
        ------------------------------------------------
        --
        _entryDateStart := DateAdd(second, -_entryTimeWindowSeconds, _currentTime);
        _entryDateEnd := DateAdd(second, 1, _currentTime);

        If _infoOnly <> 0 Then
            RAISE INFO '%', 'Filtering on entries dated between ' || Convert(text, _entryDateStart, 120) || ' and ' || Convert(text, _entryDateEnd, 120) || ' (Window = ' || _entryTimeWindowSeconds::text || ' seconds)';
        End If;

        _entryFilterSql := ' [' || _entryDateColumnName || '] Between ''' || Convert(text, _entryDateStart, 120) || ''' And ''' || Convert(text, _entryDateEnd, 120) || '''';
        _s := _s || ' AND ' || _entryFilterSql;

        _entryDescription := _entryDescription || ' with ' || _entryFilterSql;
    End If;

    _paramDef := '_targetIDMatch int output, _enteredBy text output';

    If _previewSql <> 0 Then
        RAISE INFO '%', _s;
        _enteredBy := session_user || '_Simulated';
    Else
        Call _result => sp_executesql _s, _paramDef, _targetIDMatch => _targetIDMatch output, _enteredBy => _enteredBy output;
    End If;
    --
    GET DIAGNOSTICS _myRowCount = ROW_COUNT;

    If _myError <> 0 Then
        _message := 'Error looking for ' || _entryDescription;
        Return;
    End If;

    If _previewSql = 0 AND (_myRowCount <= 0 Or _targetIDMatch <> _targetID) Then
        _message := 'Match not found for ' || _entryDescription;
    Else
        -- Confirm that _enteredBy doesn't already contain _newUser
        -- If it does, then there's no need to update it

        _matchIndex := position(_newUser in _enteredBy);
        If _matchIndex > 0 Then
            _message := 'Entry ' || _entryDescription || ' is already attributed to ' || _newUser || ': "' || _enteredBy || '"';
            Return;
        End If;

        -- Look for a semicolon in _enteredBy

        _matchIndex := position(';' in _enteredBy);

        If _matchIndex > 0 Then
            _enteredByNew := _newUser || ' (via ' || SubString(_enteredBy, 1, _matchIndex-1) || ')' || SubString(_enteredBy, _matchIndex, char_length(_enteredBy));
        Else
            _enteredByNew := _newUser || ' (via ' || _enteredBy || ')';
        End If;

        If char_length(Coalesce(_enteredByNew, '')) > 0 Then

            If _infoOnly = 0 Then
                _s := '';
                _s := _s || ' UPDATE [' || _targetTableName || ']';
                _s := _s || ' SET [' || _enteredByColumnName || '] = ''' || _enteredByNew || '''';
                _s := _s || ' WHERE [' || _targetIDColumnName || '] = ' || _targetID::text;

                If char_length(_entryFilterSql) > 0 Then
                    _s := _s || ' AND ' || _entryFilterSql;
                End If;

                If _previewSql <> 0 Then
                    RAISE INFO '%', @S;
                Else
                    Call (_s);
                End If;
                --
                GET DIAGNOSTICS _myRowCount = ROW_COUNT;

                If _myError <> 0 Then
                    _message := 'Error updating ' || _entryDescription;
                    Call post_log_entry 'Error', _message, 'AlterEventLogEntryUser'
                    Return;
                Else
                    _message := 'Updated ' || _entryDescription || ' to indicate "' || _enteredByNew || '"';
                End If;
            Else
                _s := '';
                _s := _s || ' SELECT *, ''' || _enteredByNew || ''' AS Entered_By_New';
                _s := _s || ' FROM [' || _targetTableName || ']';
                _s := _s || ' WHERE [' || _targetIDColumnName || '] = ' || _targetID::text;

                If char_length(_entryFilterSql) > 0 Then
                    _s := _s || ' AND ' || _entryFilterSql;
                End If;

                If _previewSql <> 0 Then
                    RAISE INFO '%', @S;
                Else
                    Call (_s);
                End If;
                --
                GET DIAGNOSTICS _myRowCount = ROW_COUNT;

                _message := 'Would update ' || _entryDescription || ' to indicate "' || _enteredByNew || '"';
            End If;

        Else
            _message := 'Match not found; unable to continue';
        End If;

    End If;

Done:
    return _myError

END
$$;

COMMENT ON PROCEDURE mc.alter_entered_by_user IS 'AlterEnteredByUser';

CREATE OR REPLACE PROCEDURE mc.alter_entered_by_user_multi_id
(
    _targetTableName text,
    _targetIDColumnName text,
    _newUser text,
    _applyTimeFilter int = 1,
    _entryTimeWindowSeconds int = 15,
    _entryDateColumnName text = 'Entered',
    _enteredByColumnName text = 'Entered_By',
    INOUT _message text = '',
    _infoOnly int = 0,
    _previewSql int = 0
)
LANGUAGE plpgsql
AS $$
/****************************************************
**
**  Desc:   Calls AlterEnteredByUser for each entry in TmpIDUpdateList
**
**          The calling procedure must create and populate temporary table TmpIDUpdateList:
**              CREATE TABLE TmpIDUpdateList (
**                  TargetID int NOT NULL
**              )
**
**          Increased performance can be obtained by adding an index to the table; thus
**          it is advisable that the calling procedure also create this index:
**              CREATE CLUSTERED INDEX #IX_TmpIDUpdateList ON TmpIDUpdateList (TargetID)
**
**  Arguments:
**    _applyTimeFilter          If 1, then filters by the current date and time; if 0, looks for the most recent matching entry
**    _entryTimeWindowSeconds   Only used if _applyTimeFilter = 1
**
**  Auth:   mem
**  Date:   03/28/2008 mem - Initial version (Ticket: #644)
**          05/23/2008 mem - Expanded _entryDescription to varchar(512)
**          09/21/2022 mem - Ported to PostgreSQL
**
*****************************************************/
DECLARE
    _myRowCount int := 0;
    _entryDateStart timestamp;
    _entryDateEnd timestamp;
    _entryDescription text;
    _entryIndex int;
    _matchIndex int;
    _enteredBy text;
    _enteredByNew text;
    _currentTime timestamp;
    _targetID int;
    _countUpdated int;
    _continue int;
    _startTime timestamp;
    _entryTimeWindowSecondsCurrent int;
    _elapsedSeconds int;
BEGIN
    _enteredByNew := '';

    _currentTime := CURRENT_TIMESTAMP;

    ------------------------------------------------
    -- Validate the inputs
    ------------------------------------------------

    _newUser := Coalesce(_newUser, '');
    _applyTimeFilter := Coalesce(_applyTimeFilter, 0);
    _entryTimeWindowSeconds := Coalesce(_entryTimeWindowSeconds, 15);
    _message := '';
    _infoOnly := Coalesce(_infoOnly, 0);
    _previewSql := Coalesce(_previewSql, 0);

    If _targetTableName Is Null Or _targetIDColumnName Is Null Then
        _message := '_targetTableName and _targetIDColumnName must be defined; unable to continue';
        _myError := 50201;
        Return;
    End If;

    If char_length(_newUser) = 0 Then
        _message := '_newUser is empty; unable to continue';
        _myError := 50202;
        Return;
    End If;

    -- Make sure TmpIDUpdateList is not empty
    SELECT COUNT(*) INTO _myRowCount
    FROM TmpIDUpdateList

    If _myRowCount <= 0 Then
        _message := 'TmpIDUpdateList is empty; nothing to do';
        Return;
    End If;

    ------------------------------------------------
    -- Initialize _entryTimeWindowSecondsCurrent
    -- This variable will be automatically increased
    --  if too much time elapses
    ------------------------------------------------
    --
    _startTime := CURRENT_TIMESTAMP;
    _entryTimeWindowSecondsCurrent := _entryTimeWindowSeconds;

    ------------------------------------------------
    -- Determine the minimum value in TmpIDUpdateList
    ------------------------------------------------

    SELECT Min(TargetID)-1 INTO _targetID
    FROM TmpIDUpdateList
    --
    GET DIAGNOSTICS _myRowCount = ROW_COUNT;

    _targetID := Coalesce(_targetID, -1);

    ------------------------------------------------
    -- Parse the values in TmpIDUpdateList
    -- Call public.alter_entered_by_user for each
    ------------------------------------------------

    _countUpdated := 0;
    _continue := 1;

    While _continue = 1 Loop
        -- This While loop can probably be converted to a For loop; for example:
        --    For _itemName In
        --        SELECT item_name
        --        FROM TmpSourceTable
        --        ORDER BY entry_id
        --    Loop
        --        ...
        --    End Loop

        -- Moved to bottom of query: TOP 1
        SELECT TargetID INTO _targetID
        FROM TmpIDUpdateList
        WHERE TargetID > _targetID
        ORDER BY TargetID
        LIMIT 1;
        --
        GET DIAGNOSTICS _myRowCount = ROW_COUNT;

        If _myRowCount = 0 Then
            _continue := 0;
        Else
            Call alter_entered_by_user
                                _targetTableName,
                                _targetIDColumnName,
                                _targetID,
                                _newUser,
                                _applyTimeFilter,
                                _entryTimeWindowSecondsCurrent,
                                _entryDateColumnName,
                                _enteredByColumnName,
                                _message output,
                                _infoOnly,
                                _previewSql

            If _myError <> 0 Then
                Goto Done;
            End If;

            _countUpdated := _countUpdated + 1;
            If _countUpdated % 5 = 0 Then
                _elapsedSeconds := DateDiff(second, _startTime, CURRENT_TIMESTAMP);

                If _elapsedSeconds * 2 > _entryTimeWindowSecondsCurrent Then
                    _entryTimeWindowSecondsCurrent := _elapsedSeconds * 4;
                End If;
            End If;
        End If;
    End Loop;

Done:
    return _myError

END
$$;

COMMENT ON PROCEDURE mc.alter_entered_by_user_multi_id IS 'AlterEnteredByUserMultiID';

CREATE OR REPLACE PROCEDURE mc.alter_event_log_entry_user
(
    _targetType int,
    _targetID int,
    _targetState int,
    _newUser text,
    _applyTimeFilter int = 1,
    _entryTimeWindowSeconds int = 15,
    INOUT _message text = '',
    _infoOnly int = 0
)
LANGUAGE plpgsql
AS $$
/****************************************************
**
**  Desc:   Updates the user associated with a given event log entry to be _newUser
**
**          If _applyTimeFilter is non-zero, then only matches entries made within the last
**            _entryTimeWindowSeconds seconds
**
**          Use _infoOnly = 1 to preview updates
**
**  Arguments:
**    _targetType               1=Manager Enable/Disable
**    _applyTimeFilter          If 1, then filters by the current date and time; if 0, looks for the most recent matching entry
**    _entryTimeWindowSeconds   Only used if _applyTimeFilter = 1
**
**  Auth:   mem
**  Date:   02/29/2008 mem - Initial version (Ticket: #644)
**          05/23/2008 mem - Expanded _entryDescription to varchar(512)
**          03/30/2009 mem - Ported to the Manager Control DB
**          09/21/2022 mem - Ported to PostgreSQL
**
*****************************************************/
DECLARE
    _myRowCount int := 0;
    _entryDateStart timestamp;
    _entryDateEnd timestamp;
    _entryDescription text;
    _eventID int;
    _matchIndex int;
    _enteredBy text;
    _enteredByNew text;
    _currentTime timestamp;
BEGIN
    _enteredByNew := '';

    _currentTime := CURRENT_TIMESTAMP;

    ------------------------------------------------
    -- Validate the inputs
    ------------------------------------------------

    _newUser := Coalesce(_newUser, '');
    _applyTimeFilter := Coalesce(_applyTimeFilter, 0);
    _entryTimeWindowSeconds := Coalesce(_entryTimeWindowSeconds, 15);
    _message := '';
    _infoOnly := Coalesce(_infoOnly, 0);

    If _targetType Is Null Or _targetID Is Null Or _targetState Is Null Then
        _message := '_targetType and _targetID and _targetState must be defined; unable to continue';
        _myError := 50201;
        Return;
    End If;

    If char_length(_newUser) = 0 Then
        _message := '_newUser is empty; unable to continue';
        _myError := 50202;
        Return;
    End If;

    _entryDescription := 'ID ' || _targetID::text || ' (type ' || _targetType::text || ') with state ' || _targetState::text;
    If _applyTimeFilter <> 0 And Coalesce(_entryTimeWindowSeconds, 0) >= 1 Then
        ------------------------------------------------
        -- Filter using the current date/time
        ------------------------------------------------
        --
        _entryDateStart := DateAdd(second, -_entryTimeWindowSeconds, _currentTime);
        _entryDateEnd := DateAdd(second, 1, _currentTime);

        If _infoOnly <> 0 Then
            RAISE INFO '%', 'Filtering on entries dated between ' || Convert(text, _entryDateStart, 120) || ' and ' || Convert(text, _entryDateEnd, 120) || ' (Window = ' || _entryTimeWindowSeconds::text || ' seconds)';
        End If;

        SELECT EL.event_id, INTO _eventID
               _enteredBy = EL.entered_by
        FROM mc.t_event_log EL INNER JOIN
                (SELECT MAX(event_id) AS Event_ID
                 FROM dbo.t_event_log
                 WHERE target_type = _targetType AND
                       target_id = _targetID AND
                       target_state = _targetState AND
                       entered Between _entryDateStart And _entryDateEnd
                ) LookupQ ON EL.event_id = LookupQ.event_id
        --
        GET DIAGNOSTICS _myRowCount = ROW_COUNT;

        _entryDescription := _entryDescription || ' and Entry Time between ' || Convert(text, _entryDateStart, 120) || ' and ' || Convert(text, _entryDateEnd, 120);
    Else
        ------------------------------------------------
        -- Do not filter by time
        ------------------------------------------------
        --
        SELECT EL.event_id, INTO _eventID
               _enteredBy = EL.entered_by
        FROM mc.t_event_log EL INNER JOIN
                (SELECT MAX(event_id) AS Event_ID
                 FROM dbo.t_event_log
                 WHERE target_type = _targetType AND
                       target_id = _targetID AND
                       target_state = _targetState
                ) LookupQ ON EL.event_id = LookupQ.event_id
        --
        GET DIAGNOSTICS _myRowCount = ROW_COUNT;
    End If;

    If _myError <> 0 Then
        _message := 'Error looking for ' || _entryDescription;
        Return;
    End If;

    If _myRowCount <= 0 Then
        _message := 'Match not found for ' || _entryDescription;
    Else
        -- Confirm that _enteredBy doesn't already contain _newUser
        -- If it does, then there's no need to update it

        _matchIndex := position(_newUser in _enteredBy);
        If _matchIndex > 0 Then
            _message := 'Entry ' || _entryDescription || ' is already attributed to ' || _newUser || ': "' || _enteredBy || '"';
            Return;
        End If;

        -- Look for a semicolon in _enteredBy

        _matchIndex := position(';' in _enteredBy);

        If _matchIndex > 0 Then
            _enteredByNew := _newUser || ' (via ' || SubString(_enteredBy, 1, _matchIndex-1) || ')' || SubString(_enteredBy, _matchIndex, char_length(_enteredBy));
        Else
            _enteredByNew := _newUser || ' (via ' || _enteredBy || ')';
        End If;

        If char_length(Coalesce(_enteredByNew, '')) > 0 Then

            If _infoOnly = 0 Then
                UPDATE mc.t_event_log
                SET entered_by = _enteredByNew
                WHERE event_id = _eventID
                --
                GET DIAGNOSTICS _myRowCount = ROW_COUNT;

                If _myError <> 0 Then
                    _message := 'Error updating ' || _entryDescription;
                    Call post_log_entry 'Error', _message, 'AlterEventLogEntryUser'
                    Return;
                Else
                    _message := 'Updated ' || _entryDescription || ' to indicate "' || _enteredByNew || '"';
                End If;
            Else
                SELECT event_id, target_type, target_id, Target_State,
                       prev_target_state, entered,
                       entered_by AS Entered_By_Old,
                       _enteredByNew AS Entered_By_New
                FROM mc.t_event_log
                WHERE event_id = _eventID
                --
                GET DIAGNOSTICS _myRowCount = ROW_COUNT;

                _message := 'Would update ' || _entryDescription || ' to indicate "' || _enteredByNew || '"';
            End If;

        Else
            _message := 'Match not found; unable to continue';
        End If;

    End If;

Done:
    return _myError

END
$$;

COMMENT ON PROCEDURE mc.alter_event_log_entry_user IS 'AlterEventLogEntryUser';

CREATE OR REPLACE PROCEDURE mc.alter_event_log_entry_user_multi_id
(
    _targetType int,
    _targetState int,
    _newUser text,
    _applyTimeFilter int = 1,
    _entryTimeWindowSeconds int = 15,
    INOUT _message text = '',
    _infoOnly int = 0
)
LANGUAGE plpgsql
AS $$
/****************************************************
**
**  Desc:   Calls AlterEventLogEntryUser for each entry in TmpIDUpdateList
**
**          The calling procedure must create and populate temporary table TmpIDUpdateList:
**              CREATE TABLE TmpIDUpdateList (
**                  TargetID int NOT NULL
**              )
**
**          Increased performance can be obtained by adding an index to the table; thus
**          it is advisable that the calling procedure also create this index:
**              CREATE CLUSTERED INDEX #IX_TmpIDUpdateList ON TmpIDUpdateList (TargetID)
**
**  Arguments:
**    _targetType               1=Manager Enable/Disable
**    _applyTimeFilter          If 1, then filters by the current date and time; if 0, looks for the most recent matching entry
**    _entryTimeWindowSeconds   Only used if _applyTimeFilter = 1
**
**  Auth:   mem
**  Date:   02/29/2008 mem - Initial version (Ticket: #644)
**          05/23/2008 mem - Expanded _entryDescription to varchar(512)
**          03/30/2009 mem - Ported to the Manager Control DB
**          09/21/2022 mem - Ported to PostgreSQL
**
*****************************************************/
DECLARE
    _myRowCount int := 0;
    _entryDateStart timestamp;
    _entryDateEnd timestamp;
    _entryDescription text;
    _entryIndex int;
    _matchIndex int;
    _enteredBy text;
    _enteredByNew text;
    _currentTime timestamp;
    _targetID int;
    _countUpdated int;
    _continue int;
    _startTime timestamp;
    _entryTimeWindowSecondsCurrent int;
    _elapsedSeconds int;
BEGIN
    _enteredByNew := '';

    _currentTime := CURRENT_TIMESTAMP;

    ------------------------------------------------
    -- Validate the inputs
    ------------------------------------------------

    _newUser := Coalesce(_newUser, '');
    _applyTimeFilter := Coalesce(_applyTimeFilter, 0);
    _entryTimeWindowSeconds := Coalesce(_entryTimeWindowSeconds, 15);
    _message := '';
    _infoOnly := Coalesce(_infoOnly, 0);

    If _targetType Is Null Or _targetState Is Null Then
        _message := '_targetType and _targetState must be defined; unable to continue';
        _myError := 50201;
        Return;
    End If;

    If char_length(_newUser) = 0 Then
        _message := '_newUser is empty; unable to continue';
        _myError := 50202;
        Return;
    End If;

    -- Make sure TmpIDUpdateList is not empty
    SELECT COUNT(*) INTO _myRowCount
    FROM TmpIDUpdateList

    If _myRowCount <= 0 Then
        _message := 'TmpIDUpdateList is empty; nothing to do';
        Return;
    End If;

    ------------------------------------------------
    -- Initialize _entryTimeWindowSecondsCurrent
    -- This variable will be automatically increased
    --  if too much time elapses
    ------------------------------------------------
    --
    _startTime := CURRENT_TIMESTAMP;
    _entryTimeWindowSecondsCurrent := _entryTimeWindowSeconds;

    ------------------------------------------------
    -- Determine the minimum value in TmpIDUpdateList
    ------------------------------------------------

    SELECT Min(TargetID)-1 INTO _targetID
    FROM TmpIDUpdateList
    --
    GET DIAGNOSTICS _myRowCount = ROW_COUNT;

    _targetID := Coalesce(_targetID, -1);

    ------------------------------------------------
    -- Parse the values in TmpIDUpdateList
    -- Call public.alter_event_log_entry_user for each
    ------------------------------------------------

    _countUpdated := 0;
    _continue := 1;

    While _continue = 1 Loop
        -- This While loop can probably be converted to a For loop; for example:
        --    For _itemName In
        --        SELECT item_name
        --        FROM TmpSourceTable
        --        ORDER BY entry_id
        --    Loop
        --        ...
        --    End Loop

        -- Moved to bottom of query: TOP 1
        SELECT TargetID INTO _targetID
        FROM TmpIDUpdateList
        WHERE TargetID > _targetID
        ORDER BY TargetID
        LIMIT 1;
        --
        GET DIAGNOSTICS _myRowCount = ROW_COUNT;

        If _myRowCount = 0 Then
            _continue := 0;
        Else
            Call alter_event_log_entry_user
                                _targetType,
                                _targetID,
                                _targetState,
                                _newUser,
                                _applyTimeFilter,
                                _entryTimeWindowSeconds,
                                _message output,
                                _infoOnly

            If _myError <> 0 Then
                Goto Done;
            End If;

            _countUpdated := _countUpdated + 1;
            If _countUpdated % 5 = 0 Then
                _elapsedSeconds := DateDiff(second, _startTime, CURRENT_TIMESTAMP);

                If _elapsedSeconds * 2 > _entryTimeWindowSecondsCurrent Then
                    _entryTimeWindowSecondsCurrent := _elapsedSeconds * 4;
                End If;
            End If;
        End If;
    End Loop;

Done:
    return _myError

END
$$;

COMMENT ON PROCEDURE mc.alter_event_log_entry_user_multi_id IS 'AlterEventLogEntryUserMultiID';

CREATE OR REPLACE PROCEDURE mc.archive_old_managers_and_params
(
    _mgrList text,
    _infoOnly int = 1,
    INOUT _message text=''
)
LANGUAGE plpgsql
AS $$
/****************************************************
**
**  Desc:   Moves managers from T_Mgrs to T_OldManagers
**          and moves manager parameters from T_ParamValue to T_ParamValue_OldManagers
**
**          To reverse this process, use procedure UnarchiveOldManagersAndParams
**
**  Arguments:
**    _mgrList   One or more manager names (comma-separated list); supports wildcards because uses stored procedure ParseManagerNameList
**
**  Auth:   mem
**  Date:   05/14/2015 mem - Initial version
**          02/25/2016 mem - Add Set XACT_ABORT On
**          04/22/2016 mem - Now updating M_Comment in T_OldManagers
**          09/21/2022 mem - Ported to PostgreSQL
**
*****************************************************/
DECLARE
    _myRowCount int := 0;
    _moveParams text := 'Move params transaction';
BEGIN
    Set XACT_ABORT, NoCount On

    ---------------------------------------------------
    -- Validate the inputs
    ---------------------------------------------------
    --
    _mgrList := Coalesce(_mgrList, '');
    _infoOnly := Coalesce(_infoOnly, 1);
    _message := '';

    CREATE TEMP TABLE TmpManagerList (
        Manager_Name text NOT NULL,
        M_ID int NULL,
        M_ControlFromWebsite int null
    )

    ---------------------------------------------------
    -- Populate TmpManagerList with the managers in _mgrList
    ---------------------------------------------------
    --

    Call parse_manager_name_list _mgrList, _removeUnknownManagers => 0

    If Not Exists (Select * from TmpManagerList) Then
        _message := '_mgrList was empty; no match in mc.t_mgrs to ' || _mgrList;
        Select _message as Warning
        Return;
    End If;

    ---------------------------------------------------
    -- Validate the manager names
    ---------------------------------------------------
    --
    UPDATE TmpManagerList
    SET M_ID = M.M_ID,
        M_ControlFromWebsite = M.M_ControlFromWebsite
    FROM TmpManagerList Target

    /********************************************************************************
    ** This UPDATE query includes the target table name in the FROM clause
    ** The WHERE clause needs to have a self join to the target table, for example:
    **   UPDATE #TmpManagerList
    **   SET ...
    **   FROM source
    **   WHERE source.id = #TmpManagerList.id;
    ********************************************************************************/

                           ToDo: Fix this query

         INNER JOIN mc.t_mgrs M
           ON Target.Manager_Name = M.mgr_name
    --
    GET DIAGNOSTICS _myRowCount = ROW_COUNT;

    If Exists (Select * from TmpManagerList where M_ID Is Null) Then
        SELECT 'Unknown manager (not in mc.t_mgrs)' AS Warning, Manager_Name
        FROM TmpManagerList
        ORDER BY Manager_Name
    End If;

    If Exists (Select * from TmpManagerList WHERE NOT M_ID is Null And M_ControlFromWebsite > 0) Then
        SELECT 'Manager has M_ControlFromWebsite=1; cannot archive' AS Warning,
               Manager_Name
        FROM TmpManagerList
        WHERE NOT M_ID IS NULL AND
              M_ControlFromWebsite > 0
        ORDER BY Manager_Name
    End If;

    If Exists (Select * From TmpManagerList Where Manager_Name Like '%Params%') Then
        SELECT 'Will not process managers with "Params" in the name (for safety)' AS Warning,
               Manager_Name
        FROM TmpManagerList
        WHERE Manager_Name Like '%Params%'
        ORDER BY Manager_Name

        DELETE From TmpManagerList Where Manager_Name Like '%Params%'
    End If;

    If _infoOnly <> 0 Then
        SELECT Src.Manager_Name,
               Src.M_ControlFromWebsite,
               PV.M_TypeID,
               PV.ParamName,
               PV.Entry_ID,
               PV.TypeID,
               PV.[Value],
               PV.MgrID,
               PV.[Comment],
               PV.Last_Affected,
               PV.Entered_By
        FROM TmpManagerList Src
             LEFT OUTER JOIN V_ParamValue PV
               ON PV.MgrID = Src.M_ID
        ORDER BY Src.Manager_Name, ParamName

    Else
        DELETE FROM TmpManagerList WHERE M_ID is Null OR M_ControlFromWebsite > 0

        Begin Tran _moveParams

        INSERT INTO mc.t_old_managers( mgr_id,
                                   mgr_name,
                                   mgr_type_id,
                                   param_value_changed,
                                   control_from_website,
                                   comment )
        SELECT M.mgr_id,
               M.mgr_name,
               M.mgr_type_id,
               M.param_value_changed,
               M.control_from_website,
               M.comment
        FROM mc.t_mgrs M
             INNER JOIN TmpManagerList Src
               ON M.mgr_id = Src.mgr_id
          LEFT OUTER JOIN mc.t_old_managers Target
               ON Src.mgr_id = Target.mgr_id
        WHERE Target.mgr_id IS NULL
        --
        GET DIAGNOSTICS _myRowCount = ROW_COUNT;
        --
        If _myError <> 0 Then
            Rollback
            Select 'Aborted (rollback)' as Warning, _myError as ErrorCode
            Return;
        End If;

        INSERT INTO mc.t_param_value_old_managers(
                 entry_id,
                 type_id,
                 "value",
                 mgr_id,
                 "comment",
                 last_affected,
                 entered_by )
        SELECT PV.entry_id,
               PV.type_id,
               PV."value",
               PV.mgr_id,
               PV."comment",
               PV.last_affected,
               PV.entered_by
        FROM mc.t_param_value PV
             INNER JOIN TmpManagerList Src
               ON PV.mgr_id = Src.M_ID
        --
        GET DIAGNOSTICS _myRowCount = ROW_COUNT;
        --
        If _myError <> 0 Then
            Rollback
            Select 'Aborted (rollback)' as Warning, _myError as ErrorCode
            Return;
        End If;

        DELETE mc.t_param_value
        FROM mc.t_param_value PV

        /********************************************************************************
        ** This DELETE query includes the target table name in the FROM clause
        ** The WHERE clause needs to have a self join to the target table, for example:
        **   UPDATE mc.t_param_value
        **   SET ...
        **   FROM source
        **   WHERE source.id = mc.t_param_value.id;
        **
        ** Delete queries must also include the USING keyword
        ** Alternatively, the more standard approach is to rearrange the query to be similar to
        **   DELETE FROM mc.t_param_value WHERE id in (SELECT id from ...)
        ********************************************************************************/

                               ToDo: Fix this query

             INNER JOIN TmpManagerList Src
               ON PV.MgrID = Src.M_ID

        DELETE mc.t_mgrs
        FROM mc.t_mgrs M

        /********************************************************************************
        ** This DELETE query includes the target table name in the FROM clause
        ** The WHERE clause needs to have a self join to the target table, for example:
        **   UPDATE mc.t_mgrs
        **   SET ...
        **   FROM source
        **   WHERE source.id = mc.t_mgrs.id;
        **
        ** Delete queries must also include the USING keyword
        ** Alternatively, the more standard approach is to rearrange the query to be similar to
        **   DELETE FROM mc.t_mgrs WHERE id in (SELECT id from ...)
        ********************************************************************************/

                               ToDo: Fix this query

             INNER JOIN TmpManagerList Src
               ON M.M_ID = Src.M_ID

        Commit Tran _moveParams

        SELECT 'Moved to mc.t_old_managers and mc.t_param_value_old_managers' as Message,
               Src.Manager_Name,
               Src.control_from_website,
               PT.param_name,
               PV.entry_id,
               PV.type_id,
               PV."value",
               PV.mgr_id,
               PV."comment",
               PV.last_affected,
               PV.entered_by
        FROM TmpManagerList Src
             LEFT OUTER JOIN mc.t_param_value_old_managers PV
               ON PV.mgr_id = Src.mgr_id
             LEFT OUTER JOIN mc.t_param_type PT ON
             PV.type_id = PT.param_id
        ORDER BY Src.Manager_Name, param_name
    End If;

Done:
    RETURN _myError
    DROP TABLE TmpManagerList

END
$$;

COMMENT ON PROCEDURE mc.archive_old_managers_and_params IS 'ArchiveOldManagersAndParams';

CREATE OR REPLACE PROCEDURE mc.disable_analysis_managers
(
    _previewUpdates int = 0,
    INOUT _message text=''
)
LANGUAGE plpgsql
AS $$
/****************************************************
**
**  Desc:   Disables all analysis managers
**
**  Auth:   mem
**  Date:   05/09/2008
**          10/09/2009 mem - Changed _managerTypeIDList to 11
**          06/09/2011 mem - Now calling EnableDisableAllManagers
**          09/21/2022 mem - Ported to PostgreSQL
**
*****************************************************/
BEGIN
    Call enable_disable_all_managers _managerTypeIDList => '11', _managerNameList => '', _enable => 0,
                                             _previewUpdates=@PreviewUpdates, _message = _message output

    Return _myError

END
$$;

COMMENT ON PROCEDURE mc.disable_analysis_managers IS 'DisableAnalysisManagers';

CREATE OR REPLACE PROCEDURE mc.disable_archive_dependent_managers
(
    _previewUpdates int = 0,
    INOUT _message text=''
)
LANGUAGE plpgsql
AS $$
/****************************************************
**
**  Desc:   Disables managers that rely on the NWFS archive
**
**  Auth:   mem
**  Date:   05/09/2008
**          07/24/2008 mem - Changed _managerTypeIDList from '1,2,3,4,8' to '2,3,8'
**          07/24/2008 mem - Changed _managerTypeIDList from '2,3,8' to '8'
**                         - Note that we do not include 15=CaptureTaskManager because capture tasks can still occur when the archive is unavailable
**                         - However, you should run Stored Procedure EnableDisableArchiveStepTools in the DMS_Capture database to disable the archive-dependent step tools
**          09/21/2022 mem - Ported to PostgreSQL
**
*****************************************************/
BEGIN
    Call enable_disable_all_managers _managerTypeIDList => '8', _managerNameList => '', _enable => 0,
                                             _previewUpdates=@PreviewUpdates, _message = _message output

    Return _myError

END
$$;

COMMENT ON PROCEDURE mc.disable_archive_dependent_managers IS 'DisableArchiveDependentManagers';

CREATE OR REPLACE PROCEDURE mc.disable_sequest_clusters
(
    _previewUpdates int = 0,
    INOUT _message text=''
)
LANGUAGE plpgsql
AS $$
/****************************************************
**
**  Desc:   Disables the Sequest Clusters
**
**  Auth:   mem
**  Date:   07/24/2008
**          10/09/2009 mem - Changed _managerTypeIDList to 11
**          09/21/2022 mem - Ported to PostgreSQL
**
*****************************************************/
BEGIN
    Call enable_disable_all_managers _managerTypeIDList => '11', _managerNameList => '%SeqCluster%', _enable => 0,
                                             _previewUpdates=@PreviewUpdates, _message = _message output

    Return _myError

END
$$;

COMMENT ON PROCEDURE mc.disable_sequest_clusters IS 'DisableSequestClusters';

CREATE OR REPLACE PROCEDURE mc.duplicate_manager_parameter
(
    _sourceParamTypeID int,
    _newParamTypeID int,
    _paramValueOverride text = null,
    _commentOverride text = null,
    _paramValueSearchText text = null,
    _paramValueReplaceText text = null,
    _infoOnly int = 1
)
LANGUAGE plpgsql
AS $$
/****************************************************
**
**  Desc:   Duplicates an existing parameter for all managers,
**          creating a new entry with a new TypeID value
**
**  Example usage:
**    exec DuplicateManagerParameter 157, 172, _paramValueSearchText='msfileinfoscanner', _paramValueReplaceText='AgilentToUimfConverter', _infoOnly=1
**
**    exec DuplicateManagerParameter 179, 182, _paramValueSearchText='PbfGen', _paramValueReplaceText='ProMex', _infoOnly=1
**
**  Arguments:
**    _paramValueOverride      Optional: New parameter value; ignored if _paramValueSearchText is defined
**    _paramValueSearchText    Optional: text to search for in the source parameter value
**    _paramValueReplaceText   Optional: replacement text (ignored if _paramValueReplaceText is null)
**
**  Auth:   mem
**  Date:   08/26/2013 mem - Initial release
**          09/21/2022 mem - Ported to PostgreSQL
**
*****************************************************/
DECLARE
    _myRowCount int := 0;
BEGIN
    ---------------------------------------------------
    -- Validate input fields
    ---------------------------------------------------

    _infoOnly := Coalesce(_infoOnly, 1);

    If _sourceParamTypeID Is Null Then
        RAISE INFO '%', '_sourceParamTypeID cannot be null; unable to continue';
        return 52000
    End If;

    If _newParamTypeID Is Null Then
        RAISE INFO '%', '_newParamTypeID cannot be null; unable to continue';
        return 52001
    End If;

    If Not _paramValueSearchText Is Null AND _paramValueReplaceText Is Null Then
        RAISE INFO '%', '_paramValueReplaceText cannot be null when _paramValueSearchText is defined; unable to continue';
        return 52002
    End If;

    ---------------------------------------------------
    -- Make sure the soure parameter exists
    ---------------------------------------------------

    If Not Exists (Select * From mc.t_param_value Where type_id = _sourceParamTypeID) Then
        RAISE INFO '%', '_sourceParamTypeID ' || _sourceParamTypeID::text || ' not found in mc.t_param_value; unable to continue';
        return 52003
    End If;

    If Exists (Select * From mc.t_param_value Where type_id = _newParamTypeID) Then
        RAISE INFO '%', '_newParamTypeID ' || _newParamTypeID::text || ' already exists in mc.t_param_value; unable to continue';
        return 52004
    End If;

    If Not Exists (Select * From mc.t_param_type Where param_id = _newParamTypeID) Then
        RAISE INFO '%', '_newParamTypeID ' || _newParamTypeID::text || ' not found in mc.t_param_type; unable to continue';
        return 52005
    End If;

    If Not _paramValueSearchText Is Null Then
        If _infoOnly <> 0 Then
            SELECT _newParamTypeID AS TypeID,;
        End If;
                REPLACE("value", _paramValueSearchText, _paramValueReplaceText) AS "Value",
                mgr_id,
                Coalesce(_commentOverride, '') AS "Comment"
            FROM mc.t_param_value
            WHERE (type_id = _sourceParamTypeID)
        Else
            INSERT INTO mc.t_param_value( type_id,
                                    "value",
                                    mgr_id,
                                    "comment" )
            SELECT _newParamTypeID AS TypeID,
                REPLACE("value", _paramValueSearchText, _paramValueReplaceText) AS "Value",
                mgr_id,
                Coalesce(_commentOverride, '') AS "Comment"
            FROM mc.t_param_value
            WHERE (type_id = _sourceParamTypeID)
        --
        GET DIAGNOSTICS _myRowCount = ROW_COUNT;
    Else
        If _infoOnly <> 0 Then
            SELECT _newParamTypeID AS TypeID,;
        End If;
                   Coalesce(_paramValueOverride, "value") AS "Value",
                   mgr_id,
                   Coalesce(_commentOverride, '') AS "Comment"
            FROM mc.t_param_value
            WHERE (type_id = _sourceParamTypeID)
        Else
            INSERT INTO mc.t_param_value( type_id,
                                      "value",
                                      mgr_id,
                                      "comment" )
            SELECT _newParamTypeID AS TypeID,
                   Coalesce(_paramValueOverride, "value") AS "Value",
                   mgr_id,
                   Coalesce(_commentOverride, '') AS "Comment"
            FROM mc.t_param_value
            WHERE (type_id = _sourceParamTypeID)
        --
        GET DIAGNOSTICS _myRowCount = ROW_COUNT;
    End If;

    return 0

END
$$;

COMMENT ON PROCEDURE mc.duplicate_manager_parameter IS 'DuplicateManagerParameter';

CREATE OR REPLACE PROCEDURE mc.duplicate_manager_parameters
(
    _sourceMgrID int,
    _targetMgrID int,
    _mergeSourceWithTarget int = 0,
    _infoOnly int = 0
)
LANGUAGE plpgsql
AS $$
/****************************************************
**
**  Desc:   Duplicates the parameters for a given manager
**          to create new parameters for a new manager
**
**  Example usage:
**    exec DuplicateManagerParameter 157, 172
**
**  Arguments:
**    _mergeSourceWithTarget   When 0, then the target manager cannot have any parameters; if 1, then will add missing parameters to the target manager
**
**  Auth:   mem
**  Date:   10/10/2014 mem - Initial release
**          09/21/2022 mem - Ported to PostgreSQL
**
*****************************************************/
DECLARE
    _myRowCount int := 0;
BEGIN
    ---------------------------------------------------
    -- Validate input fields
    ---------------------------------------------------

    _infoOnly := Coalesce(_infoOnly, 1);

    If _sourceMgrID Is Null Then
        RAISE INFO '%', '_sourceMgrID cannot be null; unable to continue';
        return 52000
    End If;

    If _targetMgrID Is Null Then
        RAISE INFO '%', '_targetMgrID cannot be null; unable to continue';
        return 52001
    End If;

    _mergeSourceWithTarget := Coalesce(_mergeSourceWithTarget, 0);

    ---------------------------------------------------
    -- Make sure the source and target managers exist
    ---------------------------------------------------

    If Not Exists (Select * From mc.t_mgrs Where mgr_id = _sourceMgrID) Then
        RAISE INFO '%', '_sourceMgrID ' || _sourceMgrID::text || ' not found in mc.t_mgrs; unable to continue';
        return 52003
    End If;

    If Not Exists (Select * From mc.t_mgrs Where mgr_id = _targetMgrID) Then
        RAISE INFO '%', '_targetMgrID ' || _targetMgrID::text || ' not found in mc.t_mgrs; unable to continue';
        return 52004
    End If;

    If _mergeSourceWithTarget = 0 Then
        -- Make sure the target manager does not have any parameters
        --
        If Exists (SELECT * FROM mc.t_param_value WHERE mgr_id = _targetMgrID) Then
            RAISE INFO '%', '_targetMgrID ' || _targetMgrID::text || ' has existing parameters in mc.t_param_value; aborting since _mergeSourceWithTarget = 0';
            return 52005
        End If;
    End If;

    If _infoOnly <> 0 Then
            SELECT Source.type_id,
                   Source.value,
                   _targetMgrID AS MgrID,
                   Source.comment
            FROM mc.t_param_value AS Source
                 LEFT OUTER JOIN ( SELECT type_id
                                   FROM mc.t_param_value
                                   WHERE mgr_id = _targetMgrID ) AS ExistingParams
                   ON Source.type_id = ExistingParams.type_id
            WHERE mgr_id = _sourceMgrID AND
                  ExistingParams.type_id IS NULL

    Else
        INSERT INTO mc.t_param_value (type_id, value, mgr_id, Comment)
        SELECT Source.type_id,
               Source.value,
               _targetMgrID AS MgrID,
               Source.comment
        FROM mc.t_param_value AS Source
             LEFT OUTER JOIN ( SELECT type_id
                               FROM mc.t_param_value
                               WHERE mgr_id = _targetMgrID ) AS ExistingParams
               ON Source.type_id = ExistingParams.type_id
        WHERE mgr_id = _sourceMgrID AND
              ExistingParams.type_id IS NULL
        --
        GET DIAGNOSTICS _myRowCount = ROW_COUNT;
    End If;

    return 0

END
$$;

COMMENT ON PROCEDURE mc.duplicate_manager_parameters IS 'DuplicateManagerParameters';

CREATE OR REPLACE PROCEDURE mc.enable_archive_dependent_managers
(
    _previewUpdates int = 0,
    INOUT _message text=''
)
LANGUAGE plpgsql
AS $$
/****************************************************
**
**  Desc:   Disables managers that rely on the NWFS archive
**
**  Auth:   mem
**  Date:   06/09/2011 mem - Initial Version
**          09/21/2022 mem - Ported to PostgreSQL
**
*****************************************************/
BEGIN
    Call enable_disable_all_managers _managerTypeIDList => '8,15', _managerNameList => 'All', _enable => 1,
                                             _previewUpdates=@PreviewUpdates, _message = _message output

    Return _myError

END
$$;

COMMENT ON PROCEDURE mc.enable_archive_dependent_managers IS 'EnableArchiveDependentManagers';

CREATE OR REPLACE PROCEDURE mc.enable_disable_all_managers
(
    _managerTypeIDList text = '',
    _managerNameList text = '',
    _enable int = 1,
    _previewUpdates int = 0,
    INOUT _message text=''
)
LANGUAGE plpgsql
AS $$
/****************************************************
**
**  Desc:   Enables or disables all managers, optionally filtering by manager type ID or manager name
**
**  Arguments:
**    _managerTypeIDList   Optional: list of manager type IDs to disable, e.g. "1, 2, 3"
**    _managerNameList     Optional: if defined, then only managers specified here will be enabled; supports the % wildcard
**    _enable              1 to enable, 0 to disable
**
**  Auth:   mem
**  Date:   05/09/2008
**          06/09/2011 - Created by extending code in DisableAllManagers
**                     - Now filtering on MT_Active > 0 in T_MgrTypes
**          09/21/2022 mem - Ported to PostgreSQL
**
*****************************************************/
DECLARE
    _myRowCount int := 0;
    _mgrTypeID int;
    _continue int;
BEGIN
    -----------------------------------------------
    -- Validate the inputs
    -----------------------------------------------
    --
    _enable := Coalesce(_enable, 0);
    _managerTypeIDList := Coalesce(_managerTypeIDList, '');
    _managerNameList := Coalesce(_managerNameList, '');
    _previewUpdates := Coalesce(_previewUpdates, 0);
    _message := '';

    CREATE TEMP TABLE TmpManagerTypeIDs (
        MgrTypeID int NOT NULL
    )

    If char_length(_managerTypeIDList) > 0 Then
        -- Parse _managerTypeIDList
        --
        INSERT INTO TmpManagerTypeIDs (MgrTypeID)
        SELECT DISTINCT Value
        FROM public.parse_delimited_integer_list(_managerTypeIDList, ',')
        ORDER BY Value
        --
        GET DIAGNOSTICS _myRowCount = ROW_COUNT;
    Else
        -- Populate TmpManagerTypeIDs with all manager types in mc.t_mgr_types
        --
        INSERT INTO TmpManagerTypeIDs (MgrTypeID)
        SELECT DISTINCT mgr_type_id
        FROM mc.t_mgr_types
        WHERE mgr_type_active > 0
        ORDER BY mgr_type_id
        --
        GET DIAGNOSTICS _myRowCount = ROW_COUNT;
    End If;

    -----------------------------------------------
    -- Loop through the manager types in TmpManagerTypeIDs
    -- For each, call EnableDisableManagers
    -----------------------------------------------

    _mgrTypeID := 0;
    SELECT MIN(MgrTypeID)-1 INTO _mgrTypeID
    FROM TmpManagerTypeIDs
    --
    GET DIAGNOSTICS _myRowCount = ROW_COUNT;

    _continue := 1;
    While _continue = 1 Loop
        -- This While loop can probably be converted to a For loop; for example:
        --    For _itemName In
        --        SELECT item_name
        --        FROM TmpSourceTable
        --        ORDER BY entry_id
        --    Loop
        --        ...
        --    End Loop

        -- Moved to bottom of query: TOP 1
        SELECT MgrTypeID INTO _mgrTypeID
        FROM TmpManagerTypeIDs
        WHERE MgrTypeID > _mgrTypeID
        ORDER BY MgrTypeID
        LIMIT 1;
        --
        GET DIAGNOSTICS _myRowCount = ROW_COUNT;

        If _myRowCount = 0 Then
            _continue := 0;
        Else
            Call enable_disable_managers _enable => @Enable, _managerTypeID => @MgrTypeID, _managerNameList => @ManagerNameList, _previewUpdates => _previewUpdates, _message => _message output
        End If;
    End Loop;

Done:
    Return _myError

    DROP TABLE TmpManagerTypeIDs

END
$$;

COMMENT ON PROCEDURE mc.enable_disable_all_managers IS 'EnableDisableAllManagers';

CREATE OR REPLACE PROCEDURE mc.enable_disable_managers
(
    _enable int,
    _managerTypeID int=11,
    _managerNameList text = '',
    _previewUpdates int = 0,
    INOUT _message text=''
)
LANGUAGE plpgsql
AS $$
/****************************************************
**
**  Desc:   Enables or disables all managers of the given type
**
**  Arguments:
**    _enable            0 to disable, 1 to enable
**    _managerTypeID     Defined in table T_MgrTypes.  8=Space, 9=DataImport, 11=Analysis Tool Manager, 15=CaptureTaskManager
**    _managerNameList   Required when _enable = 1.  Only managers specified here will be enabled, though you can use "All" to enable All managers.  When _enable = 0, if this parameter is blank (or All) then all managers of the given type will be disabled; supports the % wildcard
**
**  Auth:   mem
**  Date:   07/12/2007
**          05/09/2008 mem - Added parameter _managerNameList
**          06/09/2011 mem - Now filtering on MT_Active > 0 in T_MgrTypes
**                         - Now allowing _managerNameList to be All when _enable = 1
**          10/12/2017 mem - Allow _managerTypeID to be 0 if _managerNameList is provided
**          03/28/2018 mem - Use different messages when updating just one manager
**          09/21/2022 mem - Ported to PostgreSQL
**
*****************************************************/
DECLARE
    _myRowCount int := 0;
    _newValue text;
    _managerTypeName text;
    _activeStateDescription text;
    _countToUpdate int;
    _countUnchanged int;
BEGIN
    -----------------------------------------------
    -- Validate the inputs
    -----------------------------------------------
    --
    _managerNameList := Coalesce(_managerNameList, '');
    _previewUpdates := Coalesce(_previewUpdates, 0);

    If _enable Is Null Then
        _myError := 40000;
        _message := '_enable cannot be null';
        SELECT _message AS Message
        Return;
    End If;

    If _managerTypeID Is Null Then
        _myError := 40001;
        _message := '_managerTypeID cannot be null';
        SELECT _message AS Message
        Return;
    End If;

    If _managerTypeID = 0 And char_length(_managerNameList) > 0 And _managerNameList <> 'All' Then
        _managerTypeName := 'Any';
    Else
        -- Make sure _managerTypeID is valid
        _managerTypeName := '';
        SELECT mgr_type_name INTO _managerTypeName
        FROM mc.t_mgr_types
        WHERE mgr_type_id = _managerTypeID AND
            mgr_type_active > 0
        --
        GET DIAGNOSTICS _myRowCount = ROW_COUNT;

        If _myRowCount = 0 Then
            If Exists (SELECT * FROM mc.t_mgr_types WHERE mgr_type_id = _managerTypeID AND mgr_type_active = 0) Then
                _message := '_managerTypeID ' || _managerTypeID::text || ' has mgr_type_active = 0 in mc.t_mgr_types; unable to continue';
            Else
                _message := '_managerTypeID ' || _managerTypeID::text || ' not found in mc.t_mgr_types';
            End If;

            SELECT _message AS Message
            _myError := 40002;
            Return;
        End If;
    End If;

    If _enable <> 0 AND char_length(_managerNameList) = 0 Then
        _message := '_managerNameList cannot be blank when _enable is non-zero; to update all managers, set _managerNameList to All';
        SELECT _message AS Message
        _myError := 40003;
        Return;
    End If;

    -----------------------------------------------
    -- Creata a temporary table
    -----------------------------------------------

    CREATE TEMP TABLE TmpManagerList (
        Manager_Name text NOT NULL
    )

    If char_length(_managerNameList) > 0 And _managerNameList <> 'All' Then
        -- Populate TmpMangerList using ParseManagerNameList

        Call parse_manager_name_list _managerNameList, _removeUnknownManagers => 1, _message => @message output

        If _myError <> 0 Then
            If char_length(_message) = 0 Then
                _message := 'Error calling ParseManagerNameList: ' || _myError::text;
            End If;

            Return;
        End If;

        If _managerTypeID > 0 Then
            -- Delete entries from TmpManagerList that don't match entries in M_Name of the given type
            DELETE TmpManagerList
            FROM TmpManagerList U LEFT OUTER JOIN

            /********************************************************************************
            ** This DELETE query includes the target table name in the FROM clause
            ** The WHERE clause needs to have a self join to the target table, for example:
            **   UPDATE #TmpManagerList
            **   SET ...
            **   FROM source
            **   WHERE source.id = #TmpManagerList.id;
            **
            ** Delete queries must also include the USING keyword
            ** Alternatively, the more standard approach is to rearrange the query to be similar to
            **   DELETE FROM #TmpManagerList WHERE id in (SELECT id from ...)
            ********************************************************************************/

                                   ToDo: Fix this query

                mc.t_mgrs M ON M.mgr_name = U.Manager_Name AND M.mgr_type_id = _managerTypeID
            WHERE M.mgr_name Is Null
            --
            GET DIAGNOSTICS _myRowCount = ROW_COUNT;

            If _myRowCount > 0 Then
                _message := 'Found ' || _myRowCount::text || ' entries in _managerNameList that are not ' || _managerTypeName || ' managers';
                _message := '';
            End If;
        End If;

    Else
        -- Populate TmpManagerList with all managers in mc.t_mgrs
        --
        INSERT INTO TmpManagerList (Manager_Name)
        SELECT mgr_name
        FROM mc.t_mgrs
        WHERE mgr_type_id = _managerTypeID
        --
        GET DIAGNOSTICS _myRowCount = ROW_COUNT;
    End If;

    -- Set _newValue based on _enable
    If _enable = 0 Then
        _newValue := 'False';
        _activeStateDescription := 'Inactive';
    Else
        _newValue := 'True';
        _activeStateDescription := 'Active';
    End If;

    -- Count the number of managers that need to be updated
    _countToUpdate := 0;
    SELECT COUNT(*) INTO _countToUpdate
    FROM mc.t_param_value PV
         INNER JOIN mc.t_param_type PT
           ON PV.type_id = PT.param_id
         INNER JOIN mc.t_mgrs M
           ON PV.mgr_id = M.mgr_id
         INNER JOIN mc.t_mgr_types MT
           ON M.mgr_type_id = MT.mgr_type_id
         INNER JOIN TmpManagerList U
           ON M.mgr_name = U.Manager_Name
    WHERE PT.param_name = 'mgractive' AND
          PV.value <> _newValue AND
          MT.mgr_type_active > 0
    --
    GET DIAGNOSTICS _myRowCount = ROW_COUNT;

    -- Count the number of managers already in the target state
    _countUnchanged := 0;
    SELECT COUNT(*) INTO _countUnchanged
    FROM mc.t_param_value PV
         INNER JOIN mc.t_param_type PT
           ON PV.type_id = PT.param_id
         INNER JOIN mc.t_mgrs M
           ON PV.mgr_id = M.mgr_id
         INNER JOIN mc.t_mgr_types MT
           ON M.mgr_type_id = MT.mgr_type_id
         INNER JOIN TmpManagerList U
           ON M.mgr_name = U.Manager_Name
    WHERE PT.param_name = 'mgractive' AND
          PV.value = _newValue AND
          MT.mgr_type_active > 0
    --
    GET DIAGNOSTICS _myRowCount = ROW_COUNT;

    If _countToUpdate = 0 Then
        If _countUnchanged = 0 Then
            If char_length(_managerNameList) > 0 Then
                If _managerTypeID = 0 Then
                    _message := 'None of the managers in _managerNameList was recognized';
                Else
                    _message := 'No ' || _managerTypeName || ' managers were found matching _managerNameList';
                End If;
            Else
                _message := 'No ' || _managerTypeName || ' managers were found in mc.t_mgrs';
            End If;
        Else
            If _countUnchanged = 1 Then
                _message := 'The manager is already ' || _activeStateDescription;
            Else
                If _managerTypeID = 0 Then
                    _message := 'All ' || _countUnchanged::text || ' managers are already ' || _activeStateDescription;
                Else
                    _message := 'All ' || _countUnchanged::text || ' ' || _managerTypeName || ' managers are already ' || _activeStateDescription;
                End If;
            End If;
        End If;

        SELECT _message AS Message

    Else
        If _previewUpdates <> 0 Then
            SELECT Convert(text, PV.value || '-->' || _newValue) AS State_Change_Preview,
                   PT.param_name AS Parameter_Name,
                   M.mgr_name AS Manager_Name,
                   MT.mgr_type_name AS Manager_Type
            FROM mc.t_param_value PV
                 INNER JOIN mc.t_param_type PT
                   ON PV.type_id = PT.param_id
                 INNER JOIN mc.t_mgrs M
                   ON PV.mgr_id = M.mgr_id
                 INNER JOIN mc.t_mgr_types MT
                   ON M.mgr_type_id = MT.mgr_type_id
                 INNER JOIN TmpManagerList U
                   ON M.mgr_name = U.Manager_Name
            WHERE PT.param_name = 'mgractive' AND
                  PV.value <> _newValue AND
                  MT.mgr_type_active > 0
            --
            GET DIAGNOSTICS _myRowCount = ROW_COUNT;
        Else
            UPDATE mc.t_param_value
            SET value = _newValue
            FROM mc.t_param_value PV

            /********************************************************************************
            ** This UPDATE query includes the target table name in the FROM clause
            ** The WHERE clause needs to have a self join to the target table, for example:
            **   UPDATE mc.t_param_value
            **   SET ...
            **   FROM source
            **   WHERE source.id = mc.t_param_value.id;
            ********************************************************************************/

                                   ToDo: Fix this query

                 INNER JOIN mc.t_param_type PT
                   ON PV.TypeID = PT.param_id
                 INNER JOIN mc.t_mgrs M
                   ON PV.MgrID = M.mgr_id
                 INNER JOIN mc.t_mgr_types MT
                   ON M.mgr_type_id = MT.mgr_type_id
                 INNER JOIN TmpManagerList U
                   ON M.mgr_name = U.Manager_Name
            WHERE PT.param_name = 'mgractive' AND
                  PV.Value <> _newValue AND
                  MT.mgr_type_active > 0
            --
            GET DIAGNOSTICS _myRowCount = ROW_COUNT;

            If _myRowCount = 1 And _countUnchanged = 0 Then
                _message := 'The manager is now ' || _activeStateDescription;
            Else
                If _managerTypeID = 0 Then
                    _message := 'Set ' || _myRowCount::text || ' managers to state ' || _activeStateDescription;
                Else
                    _message := 'Set ' || _myRowCount::text || ' ' || _managerTypeName || ' managers to state ' || _activeStateDescription;
                End If;

                If _countUnchanged <> 0 Then
                    _message := _message || ' (' || _countUnchanged::text || ' managers were already ' || _activeStateDescription || ')';
                End If;
            End If;

            SELECT _message AS Message
        End If;
    End If;

Done:
    Return _myError

    DROP TABLE TmpManagerList

END
$$;

COMMENT ON PROCEDURE mc.enable_disable_managers IS 'EnableDisableManagers';

CREATE OR REPLACE PROCEDURE mc.enable_disable_run_jobs_remotely
(
    _enable int,
    _managerNameList text = '',
    _previewUpdates int = 0,
    _addMgrParamsIfMissing int = 0,
    INOUT _message text = ''
)
LANGUAGE plpgsql
AS $$
/****************************************************
**
**  Desc:   Enables or disables a manager to run jobs remotely
**
**  Arguments:
**    _enable                  0 to disable running jobs remotely, 1 to enable running jobs remotely
**    _managerNameList         Manager(s) to update; supports % for wildcards
**    _addMgrParamsIfMissing   When 1, if manger(s) are missing parameters RunJobsRemotely or RemoteHostName, will auto-add those parameters
**
**  Auth:   mem
**  Date:   03/28/2018 mem - Initial version
**          03/29/2018 mem - Add parameter _addMgrParamsIfMissing
**          09/21/2022 mem - Ported to PostgreSQL
**
*****************************************************/
DECLARE
    _myRowCount int := 0;
    _newValue text;
    _activeStateDescription text;
    _countToUpdate int;
    _countUnchanged int;
    _mgrName text := '';
    _mgrId int := 0;
    _paramTypeId int := 0;
    _continue int := 1;
BEGIN
    -----------------------------------------------
    -- Validate the inputs
    -----------------------------------------------
    --
    _managerNameList := Coalesce(_managerNameList, '');
    _previewUpdates := Coalesce(_previewUpdates, 0);
    _addMgrParamsIfMissing := Coalesce(_addMgrParamsIfMissing, 0);

    If _enable Is Null Then
        _myError := 40000;
        _message := '_enable cannot be null';
        SELECT _message AS Message
        Return;
    End If;

    If char_length(_managerNameList) = 0 Then
        _myError := 40003;
        _message := '_managerNameList cannot be blank';
        SELECT _message AS Message
        Return;
    End If;

    -----------------------------------------------
    -- Creata a temporary table
    -----------------------------------------------

    CREATE TEMP TABLE TmpManagerList (
        Manager_Name text NOT NULL
    )

    -- Populate TmpMangerList using ParseManagerNameList
    --
    Call parse_manager_name_list _managerNameList, _removeUnknownManagers => 1, _message => @message output

    If _myError <> 0 Then
        If char_length(_message) = 0 Then
            _message := 'Error calling ParseManagerNameList: ' || _myError::text;
        End If;

        Return;
    End If;

    -- Set _newValue based on _enable
    If _enable = 0 Then
        _newValue := 'False';
        _activeStateDescription := 'run jobs locally';
    Else
        _newValue := 'True';
        _activeStateDescription := 'run jobs remotely';
    End If;

    If Exists (Select * From TmpManagerList Where Manager_Name = 'Default_AnalysisMgr_Params') Then
        Delete From TmpManagerList Where Manager_Name = 'Default_AnalysisMgr_Params'

        _message := 'For safety, not updating RunJobsRemotely for manager Default_AnalysisMgr_Params';

        If Exists (Select * From TmpManagerList) Then
            -- TmpManagerList contains other managers; update them
            RAISE INFO '%', _message;
        Else
            -- TmpManagerList is now empty; abort
            SELECT _message AS Message
            Return;
        End If;
    End If;

    If _addMgrParamsIfMissing > 0 Then
    -- <a>

        While _continue > 0 Loop
            -- This While loop can probably be converted to a For loop; for example:
            --    For _itemName In
            --        SELECT item_name
            --        FROM TmpSourceTable
            --        ORDER BY entry_id
            --    Loop
            --        ...
            --    End Loop

            -- Moved to bottom of query: TOP 1
            SELECT TmpManagerList.Manager_Name, INTO _mgrName
                         _mgrId = mc.t_mgrs.mgr_id
            FROM TmpManagerList
                 INNER JOIN mc.t_mgrs
                   ON TmpManagerList.Manager_Name = mc.t_mgrs.mgr_name
            WHERE Manager_Name > _mgrName
            ORDER BY Manager_Name
            LIMIT 1;
            --
            GET DIAGNOSTICS _myRowCount = ROW_COUNT;

            If _myRowCount = 0 Then
                _continue := 0;
            Else
            -- <c>
                If Not Exists (SELECT * FROM V_MgrParams Where ParameterName = 'RunJobsRemotely' And ManagerName = _mgrName) Then
                -- <d1>
                    _paramTypeId := null;
                    SELECT param_id INTO _paramTypeId
                    FROM "mc.t_param_type"
                    Where param_name = 'RunJobsRemotely'

                    If Coalesce(_paramTypeId, 0) = 0 Then
                        RAISE INFO '%', 'Error: could not find parameter "RunJobsRemotely" in "mc.t_param_type"';
                    Else
                        If _previewUpdates > 0 Then
                            RAISE INFO '%', 'Create parameter RunJobsRemotely for Manager ' || _mgrName || ', value ' || _newValue;

                            -- Actually do go ahead and create the parameter, but use a value of False even if _newValue is True
                            -- We need to do this so the managers are included in the query below with PT.ParamName = 'RunJobsRemotely'
                            Insert Into mc.t_param_value (mgr_id, type_id, value)
                            Values (_mgrId, _paramTypeId, 'False')
                        Else
                            Insert Into mc.t_param_value (mgr_id, type_id, value)
                            Values (_mgrId, _paramTypeId, _newValue)
                        End If;
                    End If;
                End If; -- </d1>

                If Not Exists (SELECT * FROM V_MgrParams Where ParameterName = 'RemoteHostName' And ManagerName = _mgrName) Then
                -- <d2>
                    _paramTypeId := null;
                    SELECT param_id INTO _paramTypeId
                    FROM "mc.t_param_type"
                    Where param_name = 'RemoteHostName'

                    If Coalesce(_paramTypeId, 0) = 0 Then
                        RAISE INFO '%', 'Error: could not find parameter "RemoteHostName" in "mc.t_param_type"';
                    Else
                        If _previewUpdates > 0 Then
                            RAISE INFO '%', 'Create parameter RemoteHostName for Manager ' || _mgrName || ', value PrismWeb2';
                        Else
                            Insert Into mc.t_param_value (mgr_id, type_id, value)
                            Values (_mgrId, _paramTypeId, 'PrismWeb2')
                        End If;
                    End If;
                End If; -- </d1>
            End If; -- </c>
        End Loop; -- </b>
    End If; -- </a>

    -- Count the number of managers that need to be updated
    _countToUpdate := 0;
    SELECT COUNT(*) INTO _countToUpdate
    FROM mc.t_param_value PV
         INNER JOIN mc.t_param_type PT
           ON PV.type_id = PT.param_id
         INNER JOIN mc.t_mgrs M
           ON PV.mgr_id = M.mgr_id
         INNER JOIN mc.t_mgr_types MT
           ON M.mgr_type_id = MT.mgr_type_id
         INNER JOIN TmpManagerList U
           ON M.mgr_name = U.Manager_Name
    WHERE PT.param_name = 'RunJobsRemotely' AND
          PV.value <> _newValue AND
          MT.mgr_type_active > 0
    --
    GET DIAGNOSTICS _myRowCount = ROW_COUNT;

    -- Count the number of managers already in the target state
    _countUnchanged := 0;
    SELECT COUNT(*) INTO _countUnchanged
    FROM mc.t_param_value PV
         INNER JOIN mc.t_param_type PT
           ON PV.type_id = PT.param_id
         INNER JOIN mc.t_mgrs M
           ON PV.mgr_id = M.mgr_id
         INNER JOIN mc.t_mgr_types MT
           ON M.mgr_type_id = MT.mgr_type_id
         INNER JOIN TmpManagerList U
           ON M.mgr_name = U.Manager_Name
    WHERE PT.param_name = 'RunJobsRemotely' AND
          PV.value = _newValue AND
          MT.mgr_type_active > 0
    --
    GET DIAGNOSTICS _myRowCount = ROW_COUNT;

    If _countToUpdate = 0 Then
        If _countUnchanged = 0 Then
            _message := 'No managers were found matching _managerNameList';
        Else
            If _countUnchanged = 1 Then
                _message := 'The manager is already set to ' || _activeStateDescription;
            Else
                _message := 'All ' || _countUnchanged::text || ' managers are already set to ' || _activeStateDescription;
            End If;
        End If;

        SELECT _message AS Message
    Else
        If _previewUpdates <> 0 Then
            SELECT Convert(text, PV.value || '-->' || _newValue) AS State_Change_Preview,
                   PT.param_name AS Parameter_Name,
                   M.mgr_name AS Manager_Name,
                   MT.mgr_type_name AS Manager_Type
            FROM mc.t_param_value PV
                 INNER JOIN mc.t_param_type PT
                   ON PV.type_id = PT.param_id
                 INNER JOIN mc.t_mgrs M
                   ON PV.mgr_id = M.mgr_id
                 INNER JOIN mc.t_mgr_types MT
                   ON M.mgr_type_id = MT.mgr_type_id
                 INNER JOIN TmpManagerList U
                   ON M.mgr_name = U.Manager_Name
            WHERE PT.param_name = 'RunJobsRemotely' AND
                  PV.value <> _newValue AND
                  MT.mgr_type_active > 0
            --
            GET DIAGNOSTICS _myRowCount = ROW_COUNT;
        Else
            UPDATE mc.t_param_value
            SET value = _newValue
            FROM mc.t_param_value PV

            /********************************************************************************
            ** This UPDATE query includes the target table name in the FROM clause
            ** The WHERE clause needs to have a self join to the target table, for example:
            **   UPDATE mc.t_param_value
            **   SET ...
            **   FROM source
            **   WHERE source.id = mc.t_param_value.id;
            ********************************************************************************/

                                   ToDo: Fix this query

                 INNER JOIN mc.t_param_type PT
                   ON PV.TypeID = PT.param_id
                 INNER JOIN mc.t_mgrs M
                   ON PV.MgrID = M.mgr_id
                 INNER JOIN mc.t_mgr_types MT
                   ON M.mgr_type_id = MT.mgr_type_id
                 INNER JOIN TmpManagerList U
                   ON M.mgr_name = U.Manager_Name
            WHERE PT.param_name = 'RunJobsRemotely' AND
                  PV.Value <> _newValue AND
                  MT.mgr_type_active > 0
            --
            GET DIAGNOSTICS _myRowCount = ROW_COUNT;

            If _myRowCount = 1 And _countUnchanged = 0 Then
                _message := 'Configured the manager to ' || _activeStateDescription;
            Else
                _message := 'Configured ' || _myRowCount::text || ' managers to ' || _activeStateDescription;

                If _countUnchanged <> 0 Then
                    _message := _message || ' (' || _countUnchanged::text || ' managers were already set to ' || _activeStateDescription || ')';
                End If;
            End If;

            SELECT _message AS Message
        End If;
    End If;

Done:
    Return _myError

    DROP TABLE TmpManagerList

END
$$;

COMMENT ON PROCEDURE mc.enable_disable_run_jobs_remotely IS 'EnableDisableRunJobsRemotely';

CREATE OR REPLACE PROCEDURE mc.get_default_remote_info_for_manager
(
    _managerName text,
    _remoteInfoXML text Output
)
LANGUAGE plpgsql
AS $$
/****************************************************
**
**  Desc:   Gets the default remote info parameters for the given manager
**          Retrieves parameters using GetManagerParametersWork, so properly retrieves parent group parameters, if any
**          If the manager does not have parameters RunJobsRemotely and RemoteHostName defined, returns an empty string
**          Also returns an empty string if RunJobsRemotely is not True
**
**          Example value for _remoteInfoXML
**          <host>prismweb2</host><user>svc-dms</user><taskQueue>/file1/temp/DMSTasks</taskQueue><workDir>/file1/temp/DMSWorkDir</workDir><orgDB>/file1/temp/DMSOrgDBs</orgDB><privateKey>Svc-Dms.key</privateKey><passphrase>Svc-Dms.pass</passphrase>
**
**  Arguments:
**    _managerName     Manager name
**    _remoteInfoXML   Output XML if valid remote info parameters are defined, otherwise an empty string
**
**  Auth:   mem
**  Date:   05/18/2017 mem - Initial version
**          03/14/2018 mem - Use GetManagerParametersWork to lookup manager parameters, allowing for getting remote info parameters from parent groups
**          03/29/2018 mem - Return an empty string if the manager does not have parameters RunJobsRemotely and RemoteHostName defined, or if RunJobsRemotely is false
**          09/21/2022 mem - Ported to PostgreSQL
**
*****************************************************/
DECLARE
    _myRowCount int := 0;
    _managerID int := 0;
BEGIN
    _remoteInfoXML := '';

    SELECT mgr_id INTO _managerID
    FROM mc.t_mgrs
    WHERE mgr_name = _managerName
    --
    GET DIAGNOSTICS _myRowCount = ROW_COUNT;

    If _myRowCount = 0 Then
        -- Manager not found
        Return;
    End If;

    -----------------------------------------------
    -- Create the Temp Table to hold the manager parameters
    -----------------------------------------------

    CREATE TEMP TABLE Tmp_Mgr_Params (
        M_Name text NOT NULL,
        ParamName text NOT NULL,
        Entry_ID int NOT NULL,
        TypeID int NOT NULL,
        Value text NOT NULL,
        MgrID int NOT NULL,
        Comment text NULL,
        Last_Affected timestamp NULL,
        Entered_By text NULL,
        M_TypeID int NOT NULL,
        ParentParamPointerState int,
        Source text NOT NULL
    )

    -- Populate the temporary table with the manager parameters
    Call get_manager_parameters_work _managerName, 0, 50

    If Not Exists ( SELECT [Value] Then
                    FROM #Tmp_Mgr_Params;
    End If;
                    WHERE M_Name = _managerName And
                          ParamName = 'RunJobsRemotely' AND
                          Value = 'True' )
       OR
       Not Exists ( SELECT [Value]
                    FROM Tmp_Mgr_Params
                    WHERE M_Name = _managerName And
                          ParamName = 'RemoteHostName' AND
                          char_length(Value) > 0 )
    Begin
        _remoteInfoXML := '';
    End
    Else
    Begin
        SELECT @remoteInfoXML + SourceQ.[Value] INTO _remoteInfoXML
        FROM (SELECT 1 AS Sort,
                     '<host>' || [Value] || '</host>' AS [Value]
              FROM Tmp_Mgr_Params
              WHERE (ParamName = 'RemoteHostName' And M_Name = _managerName)
              UNION
              SELECT 2 AS Sort,
                     '<user>' || [Value] || '</user>' AS [Value]
              FROM Tmp_Mgr_Params
              WHERE (ParamName = 'RemoteHostUser' And M_Name = _managerName)
              UNION
              SELECT 3 AS Sort,
                     '<dmsPrograms>' || [Value] || '</dmsPrograms>' AS [Value]
              FROM Tmp_Mgr_Params
              WHERE (ParamName = 'RemoteHostDMSProgramsPath' And M_Name = _managerName)
              UNION
              SELECT 4 AS Sort,
                     '<taskQueue>' || [Value] || '</taskQueue>' AS [Value]
              FROM Tmp_Mgr_Params
              WHERE (ParamName = 'RemoteTaskQueuePath' And M_Name = _managerName)
              UNION
              SELECT 5 AS Sort,
                     '<workDir>' || [Value] || '</workDir>' AS [Value]
              FROM Tmp_Mgr_Params
              WHERE (ParamName = 'RemoteWorkDirPath' And M_Name = _managerName)
              UNION
              SELECT 6 AS Sort,
                     '<orgDB>' || [Value] || '</orgDB>' AS [Value]
              FROM Tmp_Mgr_Params
              WHERE (ParamName = 'RemoteOrgDBPath' And M_Name = _managerName)
              UNION
              SELECT 7 AS Sort,
                     '<privateKey>' || public.get_filename([Value]) || '</privateKey>' AS [Value]
              FROM Tmp_Mgr_Params
              WHERE (ParamName = 'RemoteHostPrivateKeyFile' And M_Name = _managerName)
              UNION
              SELECT 8 AS Sort,
                     '<passphrase>' || public.get_filename([Value]) || '</passphrase>' AS [Value]
              FROM Tmp_Mgr_Params
              WHERE (ParamName = 'RemoteHostPassphraseFile' And M_Name = _managerName)
              ) SourceQ
        ORDER BY SourceQ.Sort
    End

Done:
    Return _myError

    DROP TABLE Tmp_Mgr_Params

END
$$;

COMMENT ON PROCEDURE mc.get_default_remote_info_for_manager IS 'GetDefaultRemoteInfoForManager';

CREATE OR REPLACE PROCEDURE mc.get_manager_parameters
(
    _managerNameList text = '',
    _sortMode int = 0,
    _maxRecursion int = 50,
    INOUT _message text = ''
)
LANGUAGE plpgsql
AS $$
/****************************************************
**
**  Desc:   Gets the parameters for the given analysis manager(s)
**          Uses MgrSettingGroupName to lookup parameters from the parent group, if any
**
**  Arguments:
**    _sortMode   0 means sort by ParamTypeID then MgrName, 1 means ParamName, then MgrName, 2 means MgrName, then ParamName, 3 means Value then ParamName
**
**  Auth:   mem
**  Date:   05/07/2015 mem - Initial version
**          08/10/2015 mem - Add _sortMode=3
**          09/02/2016 mem - Increase the default for parameter _maxRecursion from 5 to 50
**          03/14/2018 mem - Refactor actual parameter lookup into stored procedure GetManagerParametersWork
**          09/21/2022 mem - Ported to PostgreSQL
**
*****************************************************/
DECLARE
    _myRowCount int := 0;
BEGIN
    -----------------------------------------------
    -- Validate the inputs
    -----------------------------------------------
    --
    _managerNameList := Coalesce(_managerNameList, '');

    _sortMode := Coalesce(_sortMode, 0);

    If _maxRecursion > 10 Then
        _maxRecursion := 10;
    End If;

    -----------------------------------------------
    -- Create the Temp Table to hold the manager parameters
    -----------------------------------------------

    CREATE TEMP TABLE Tmp_Mgr_Params (
        M_Name text NOT NULL,
        ParamName text NOT NULL,
        Entry_ID int NOT NULL,
        TypeID int NOT NULL,
        Value text NOT NULL,
        MgrID int NOT NULL,
        Comment text NULL,
        Last_Affected timestamp NULL,
        Entered_By text NULL,
        M_TypeID int NOT NULL,
        ParentParamPointerState int,
        Source text NOT NULL
    )

    -- Populate the temporary table with the manager parameters
    Call get_manager_parameters_work _managerNameList, _sortMode, _maxRecursion, _message => _message Output

    -- Return the parameters as a result set

    If _sortMode = 0 Then
        SELECT *;
    End If;
        FROM Tmp_Mgr_Params
        ORDER BY TypeID, M_Name

    If _sortMode = 1 Then
        SELECT *;
    End If;
        FROM Tmp_Mgr_Params
        ORDER BY ParamName, M_Name

    If _sortMode = 2 Then
        SELECT *;
    End If;
        FROM Tmp_Mgr_Params
        ORDER BY M_Name, ParamName

    If _sortMode Not In (0,1,2) Then
        SELECT *;
    End If;
        FROM Tmp_Mgr_Params
        ORDER BY Value, ParamName

     Drop Table Tmp_Mgr_Params

Done:
    Return _myError

    DROP TABLE Tmp_Mgr_Params

END
$$;

COMMENT ON PROCEDURE mc.get_manager_parameters IS 'GetManagerParameters';

CREATE OR REPLACE PROCEDURE mc.get_manager_parameters_work
(
    _managerNameList text = '',
    _sortMode int = 0,
    _maxRecursion int = 50,
    INOUT _message text=''
)
LANGUAGE plpgsql
AS $$
/****************************************************
**
**  Desc:   Populates temporary tables with the parameters for the given analysis manager(s)
**          Uses MgrSettingGroupName to lookup parameters from the parent group, if any
**
**  Requires that the calling procedure create temporary table Tmp_Mgr_Params
**
**  Arguments:
**    _sortMode   0 means sort by ParamTypeID then MgrName, 1 means ParamName, then MgrName, 2 means MgrName, then ParamName, 3 means Value then ParamName
**
**  Auth:   mem
**  Date:   03/14/2018 mem - Initial version (code refactored from GetManagerParameters)
**          09/21/2022 mem - Ported to PostgreSQL
**
*****************************************************/
DECLARE
    _myRowCount int := 0;
    _iterations int := 0;
BEGIN
    -----------------------------------------------
    -- Create the Temp Table to hold the manager group information
    -----------------------------------------------

    CREATE TEMP TABLE Tmp_Manager_Group_Info (
        M_Name text NOT NULL,
        Group_Name text NOT NULL
    )

    -----------------------------------------------
    -- Lookup the initial manager parameters
    -----------------------------------------------
    --

    INSERT INTO Tmp_Mgr_Params( M_Name,
                                 ParamName,
                                 Entry_ID,
                                 TypeID,
                                 Value,
                                 MgrID,
                                 Comment,
                                 Last_Affected,
                                 Entered_By,
                                 M_TypeID,
                                 ParentParamPointerState,
                                 Source )
    SELECT M_Name,
           ParamName,
           Entry_ID,
           TypeID,
           Value,
           MgrID,
           Comment,
           Last_Affected,
           Entered_By,
           M_TypeID,
           CASE
               WHEN TypeID = 162 THEN 1        -- ParamName 'Default_AnalysisMgr_Params'
               ELSE 0
           End As ParentParamPointerState,
           M_Name
    FROM V_ParamValue
    WHERE (M_Name IN (Select Value From public.parse_delimited_list(_managerNameList, ',')))
    --
    GET DIAGNOSTICS _myRowCount = ROW_COUNT;

    -----------------------------------------------
    -- Append parameters for parent groups, which are
    -- defined by parameter Default_AnalysisMgr_Params (TypeID 162)
    -----------------------------------------------
    --

    While Exists (Select * from Tmp_Mgr_Params Where ParentParamPointerState = 1) And _iterations < _maxRecursion Loop
        Truncate table Tmp_Manager_Group_Info

        INSERT INTO Tmp_Manager_Group_Info (M_Name, Group_Name)
        SELECT M_Name, Value
        FROM Tmp_Mgr_Params
        WHERE (ParentParamPointerState = 1)

        UPDATE Tmp_Mgr_Params
        Set ParentParamPointerState = 2
        WHERE (ParentParamPointerState = 1)

        INSERT INTO Tmp_Mgr_Params( M_Name,
                                     ParamName,
                                     Entry_ID,
                                     TypeID,
                                     Value,
                                     MgrID,
                                     Comment,
                                     Last_Affected,
                                     Entered_By,
                                     M_TypeID,
                                     ParentParamPointerState,
                                     Source )
        SELECT ValuesToAppend.M_Name,
               ValuesToAppend.ParamName,
               ValuesToAppend.Entry_ID,
               ValuesToAppend.TypeID,
               ValuesToAppend.Value,
               ValuesToAppend.MgrID,
               ValuesToAppend.Comment,
               ValuesToAppend.Last_Affected,
               ValuesToAppend.Entered_By,
               ValuesToAppend.M_TypeID,
               CASE
                   WHEN ValuesToAppend.TypeID = 162 THEN 1
                   ELSE 0
               End Loop; As ParentParamPointerState,
               ValuesToAppend.Source
        FROM Tmp_Mgr_Params Target
             RIGHT OUTER JOIN ( SELECT FilterQ.M_Name,
                                       PV.ParamName,
                                       PV.Entry_ID,
                                       PV.TypeID,
                                       PV.Value,
                                       PV.MgrID,
                                       PV.Comment,
                                       PV.Last_Affected,
                                       PV.Entered_By,
                                       PV.M_TypeID,
                                       PV.M_Name AS Source
                                FROM V_ParamValue PV
                                     INNER JOIN ( SELECT M_Name,
                                                         Group_Name
                                                  FROM Tmp_Manager_Group_Info ) FilterQ
                                       ON PV.M_Name = FilterQ.Group_Name ) ValuesToAppend
               ON Target.M_Name = ValuesToAppend.M_Name AND
                  Target.TypeID = ValuesToAppend.TypeID
        WHERE (Target.TypeID IS NULL Or ValuesToAppend.typeID = 162)
        --
        GET DIAGNOSTICS _myRowCount = ROW_COUNT;

        -- This is a safety check in case a manager has a Default_AnalysisMgr_Params value pointing to itself
        _iterations := _iterations + 1;

    End

    Drop Table Tmp_Manager_Group_Info

Done:
    Return _myError

    DROP TABLE Tmp_Manager_Group_Info

END
$$;

COMMENT ON PROCEDURE mc.get_manager_parameters_work IS 'GetManagerParametersWork';

CREATE OR REPLACE PROCEDURE mc.parse_manager_name_list
(
    _managerNameList text = '',
    _removeUnknownManagers int = 1,
    INOUT _message text=''
)
LANGUAGE plpgsql
AS $$
/****************************************************
**
**  Desc:   Parses the list of managers in _managerNameList and populates
**          a temporary tables with the manager names
**
**          If _removeUnknownManagers = 1, then deletes manager names that are not defined in T_Mgrs
**
**          The calling procedure must create the following temporary table:
**          CREATE TABLE TmpManagerList (
**              Manager_Name varchar(128) NOT NULL
**          )
**
**  Auth:   mem
**  Date:   05/09/2008
**          05/14/2015 mem - Update Insert query to explicitly list field Manager_Name
**          09/21/2022 mem - Ported to PostgreSQL
**
*****************************************************/
DECLARE
    _myRowCount int := 0;
    _entryID int;
    _continue int;
    _managerFilter text;
    _s text;
BEGIN
    -----------------------------------------------
    -- Validate the inputs
    -----------------------------------------------
    --
    _managerNameList := Coalesce(_managerNameList, '');
    _removeUnknownManagers := Coalesce(_removeUnknownManagers, 1);
    _message := '';

    -----------------------------------------------
    -- Creata a temporary table
    -----------------------------------------------

    CREATE TEMP TABLE TmpMangerSpecList (
        Entry_ID int PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
        Manager_Name text NOT NULL
    )

    -----------------------------------------------
    -- Parse _managerNameList
    -----------------------------------------------

    If char_length(_managerNameList) > 0 Then
    -- <a>

        -- Populate TmpMangerSpecList with the data in _managerNameList
        INSERT INTO TmpMangerSpecList (Manager_Name)
        SELECT Value
        FROM public.parse_delimited_list(_managerNameList, ',')
        --
        GET DIAGNOSTICS _myRowCount = ROW_COUNT;

        -- Populate TmpManagerList with the entries in TmpMangerSpecList that do not contain a % wildcard
        INSERT INTO TmpManagerList (Manager_Name)
        SELECT Manager_Name
        FROM TmpMangerSpecList
        WHERE NOT Manager_Name SIMILAR TO '%[%]%'
        --
        GET DIAGNOSTICS _myRowCount = ROW_COUNT;

        -- Delete the non-wildcard entries from TmpMangerSpecList
        DELETE FROM TmpMangerSpecList
        WHERE NOT Manager_Name SIMILAR TO '%[%]%'

        -- Parse the entries in TmpMangerSpecList (all should have a wildcard)
        _entryID := 0;

        _continue := 1;
        While _continue = 1 Loop
            -- This While loop can probably be converted to a For loop; for example:
            --    For _itemName In
            --        SELECT item_name
            --        FROM TmpSourceTable
            --        ORDER BY entry_id
            --    Loop
            --        ...
            --    End Loop

            -- Moved to bottom of query: TOP 1
            SELECT Entry_ID, INTO _entryID
                         _managerFilter = Manager_Name
            FROM TmpMangerSpecList
            WHERE Entry_ID > _entryID
            ORDER BY Entry_ID
            LIMIT 1;
            --
            GET DIAGNOSTICS _myRowCount = ROW_COUNT;

            If _myRowCount = 0 Then
                _continue := 0;
            Else
            -- <c>
                _s := '';
                _s := _s || ' INSERT INTO TmpManagerList (Manager_Name)';
                _s := _s || ' SELECT mgr_name';
                _s := _s || ' FROM mc.t_mgrs';
                _s := _s || ' WHERE mgr_name LIKE ''' || _managerFilter || '''';

                Call (_s)
                --
                GET DIAGNOSTICS _myRowCount = ROW_COUNT;

            End If; -- </c>

        End Loop; -- </b1>

        If _removeUnknownManagers <> 0 Then
        -- <b2>
            -- Delete entries from TmpManagerList that don't match entries in M_Name of the given type
            DELETE TmpManagerList
            FROM TmpManagerList U LEFT OUTER JOIN

            /********************************************************************************
            ** This DELETE query includes the target table name in the FROM clause
            ** The WHERE clause needs to have a self join to the target table, for example:
            **   UPDATE #TmpManagerList
            **   SET ...
            **   FROM source
            **   WHERE source.id = #TmpManagerList.id;
            **
            ** Delete queries must also include the USING keyword
            ** Alternatively, the more standard approach is to rearrange the query to be similar to
            **   DELETE FROM #TmpManagerList WHERE id in (SELECT id from ...)
            ********************************************************************************/

                                   ToDo: Fix this query

                mc.t_mgrs M ON M.mgr_name = U.Manager_Name
            WHERE M.mgr_name Is Null
            --
            GET DIAGNOSTICS _myRowCount = ROW_COUNT;

            If _myRowCount > 0 Then
                _message := 'Found ' || _myRowCount::text || ' entries in _managerNameList that are not defined in mc.t_mgrs';
                RAISE INFO '%', _message;

                _message := '';
            End If;

        End If; -- </b2>

    End If; -- </a>

    Return _myError

    DROP TABLE TmpMangerSpecList

END
$$;

COMMENT ON PROCEDURE mc.parse_manager_name_list IS 'ParseManagerNameList';

CREATE OR REPLACE PROCEDURE mc.post_log_entry
(
    VALUES ( _postedBy, GETDATE(), _type, _message)
    --
    if @@rowcount <> 1
    begin
    RAISERROR ('Update was unsuccessful for T_Log_Entries table', 10, 1)
    return 51191
    end
    End
    return 0
)
LANGUAGE plpgsql
AS $$
/****************************************************
**
**  Desc:   Put new entry into the main log table
**
*
**  Auth:   grk
**  Date:   10/31/2001
**          02/17/2005 mem - Added parameter _duplicateEntryHoldoffHours
**          05/31/2007 mem - Expanded the size of _type, _message, and _postedBy
**          09/21/2022 mem - Ported to PostgreSQL
**
*****************************************************/
DECLARE
    _duplicateRowCount int;
BEGIN
    _type text,
    _message text,
    _postedBy text= 'na',
    _duplicateEntryHoldoffHours int = 0            -- Set this to a value greater than 0 to prevent duplicate entries being posted within the given number of hours

    _duplicateRowCount := 0;

    If Coalesce(_duplicateEntryHoldoffHours, 0) > 0 Then
        SELECT COUNT(*) INTO _duplicateRowCount
        FROM mc.t_log_entries
        WHERE message = _message AND type = _type AND posting_time >= (CURRENT_TIMESTAMP - _duplicateEntryHoldoffHours)
    End If;

    If _duplicateRowCount = 0 Then
        INSERT INTO mc.t_log_entries
END
$$;

COMMENT ON PROCEDURE mc.post_log_entry IS 'PostLogEntry';

CREATE OR REPLACE PROCEDURE mc.post_usage_log_entry
(
    _postedBy text,
    _message text = '',
    _minimumUpdateInterval int = 1
)
LANGUAGE plpgsql
AS $$
/****************************************************
**
**  Desc:   Put new entry into T_Usage_Log and update T_Usage_Stats
**
**  Arguments:
**    _minimumUpdateInterval   Set to a value greater than 0 to limit the entries to occur at most every _minimumUpdateInterval hours
**
**  Auth:   mem
**  Date:   10/22/2004
**          07/29/2005 mem - Added parameter _minimumUpdateInterval
**          03/16/2006 mem - Now updating T_Usage_Stats
**          03/17/2006 mem - Now populating Usage_Count in T_Usage_Log and changed _minimumUpdateInterval from 6 hours to 1 hour
**          05/03/2009 mem - Removed parameter _dBName
**          09/21/2022 mem - Ported to PostgreSQL
**
*****************************************************/
DECLARE
    _myRowCount int := 0;
    _callingUser text;
    _postEntry int;
    _lastUpdated text;
BEGIN
    _callingUser := session_user;

    _postEntry := 1;

    -- Update entry for _postedBy in T_Usage_Stats
    If Not Exists (SELECT Posted_By FROM T_Usage_Stats WHERE Posted_By = _postedBy) Then
        INSERT INTO T_Usage_Stats (Posted_By, Last_Posting_Time, Usage_Count);
    End If;
        VALUES (_postedBy, CURRENT_TIMESTAMP, 1)
    Else
        UPDATE T_Usage_Stats
        SET Last_Posting_Time = CURRENT_TIMESTAMP, Usage_Count = Usage_Count + 1
        WHERE Posted_By = _postedBy

    if _minimumUpdateInterval > 0 Then
        -- See if the last update was less than _minimumUpdateInterval hours ago

        _lastUpdated := '1/1/1900';

        SELECT MAX(Posting_time) INTO _lastUpdated
        FROM T_Usage_Log
        WHERE Posted_By = _postedBy AND Calling_User = _callingUser
        --
        GET DIAGNOSTICS _myRowCount = ROW_COUNT;

        IF _myRowCount = 1 Then
            If CURRENT_TIMESTAMP <= DateAdd(hour, _minimumUpdateInterval, Coalesce(_lastUpdated, '1/1/1900')) Then
                _postEntry := 0;
            End If;
        End If;
    End If;

    If _postEntry = 1 Then
        INSERT INTO T_Usage_Log
                (Posted_By, Posting_Time, Message, Calling_User, Usage_Count)
        SELECT _postedBy, CURRENT_TIMESTAMP, _message, _callingUser, S.Usage_Count
        FROM T_Usage_Stats S
        WHERE S.Posted_By = _postedBy
        --
        GET DIAGNOSTICS _myRowCount = ROW_COUNT;
        --
        if _myRowCount <> 1 Or _myError <> 0 Then
            _message := 'Update was unsuccessful for T_Usage_Log table: _myRowCount = ' || _myRowCount::text || '; _myError = ' || _myError::text;
            execute PostLogEntry 'Error', _message, 'PostUsageLogEntry'
        End If;
    End If;

    RETURN 0

END
$$;

COMMENT ON PROCEDURE mc.post_usage_log_entry IS 'PostUsageLogEntry';

CREATE OR REPLACE PROCEDURE mc.report_manager_error_cleanup
(
    _managerName text,
    _state int = 0,
    _failureMsg text = '',
    INOUT _message text = ''
)
LANGUAGE plpgsql
AS $$
/****************************************************
**
**  Desc:
**      Reports that the manager tried to auto-cleanup
**      when there is a flag file or non-empty working directory
**
**  Arguments:
**    _state   1 = Cleanup Attempt start, 2 = Cleanup Successful, 3 = Cleanup Failed
**
**  Auth:   mem
**  Date:   09/10/2009 mem - Initial version
**          09/21/2022 mem - Ported to PostgreSQL
**
*****************************************************/
DECLARE
    _myRowCount int := 0;
    _mgrID int;
    _mgrNameLocal text;
    _paramID int;
    _messageType text;
    _cleanupMode text;
BEGIN
    ---------------------------------------------------
    -- Cleanup the inputs
    ---------------------------------------------------

    _managerName := Coalesce(_managerName, '');
    _state := Coalesce(_state, 0);
    _failureMsg := Coalesce(_failureMsg, '');
    _message := '';

    ---------------------------------------------------
    -- Confirm that the manager name is valid
    ---------------------------------------------------

    SELECT mgr_id, INTO _mgrID
            _mgrNameLocal = mgr_name
    FROM mc.t_mgrs
    WHERE (mgr_name = _managerName)
    --
    GET DIAGNOSTICS _myRowCount = ROW_COUNT;

    if _myRowCount <> 1 Then
        _myError := 52002;
        _message := 'Could not find entry for manager: ' || _managerName;
        Return;
    End If;

    _managerName := _mgrNameLocal;

    ---------------------------------------------------
    -- Validate _state
    ---------------------------------------------------

    If _state < 1 or _state > 3 Then
        _myError := 52003;
        _message := 'Invalid value for _state; should be 1, 2 or 3';
        Return;
    End If;

    ---------------------------------------------------
    -- Log this cleanup event
    ---------------------------------------------------

    _messageType := 'Error';
    _message := 'Unknown _state value';

    If _state = 1 Then
        _messageType := 'Normal';
        _message := 'Manager ' || _managerName || ' is attempting auto error cleanup';
    End If;

    If _state = 2 Then
        _messageType := 'Normal';
        _message := 'Automated error cleanup succeeded for ' || _managerName;
    End If;

    If _state = 3 Then
        _messageType := 'Normal';
        _message := 'Automated error cleanup failed for ' || _managerName;
        If _failureMsg <> '' Then
            _message := _message || '; ' || _failureMsg;
        End If;
    End If;

    Call post_log_entry _messageType, _message, 'ReportManagerErrorCleanup'

    ---------------------------------------------------
    -- Lookup the value of ManagerErrorCleanupMode in mc.t_param_value
    ---------------------------------------------------

    _cleanupMode := '0';

    SELECT mc.t_param_value.value INTO _cleanupMode
    FROM mc.t_param_value
         INNER JOIN mc.t_param_type
           ON mc.t_param_value.type_id = mc.t_param_type.param_id
    WHERE (mc.t_param_type.param_name = 'ManagerErrorCleanupMode') AND
          (mc.t_param_value.mgr_id = _mgrID)
    --
    GET DIAGNOSTICS _myRowCount = ROW_COUNT;

    If _myRowCount = 0 Then
        -- Entry not found; make a new entry for 'ManagerErrorCleanupMode' in the mc.t_param_value table
        _paramID := 0;

        SELECT param_id INTO _paramID
        FROM mc.t_param_type
        WHERE (param_name = 'ManagerErrorCleanupMode')
        --
        GET DIAGNOSTICS _myRowCount = ROW_COUNT;

        If _paramID > 0 Then
            INSERT INTO mc.t_param_value (mgr_id, type_id, value)
            VALUES (_mgrID, _paramID, '0')

            _cleanupMode := '0';
        End If;
    End If;

    If Trim(_cleanupMode) = '1' Then
        -- Manager is set to auto-cleanup only once; change 'ManagerErrorCleanupMode' to 0
        UPDATE mc.t_param_value
        SET value = '0'
        FROM mc.t_param_value

        /********************************************************************************
        ** This UPDATE query includes the target table name in the FROM clause
        ** The WHERE clause needs to have a self join to the target table, for example:
        **   UPDATE mc.t_param_value
        **   SET ...
        **   FROM source
        **   WHERE source.id = mc.t_param_value.id;
        ********************************************************************************/

                               ToDo: Fix this query

             INNER JOIN mc.t_param_type
               ON mc.t_param_value.type_id = mc.t_param_type.param_id
        WHERE (mc.t_param_type.param_name = 'ManagerErrorCleanupMode') AND
              (mc.t_param_value.mgr_id = _mgrID)
        --
        GET DIAGNOSTICS _myRowCount = ROW_COUNT;

        if _myError <> 0 Then
            _message := 'Error setting ManagerErrorCleanupMode to 0 in mc.t_param_value for manager ' || _managerName;
            Call post_log_entry 'Error', _message, 'ReportManagerErrorCleanup'
        Else
            If _myRowCount = 0 Then
                _message := _message || '; Entry not found in mc.t_param_value for ManagerErrorCleanupMode; this is unexpected';
            Else
                _message := _message || '; Decremented ManagerErrorCleanupMode to 0 in mc.t_param_value';
            End If;
        End If;
    End If;

    ---------------------------------------------------
    -- Exit the procedure
    ---------------------------------------------------
Done:
    return _myError

END
$$;

COMMENT ON PROCEDURE mc.report_manager_error_cleanup IS 'ReportManagerErrorCleanup';

CREATE OR REPLACE PROCEDURE mc.set_manager_error_cleanup_mode
(
    _managerList text = '',
    _cleanupMode int = 1,
    _showTable int = 1,
    _infoOnly int = 0,
    INOUT _message text = ''
)
LANGUAGE plpgsql
AS $$
/****************************************************
**
**  Desc:
**      Sets ManagerErrorCleanupMode to _cleanupMode for the given list of managers
**      If _managerList is blank, then sets it to _cleanupMode for all "Analysis Tool Manager" managers
**
**  Arguments:
**    _cleanupMode   0 = No auto cleanup, 1 = Attempt auto cleanup once, 2 = Auto cleanup always
**
**  Auth:   mem
**  Date:   09/10/2009 mem - Initial version
**          09/29/2014 mem - Expanded _managerList to varchar(max) and added parameters _showTable and _infoOnly
**                         - Fixed where clause bug in final update query
**          09/21/2022 mem - Ported to PostgreSQL
**
*****************************************************/
DECLARE
    _myRowCount int := 0;
    _mgrID int;
    _paramID int;
    _cleanupModeString text;
BEGIN
    ---------------------------------------------------
    -- Validate the inputs
    ---------------------------------------------------

    _managerList := Coalesce(_managerList, '');
    _cleanupMode := Coalesce(_cleanupMode, 1);
    _showTable := Coalesce(_showTable, 1);
    _infoOnly := Coalesce(_infoOnly, 0);
    _message := '';

    If _cleanupMode < 0 Then
        _cleanupMode := 0;
    End If;

    If _cleanupMode > 2 Then
        _cleanupMode := 2;
    End If;

    CREATE TEMP TABLE TmpManagerList (
        ManagerName text NOT NULL,
        MgrID int NULL
    )

    ---------------------------------------------------
    -- Confirm that the manager names are valid
    ---------------------------------------------------

    If char_length(_managerList) > 0 Then
        INSERT INTO #TmpManagerList (ManagerName);
    End If;
        SELECT Value
        FROM public.parse_delimited_list(_managerList, ',')
        WHERE char_length(Coalesce(Value, '')) > 0
    Else
        INSERT INTO TmpManagerList (ManagerName)
        SELECT mgr_name
        FROM mc.t_mgrs
        WHERE (mgr_type_id = 11)

    UPDATE TmpManagerList
    SET MgrID = mc.t_mgrs.mgr_id
    FROM TmpManagerList INNER JOIN mc.t_mgrs

    /********************************************************************************
    ** This UPDATE query includes the target table name in the FROM clause
    ** The WHERE clause needs to have a self join to the target table, for example:
    **   UPDATE #TmpManagerList
    **   SET ...
    **   FROM source
    **   WHERE source.id = #TmpManagerList.id;
    ********************************************************************************/

                           ToDo: Fix this query

            ON mc.t_mgrs.mgr_name = TmpManagerList.ManagerName
    --
    GET DIAGNOSTICS _myRowCount = ROW_COUNT;

    DELETE FROM TmpManagerList
    WHERE MgrID IS NULL
    --
    GET DIAGNOSTICS _myRowCount = ROW_COUNT;

    If _myRowCount <> 0 Then
        _message := 'Removed ' || _myRowCount::text || ' invalid manager';
        If _myRowCount > 1 Then
            _message := _message || 's';
        End If;

        _message := _message || ' from TmpManagerList';
        RAISE INFO '%', _message;
    End If;

    ---------------------------------------------------
    -- Lookup the ParamID value for 'ManagerErrorCleanupMode'
    ---------------------------------------------------

    _paramID := 0;
    --
    SELECT param_id INTO _paramID
    FROM mc.t_param_type
    WHERE (param_name = 'ManagerErrorCleanupMode')

    ---------------------------------------------------
    -- Make sure each manager in TmpManagerList has an entry
    --  in mc.t_param_value for 'ManagerErrorCleanupMode'
    ---------------------------------------------------

    INSERT INTO mc.t_param_value (mgr_id, type_id, value)
    SELECT A.mgr_id, _paramID, '0'
    FROM ( SELECT mgr_id
           FROM TmpManagerList
         ) A
         LEFT OUTER JOIN
          ( SELECT TmpManagerList.mgr_id
            FROM TmpManagerList
                 INNER JOIN mc.t_param_value
                   ON TmpManagerList.mgr_id = mc.t_param_value.mgr_id
            WHERE mc.t_param_value.type_id = _paramID
         ) B
           ON A.mgr_id = B.mgr_id
    WHERE B.mgr_id IS NULL
    --
    GET DIAGNOSTICS _myRowCount = ROW_COUNT;

    If _myRowCount <> 0 Then
        _message := 'Added entry for "ManagerErrorCleanupMode" to mc.t_param_value for ' || _myRowCount::text || ' manager';
        If _myRowCount > 1 Then
            _message := _message || 's';
        End If;

        RAISE INFO '%', _message;
    End If;

    ---------------------------------------------------
    -- Update the 'ManagerErrorCleanupMode' entry for each manager in TmpManagerList
    ---------------------------------------------------

    _cleanupModeString := _cleanupMode::text;

    If _infoOnly <> 0 Then
        SELECT MP.*, _cleanupMode As NewCleanupMode
        FROM V_AnalysisMgrParams_ActiveAndDebugLevel MP
            INNER JOIN TmpManagerList
            ON MP.MgrID = TmpManagerList.MgrID
        WHERE MP.ParamTypeID = 120
        ORDER BY MP.Manager
    Else

        UPDATE mc.t_param_value
        SET value = _cleanupModeString
        FROM mc.t_param_value

        /********************************************************************************
        ** This UPDATE query includes the target table name in the FROM clause
        ** The WHERE clause needs to have a self join to the target table, for example:
        **   UPDATE mc.t_param_value
        **   SET ...
        **   FROM source
        **   WHERE source.id = mc.t_param_value.id;
        ********************************************************************************/

                               ToDo: Fix this query

            INNER JOIN TmpManagerList
            ON mc.t_param_value.mgr_id = TmpManagerList.mgr_id
        WHERE mc.t_param_value.type_id = _paramID AND
            mc.t_param_value.value <> _cleanupModeString
        --
        GET DIAGNOSTICS _myRowCount = ROW_COUNT;

        If _myRowCount <> 0 Then
            _message := 'Set "ManagerErrorCleanupMode" to ' || _cleanupModeString || ' for ' || _myRowCount::text || ' manager';
            If _myRowCount > 1 Then
                _message := _message || 's';
            End If;

            RAISE INFO '%', _message;
        End If;
    End If;

    ---------------------------------------------------
    -- Show the new values
    ---------------------------------------------------

    If _infoOnly = 0 And _showTable <> 0 Then
        SELECT MP.*
        FROM V_AnalysisMgrParams_ActiveAndDebugLevel MP
            INNER JOIN TmpManagerList
            ON MP.MgrID = TmpManagerList.MgrID
        WHERE MP.ParamTypeID = 120
        ORDER BY MP.Manager
    End If;

    ---------------------------------------------------
    -- Exit the procedure
    ---------------------------------------------------
Done:
    return _myError
    DROP TABLE TmpManagerList

END
$$;

COMMENT ON PROCEDURE mc.set_manager_error_cleanup_mode IS 'SetManagerErrorCleanupMode';

CREATE OR REPLACE PROCEDURE mc.set_manager_update_required
(
    _managerList text = '',
    _showTable int = 0,
    INOUT _message text = ''
)
LANGUAGE plpgsql
AS $$
/****************************************************
**
**  Desc:
**      Sets ManagerUpdateRequired to true for the given list of managers
**      If _managerList is blank, then sets it to true for all "Analysis Tool Manager" managers
**
**  Auth:   mem
**  Date:   01/24/2009 mem - Initial version
**          04/17/2014 mem - Expanded _managerList to varchar(max) and added parameter _showTable
**          09/21/2022 mem - Ported to PostgreSQL
**
*****************************************************/
DECLARE
    _myRowCount int := 0;
    _mgrID int;
    _paramID int;
BEGIN
    _showTable := Coalesce(_showTable, 0);
    _message := '';

    CREATE TEMP TABLE TmpManagerList (
        ManagerName text NOT NULL,
        MgrID int NULL
    )

    ---------------------------------------------------
    -- Confirm that the manager name is valid
    ---------------------------------------------------

    _managerList := Coalesce(_managerList, '');

    If char_length(_managerList) > 0 Then
        INSERT INTO #TmpManagerList (ManagerName);
    End If;
        SELECT Value
        FROM public.parse_delimited_list(_managerList, ',')
        WHERE char_length(Coalesce(Value, '')) > 0
    Else
        INSERT INTO TmpManagerList (ManagerName)
        SELECT mgr_name
        FROM mc.t_mgrs
        WHERE (mgr_type_id = 11)

    UPDATE TmpManagerList
    SET MgrID = mc.t_mgrs.mgr_id
    FROM TmpManagerList INNER JOIN mc.t_mgrs

    /********************************************************************************
    ** This UPDATE query includes the target table name in the FROM clause
    ** The WHERE clause needs to have a self join to the target table, for example:
    **   UPDATE #TmpManagerList
    **   SET ...
    **   FROM source
    **   WHERE source.id = #TmpManagerList.id;
    ********************************************************************************/

                           ToDo: Fix this query

            ON mc.t_mgrs.mgr_name = TmpManagerList.ManagerName
    --
    GET DIAGNOSTICS _myRowCount = ROW_COUNT;

    DELETE FROM TmpManagerList
    WHERE MgrID IS NULL
    --
    GET DIAGNOSTICS _myRowCount = ROW_COUNT;

    If _myRowCount <> 0 Then
        _message := 'Removed ' || _myRowCount::text || ' invalid manager';
        If _myRowCount > 1 Then
            _message := _message || 's';
        End If;

        _message := _message || ' from TmpManagerList';
        RAISE INFO '%', _message;
    End If;

    ---------------------------------------------------
    -- Lookup the ParamID value for 'ManagerUpdateRequired'
    ---------------------------------------------------

    _paramID := 0;
    --
    SELECT param_id INTO _paramID
    FROM mc.t_param_type
    WHERE (param_name = 'ManagerUpdateRequired')

    ---------------------------------------------------
    -- Make sure each manager in TmpManagerList has an entry
    --  in mc.t_param_value for 'ManagerUpdateRequired'
    ---------------------------------------------------

    INSERT INTO mc.t_param_value (mgr_id, type_id, value)
    SELECT A.mgr_id, _paramID, 'False'
    FROM ( SELECT mgr_id
           FROM TmpManagerList
         ) A
         LEFT OUTER JOIN
          ( SELECT TmpManagerList.mgr_id
            FROM TmpManagerList
                 INNER JOIN mc.t_param_value
                   ON TmpManagerList.mgr_id = mc.t_param_value.mgr_id
            WHERE mc.t_param_value.type_id = _paramID
         ) B
           ON A.mgr_id = B.mgr_id
    WHERE B.mgr_id IS NULL
    --
    GET DIAGNOSTICS _myRowCount = ROW_COUNT;

    If _myRowCount <> 0 Then
        _message := 'Added entry for "ManagerUpdateRequired" to mc.t_param_value for ' || _myRowCount::text || ' manager';
        If _myRowCount > 1 Then
            _message := _message || 's';
        End If;

        RAISE INFO '%', _message;
    End If;

    ---------------------------------------------------
    -- Update the 'ManagerUpdateRequired' entry for each manager in TmpManagerList
    ---------------------------------------------------

    UPDATE mc.t_param_value
    SET value = 'True'
    FROM mc.t_param_value

    /********************************************************************************
    ** This UPDATE query includes the target table name in the FROM clause
    ** The WHERE clause needs to have a self join to the target table, for example:
    **   UPDATE mc.t_param_value
    **   SET ...
    **   FROM source
    **   WHERE source.id = mc.t_param_value.id;
    ********************************************************************************/

                           ToDo: Fix this query

         INNER JOIN TmpManagerList
           ON mc.t_param_value.mgr_id = TmpManagerList.mgr_id
    WHERE (mc.t_param_value.type_id = _paramID) AND
          mc.t_param_value.value <> 'True'
    --
    GET DIAGNOSTICS _myRowCount = ROW_COUNT;

    If _myRowCount <> 0 Then
        _message := 'Set "ManagerUpdateRequired" to True for ' || _myRowCount::text || ' manager';
        If _myRowCount > 1 Then
            _message := _message || 's';
        End If;

        RAISE INFO '%', _message;
    End If;

    If _showTable <> 0 Then
        SELECT U.*
        FROM V_AnalysisMgrParams_UpdateRequired U
             INNER JOIN TmpManagerList L
               ON U.MgrID = L.MgrId
        ORDER BY Manager DESC
    End If;

    ---------------------------------------------------
    -- Exit the procedure
    ---------------------------------------------------
Done:
    return _myError

    DROP TABLE TmpManagerList

END
$$;

COMMENT ON PROCEDURE mc.set_manager_update_required IS 'SetManagerUpdateRequired';

CREATE OR REPLACE PROCEDURE mc.unarchive_old_managers_and_params
(
    _mgrList text,
    _infoOnly int = 1,
    _enableControlFromWebsite int = 0,
    INOUT _message text=''
)
LANGUAGE plpgsql
AS $$
/****************************************************
**
**  Desc:   Moves managers from T_OldManagers to T_Mgrs
**          and moves manager parameters from T_ParamValue_OldManagers to T_ParamValue
**
**          To reverse this process, use procedure UnarchiveOldManagersAndParams
**
**  Arguments:
**    _mgrList   One or more manager names (comma-separated list); supports wildcards because uses stored procedure ParseManagerNameList
**
**  Auth:   mem
**  Date:   02/25/2016 mem - Initial version
**          04/22/2016 mem - Now updating M_Comment in T_Mgrs
**          09/21/2022 mem - Ported to PostgreSQL
**
*****************************************************/
DECLARE
    _myRowCount int := 0;
    _moveParams text := 'Move params transaction';
BEGIN
    Set XACT_ABORT, NoCount On

    ---------------------------------------------------
    -- Validate the inputs
    ---------------------------------------------------
    --
    _mgrList := Coalesce(_mgrList, '');
    _infoOnly := Coalesce(_infoOnly, 1);
    _enableControlFromWebsite := Coalesce(_enableControlFromWebsite, 1);
    _message := '';

    If _enableControlFromWebsite > 0 Then
        _enableControlFromWebsite := 1;
    End If;

    CREATE TEMP TABLE TmpManagerList (
        Manager_Name text NOT NULL,
        M_ID int NULL
    )

    ---------------------------------------------------
    -- Populate TmpManagerList with the managers in _mgrList
    ---------------------------------------------------
    --

    Call parse_manager_name_list _mgrList, _removeUnknownManagers => 0

    If Not Exists (Select * from TmpManagerList) Then
        _message := '_mgrList was empty';
        Select _message as Warning
        Return;
    End If;

    ---------------------------------------------------
    -- Validate the manager names
    ---------------------------------------------------
    --
    UPDATE TmpManagerList
    SET M_ID = M.M_ID
    FROM TmpManagerList Target

    /********************************************************************************
    ** This UPDATE query includes the target table name in the FROM clause
    ** The WHERE clause needs to have a self join to the target table, for example:
    **   UPDATE #TmpManagerList
    **   SET ...
    **   FROM source
    **   WHERE source.id = #TmpManagerList.id;
    ********************************************************************************/

                           ToDo: Fix this query

         INNER JOIN mc.t_old_managers M
           ON Target.Manager_Name = M.mgr_name
    --
    GET DIAGNOSTICS _myRowCount = ROW_COUNT;

    If Exists (Select * from TmpManagerList where M_ID Is Null) Then
        SELECT 'Unknown manager (not in mc.t_old_managers)' AS Warning, Manager_Name
        FROM TmpManagerList
        WHERE mgr_id  Is Null
        ORDER BY Manager_Name
    End If;

    If Exists (Select * From TmpManagerList Where Manager_Name Like '%Params%') Then
        SELECT 'Will not process managers with "Params" in the name (for safety)' AS Warning,
               Manager_Name
        FROM TmpManagerList
        WHERE Manager_Name Like '%Params%'
        ORDER BY Manager_Name
        --
        DELETE From TmpManagerList Where Manager_Name Like '%Params%'
    End If;

    If Exists (Select * FROM TmpManagerList Where Manager_Name IN (Select mgr_name From mc.t_mgrs)) Then
        SELECT DISTINCT 'Will not process managers with existing entries in mc.t_mgrs' AS Warning,
                        Manager_Name
        FROM TmpManagerList Src
        WHERE Manager_Name IN (Select mgr_name From mc.t_mgrs)
        ORDER BY Manager_Name
        --
        DELETE From TmpManagerList Where Manager_Name IN (Select mgr_name From mc.t_mgrs)
    End If;

    If Exists (Select * FROM TmpManagerList Where M_ID IN (Select Distinct mgr_id From mc.t_param_value)) Then
        SELECT DISTINCT 'Will not process managers with existing entries in mc.t_param_value' AS Warning,
                        Manager_Name
        FROM TmpManagerList Src
        WHERE M_ID IN (Select Distinct mgr_id From mc.t_param_value)
        ORDER BY Manager_Name
        --
        DELETE From TmpManagerList Where M_ID IN (Select Distinct mgr_id From mc.t_param_value)
    End If;

    If _infoOnly <> 0 Then
        SELECT Src.Manager_Name,
               _enableControlFromWebsite AS M_ControlFromWebsite,
               PV.M_TypeID,
               PV.ParamName,
               PV.Entry_ID,
               PV.TypeID,
               PV.[Value],
               PV.MgrID,
               PV.[Comment],
               PV.Last_Affected,
               PV.Entered_By
        FROM TmpManagerList Src
             LEFT OUTER JOIN V_OldParamValue PV
               ON PV.MgrID = Src.M_ID
        ORDER BY Src.Manager_Name, ParamName

    Else
        DELETE FROM TmpManagerList WHERE M_ID is Null

        Begin Tran _moveParams

        SET IDENTITY_INSERT mc.t_mgrs ON

        INSERT INTO mc.t_mgrs ( mgr_id,
                             mgr_name,
                             mgr_type_id,
                             param_value_changed,
                             control_from_website,
                             comment )
        SELECT M.mgr_id,
               M.mgr_name,
               M.mgr_type_id,
               M.param_value_changed,
               _enableControlFromWebsite,
               M.comment
        FROM mc.t_old_managers M
             INNER JOIN TmpManagerList Src
               ON M.mgr_id = Src.mgr_id
          LEFT OUTER JOIN mc.t_mgrs Target
           ON Src.mgr_id = Target.mgr_id
        WHERE Target.mgr_id IS NULL
        --
        GET DIAGNOSTICS _myRowCount = ROW_COUNT;
        SET IDENTITY_INSERT mc.t_mgrs Off
        --
        If _myError <> 0 Then
            Rollback
            Select 'Aborted (rollback) due to insert error for mc.t_mgrs' as Warning, _myError as ErrorCode
            Return;
        End If;

        SET IDENTITY_INSERT mc.t_param_value On

        INSERT INTO mc.t_param_value (
                 entry_id,
                 type_id,
                 "value",
                 mgr_id,
                 "comment",
                 last_affected,
                 entered_by )
        SELECT PV.entry_id,
               PV.type_id,
               PV."value",
               PV.mgr_id,
               PV."comment",
               PV.last_affected,
               PV.entered_by
        FROM mc.t_param_value_old_managers PV
             INNER JOIN TmpManagerList Src
               ON PV.mgr_id = Src.M_ID
        --
        GET DIAGNOSTICS _myRowCount = ROW_COUNT;
        SET IDENTITY_INSERT mc.t_param_value On
        --
        If _myError <> 0 Then
            Rollback
            Select 'Aborted (rollback) due to insert error for mc.t_param_value_old_managers' as Warning, _myError as ErrorCode
            Return;
        End If;

        DELETE mc.t_param_value_old_managers
        FROM mc.t_param_value_old_managers PV

        /********************************************************************************
        ** This DELETE query includes the target table name in the FROM clause
        ** The WHERE clause needs to have a self join to the target table, for example:
        **   UPDATE mc.t_param_value_old_managers
        **   SET ...
        **   FROM source
        **   WHERE source.id = mc.t_param_value_old_managers.id;
        **
        ** Delete queries must also include the USING keyword
        ** Alternatively, the more standard approach is to rearrange the query to be similar to
        **   DELETE FROM mc.t_param_value_old_managers WHERE id in (SELECT id from ...)
        ********************************************************************************/

                               ToDo: Fix this query

             INNER JOIN TmpManagerList Src
               ON PV.MgrID = Src.M_ID

        DELETE mc.t_old_managers
        FROM mc.t_old_managers M

        /********************************************************************************
        ** This DELETE query includes the target table name in the FROM clause
        ** The WHERE clause needs to have a self join to the target table, for example:
        **   UPDATE mc.t_old_managers
        **   SET ...
        **   FROM source
        **   WHERE source.id = mc.t_old_managers.id;
        **
        ** Delete queries must also include the USING keyword
        ** Alternatively, the more standard approach is to rearrange the query to be similar to
        **   DELETE FROM mc.t_old_managers WHERE id in (SELECT id from ...)
        ********************************************************************************/

                               ToDo: Fix this query

             INNER JOIN TmpManagerList Src
               ON M.M_ID = Src.M_ID

        Commit Tran _moveParams

        SELECT 'Moved to T_Managers and mc.t_param_value' as Message,
               Src.Manager_Name,
               _enableControlFromWebsite AS M_ControlFromWebsite,
               PT.param_name,
               PV.entry_id,
               PV.type_id,
               PV."value",
               PV.mgr_id,
               PV."comment",
               PV.last_affected,
               PV.entered_by
        FROM TmpManagerList Src
             LEFT OUTER JOIN mc.t_param_value PV
               ON PV.mgr_id = Src.M_ID
             LEFT OUTER JOIN mc.t_param_type PT ON
             PV.type_id = PT.param_id
        ORDER BY Src.Manager_Name, param_name
    End If;

Done:
    RETURN _myError
    DROP TABLE TmpManagerList

END
$$;

COMMENT ON PROCEDURE mc.unarchive_old_managers_and_params IS 'UnarchiveOldManagersAndParams';

CREATE OR REPLACE PROCEDURE mc.update_single_mgr_control_param
(
    _paramName text,
    _newValue text,
    _managerIDList text,
    _callingUser text = '',
    _infoOnly int = 0
)
LANGUAGE plpgsql
AS $$
/****************************************************
**
**  Desc:
**  Changes single manager params for set of given managers
**
**
**  Arguments:
**    _paramName       The parameter name
**    _newValue        The new value to assign for this parameter
**    _managerIDList   manager ID values (numbers, not manager names)
**
**  Auth:   jds
**  Date:   06/20/2007
**          07/31/2007 grk - changed for 'controlfromwebsite' no longer a parameter
**          04/16/2009 mem - Added optional parameter _callingUser; if provided, then UpdateSingleMgrParamWork will populate field Entered_By with this name
**          04/08/2011 mem - Will now add parameter _paramValue to managers that don't yet have the parameter defined
**          04/21/2011 mem - Expanded _managerIDList to varchar(8000)
**          05/11/2011 mem - Fixed bug reporting error resolving _paramValue to _paramTypeID
**          04/29/2015 mem - Now parsing _managerIDList using udfParseDelimitedIntegerList
**                         - Added parameter _infoOnly
**                         - Renamed the first parameter from _paramValue to _paramName
**          09/21/2022 mem - Ported to PostgreSQL
**
*****************************************************/
DECLARE
    _myRowCount int := 0;
    _paramTypeID int;
    _message text := '';
BEGIN
    ---------------------------------------------------
    -- Validate the inputs
    ---------------------------------------------------

    -- Assure that _newValue is not null
    _newValue := Coalesce(_newValue, '');
    _infoOnly := Coalesce(_infoOnly, 0);

    ---------------------------------------------------
    -- Create a temporary table that will hold the entry_id
    -- values that need to be updated in mc.t_param_value
    ---------------------------------------------------
    CREATE TEMP TABLE TmpParamValueEntriesToUpdate (
        EntryID int NOT NULL
    )

    CREATE UNIQUE CLUSTERED INDEX IX_TmpParamValueEntriesToUpdate ON TmpParamValueEntriesToUpdate (EntryID)

    CREATE TEMP TABLE TmpMgrIDs (
        MgrID text NOT NULL
    )

    ---------------------------------------------------
    -- Resolve _paramName to _paramTypeID
    ---------------------------------------------------

    _paramTypeID := -1;

    SELECT param_id INTO _paramTypeID
    FROM mc.t_param_type
    WHERE param_name = _paramName
    --
    GET DIAGNOSTICS _myRowCount = ROW_COUNT;

    If _myRowCount = 0 Then
        _message := 'Error: Parameter ''' || _paramName || ''' not found in mc.t_param_type';
        RAISERROR (_message, 10, 1)
        _message := '';
        return 51309
    End If;

    ---------------------------------------------------
    -- Parse the manager ID list
    ---------------------------------------------------
    --
    INSERT INTO TmpMgrIDs (MgrID)
    SELECT Cast(Value as text)
    FROM public.parse_delimited_integer_list ( _managerIDList, ',' )

    If _infoOnly <> 0 Then

        SELECT PV.entry_id,
               M.mgr_id,
               M.mgr_name,
               PV.ParamName,
               PV.type_id,
               PV."value",
               _newValue AS NewValue,
               Case When Coalesce(PV."value", '') <> _newValue Then 'Changed' Else 'Unchanged' End As Status
        FROM mc.t_mgrs M
             INNER JOIN TmpMgrIDs
               ON M.mgr_id = TmpMgrIDs.mgr_id
             INNER JOIN V_ParamValue PV
               ON PV.mgr_id = M.mgr_id AND
                  PV.type_id = _paramTypeID
        WHERE control_from_website > 0
        UNION
        SELECT NULL AS Entry_ID,
               M.mgr_id,
               M.mgr_name,
               _paramName,
               _paramTypeID,
               NULL AS "Value",
               _newValue AS NewValue,
               'New'
        FROM mc.t_mgrs M
             INNER JOIN TmpMgrIDs
               ON M.mgr_id = TmpMgrIDs.mgr_id
             LEFT OUTER JOIN mc.t_param_value PV
               ON PV.mgr_id = M.mgr_id AND
                  PV.type_id = _paramTypeID
        WHERE PV.type_id IS NULL

    Else

        ---------------------------------------------------
        -- Add new entries for Managers in _managerIDList that
        -- don't yet have an entry in mc.t_param_value for parameter _paramName
        --
        -- Adding value '##_DummyParamValue_##' so that
        --  we'll force a call to UpdateSingleMgrParamWork
        ---------------------------------------------------

        INSERT INTO mc.t_param_value( type_id,
                                  "value",
                                  mgr_id )
        SELECT _paramTypeID,
               '##_DummyParamValue_##',
               TmpMgrIDs.mgr_id
        FROM mc.t_mgrs M
             INNER JOIN TmpMgrIDs
               ON M.mgr_id = TmpMgrIDs.mgr_id
             LEFT OUTER JOIN mc.t_param_value PV
               ON PV.mgr_id = M.mgr_id AND
                  PV.type_id = _paramTypeID
        WHERE PV.type_id IS NULL
        --
        GET DIAGNOSTICS _myRowCount = ROW_COUNT;

        ---------------------------------------------------
        -- Find the entries for the Managers in _managerIDList
        -- Populate TmpParamValueEntriesToUpdate with the entries that need to be updated
        ---------------------------------------------------
        --
        INSERT INTO TmpParamValueEntriesToUpdate( EntryID )
        SELECT PV.entry_id
        FROM mc.t_param_value PV
             INNER JOIN mc.t_mgrs M
               ON PV.mgr_id = M.mgr_id
             INNER JOIN TmpMgrIDs
               ON M.mgr_id = TmpMgrIDs.mgr_id
        WHERE control_from_website > 0 AND
              PV.type_id = _paramTypeID AND
              Coalesce(PV."value", '') <> _newValue
        --
        GET DIAGNOSTICS _myRowCount = ROW_COUNT;
        --
        if _myError <> 0 Then
            RAISERROR ('Error finding Manager params to update', 10, 1)
            return 51309
        End If;

        ---------------------------------------------------
        -- Call UpdateSingleMgrParamWork to perform the update, then call
        -- public.alter_entered_by_user_multi_id and public.alter_event_log_entry_user_multi_id for _callingUser
        ---------------------------------------------------
        --
        Call update_single_mgr_param_work _paramName, _newValue, _callingUser

    End If;

    return _myError
    DROP TABLE TmpParamValueEntriesToUpdate
    DROP TABLE TmpMgrIDs

END
$$;

COMMENT ON PROCEDURE mc.update_single_mgr_control_param IS 'UpdateSingleMgrControlParam';

CREATE OR REPLACE PROCEDURE mc.update_single_mgr_param_work
(
    _paramName text,
    _newValue text,
    _callingUser text = ''
)
LANGUAGE plpgsql
AS $$
/****************************************************
**
**  Desc:
**  Changes single manager param for the EntryID values
**  defined in table TmpParamValueEntriesToUpdate (created by the calling procedure)
**
**  Example table creation code:
**    CREATE TABLE TmpParamValueEntriesToUpdate (EntryID int NOT NULL)
**
**  Arguments:
**    _paramName   The parameter name
**    _newValue    The new value to assign for this parameter
**
**  Auth:   mem
**  Date:   04/16/2009
**          09/21/2022 mem - Ported to PostgreSQL
**
*****************************************************/
DECLARE
    _myRowCount int := 0;
    _paramID int;
    _targetState int;
    _message text;
BEGIN
    _message := '';

    -- Validate that _paramName is not blank
    If Coalesce(_paramName, '') = '' Then
        _message := 'Parameter Name is empty or null';
        RAISERROR (_message, 10, 1)
        return 51315
    End If;

    -- Assure that _newValue is not null
    _newValue := Coalesce(_newValue, '');

    -- Lookup the ParamID for param _paramName
    _paramID := 0;
    SELECT param_id INTO _paramID
    FROM mc.t_param_type
    WHERE (param_name = _paramName)
    --
    GET DIAGNOSTICS _myRowCount = ROW_COUNT;

    If _myRowCount = 0 Then
        _message := 'Unknown Parameter Name: ' || _paramName;
        RAISERROR (_message, 10, 1)
        return 51316
    End If;

    ---------------------------------------------------
    -- Update the values defined in TmpParamValueEntriesToUpdate
    ---------------------------------------------------
    --
    UPDATE mc.t_param_value
    SET "value" = _newValue
    WHERE entry_id IN (SELECT EntryID FROM TmpParamValueEntriesToUpdate) AND
          Coalesce("value", '') <> _newValue
    --
    GET DIAGNOSTICS _myRowCount = ROW_COUNT;
    --
    if _myError <> 0 Then
        _message := 'Error trying to update Manager params';
        RAISERROR (_message, 10, 1)
        return 51310
    End If;

     If _myRowCount > 0 And char_length(_callingUser) > 0 Then
        -- _callingUser is defined
        -- Items need to be updated in mc.t_param_value and possibly in mc.t_event_log

        ---------------------------------------------------
        -- Create a temporary table that will hold the entry_id
        -- values that need to be updated in mc.t_param_value
        ---------------------------------------------------
        CREATE TEMP TABLE TmpIDUpdateList (
            TargetID int NOT NULL
        )

        CREATE UNIQUE CLUSTERED INDEX IX_TmpIDUpdateList ON TmpIDUpdateList (TargetID)

        -- Populate TmpIDUpdateList with entry_id values for mc.t_param_value, then call public.alter_entered_by_user_multi_id
        --
        INSERT INTO TmpIDUpdateList (TargetID)
        SELECT EntryID
        FROM TmpParamValueEntriesToUpdate

        Call alter_entered_by_user_multi_id 'mc.t_param_value', 'entry_id', _callingUser, _entryDateColumnName => 'last_affected'

        If _paramName = 'mgractive' or _paramID = 17 Then
            -- Triggers trig_i_T_ParamValue and trig_u_T_ParamValue make an entry in
            --  mc.t_event_log whenever mgractive (param TypeID = 17) is changed

            -- Call public.alter_event_log_entry_user_multi_id
            -- to alter the entered_by field in mc.t_event_log

            If _newValue = 'True' Then
                _targetState := 1;
            Else
                _targetState := 0;
            End If;

            -- Populate TmpIDUpdateList with Manager ID values, then call public.alter_event_log_entry_user_multi_id
            Truncate Table TmpIDUpdateList

            INSERT INTO TmpIDUpdateList (TargetID)
            SELECT mgr_id
            FROM mc.t_param_value
            WHERE entry_id IN (SELECT EntryID FROM TmpParamValueEntriesToUpdate)

            Call alter_event_log_entry_user_multi_id 1, _targetState, _callingUser
        End If;

    End If;

    Return _myError
    DROP TABLE TmpIDUpdateList

END
$$;

COMMENT ON PROCEDURE mc.update_single_mgr_param_work IS 'UpdateSingleMgrParamWork';

CREATE OR REPLACE PROCEDURE mc.update_single_mgr_type_control_param
(
    _paramValue text,
    _newValue text,
    _managerTypeIDList text,
    _callingUser text = ''
)
LANGUAGE plpgsql
AS $$
/****************************************************
**
**  Desc:
**  Changes single manager params for set of given manager Types
**
**
**  Arguments:
**    _paramValue   The parameter name
**    _newValue     The new value to assign for this parameter
**
**  Auth:   jds
**  Date:   07/17/2007
**          07/31/2007 grk - changed for 'controlfromwebsite' no longer a parameter
**          03/30/2009 mem - Added optional parameter _callingUser; if provided, then will call AlterEnteredByUserMultiID and possibly AlterEventLogEntryUserMultiID
**          04/16/2009 mem - Now calling UpdateSingleMgrParamWork to perform the updates
**          09/21/2022 mem - Ported to PostgreSQL
**
*****************************************************/
DECLARE
    _myRowCount int := 0;
BEGIN
    ---------------------------------------------------
    -- Create a temporary table that will hold the entry_id
    -- values that need to be updated in mc.t_param_value
    ---------------------------------------------------
    CREATE TEMP TABLE TmpParamValueEntriesToUpdate (
        EntryID int NOT NULL
    )

    CREATE UNIQUE CLUSTERED INDEX IX_TmpParamValueEntriesToUpdate ON TmpParamValueEntriesToUpdate (EntryID)

    ---------------------------------------------------
    -- Find the _paramValue entries for the Manager Types in _managerTypeIDList
    ---------------------------------------------------
    --
    INSERT INTO TmpParamValueEntriesToUpdate (EntryID)
    SELECT mc.t_param_value.entry_id
    FROM mc.t_param_value
         INNER JOIN mc.t_param_type
           ON dbo.t_param_value.type_id = dbo.t_param_type.param_id
         INNER JOIN mc.t_mgrs
           ON mgr_id = mgr_id
    WHERE param_name = _paramValue AND
          mgr_type_id IN ( SELECT Item
                        FROM public.parse_delimited_list ( _managerTypeIDList )
                      ) AND
          mgr_id IN ( SELECT mgr_id
                     FROM mc.t_mgrs
                     WHERE control_from_website > 0
                     )
    --
    GET DIAGNOSTICS _myRowCount = ROW_COUNT;
    --
    if _myError <> 0 Then
        RAISERROR ('Error finding Manager params to update', 10, 1)
        return 51309
    End If;

    ---------------------------------------------------
    -- Call UpdateSingleMgrParamWork to perform the update, then call
    -- public.alter_entered_by_user_multi_id and public.alter_event_log_entry_user_multi_id for _callingUser
    ---------------------------------------------------
    --
    Call update_single_mgr_param_work _paramValue, _newValue, _callingUser

    return _myError
    DROP TABLE TmpParamValueEntriesToUpdate

END
$$;

COMMENT ON PROCEDURE mc.update_single_mgr_type_control_param IS 'UpdateSingleMgrTypeControlParam';
