
CREATE OR REPLACE PROCEDURE mc.ack_manager_update_required
(
    _managerName text,
    INOUT _message text = '',
    INOUT _returnCode text = ''
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
**          01/24/2020 mem - Ported to PostgreSQL
**          01/26/2020 mem - Add exception handler
**          01/29/2020 mem - Log errors to PostLogEntry
**
*****************************************************/
DECLARE
    _myRowCount int;
    _mgrID int;
    _paramID int;
    _sqlstate text;
    _exceptionMessage text;
    _exceptionContext text;
BEGIN
    _myRowCount := 0;

    _managerName := Trim(Coalesce(_managerName, ''));
    If (char_length(_managerName) = 0) Then
        _managerName := '??Undefined_Manager??';
    End If;

    _message := '';
    _returnCode := '';

    ---------------------------------------------------
    -- Confirm that the manager name is valid
    ---------------------------------------------------

    SELECT m_id INTO _mgrID
    FROM mc.t_mgrs
    WHERE m_name = _managerName::citext;

    IF NOT FOUND THEN
        _message := 'Could not find entry for manager: ' || _managername;
        _returnCode := 'U5202';
        Return;
    End If;

    ---------------------------------------------------
    -- Update the 'ManagerUpdateRequired' entry for this manager
    ---------------------------------------------------

    UPDATE mc.t_param_value PV
    SET value = 'False'
    FROM mc.t_param_type PT
    WHERE PT.param_id = PV.type_id AND
          PT.param_name = 'ManagerUpdateRequired' AND
          PV.mgr_id = _mgrID AND
          PV.value <> 'False';
    --
    GET DIAGNOSTICS _myRowCount = ROW_COUNT;

    If _myRowCount > 0 Then
        _message := 'Acknowledged that update is required';
    Else
        -- No rows were updated; may need to make a new entry for 'ManagerUpdateRequired' in the t_param_value table

        SELECT param_id INTO _paramID
        FROM mc.t_param_type
        WHERE param_name = 'ManagerUpdateRequired';

        IF FOUND THEN
            If Exists (SELECT * FROM mc.t_param_value WHERE mgr_id = _mgrID AND type_id = _paramID) Then
                _message := 'ManagerUpdateRequired was already acknowledged in t_param_value';
            Else
                INSERT INTO mc.t_param_value (mgr_id, type_id, value)
                VALUES (_mgrID, _paramID, 'False');

                _message := 'Acknowledged that update is required (added new entry to t_param_value)';
            End If;
        End If;
    End If;

EXCEPTION
    WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS
            _sqlstate = returned_sqlstate,
            _exceptionMessage = message_text,
            _exceptionContext = pg_exception_context;

    _message := 'Error updating ManagerUpdateRequired for ' || _managerName || ': ' || _exceptionMessage;
    _returnCode := _sqlstate;

    RAISE Warning 'Error: %', _message;
    RAISE warning '%', _exceptionContext;

    Call PostLogEntry ('Error', _message, 'AckManagerUpdateRequired', 'mc');

END
$$;


COMMENT ON PROCEDURE mc.ack_manager_update_required  IS 'AckManagerUpdateRequired';

CREATE OR REPLACE PROCEDURE public.AlterEnteredByUser
(
    _targetTableSchema text,
    _targetTableName text,
    _targetIDColumnName text,
    _targetID int,
    _newUser text,
    _applyTimeFilter int = 1,
    _entryTimeWindowSeconds int = 15,
    _entryDateColumnName text = 'entered',
    _enteredByColumnName text = 'entered_by',
    INOUT _message text = '',
    _infoOnly int = 0,
    _previewSql int = 0
)
LANGUAGE plpgsql
AS $_$
/****************************************************
**
**  Desc:
**      Updates the entered_by column for the specified row in the given table to contain _newUser
**
**  Arguments:
**    _targetTableSchema        Schema of the table to update; if empty or null, assumes "public"
**    _targetTableName          Table to update
**    _targetIDColumnName       ID column name
**    _targetID                 ID of the entry to update
**    _newUser                  New username to add to the entered_by field
**    _applyTimeFilter          If 1, filters by the current date and time; if 0, looks for the most recent matching entry
**    _entryTimeWindowSeconds   Only used if _applyTimeFilter = 1
**    _entryDateColumnName      Column name to use when _applyTimeFilter is non-zero
**    _enteredByColumnName      Column name to update the username
**    _message                  Warning or status message
**    _infoOnly                 If 1, preview updates
**    _previewSql               If 1, show the SQL that would be used
**
**  Auth:   mem
**  Date:   03/25/2008 mem - Initial version (Ticket: #644)
**          05/23/2008 mem - Expanded @EntryDescription to varchar(512)
**          01/25/2020 mem - Ported to PostgreSQL
**          01/28/2020 mem - Add argument _targetTableSchema
**                         - Remove exception handler and remove argument _returnCode
**
*****************************************************/
DECLARE
    _myRowCount int := 0;
    _entryDateStart timestamp;
    _entryDateEnd timestamp;
    _entryDescription text := '';
    _entryIndex int;
    _matchIndex int;
    _enteredBy text;
    _targetIDMatch int;
    _enteredByNew text := '';
    _currentTime timestamp := CURRENT_TIMESTAMP;
    _s text;
    _entryDateFilterSqlWithVariables text := '';
    _entryDateFilterSqlWithValues text := '';
    _lookupResults record;
BEGIN

    ------------------------------------------------
    -- Validate the inputs
    ------------------------------------------------

    _targetTableSchema := COALESCE(_targetTableSchema, '');
    If (char_length(_targetTableSchema) = 0) Then
        _targetTableSchema := 'public';
    End If;

    _newUser := Coalesce(_newUser, '');
    _applyTimeFilter := Coalesce(_applyTimeFilter, 0);
    _entryTimeWindowSeconds := Coalesce(_entryTimeWindowSeconds, 15);
    _message := '';
    _infoOnly := Coalesce(_infoOnly, 0);
    _previewSql := Coalesce(_previewSql, 0);

    If _targetTableName Is Null Or _targetIDColumnName Is Null Or _targetID Is Null Then
        _message := '_targetTableName and _targetIDColumnName and _targetID must be defined; unable to continue';
        RAISE EXCEPTION '%', _message;
    End If;

    If char_length(_newUser) = 0 Then
        _message := '_newUser is empty; unable to continue';
        RAISE EXCEPTION '%', _message;
    End If;

    _entryDescription := 'ID ' || _targetID::text || ' in table ' || _targetTableName || ' (column ' || _targetIDColumnName || ')';

    _s := format(
            'SELECT %I as target_id_match, %I as entered_by '
            'FROM %I.%I '
            'WHERE %I = $1',
            _targetIDColumnName, _enteredByColumnName,
            _targetTableSchema, _targetTableName,
            _targetIDColumnName);

    If _applyTimeFilter <> 0 And _entryTimeWindowSeconds >= 1 Then
        ------------------------------------------------
        -- Filter using the current date/time
        ------------------------------------------------
        --
        _entryDateStart := _currentTime - (_entryTimeWindowSeconds || ' seconds')::INTERVAL;
        _entryDateEnd   := _currentTime + INTERVAL '1 second';

        If _infoOnly <> 0 Then
            RAISE INFO 'Filtering on entries dated between % and % (Window = % seconds)',
                to_char(_entryDateStart, 'yyyy-mm-dd hh24:mi:ss'),
                to_char(_entryDateEnd,   'yyyy-mm-dd hh24:mi:ss'),
                _entryTimeWindowSeconds;
        End If;

        _entryDateFilterSqlWithValues := format(
                            '%I between ''%s'' And ''%s''',
                             _entryDateColumnName,
                            to_char(_entryDateStart, 'yyyy-mm-dd hh24:mi:ss'),
                            to_char(_entryDateEnd,   'yyyy-mm-dd hh24:mi:ss'));

        _entryDateFilterSqlWithVariables := format(
                            '%I between $2 And $3',
                             _entryDateColumnName);

        If _previewSql <> 0 Then
            _s := _s || ' AND ' || _entryDateFilterSqlWithValues;
        Else
            _s := _s || ' AND ' || _entryDateFilterSqlWithVariables;
        End If;

        _entryDescription := _entryDescription || ' with ' || _entryDateFilterSqlWithValues;
    End If;

    If _previewSql <> 0 Then
        -- Show the SQL both with the dollar signs, and with values
        RAISE INFO '%;', _s;
        RAISE INFO '%;', regexp_replace(_s, '\$1', _targetID::text);

        _enteredBy := session_user || '_simulated';
        _targetIDMatch := _targetID;
    Else
        EXECUTE _s INTO _lookupResults USING _targetID, _entryDateStart, _entryDateEnd;
        _enteredBy := _lookupResults.entered_by;
        _targetIDMatch := _lookupResults.target_id_match;
    End If;
    --
    GET DIAGNOSTICS _myRowCount = ROW_COUNT;

    If _previewSql = 0 AND (_myRowCount <= 0 Or _targetIDMatch <> _targetID) Then
        _message := 'Match not found for ' || _entryDescription;
        Return;
    End If;

    -- Confirm that _enteredBy doesn't already contain _newUser
    -- If it does, there's no need to update it

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

    If char_length(Coalesce(_enteredByNew, '')) = 0 THEN
        _message := 'Match not found; unable to continue';
    End If;

    If _infoOnly = 0 Then

        _s := format(
                'UPDATE %I.%I '
                'SET %I = $4 '
                'WHERE %I = $1',
                _targetTableSchema, _targetTableName,
                _enteredByColumnName,
                _targetIDColumnName);

        If char_length(_entryDateFilterSqlWithVariables) > 0 Then
            If _previewSql <> 0 Then
                _s := _s || ' AND ' || _entryDateFilterSqlWithValues;
            Else
                _s := _s || ' AND ' || _entryDateFilterSqlWithVariables;
            End If;
        End If;

        If _previewSql <> 0 Then
            -- Show the SQL both with the dollar signs, and with values
            RAISE INFO '%;', _s;
            _s := regexp_replace(_s, '\$1', _targetID::text);
            _s := regexp_replace(_s, '\$4', '''' || _enteredByNew || '''');
            RAISE INFO '%;', _s;
        Else
            EXECUTE _s USING _targetID, _entryDateStart, _entryDateEnd, _enteredByNew;
        End If;
        --
        GET DIAGNOSTICS _myRowCount = ROW_COUNT;

        If _previewSql = 0 Then
            _message := 'Updated ';
        Else
            _message := 'SQL previewed for updating ';
        End If;

        _message := _message || _entryDescription || ' to indicate "' || _enteredByNew || '"';

    Else
        _s := format(
                'SELECT *, ''' || _enteredByNew || ''' AS Entered_By_New '
                'FROM %I.%I '
                'WHERE %I = $1',
                _targetTableSchema, _targetTableName,
                _targetIDColumnName);

        If char_length(_entryDateFilterSqlWithVariables) > 0 Then
            If _previewSql <> 0 Then
                _s := _s || ' AND ' || _entryDateFilterSqlWithValues;
            Else
                _s := _s || ' AND ' || _entryDateFilterSqlWithVariables;
            End If;
        End If;

        If _previewSql <> 0 Then
            -- Show the SQL both with the dollar signs, and with values
            RAISE INFO '%;', _s;
            RAISE INFO '%;', regexp_replace(_s, '\$1', _targetID::text);
        Else
            Execute _s USING _targetID, _entryDateStart, _entryDateEnd;
        End If;
        --
        GET DIAGNOSTICS _myRowCount = ROW_COUNT;

        _message := 'Would update ' || _entryDescription || ' to indicate "' || _enteredByNew || '"';
    End If;

END
$_$;

COMMENT ON PROCEDURE public.AlterEnteredByUser IS 'AlterEnteredByUser';

CREATE OR REPLACE PROCEDURE public.AlterEnteredByUserMultiID
(
    _targetTableSchema text,
    _targetTableName text,
    _targetIDColumnName text,
    _newUser text,
    _applyTimeFilter int = 1,
    _entryTimeWindowSeconds int = 15,
    _entryDateColumnName text = 'entered',
    _enteredByColumnName text = 'entered_by',
    INOUT _message text = '',
    _infoOnly int = 0,
    _previewSql int = 0
)
LANGUAGE plpgsql
AS $$
/****************************************************
**
**  Desc:
**      Calls AlterEnteredByUser for each entry in temporary table TmpIDUpdateList
**
**      The calling procedure must create and populate the temporary table:
**        CREATE TEMP TABLE TmpIDUpdateList (TargetID int NOT NULL);
**
**      Increased performance can be obtained by adding an index to the table;
**      thus it is advisable that the calling procedure also create this index:
**        CREATE INDEX IX_TmpIDUpdateList ON TmpIDUpdateList (TargetID);
**
**  Arguments:
**    _targetTableSchema        Schema of the table to update; if empty or null, assumes "public"
**    _targetTableName          Table to update
**    _targetIDColumnName       ID column name
**    _newUser                  New username to add to the entered_by field
**    _applyTimeFilter          If 1, filters by the current date and time; if 0, looks for the most recent matching entry
**    _entryTimeWindowSeconds   Only used if _applyTimeFilter = 1
**    _entryDateColumnName      Column name to use when _applyTimeFilter is non-zero
**    _enteredByColumnName      Column name to update the username
**    _message                  Warning or status message
**    _infoOnly                 If 1, preview updates
**    _previewSql               If 1, show the SQL that would be used
**
**  Auth:   mem
**  Date:   03/28/2008 mem - Initial version (Ticket: #644)
**          05/23/2008 mem - Expanded @EntryDescription to varchar(512)
**          01/26/2020 mem - Ported to PostgreSQL
**          01/28/2020 mem - Add argument _targetTableSchema
**                         - Remove exception handler and remove argument _returnCode
**
*****************************************************/
DECLARE
    _myRowCount int := 0;
    _entryDateStart timestamp;
    _entryDateEnd timestamp;
    _entryIndex int;
    _matchIndex int;
    _enteredBy text;
    _targetID int;
    _enteredByNew text := '';
    _currentTime timestamp := CURRENT_TIMESTAMP;
    _countUpdated int;
    _startTime timestamp;
    _entryTimeWindowSecondsCurrent int;
    _elapsedSeconds int;
BEGIN

    ------------------------------------------------
    -- Validate the inputs
    ------------------------------------------------

     _targetTableSchema := COALESCE(_targetTableSchema, '');
    If (char_length(_targetTableSchema) = 0) Then
        _targetTableSchema := 'public';
    End If;

    _newUser := Coalesce(_newUser, '');
    _applyTimeFilter := Coalesce(_applyTimeFilter, 0);
    _entryTimeWindowSeconds := Coalesce(_entryTimeWindowSeconds, 15);
    _message := '';
    _infoOnly := Coalesce(_infoOnly, 0);
    _previewSql := Coalesce(_previewSql, 0);

    If _targetTableName Is Null Or _targetIDColumnName Is Null Then
        _message := '_targetTableName and _targetIDColumnName must be defined; unable to continue';
        RAISE EXCEPTION '%', _message;
    End If;

    If char_length(_newUser) = 0 Then
        _message := '_newUser is empty; unable to continue';
        RAISE EXCEPTION '%', _message;
    End If;

    -- Make sure TmpIDUpdateList is not empty

    If Not Exists (Select * From TmpIDUpdateList) Then
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
    -- Parse the values in TmpIDUpdateList
    -- Call AlterEnteredByUser for each
    ------------------------------------------------

    _countUpdated := 0;

    FOR _targetID IN
        SELECT TargetID
        FROM TmpIDUpdateList
        ORDER BY TargetID
    LOOP
        Call AlterEnteredByUser(
                            _targetTableSchema,
                            _targetTableName,
                            _targetIDColumnName,
                            _targetID,
                            _newUser,
                            _applyTimeFilter,
                            _entryTimeWindowSecondsCurrent,
                            _entryDateColumnName,
                            _enteredByColumnName,
                            _message,
                            _infoOnly,
                            _previewSql
                            );

        _countUpdated := _countUpdated + 1;
        If _countUpdated % 5 = 0 Then
            _elapsedSeconds := extract(epoch FROM (current_timestamp - _startTime));

            If _elapsedSeconds * 2 > _entryTimeWindowSecondsCurrent Then
                _entryTimeWindowSecondsCurrent := _elapsedSeconds * 4;
            End If;
        End If;
    End Loop;

END
$$
;

COMMENT ON PROCEDURE public.AlterEnteredByUserMultiID IS 'AlterEnteredByUserMultiID';


CREATE OR REPLACE PROCEDURE public.AlterEventLogEntryUser
(
    _eventlogschema text,
    _targetType int,
    _targetID int,
    _targetState int,
    _newUser text,
    _applyTimeFilter int = 1,
    _entryTimeWindowSeconds int = 15,
    INOUT _message text = '',
    _infoOnly int = 0,
    _previewsql int = 0
)
LANGUAGE plpgsql
AS $_$
/****************************************************
**
**  Desc:
**      Updates the user associated with a given event log entry to be _newUser
**
**  Arguments:
**    _eventLogSchema           Schema of the t_event_log table to update; if empty or null, assumes "public"
**    _targetType               Event type; 1=Manager Enable/Disable
**    _targetID                 ID of the entry to update
**    _targetState              Logged state value to match
**    _newUser                  New username to add to the entered_by field
**    _applyTimeFilter          If 1, filters by the current date and time; if 0, looks for the most recent matching entry
**    _entryTimeWindowSeconds   Only used if _applyTimeFilter = 1
**    _message                  Warning or status message
**    _infoOnly                 If 1, preview updates
**    _previewSql               If 1, show the SQL that would be used
**
**  Auth:   mem
**  Date:   02/29/2008 mem - Initial version (Ticket: #644)
**          05/23/2008 mem - Expanded @EntryDescription to varchar(512)
**          03/30/2009 mem - Ported to the Manager Control DB
**          01/26/2020 mem - Ported to PostgreSQL
**          01/28/2020 mem - Add arguments _eventLogSchema and _previewsql
**                         - Remove exception handler and remove argument _returnCode
**
*****************************************************/
DECLARE
    _myRowCount int := 0;
    _entryDateStart timestamp;
    _entryDateEnd timestamp;
    _entryDescription text := '';
    _eventID int;
    _targetIdMatched int;
    _matchIndex int;
    _enteredBy text;
    _enteredByNew text := '';
    _currentTime timestamp := CURRENT_TIMESTAMP;
    _s text;
    _entryDateFilterSqlWithVariables text := '';
    _entryDateFilterSqlWithValues text := '';
    _dateFilterSql text := '';
    _lookupResults record;
    _previewData record;
    _infoHead text;
    _infoData text;
BEGIN

    ------------------------------------------------
    -- Validate the inputs
    ------------------------------------------------

    _eventLogSchema := COALESCE(_eventLogSchema, '');
    If (char_length(_eventLogSchema) = 0) Then
        _eventLogSchema := 'public';
    End If;

    _newUser := Coalesce(_newUser, '');
    _applyTimeFilter := Coalesce(_applyTimeFilter, 0);
    _entryTimeWindowSeconds := Coalesce(_entryTimeWindowSeconds, 15);
    _message := '';
    _infoOnly := Coalesce(_infoOnly, 0);
    _previewsql := Coalesce(_previewSql, 0);

    If _targetType Is Null Or _targetID Is Null Or _targetState Is Null Then
        _message := '_targetType and _targetID and _targetState must be defined; unable to continue';
        RAISE EXCEPTION '%', _message;
    End If;

    If char_length(_newUser) = 0 Then
        _message := '_newUser is empty; unable to continue';
        RAISE EXCEPTION '%', _message;
    End If;

    _entryDescription := 'ID ' || _targetID::text || ' (type ' || _targetType::text || ') with state ' || _targetState::text;
    If _applyTimeFilter <> 0 And Coalesce(_entryTimeWindowSeconds, 0) >= 1 Then
        ------------------------------------------------
        -- Filter using the current date/time
        ------------------------------------------------
        --
        _entryDateStart := _currentTime - (_entryTimeWindowSeconds || ' seconds')::INTERVAL;
        _entryDateEnd   := _currentTime + INTERVAL '1 second';

        If _infoOnly <> 0 Then
            RAISE INFO 'Filtering on entries dated between % and % (Window = % seconds)',
                to_char(_entryDateStart, 'yyyy-mm-dd hh24:mi:ss'),
                to_char(_entryDateEnd,   'yyyy-mm-dd hh24:mi:ss'),
                _entryTimeWindowSeconds;
        End If;

        _entryDateFilterSqlWithValues := format(' AND entered BETWEEN ''%s'' AND ''%s''',
                                        to_char(_entryDateStart, 'yyyy-mm-dd hh24:mi:ss'),
                                        to_char(_entryDateEnd,   'yyyy-mm-dd hh24:mi:ss'));

        _entryDateFilterSqlWithVariables := ' AND entered BETWEEN $4 AND $5';

        If _previewSql <> 0 Then
            _dateFilterSql :=  _entryDateFilterSqlWithValues;
        Else
            _dateFilterSql :=  _entryDateFilterSqlWithVariables;
        End If;

        _entryDescription := _entryDescription ||
                                ' and Entry Time between ' ||
                                to_char(_entryDateStart, 'yyyy-mm-dd hh24:mi:ss') || ' and ' ||
                                to_char(_entryDateEnd,   'yyyy-mm-dd hh24:mi:ss');
    Else
        _dateFilterSql := '';
    End If;

    _s := format(
            'SELECT EL.event_id, EL.entered_by, EL.target_id '
            'FROM %1$I.t_event_log EL INNER JOIN '
                   ' (SELECT MAX(event_id) AS event_id '
                   '  FROM %1$I.t_event_log '
                   '  WHERE target_type = $1 AND '
                   '        target_id = $2 AND '
                   '        target_state = $3'
                   '        %s'
                   ' ) LookupQ ON EL.event_id = LookupQ.event_id',
            _eventLogSchema,
            _dateFilterSql);

    If _previewSql <> 0 Then
         -- Show the SQL both with the dollar signs, and with values
        RAISE INFO '%;', _s;
        _s := regexp_replace(_s, '\$1', _targetType::text);
        _s := regexp_replace(_s, '\$2', _targetID::text);
        _s := regexp_replace(_s, '\$3', _targetState::text);
        RAISE INFO '%;', _s;

        _eventID   := 0;
        _enteredBy := session_user || '_simulated';
        _targetIdMatched := _targetId;
    Else
        EXECUTE _s INTO _lookupResults USING _targetType, _targetID, _targetState, _entryDateStart, _entryDateEnd;
        _eventID   := _lookupResults.event_id;
        _enteredBy := _lookupResults.entered_by;
        _targetIdMatched := _lookupResults.target_id;
    End If;
    --
    GET DIAGNOSTICS _myRowCount = ROW_COUNT;

    If _previewSql = 0 AND (_myRowCount <= 0 Or _targetIdMatched <> _targetID) Then
        _message := 'Match not found for ' || _entryDescription;
        Return;
    End If;

    -- Confirm that _enteredBy doesn't already contain _newUser
    -- If it does, there's no need to update it

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

    If char_length(Coalesce(_enteredByNew, '')) = 0 Then
        _message := 'Match not found; unable to continue';
        RETURN;
    End If;

    If _infoOnly = 0 Then
        _s := format(
                        'UPDATE %I.t_event_log '
                        'SET entered_by = $2 '
                        'WHERE event_id = $1',
                        _eventLogSchema,
                        _enteredByNew);

        If _previewSql <> 0 Then
             -- Show the SQL both with the dollar signs, and with values
            RAISE INFO '%;', _s;
            _s := regexp_replace(_s, '\$1', _eventID::text);
            _s := regexp_replace(_s, '\$2', _enteredByNew);
            RAISE INFO '%;', _s;

            _message := 'Would update ' || _entryDescription || ' to indicate "' || _enteredByNew || '"';
        Else
            EXECUTE _s USING _eventID, _enteredByNew;
            GET DIAGNOSTICS _myRowCount = ROW_COUNT;

            _message := 'Updated ' || _entryDescription || ' to indicate "' || _enteredByNew || '"';
        End If;

        RETURN;
    End If;

    _s := format(
            'SELECT event_id, target_type, target_id, target_state,'
            '       prev_target_state, entered,'
            '       entered_by AS Entered_By_Old,'
            '       $2 AS Entered_By_New '
            'FROM %I.t_event_log '
            'WHERE event_id = $1',
            _eventLogSchema);

    EXECUTE _s INTO _previewData USING _eventID, _enteredByNew;
    --
    GET DIAGNOSTICS _myRowCount = ROW_COUNT;

    _infoHead := format('%-10s %-12s %-10s %-12s %-18s %-20s %-20s %-20s',
                            'event_id',
                            'target_type',
                            'target_id',
                            'target_state',
                            'prev_target_state',
                            'entered',
                            'entered_by_old',
                            'entered_by_new'
                        );

    _infoData := format('%-10s %-12s %-10s %-12s %-18s %-20s %-20s %-20s',
                            _previewData.event_id,
                            _previewData.target_type,
                            _previewData.target_id,
                            _previewData.target_state,
                            _previewData.prev_target_state,
                            to_char(_previewData.entered, 'yyyy-mm-dd hh24:mi:ss'),
                            _previewData.Entered_By_Old,
                            _previewData.Entered_By_New
                        );

    RAISE INFO '%', _infoHead;
    RAISE INFO '%', _infoData;

    _message := 'Would update ' || _entryDescription || ' to indicate "' || _enteredByNew || '"';

END
$_$;
;

COMMENT ON PROCEDURE public.AlterEventLogEntryUser IS 'AlterEventLogEntryUser';


CREATE OR REPLACE PROCEDURE public.AlterEventLogEntryUserMultiID
(
    _eventlogschema text,
    _targetType int,
    _targetState int,
    _newUser text,
    _applyTimeFilter int = 1,
    _entryTimeWindowSeconds int = 15,
    INOUT _message text = '',
    INOUT _returnCode text = '',
    _infoOnly int = 0,
    _previewsql int = 0
)
LANGUAGE plpgsql
AS $$
/****************************************************
**
**  Desc:
**      Calls AlterEventLogEntryUser for each entry in temporary table TmpIDUpdateList
**      Updates the user associated with the given event log entries to be _newUser
**
**      The calling procedure must create and populate the temporary table:
**        CREATE TEMP TABLE TmpIDUpdateList (TargetID int NOT NULL);
**
**      Increased performance can be obtained by adding an index to the table;
**      thus it is advisable that the calling procedure also create this index:
**        CREATE INDEX IX_TmpIDUpdateList ON TmpIDUpdateList (TargetID);
**
**  Arguments:
**    _eventLogSchema           Schema of the t_event_log table to update; if empty or null, assumes "public"
**    _targetType               Event type; 1=Manager Enable/Disable
**    _targetState              Logged state value to match
**    _newUser                  New username to add to the entered_by field
**    _applyTimeFilter          If 1, filters by the current date and time; if 0, looks for the most recent matching entry
**    _entryTimeWindowSeconds   Only used if _applyTimeFilter = 1
**    _message                  Warning or status message
**    _infoOnly                 If 1, preview updates
**    _previewSql               If 1, show the SQL that would be used
**
**  Auth:   mem
**  Date:   02/29/2008 mem - Initial version (Ticket: #644)
**          05/23/2008 mem - Expanded @EntryDescription to varchar(512)
**          03/30/2009 mem - Ported to the Manager Control DB
**          01/26/2020 mem - Ported to PostgreSQL
**          01/28/2020 mem - Add arguments _eventLogSchema and _previewsql
**                         - Remove exception handler and remove argument _returnCode
**
*****************************************************/
DECLARE
    _myRowCount int := 0;
    _targetID int;
    _countUpdated int;
    _continue int;
    _startTime timestamp;
    _entryTimeWindowSecondsCurrent int;
    _elapsedSeconds int;
    _sqlstate text;
    _exceptionMessage text;
    _exceptionContext text;
BEGIN

    ------------------------------------------------
    -- Validate the inputs
    ------------------------------------------------

    _eventLogSchema := COALESCE(_eventLogSchema, '');
    If (char_length(_eventLogSchema) = 0) Then
        _eventLogSchema := 'public';
    End If;

    _newUser := Coalesce(_newUser, '');
    _applyTimeFilter := Coalesce(_applyTimeFilter, 0);
    _entryTimeWindowSeconds := Coalesce(_entryTimeWindowSeconds, 15);
    _message := '';
    _infoOnly := Coalesce(_infoOnly, 0);
    _previewsql := Coalesce(_previewSql, 0);

    If _targetType Is Null Or _targetState Is Null Then
        _message := '_targetType and _targetState must be defined; unable to continue';
        RAISE EXCEPTION '%', _message;
    End If;

    If char_length(_newUser) = 0 Then
        _message := '_newUser is empty; unable to continue';
        RAISE EXCEPTION '%', _message;
    End If;

    -- Make sure TmpIDUpdateList is not empty

    If Not Exists (Select * From TmpIDUpdateList) Then
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
    -- Parse the values in TmpIDUpdateList
    -- Call AlterEventLogEntryUser for each
    ------------------------------------------------

    _countUpdated := 0;

    For _targetID In
        SELECT TargetID
        FROM TmpIDUpdateList
        ORDER BY TargetID
    Loop
        Call AlterEventLogEntryUser(
                            _eventlogschema,
                            _targetType,
                            _targetID,
                            _targetState,
                            _newUser,
                            _applyTimeFilter,
                            _entryTimeWindowSeconds,
                            _message,
                            _infoOnly,
                            _previewsql);

        _countUpdated := _countUpdated + 1;
        If _countUpdated % 5 = 0 Then
            _elapsedSeconds := extract(epoch FROM (current_timestamp - _startTime));

            If _elapsedSeconds * 2 > _entryTimeWindowSecondsCurrent Then
                _entryTimeWindowSecondsCurrent := _elapsedSeconds * 4;
            End If;
        End If;
    End Loop;

END
$$;

COMMENT ON PROCEDURE public.AlterEventLogEntryUserMultiID IS 'AlterEventLogEntryUserMultiID';

CREATE OR REPLACE FUNCTION mc.archive_old_managers_and_params
(
    _mgrlist text,
    _infoonly integer DEFAULT 1)
RETURNS TABLE
(
    message text,
    mgr_name citext,
    control_from_website smallint,
    manager_type_id integer,
    param_name citext,
    entry_id integer,
    param_type_id integer,
    param_value citext,
    mgr_id integer,
    comment citext,
    last_affected timestamp without time zone,
    entered_by citext
)  LANGUAGE plpgsql AS $$
/****************************************************
**
**  Desc:
**      Moves managers from mc.t_mgrs to mc.t_old_managers and
**      moves manager parameters from mc.t_param_value to mc.t_param_value_old_managers
**
**      To reverse this process, use Function mc.UnarchiveOldManagersAndParams
**      Select * from mc.UnarchiveOldManagersAndParams('Pub-10-1', _infoOnly := 1, _enableControlFromWebsite := 0)
**
**  Arguments:
**    _mgrList   One or more manager names (comma-separated list); supports wildcards because uses stored procedure ParseManagerNameList
**
**  Auth:   mem
**  Date:   05/14/2015 mem - Initial version
**          02/25/2016 mem - Add Set XACT_ABORT On
**          04/22/2016 mem - Now updating M_Comment in mc.t_old_managers
**          01/29/2020 mem - Ported to PostgreSQL
**          02/04/2020 mem - Rename columns to mgr_id and mgr_name
**
*****************************************************/
DECLARE
    _myRowCount int := 0;
    _message text;
    _sqlstate text;
    _exceptionMessage text;
    _exceptionContext text;
BEGIN

    ---------------------------------------------------
    -- Validate the inputs
    ---------------------------------------------------
    --
    _mgrList := Coalesce(_mgrList, '');
    _infoOnly := Coalesce(_infoOnly, 1);

    DROP TABLE IF EXISTS TmpManagerList;
    DROP TABLE IF EXISTS TmpWarningMessages;

    CREATE TEMP TABLE TmpManagerList (
        manager_name citext NOT NULL,
        mgr_id int NULL,
        control_from_web smallint null
    );

    CREATE TEMP TABLE TmpWarningMessages (
        entry_id int PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
        message text,
        manager_name citext
    );

    ---------------------------------------------------
    -- Populate TmpManagerList with the managers in _mgrList
    -- Using _removeUnknownManagers so that this procedure can be called repeatedly without raising an error
    ---------------------------------------------------
    --
    Call mc.parse_manager_name_list (_mgrList, _removeUnknownManagers => 0, _message =>_message);

    If Not Exists (Select * from TmpManagerList) Then
        _message := '_mgrList did not match any managers in mc.t_mgrs: ';
        Raise Info 'Warning: %', _message;

        RETURN QUERY
        SELECT '_mgrList did not match any managers in mc.t_mgrs' as Message,
               _mgrList::citext as manager_name,
               0::smallint as control_from_website,
               0 as manager_type_id,
               ''::citext as param_name,
               0 as entry_id,
               0 as type_id,
               ''::citext as value,
               0 as mgr_id,
               ''::citext as comment,
               current_timestamp::timestamp as last_affected,
               ''::citext as entered_by;
        RETURN;
    End If;

    ---------------------------------------------------
    -- Validate the manager names
    ---------------------------------------------------

    UPDATE TmpManagerList
    SET mgr_id = M.mgr_id,
        control_from_web = M.control_from_website
    FROM mc.t_mgrs M
    WHERE TmpManagerList.Manager_Name = M.mgr_name;

    If Exists (Select * from TmpManagerList MgrList WHERE MgrList.mgr_id Is Null) Then
        INSERT INTO TmpWarningMessages (message, manager_name)
        SELECT 'Unknown manager (not in mc.t_mgrs)',
               MgrList.manager_name
        FROM TmpManagerList MgrList
        WHERE MgrList.mgr_id Is Null
        ORDER BY MgrList.manager_name;
    End If;

    If Exists (Select * from TmpManagerList MgrList WHERE NOT MgrList.mgr_id is Null And MgrList.control_from_web > 0) Then
        INSERT INTO TmpWarningMessages (message, manager_name)
        SELECT 'Manager has control_from_website=1; cannot archive',
               MgrList.manager_name
        FROM TmpManagerList  MgrList
        WHERE NOT MgrList.mgr_id IS NULL And MgrList.control_from_web > 0
        ORDER BY MgrList.manager_name;

        DELETE FROM TmpManagerList
        WHERE manager_name IN (SELECT WarnMsgs.manager_name FROM TmpWarningMessages WarnMsgs WHERE NOT WarnMsgs.message ILIKE 'Note:%');
    End If;

    If Exists (Select * From TmpManagerList Where manager_name ILike '%Params%') Then
        INSERT INTO TmpWarningMessages (message, manager_name)
        SELECT 'Will not process managers with "Params" in the name (for safety)',
               manager_name
        FROM TmpManagerList
        WHERE manager_name ILike '%Params%'
        ORDER BY manager_name;

        DELETE FROM TmpManagerList
        WHERE manager_name IN (SELECT WarnMsgs.manager_name FROM TmpWarningMessages WarnMsgs WHERE NOT WarnMsgs.message ILIKE 'Note:%');
    End If;

    DELETE FROM TmpManagerList
    WHERE TmpManagerList.mgr_id Is Null OR
          TmpManagerList.control_from_web > 0;

    If Exists (Select * From TmpManagerList Src INNER JOIN mc.t_old_managers Target ON Src.mgr_id = Target.mgr_id) Then
        INSERT INTO TmpWarningMessages (message, manager_name)
        SELECT 'Manager already exists in t_old_managers; cannot archive',
               manager_name
        FROM TmpManagerList Src
             INNER JOIN mc.t_old_managers Target
               ON Src.mgr_id = Target.mgr_id;

        DELETE FROM TmpManagerList
        WHERE manager_name IN (SELECT WarnMsgs.manager_name FROM TmpWarningMessages WarnMsgs WHERE NOT WarnMsgs.message ILIKE 'Note:%');
    End If;

    If Exists (Select * From TmpManagerList Src INNER JOIN mc.t_param_value_old_managers Target ON Src.mgr_id = Target.mgr_id) Then
        INSERT INTO TmpWarningMessages (message, manager_name)
        SELECT 'Note: manager already has parameters in t_param_value_old_managers; will merge values from t_param_value',
               manager_name
        FROM TmpManagerList Src
             INNER JOIN mc.t_param_value_old_managers Target
               ON Src.mgr_id = Target.mgr_id;
    End If;

    If _infoOnly <> 0 OR NOT EXISTS (Select * From TmpManagerList) Then
        RETURN QUERY
        SELECT ' To be archived' as message,
               Src.manager_name,
               Src.control_from_web,
               PV.mgr_type_id,
               PV.param_name,
               PV.Entry_ID,
               PV.type_id,
               PV.Value,
               PV.mgr_id,
               PV.Comment,
               PV.Last_Affected,
               PV.Entered_By
        FROM TmpManagerList Src
             LEFT OUTER JOIN mc.v_param_value PV
               ON PV.mgr_id = Src.mgr_id
        UNION
        SELECT WarnMsgs.message,
               WarnMsgs.manager_name,
               0::smallint as control_from_website,
               0 as manager_type_id,
               ''::citext as param_name,
               0 as entry_id,
               0 as type_id,
               ''::citext as value,
               0 as mgr_id,
               ''::citext as comment,
               current_timestamp::timestamp as last_affected,
               ''::citext as entered_by
        FROM TmpWarningMessages WarnMsgs
        ORDER BY message ASC, manager_name, param_name;
        RETURN;
    End If;

    RAISE Info 'Insert into t_old_managers';

    INSERT INTO mc.t_old_managers(
                               mgr_id,
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
    WHERE Target.mgr_id IS NULL;
    --
    GET DIAGNOSTICS _myRowCount = ROW_COUNT;

    RAISE Info 'Insert into t_param_value_old_managers';

    -- The following query uses
    --   ON CONFLICT ON CONSTRAINT pk_t_param_value_old_managers
    -- instead of
    --   ON CONFLICT (entry_id)
    -- to avoid an ambiguous name error with the entry_id field
    -- returned by this function

    INSERT INTO mc.t_param_value_old_managers(
             entry_id,
             type_id,
             value,
             mgr_id,
             comment,
             last_affected,
             entered_by )
    SELECT PV.entry_id,
           PV.type_id,
           PV.value,
           PV.mgr_id,
           PV.comment,
           PV.last_affected,
           PV.entered_by
    FROM mc.t_param_value PV
         INNER JOIN TmpManagerList Src
           ON PV.mgr_id = Src.mgr_id
   ON CONFLICT ON CONSTRAINT pk_t_param_value_old_managers
   DO UPDATE SET
        type_id = EXCLUDED.type_id,
        value = EXCLUDED.value,
        mgr_id = EXCLUDED.mgr_id,
        comment = EXCLUDED.comment,
        last_affected = EXCLUDED.last_affected,
        entered_by = EXCLUDED.entered_by;
    --
    GET DIAGNOSTICS _myRowCount = ROW_COUNT;

    RAISE Info 'Delete from mc.t_param_value';

    DELETE FROM mc.t_param_value target
    WHERE target.mgr_id IN (SELECT MgrList.mgr_id FROM TmpManagerList MgrList);

    RAISE Info 'Delete from mc.t_mgrs';

    DELETE FROM mc.t_mgrs target
    WHERE target.mgr_id IN (SELECT MgrList.mgr_id FROM TmpManagerList MgrList);

    RAISE Info 'Delete succeeded; returning results';

    RETURN QUERY
    SELECT 'Moved to mc.t_old_managers and mc.t_param_value_old_managers' as Message,
           Src.Manager_Name,
           Src.control_from_web,
           OldMgrs.mgr_type_id,
           PT.param_name,
           PV.entry_id,
           PV.type_id,
           PV.value,
           PV.mgr_id,
           PV.comment,
           PV.last_affected,
           PV.entered_by
    FROM TmpManagerList Src
         LEFT OUTER JOIN mc.t_old_managers OldMgrs
           ON OldMgrs.mgr_id = Src.mgr_id
         LEFT OUTER JOIN mc.t_param_value_old_managers PV
           ON PV.mgr_id = Src.mgr_id
         LEFT OUTER JOIN mc.t_param_type PT ON
         PV.type_id = PT.param_id
    ORDER BY Src.manager_name, param_name;

EXCEPTION
    WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS
            _sqlstate = returned_sqlstate,
            _exceptionMessage = message_text,
            _exceptionContext = pg_exception_context;

    _message := 'Error archiving manager parameters for ' || _mgrList || ': ' || _exceptionMessage;

    RAISE Warning 'Error: %', _message;
    RAISE warning '%', _exceptionContext;

    Call PostLogEntry ('Error', _message, 'ArchiveOldManagersAndParams', 'mc');

    RETURN QUERY
    SELECT _message as Message,
           ''::citext as Manager_Name,
           0::smallint as control_from_website,
           0 as manager_type_id,
           ''::citext as param_name,
           0 as entry_id,
           0 as type_id,
           ''::citext as value,
           0 as mgr_id,
           ''::citext as comment,
           current_timestamp::timestamp as last_affected,
           ''::citext as entered_by;
END
$$
;


COMMENT ON FUNCTION mc.archive_old_managers_and_params IS 'ArchiveOldManagersAndParams';

CREATE OR REPLACE PROCEDURE mc.disable_analysis_managers
(
    _infoOnly int = 0,
    INOUT _message TEXT = '',
    INOUT _returnCode text = ''
)
LANGUAGE plpgsql
AS $$
/****************************************************
**
**  Desc:   Disables all analysis managers
**
**  Auth:   mem
**  Date:   05/09/2008
**          10/09/2009 mem - Changed _ManagerTypeIDList to 11
**          06/09/2011 mem - Now calling EnableDisableAllManagers
**          01/30/2020 mem - Ported to PostgreSQL
**
*****************************************************/
DECLARE

BEGIN
    Call mc.enable_disable_all_managers (
        _managerTypeIDList := '11',
        _managerNameList := '',
        _enable := 0,
        _infoOnly := _infoOnly,
        _message := _message,
        _returnCode := _returnCode);

END
$$;

COMMENT ON PROCEDURE mc.disable_analysis_managers IS 'DisableAnalysisManagers';

CREATE OR REPLACE PROCEDURE mc.disable_archive_dependent_managers
(
    _infoOnly int = 0,
    INOUT _message TEXT = '',
    INOUT _returnCode text = ''
)
LANGUAGE plpgsql
AS $$
/****************************************************
**
**  Desc:   Disables managers that rely on MyEMSL
**
**  Auth:   mem
**  Date:   05/09/2008
**          07/24/2008 mem - Changed _ManagerTypeIDList from '1,2,3,4,8' to '2,3,8'
**          07/24/2008 mem - Changed _ManagerTypeIDList from '2,3,8' to '8'
**                         - Note that we do not include 15=CaptureTaskManager because capture tasks can still occur when the archive is unavailable
**                         - However, you should run Stored Procedure EnableDisableArchiveStepTools in the DMS_Capture database to disable the archive-dependent step tools
**          01/30/2020 mem - Ported to PostgreSQL
**
*****************************************************/
DECLARE

BEGIN

    Call mc.enable_disable_all_managers (
        _managerTypeIDList := '8',
        _managerNameList := '',
        _enable := 0,
        _infoOnly := _infoOnly,
        _message := _message,
        _returnCode := _returnCode);

END
$$;

COMMENT ON PROCEDURE mc.disable_archive_dependent_managers IS 'DisableArchiveDependentManagers';

CREATE OR REPLACE PROCEDURE mc.disable_sequest_clusters
(
    _infoOnly int = 0,
    INOUT _message TEXT = '',
    INOUT _returnCode text = ''
)
LANGUAGE plpgsql
AS $$
/****************************************************
**
**  Desc:   Disables the Sequest Clusters
**
**  Auth:   mem
**  Date:   07/24/2008
**          10/09/2009 mem - Changed _ManagerTypeIDList to 11
**          01/30/2020 mem - Ported to PostgreSQL
**
*****************************************************/
DECLARE

BEGIN

    Call mc.enable_disable_all_managers (
        _managerTypeIDList := '11',
        _managerNameList := '%SeqCluster%',
        _enable := 0,
        _infoOnly := _infoOnly,
        _message := _message,
        _returnCode := _returnCode);

END
$$;

COMMENT ON PROCEDURE mc.disable_sequest_clusters IS 'DisableSequestClusters';

CREATE OR REPLACE FUNCTION mc.duplicate_manager_parameter
(
    _sourceParamTypeID int,
    _newParamTypeID int,
    _paramValueOverride text = null,
    _commentOverride text = null,
    _paramValueSearchText text = null,
    _paramValueReplaceText text = null,
    _infoOnly int = 1
)
RETURNS TABLE
(
    status text,
    type_id integer,
    value public.citext,
    mgr_id integer,
    comment public.citext
)
LANGUAGE plpgsql
AS $$
/****************************************************
**
**  Desc:
**      Duplicates an existing parameter for all managers,
**      creating new entries using the new param TypeID value
**
**      The new parameter type must already exist in mc.t_param_type
**
**  Example usage:
**    Select * From DuplicateManagerParameter (157, 172, _paramValueSearchText := 'msfileinfoscanner', _paramValueReplaceText := 'AgilentToUimfConverter', _infoOnly := 1);
**
**    Select * From DuplicateManagerParameter (179, 182, _paramValueSearchText := 'PbfGen', _paramValueReplaceText := 'ProMex', _infoOnly := 1);
**
**  Arguments:
**    _paramValueOverride      Optional: new parameter value; ignored if _paramValueSearchText is defined
**    _paramValueSearchText    Optional: text to search for in the source parameter value
**    _paramValueReplaceText   Optional: replacement text (ignored if _paramValueReplaceText is null)
**
**  Auth:   mem
**  Date:   08/26/2013 mem - Initial release
**          01/30/2020 mem - Ported to PostgreSQL
**
*****************************************************/
DECLARE
    _myRowCount int := 0;
    _message text = '';
    _returnCode text = '';
    _sqlstate text;
    _exceptionMessage text;
    _exceptionContext text;
BEGIN

    ---------------------------------------------------
    -- Validate input fields
    ---------------------------------------------------

    _infoOnly := Coalesce(_infoOnly, 1);

    _message := '';
    _returnCode := '';

    If _returnCode = '' And _sourceParamTypeID Is Null Then
        _message := '_sourceParamTypeID cannot be null; unable to continue';
        RAISE WARNING '%', _message;
        _returnCode := 'U5200';
    End If;

    If _returnCode = '' And  _newParamTypeID Is Null Then
        _message := '_newParamTypeID cannot be null; unable to continue';
        RAISE WARNING '%', _message;
        _returnCode := 'U5201';
    End If;

    If _returnCode = '' And Not _paramValueSearchText Is Null AND _paramValueReplaceText Is Null Then
        _message := '_paramValueReplaceText cannot be null when _paramValueSearchText is defined; unable to continue';
        RAISE WARNING '%', _message;
        _returnCode := 'U5202';
    End If;

    If _returnCode <> '' Then
        RETURN QUERY
        SELECT 'Warning' AS status,
               0 as type_id,
               _message::citext as value,
               0,
               ''::citext as comment;
        RETURN;
    End If;

    ---------------------------------------------------
    -- Make sure the soure parameter exists
    ---------------------------------------------------

    If _returnCode = '' And Not Exists (Select * From mc.t_param_value PV Where PV.type_id = _sourceParamTypeID) Then
        _message := '_sourceParamTypeID ' || _sourceParamTypeID || ' not found in mc.t_param_value; unable to continue';
        RAISE WARNING '%', _message;
        _returnCode := 'U5203';
    End If;

    If _returnCode = '' And Exists (Select * From mc.t_param_value PV Where PV.type_id = _newParamTypeID) Then
        _message := '_newParamTypeID ' || _newParamTypeID || ' already exists in mc.t_param_value; unable to continue';
        RAISE WARNING '%', _message;
        _returnCode := 'U5204';
    End If;

    If _returnCode = '' And Not Exists (Select * From mc.t_param_type PT Where PT.param_id = _newParamTypeID) Then
        _message := '_newParamTypeID ' || _newParamTypeID || ' not found in mc.t_param_type; unable to continue';
        RAISE WARNING '%', _message;
        _returnCode := 'U5205';
    End If;

    If _returnCode <> '' Then
        RETURN QUERY
        SELECT 'Warning' AS status,
               0 as type_id,
               _message::citext as value,
               0 as mgr_id,
               ''::citext as comment;
        RETURN;
    End If;

    If Not _paramValueSearchText Is Null Then
        If _infoOnly <> 0 Then
            RETURN QUERY
            SELECT 'Preview' as Status,
                   _newParamTypeID AS TypeID,
                   (Replace(PV.value::citext, _paramValueSearchText::citext, _paramValueReplaceText::citext))::citext AS value,
                   PV.mgr_id,
                   Coalesce(_commentOverride, '')::citext AS comment
            FROM mc.t_param_value PV
            WHERE PV.type_id = _sourceParamTypeID;
            Return;
        End If;

        INSERT INTO mc.t_param_value( type_id, value, mgr_id, comment )
        SELECT _newParamTypeID AS type_id,
               Replace(PV.value::citext, _paramValueSearchText::citext, _paramValueReplaceText::citext) AS value,
               PV.mgr_id,
               Coalesce(_commentOverride, '') AS comment
        FROM mc.t_param_value PV
        WHERE PV.type_id = _sourceParamTypeID;
        --
        GET DIAGNOSTICS _myRowCount = ROW_COUNT;
    Else

        If _infoOnly <> 0 Then
            RETURN QUERY
            SELECT 'Preview' as Status,
                   _newParamTypeID AS TypeID,
                   Coalesce(_paramValueOverride, PV.value)::citext AS value,
                   PV.mgr_id,
                   Coalesce(_commentOverride, '')::citext AS comment
            FROM mc.t_param_value PV
            WHERE PV.type_id = _sourceParamTypeID;
            Return;
        End If;

        INSERT INTO mc.t_param_value( type_id, value, mgr_id, comment )
        SELECT _newParamTypeID AS type_id,
               Coalesce(_paramValueOverride, PV.value) AS value,
               PV.mgr_id,
               Coalesce(_commentOverride, '') AS comment
        FROM mc.t_param_value PV
        WHERE PV.type_id = _sourceParamTypeID;
        --
        GET DIAGNOSTICS _myRowCount = ROW_COUNT;
    End If;

    RETURN QUERY
        SELECT 'Duplicated' as Status, PV.type_id, PV.value, PV.mgr_id, PV.comment
        FROM mc.t_param_value PV
        WHERE PV.type_id = _newParamTypeID;

EXCEPTION
    WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS
            _sqlstate = returned_sqlstate,
            _exceptionMessage = message_text,
            _exceptionContext = pg_exception_context;

    _message := 'Error duplicating a manager parameter: ' || _exceptionMessage;
    _returnCode := _sqlstate;

    RAISE Warning 'Error: %', _message;
    RAISE warning '%', _exceptionContext;

    RETURN QUERY
    SELECT 'Error' AS Status,
           0 as type_id,
           _message::citext as value,
           0 as mgr_id,
           ''::citext as comment;

END
$$;

COMMENT ON FUNCTION mc.duplicate_manager_parameter IS 'DuplicateManagerParameter';


CREATE OR REPLACE FUNCTION mc.duplicate_manager_parameters
(
    _sourceMgrID int,
    _targetMgrID int,
    _mergeSourceWithTarget int = 0,
    _infoOnly int = 0
)
RETURNS TABLE(type_id integer, value citext, mgr_id integer, comment citext)
LANGUAGE plpgsql
AS $$
/****************************************************
**
**  Desc:
**      Duplicates the parameters for a given manager
**      to create new parameters for a new manager
**
**  Example usage:
**    select * from DuplicateManagerParameter(157, 172)
**
**  Arguments:
**    _mergeSourceWithTarget    When 0, then the target manager cannot have any parameters; if 1, then will add missing parameters to the target manager
**
**  Auth:   mem
**  Date:   10/10/2014 mem - Initial release
**          02/01/2020 mem - Ported to PostgreSQL
**
*****************************************************/
DECLARE
    _myRowCount int := 0;
    _message text = '';
    _returnCode text = '';
    _sqlstate text;
    _exceptionMessage text;
    _exceptionContext text;
BEGIN

    ---------------------------------------------------
    -- Validate input fields
    ---------------------------------------------------

    _infoOnly := Coalesce(_infoOnly, 1);
    _mergeSourceWithTarget := Coalesce(_mergeSourceWithTarget, 0);

    If _returnCode = '' And _sourceMgrID Is Null Then
        _message := '_sourceMgrID cannot be null; unable to continue';
        RAISE WARNING '%', _message;
        _returnCode := 'U5200';
    End If;

    If _returnCode = '' And _targetMgrID Is Null Then
        _message := '_targetMgrID cannot be null; unable to continue';
        RAISE WARNING '%', _message;
        _returnCode := 'U5201';
    End If;

    If _returnCode <> '' Then
        RETURN QUERY
        SELECT 0 as type_id,
               'Warning'::citext as value,
               0,
               _message::citext as comment;
        RETURN;
    End If;

    ---------------------------------------------------
    -- Make sure the source and target managers exist
    ---------------------------------------------------

    If _returnCode = '' And Not Exists (Select * From mc.t_mgrs Where mgr_id = _sourceMgrID) Then
        _message := '_sourceMgrID ' || _sourceMgrID || ' not found in mc.t_mgrs; unable to continue';
        RAISE WARNING '%', _message;
        _returnCode := 'U5203';
    End If;

    If _returnCode = '' And Not Exists (Select * From mc.t_mgrs Where mgr_id = _targetMgrID) Then
        _message := '_targetMgrID ' || _targetMgrID || ' not found in mc.t_mgrs; unable to continue';
        RAISE WARNING '%', _message;
        _returnCode := 'U5204';
    End If;

    If _returnCode = '' And _mergeSourceWithTarget = 0 Then
        -- Make sure the target manager does not have any parameters
        --
        If Exists (SELECT * FROM mc.t_param_value WHERE mgr_id = _targetMgrID) Then
            _message := '_targetMgrID ' + _targetMgrID + ' has existing parameters in mc.t_param_value; aborting since _mergeSourceWithTarget = 0';
            _returnCode := 'U5205';
        End If;
    End If;

    If _returnCode <> '' Then
        RETURN QUERY
        SELECT 0 as type_id,
               'Warning'::citext as value,
               0 as mgr_id,
               _message::citext as comment;
        RETURN;
    End If;

    If _infoOnly <> 0 Then
            RETURN QUERY
            SELECT Source.type_id,
                   Source.value,
                   _targetMgrID AS mgr_id,
                   Source.comment
            FROM mc.t_param_value AS Source
                 LEFT OUTER JOIN ( SELECT PV.type_id
                                   FROM mc.t_param_value PV
                                   WHERE PV.mgr_id = _targetMgrID ) AS ExistingParams
                   ON Source.type_id = ExistingParams.type_id
            WHERE Source.mgr_id = _sourceMgrID AND
                  ExistingParams.type_id IS NULL;
            Return;
    End If;

    INSERT INTO mc.t_param_value (type_id, value, mgr_id, comment)
    SELECT Source.type_id,
           Source.value,
           _targetMgrID AS mgr_id,
           Source.comment
    FROM mc.t_param_value AS Source
         LEFT OUTER JOIN ( SELECT PV.type_id
                           FROM mc.t_param_value PV
                           WHERE PV.mgr_id = _targetMgrID ) AS ExistingParams
           ON Source.type_id = ExistingParams.type_id
    WHERE Source.mgr_id = _sourceMgrID AND
          ExistingParams.type_id IS NULL;
    --
    GET DIAGNOSTICS _myRowCount = ROW_COUNT;

    RETURN QUERY
        SELECT PV.type_id, PV.value, PV.mgr_id, PV.comment
        FROM mc.t_param_value PV
        WHERE PV.mgr_id = _targetMgrID;


EXCEPTION
    WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS
            _sqlstate = returned_sqlstate,
            _exceptionMessage = message_text,
            _exceptionContext = pg_exception_context;

    _message := 'Error duplicating manager parameters: ' || _exceptionMessage;
    _returnCode := _sqlstate;

    RAISE Warning 'Error: %', _message;
    RAISE warning '%', _exceptionContext;

    RETURN QUERY
    SELECT 0 as type_id,
           'Error'::citext as value,
           0 as mgr_id,
           _message::citext as comment;

END
$$;

COMMENT ON FUNCTION mc.duplicate_manager_parameters IS 'DuplicateManagerParameters';


CREATE OR REPLACE PROCEDURE mc.enable_disable_managers
(
    _enable int,
    _managerTypeID int = 11,
    _managerNameList text = '',
    _infoOnly int = 0,
    _includeDisabled int = 0,
    INOUT _message text = '',
    INOUT _returnCode text = ''
)
LANGUAGE plpgsql
AS $procedure$
/****************************************************
**
**  Desc:
**      Enables or disables all managers of the given type
**
**  Arguments:
**    _enable            0 to disable, 1 to enable
**    _managerTypeID     Defined in table T_MgrTypes.  8=Space, 9=DataImport, 11=Analysis Tool Manager, 15=CaptureTaskManager
**    _managerNameList   Required when _enable = 1.  Only managers specified here will be enabled, though you can use 'All' to enable All managers.
**                       When _enable = 0, if this parameter is blank (or All) then all managers of the given type will be disabled
**                       supports the % wildcard
**   _infoOnly           When non-zero, show the managers that would be updated
**   _includeDisabled    By default, this procedure skips managers with control_from_website = 0 in t_mgrs; set _includeDisabled to 1 to also include them
**
**  Auth:   mem
**  Date:   07/12/2007
**          05/09/2008 mem - Added parameter @ManagerNameList
**          06/09/2011 mem - Now filtering on MT_Active > 0 in T_MgrTypes
**                         - Now allowing @ManagerNameList to be All when @Enable = 1
**          10/12/2017 mem - Allow @ManagerTypeID to be 0 if @ManagerNameList is provided
**          03/28/2018 mem - Use different messages when updating just one manager
**          01/30/2020 mem - Ported to PostgreSQL
**          02/04/2020 mem - Rename columns to mgr_id and mgr_name
**          02/05/2020 mem - Update _message when previewing updates
**
*****************************************************/
DECLARE
    _myRowCount int := 0;
    _newValue text;
    _managerTypeName text;
    _activeStateDescription text;
    _countToUpdate int;
    _countUnchanged int;
    _infoHead text;
    _infoData text;
    _previewData record;
    _sqlstate text;
    _exceptionMessage text;
    _exceptionContext text;
BEGIN

    -----------------------------------------------
    -- Validate the inputs
    -----------------------------------------------
    --
    _managerNameList := Coalesce(_managerNameList, '');
    _infoOnly        := Coalesce(_infoOnly, 0);
    _includeDisabled := Coalesce(_includeDisabled, 0);

    _message := '';
    _returnCode := '';

    If _enable Is Null Then
        _message := '_enable cannot be null';
        _returnCode := 'U4000';
        Return;
    End If;

    If _managerTypeID Is Null Then
        _message := '_managerTypeID cannot be null';
        _returnCode := 'U4001';
        Return;
    End If;

    If _managerTypeID = 0 And char_length(_managerNameList) > 0 And _managerNameList::citext <> 'All' Then
        _managerTypeName := 'Any';
    Else
        -- Make sure _managerTypeID is valid
        _managerTypeName := '';

        SELECT mgr_type_name INTO _managerTypeName
        FROM mc.t_mgr_types
        WHERE mgr_type_id = _managerTypeID AND
              mgr_type_active > 0;

        If Not Found Then
            If Exists (SELECT * FROM mc.t_mgr_types WHERE mgr_type_id = _managerTypeID AND mgr_type_active = 0) Then
                _message := '_managerTypeID ' || _managerTypeID::text || ' has mgr_type_active = 0 in mc.t_mgr_types; unable to continue';
            Else
                _message := '_managerTypeID ' || _managerTypeID::text || ' not found in mc.t_mgr_types';
            End If;

            _returnCode := 'U4002';
            Return;
        End If;
    End If;

    If _enable <> 0 AND char_length(_managerNameList) = 0 Then
        _message := '_managerNameList cannot be blank when _enable is non-zero; to update all managers, set _managerNameList to ''All''';
        _returnCode := 'U4003';
        Return;
    End If;

    -----------------------------------------------
    -- Creata a temporary table
    -----------------------------------------------

    DROP TABLE IF EXISTS TmpManagerList;

    CREATE TEMP TABLE TmpManagerList (
        manager_name citext NOT NULL
    );

    If char_length(_managerNameList) > 0 And _managerNameList::citext <> 'All' Then
        -- Populate TmpMangerList using ParseManagerNameList

        Call mc.parse_manager_name_list (_managerNameList, _removeUnknownManagers => 1, _message => _message);

        If _managerTypeID > 0 Then
            -- Delete entries from TmpManagerList that don't match entries in mgr_name of the given type
            DELETE FROM TmpManagerList
            WHERE NOT manager_name IN ( SELECT M.mgr_name
                                        FROM TmpManagerList U
                                             INNER JOIN mc.t_mgrs M
                                               ON M.mgr_name = U.manager_name AND
                                                  M.mgr_type_id = _managerTypeID );
            --
            GET DIAGNOSTICS _myRowCount = ROW_COUNT;

            If _myRowCount > 0 Then
                _message := 'Found ' || _myRowCount || ' entries in _managerNameList that are not ' || _managerTypeName || ' managers';
                RAISE INFO '%', _message;
                _message := '';
            End If;
        End If;

        IF _includeDisabled = 0 THEN
            DELETE FROM TmpManagerList
            WHERE NOT manager_name IN ( SELECT M.mgr_name
                                        FROM TmpManagerList U
                                             INNER JOIN mc.t_mgrs M
                                               ON M.mgr_name = U.manager_name AND
                                                  M.mgr_type_id = _managerTypeID
                                        WHERE control_from_website > 0);
        END IF;
    Else
        -- Populate TmpManagerList with all managers in mc.t_mgrs (of type _managerTypeID)
        --
        INSERT INTO TmpManagerList (manager_name)
        SELECT mgr_name
        FROM mc.t_mgrs
        WHERE mgr_type_id = _managerTypeID And
              (control_from_website > 0 Or _includeDisabled > 0);
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
    --
    SELECT COUNT(*) INTO _countToUpdate
    FROM mc.t_param_value PV
         INNER JOIN mc.t_param_type PT
           ON PV.type_id = PT.param_id
         INNER JOIN mc.t_mgrs M
           ON PV.mgr_id = M.mgr_id
         INNER JOIN mc.t_mgr_types MT
           ON M.mgr_type_id = MT.mgr_type_id
         INNER JOIN TmpManagerList U
           ON M.mgr_name = U.manager_name
    WHERE PT.param_name = 'mgractive' AND
          PV.value <> _newValue AND
          MT.mgr_type_active > 0;
    --
    GET DIAGNOSTICS _myRowCount = ROW_COUNT;

    -- Count the number of managers already in the target state
    --
    SELECT COUNT(*) INTO _countUnchanged
    FROM mc.t_param_value PV
         INNER JOIN mc.t_param_type PT
           ON PV.type_id = PT.param_id
         INNER JOIN mc.t_mgrs M
           ON PV.mgr_id = M.mgr_id
         INNER JOIN mc.t_mgr_types MT
           ON M.mgr_type_id = MT.mgr_type_id
         INNER JOIN TmpManagerList U
           ON M.mgr_name = U.manager_name
    WHERE PT.param_name = 'mgractive' AND
          PV.value = _newValue AND
          MT.mgr_type_active > 0;
    --
    GET DIAGNOSTICS _myRowCount = ROW_COUNT;

    _countToUpdate  := COALESCE(_countToUpdate, 0);
    _countUnchanged := COALESCE(_countUnchanged, 0);

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

        RAISE INFO '%', _message;
        Return;
    End If;

    If _infoOnly <> 0 Then

        _infoHead := format('%-22s %-15s %-20s %-25s %-25s',
                            'State Change Preview',
                            'Parameter Name',
                            'Manager Name',
                            'Manager Type',
                            'Enabled (control_from_website=1)'
                        );

        RAISE INFO '%', _infoHead;

        FOR _previewData IN
            SELECT PV.value || ' --> ' || _newValue AS State_Change_Preview,
                   PT.param_name AS Parameter_Name,
                   M.mgr_name AS manager_name,
                   MT.mgr_type_name AS Manager_Type,
                   M.control_from_website
            FROM mc.t_param_value PV
                 INNER JOIN mc.t_param_type PT
                   ON PV.type_id = PT.param_id
                 INNER JOIN mc.t_mgrs M
                   ON PV.mgr_id = M.mgr_id
                 INNER JOIN mc.t_mgr_types MT
                   ON M.mgr_type_id = MT.mgr_type_id
                 INNER JOIN TmpManagerList U
                   ON M.mgr_name = U.manager_name
            WHERE PT.param_name = 'mgractive' AND
                  PV.value <> _newValue AND
                  MT.mgr_type_active > 0
        LOOP

            _infoData := format('%-22s %-15s %-20s %-25s %-25s',
                                    _previewData.State_Change_Preview,
                                    _previewData.Parameter_Name,
                                    _previewData.manager_name,
                                    _previewData.Manager_Type,
                                    _previewData.control_from_website
                            );

            RAISE INFO '%', _infoData;

        END LOOP;

        _message := format('Would set %s managers to %s; see the Output window for details',
                            _countToUpdate,
                            _activeStateDescription);

        RETURN;
    End If;

    -- Update mgractive for the managers in TmpManagerList
    --
    UPDATE mc.t_param_value
    SET value = _newValue
    FROM mc.t_param_value PV
         INNER JOIN mc.t_param_type PT
           ON PV.type_id = PT.param_id
         INNER JOIN mc.t_mgrs M
           ON PV.mgr_id = M.mgr_id
         INNER JOIN mc.t_mgr_types MT
           ON M.mgr_type_id = MT.mgr_type_id
         INNER JOIN TmpManagerList U
           ON M.mgr_name = U.manager_name
    WHERE mc.t_param_value.entry_ID = PV.Entry_ID AND
          PT.param_name = 'mgractive' AND
          PV.value <> _newValue AND
          MT.mgr_type_active > 0;
    --
    GET DIAGNOSTICS _myRowCount = ROW_COUNT;

    If _myRowCount = 1 And _countUnchanged = 0 Then
        _message := 'The manager is now ' || _activeStateDescription;
    Else
        If _managerTypeID = 0 Then
            _message := 'Set ' || _myRowCount || ' managers to state ' || _activeStateDescription;
        Else
            _message := 'Set ' || _myRowCount || ' ' || _managerTypeName || ' managers to state ' || _activeStateDescription;
        End If;

        If _countUnchanged <> 0 Then
            _message := _message || ' (' || _countUnchanged || ' managers were already ' || _activeStateDescription || ')';
        End If;
    End If;

    RAISE INFO '%', _message;

EXCEPTION
    WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS
            _sqlstate = returned_sqlstate,
            _exceptionMessage = message_text,
            _exceptionContext = pg_exception_context;

    _message := 'Error enabling/disabling managers: ' || _exceptionMessage;
    _returnCode := _sqlstate;

    RAISE Warning 'Error: %', _message;
    RAISE warning '%', _exceptionContext;

    Call PostLogEntry ('Error', _message, 'EnableDisableManagers', 'mc');

END
$procedure$
;

COMMENT ON PROCEDURE mc.enable_disable_managers IS 'EnableDisableManagers';


CREATE OR REPLACE PROCEDURE mc.enable_disable_all_managers
(
    _managerTypeIDList text = '',
    _managerNameList text = '',
    _enable int = 1,
    _infoOnly int = 0,
    INOUT _message TEXT = '',
    INOUT _returnCode text = ''
)
LANGUAGE plpgsql
AS $$
/****************************************************
**
**  Desc:
**      Enables or disables all managers, optionally filtering by manager type ID or manager name
**
**  Arguments:
**    _managerTypeIDList   Optional: comma separated list of manager type IDs to disable, e.g. '1, 2, 3'
**    _managerNameList     Optional: if defined, only managers specified here will be enabled;
**                         Supports the % wildcard; also supports 'all'
**    _enable              1 to enable, 0 to disable
**    _infoOnly            When non-zero, show the managers that would be updated
**
**  Auth:   mem
**  Date:   05/09/2008
**          06/09/2011 - Created by extending code in DisableAllManagers
**                     - Now filtering on MT_Active > 0 in T_MgrTypes
**          01/30/2020 mem - Ported to PostgreSQL
**
*****************************************************/
DECLARE
    _mgrTypeID int;
    _sqlstate text;
    _exceptionMessage text;
    _exceptionContext text;
BEGIN

    -----------------------------------------------
    -- Validate the inputs
    -----------------------------------------------
    --
    _enable := Coalesce(_enable, 0);
    _managerTypeIDList := Coalesce(_managerTypeIDList, '');
    _managerNameList := Coalesce(_managerNameList, '');
    _infoOnly := Coalesce(_infoOnly, 0);

    _message := '';
    _returnCode := '';

    DROP TABLE IF EXISTS TmpManagerTypeIDs;

    CREATE TEMP TABLE TmpManagerTypeIDs (
        mgr_type_id int NOT NULL
    );

    If char_length(_managerTypeIDList) > 0 THEN
        -- Parse _managerTypeIDList
        --
        INSERT INTO TmpManagerTypeIDs (mgr_type_id)
        SELECT DISTINCT value
        FROM public.udf_parse_delimited_integer_list(_managerTypeIDList, ',')
        ORDER BY Value;
    Else
        -- Populate TmpManagerTypeIDs with all manager types in mc.t_mgr_types
        --
        INSERT INTO TmpManagerTypeIDs (mgr_type_id)
        SELECT DISTINCT mgr_type_id
        FROM mc.t_mgr_types
        WHERE mgr_type_active > 0
        ORDER BY mgr_type_id;
    End If;

    -----------------------------------------------
    -- Loop through the manager types in TmpManagerTypeIDs
    -- For each, call EnableDisableManagers
    -----------------------------------------------

    FOR _mgrTypeID IN
        SELECT mgr_type_id
        FROM TmpManagerTypeIDs
    LOOP

        Call mc.enable_disable_managers (
            _enable := _enable,
            _managerTypeID := _mgrTypeID,
            _managerNameList := _managerNameList,
            _infoOnly := _infoOnly,
            _message := _message,
            _returnCode := _returnCode);

    End Loop;

EXCEPTION
    WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS
            _sqlstate = returned_sqlstate,
            _exceptionMessage = message_text,
            _exceptionContext = pg_exception_context;

    _message := 'Error updating enabling/disabling all managers: ' || _exceptionMessage;
    _returnCode := _sqlstate;

    RAISE Warning 'Error: %', _message;
    RAISE warning '%', _exceptionContext;

    Call PostLogEntry ('Error', _message, 'EnableDisableAllManagers', 'mc');
END
$$;

COMMENT ON PROCEDURE mc.enable_disable_all_managers IS 'EnableDisableAllManagers';

CREATE OR REPLACE PROCEDURE mc.enable_archive_dependent_managers
(
    _infoonly integer DEFAULT 0,
    INOUT _message text DEFAULT ''::text,
    INOUT _returncode text DEFAULT ''::TEXT
)
LANGUAGE plpgsql
AS $$
/****************************************************
**
**  Desc:   Enables managers that rely on MyEMSL
**
**  Auth:   mem
**  Date:   06/09/2011 mem - Initial Version
**          02/05/2020 mem - Ported to PostgreSQL
**
*****************************************************/
DECLARE

BEGIN

    -- Enable Space managers (type 8)
    Call mc.enable_disable_all_managers (
        _managerTypeIDList := '8',
        _managerNameList := '',
        _enable := 1,
        _infoOnly := _infoOnly,
        _message := _message,
        _returnCode := _returnCode);

END
$$;

COMMENT ON PROCEDURE mc.enable_archive_dependent_managers IS 'EnableArchiveDependentManagers';

CREATE OR REPLACE PROCEDURE mc.enable_disable_run_jobs_remotely
(
    _enable int,
    _managerNameList text = '',
    _infoOnly int = 0,
    _addMgrParamsIfMissing int = 0,
    INOUT _message text = '',
    INOUT _returncode text = ''
)
LANGUAGE plpgsql
AS $$
/****************************************************
**
**  Desc:
**      Enables or disables a manager to run jobs remotely
**
**  Arguments:
**    _enable                  0 to disable running jobs remotely, 1 to enable
**    _managerNameList         Manager(s) to update; supports % for wildcards
**    _infoOnly                When non-zero, show the managers that would be updated
**    _addMgrParamsIfMissing   When 1, if manger(s) are missing parameters RunJobsRemotely or RemoteHostName, will auto-add those parameters
**
**  Auth:   mem
**  Date:   03/28/2018 mem - Initial version
**          03/29/2018 mem - Add parameter _addMgrParamsIfMissing
**          02/05/2020 mem - Ported to PostgreSQL
**
*****************************************************/
DECLARE
    _myRowCount int := 0;
    _newValue text;
    _activeStateDescription text;
    _countToUpdate int;
    _countUnchanged int;
    _mgrRecord record;
    _mgrName text := '';
    _mgrId int := 0;
    _paramTypeId int := 0;
    _infoHead text;
    _infoData text;
    _previewData record;
    _sqlstate text;
    _exceptionMessage text;
    _exceptionContext text;
BEGIN

    -----------------------------------------------
    -- Validate the inputs
    -----------------------------------------------
    --
    _managerNameList := Coalesce(_managerNameList, '');
    _infoOnly := Coalesce(_infoOnly, 0);
    _addMgrParamsIfMissing := Coalesce(_addMgrParamsIfMissing, 0);

    _message := '';
    _returnCode := '';

    If _enable Is Null Then
        _message := '_enable cannot be null';
        _returnCode := 'U4000';
        Return;
    End If;

    If char_length(_managerNameList) = 0 Then
        _message := '_managerNameList cannot be blank';
        _returnCode := 'U4003';
        Return;
    End If;

    -----------------------------------------------
    -- Creata a temporary table
    -----------------------------------------------

    DROP TABLE IF EXISTS TmpManagerList;

    CREATE TEMP TABLE TmpManagerList (
        manager_name citext NOT NULL
    );

    -- Populate TmpMangerList using ParseManagerNameList
    --
    Call mc.parse_manager_name_list (_managerNameList, _removeUnknownManagers => 1, _message => _message);

    IF NOT EXISTS (SELECT * FROM TmpManagerList) THEN
        _message := 'No valid managers were found in _managerNameList';
        RAISE INFO '%', _message;
        Return;
    END IF;

    -- Set _newValue based on _enable
    If _enable = 0 Then
        _newValue := 'False';
        _activeStateDescription := 'run jobs locally';
    Else
        _newValue := 'True';
        _activeStateDescription := 'run jobs remotely';
    End If;

    If Exists (Select * From TmpManagerList Where manager_name = 'Default_AnalysisMgr_Params') Then
        Delete From TmpManagerList Where manager_name = 'Default_AnalysisMgr_Params';

        _message := 'For safety, not updating RunJobsRemotely for manager Default_AnalysisMgr_Params';

        If Exists (Select * From TmpManagerList) Then
            -- TmpManagerList contains other managers; update them
            RAISE INFO '%', _message;
        Else
            -- TmpManagerList is now empty; abort
            RAISE INFO '%', _message
            Return;
        End If;
    End If;

    If _addMgrParamsIfMissing > 0 Then
        -- <a>
        FOR _mgrRecord IN
            SELECT U.manager_name,
                   M.mgr_id
            FROM TmpManagerList U
                 INNER JOIN mc.t_mgrs M
                   ON U.manager_name = M.mgr_name
            ORDER BY U.manager_name
        LOOP

            _mgrName := _mgrRecord.manager_name;
            _mgrId   := _mgrRecord.mgr_id;

            If Not Exists (SELECT * FROM mc.v_mgr_params Where ParameterName = 'RunJobsRemotely' And ManagerName = _mgrName) Then
                -- <d1>

                SELECT param_id INTO _paramTypeId
                FROM mc.t_param_type
                Where param_name = 'RunJobsRemotely';

                If Coalesce(_paramTypeId, 0) = 0 Then
                    RAISE WARNING '%', 'Error: could not find parameter "RunJobsRemotely" in mc.t_param_type';
                Else
                    If _infoOnly > 0 Then
                        RAISE INFO '%', 'Would create parameter RunJobsRemotely for Manager ' || _mgrName || ', value ' || _newValue;

                        -- Actually do go ahead and create the parameter, but use a value of False even if _newValue is True
                        -- We need to do this so the managers are included in the query below with PT.ParamName = 'RunJobsRemotely'
                        Insert Into mc.t_param_value (mgr_id, type_id, value)
                        Values (_mgrId, _paramTypeId, 'False');
                    Else
                        Insert Into mc.t_param_value (mgr_id, type_id, value)
                        Values (_mgrId, _paramTypeId, _newValue);
                    End If;
                End If;
            End If; -- </d1>

            If Not Exists (SELECT * FROM mc.v_mgr_params Where ParameterName = 'RemoteHostName' And ManagerName = _mgrName) Then
                -- <d2>

                SELECT param_id INTO _paramTypeId
                FROM mc.t_param_type
                Where param_name = 'RemoteHostName';

                If Coalesce(_paramTypeId, 0) = 0 Then
                    RAISE WARNING '%', 'Error: could not find parameter "RemoteHostName" in mc.t_param_type';
                Else
                    If _infoOnly > 0 Then
                        RAISE INFO '%', 'Would create parameter RemoteHostName  for Manager ' || _mgrName || ', value PrismWeb2';
                    Else
                        Insert Into mc.t_param_value (mgr_id, type_id, value)
                        Values (_mgrId, _paramTypeId, 'PrismWeb2');
                    End If;
                End If;
            End If; -- </d1>

        End Loop;

        If _infoOnly > 0 Then
            RAISE INFO '%', '';
        End If;

    End If; -- </a>

    -- Count the number of managers that need to be updated
    --
    SELECT COUNT(*) INTO _countToUpdate
    FROM mc.t_param_value PV
         INNER JOIN mc.t_param_type PT
           ON PV.type_id = PT.param_id
         INNER JOIN mc.t_mgrs M
           ON PV.mgr_id = M.mgr_id
         INNER JOIN mc.t_mgr_types MT
           ON M.mgr_type_id = MT.mgr_type_id
         INNER JOIN TmpManagerList U
           ON M.mgr_name = U.manager_name
    WHERE PT.param_name = 'RunJobsRemotely' AND
          PV.value <> _newValue AND
          MT.mgr_type_active > 0;
    --
    GET DIAGNOSTICS _myRowCount = ROW_COUNT;

    -- Count the number of managers already in the target state
    --
    SELECT COUNT(*) INTO _countUnchanged
    FROM mc.t_param_value PV
         INNER JOIN mc.t_param_type PT
           ON PV.type_id = PT.param_id
         INNER JOIN mc.t_mgrs M
           ON PV.mgr_id = M.mgr_id
         INNER JOIN mc.t_mgr_types MT
           ON M.mgr_type_id = MT.mgr_type_id
         INNER JOIN TmpManagerList U
           ON M.mgr_name = U.manager_name
    WHERE PT.param_name = 'RunJobsRemotely' AND
          PV.value = _newValue AND
          MT.mgr_type_active > 0;
    --
    GET DIAGNOSTICS _myRowCount = ROW_COUNT;

    _countToUpdate  := COALESCE(_countToUpdate, 0);
    _countUnchanged := COALESCE(_countUnchanged, 0);

    If _countToUpdate = 0 Then
        If _countUnchanged = 0 Then
            If _addMgrParamsIfMissing = 0 THEN
                _message := 'None of the managers in _managerNameList has parameter "RunJobsRemotely" defined; use _addMgrParamsIfMissing := 1 to auto-add it';
            Else
                _message := 'No managers were found matching _managerNameList';
            End If;
        Else
            If _countUnchanged = 1 Then
                _message := 'The manager is already set to ' || _activeStateDescription;
            Else
                _message := 'All ' || _countUnchanged::text || ' managers are already set to ' || _activeStateDescription;
            End If;
        End If;

        RAISE INFO '%', _message;
        Return;
    End If;

    If _infoOnly <> 0 Then

        _infoHead := format('%-22s %-17s %-20s',
                            'State Change Preview',
                            'Parameter Name',
                            'Manager Name'
                        );

        RAISE INFO '%', _infoHead;

        FOR _previewData IN
            SELECT PV.value || ' --> ' || _newValue AS State_Change_Preview,
                   PT.param_name AS Parameter_Name,
                   M.mgr_name AS manager_name
            FROM mc.t_param_value PV
                 INNER JOIN mc.t_param_type PT
                   ON PV.type_id = PT.param_id
                 INNER JOIN mc.t_mgrs M
                   ON PV.mgr_id = M.mgr_id
                 INNER JOIN mc.t_mgr_types MT
                   ON M.mgr_type_id = MT.mgr_type_id
                 INNER JOIN TmpManagerList U
                   ON M.mgr_name = U.manager_name
            WHERE PT.param_name = 'RunJobsRemotely' AND
                  PV.value <> _newValue AND
                  MT.mgr_type_active > 0
        LOOP

            _infoData := format('%-22s %-17s %-20s',
                                    _previewData.State_Change_Preview,
                                    _previewData.Parameter_Name,
                                    _previewData.manager_name
                            );

            RAISE INFO '%', _infoData;

        END LOOP;

        _message := format('Would set %s managers to have RunJobsRemotely set to %s; see the Output window for details',
                            _countToUpdate,
                            _newValue);

        Return;
    End If;

    -- Update RunJobsRemotely for the managers in TmpManagerList
    --
    UPDATE mc.t_param_value
    SET value = _newValue
    FROM mc.t_param_value PV
         INNER JOIN mc.t_param_type PT
           ON PV.type_id = PT.param_id
         INNER JOIN mc.t_mgrs M
           ON PV.mgr_id = M.mgr_id
         INNER JOIN mc.t_mgr_types MT
           ON M.mgr_type_id = MT.mgr_type_id
         INNER JOIN TmpManagerList U
           ON M.mgr_name = U.manager_name
    WHERE mc.t_param_value.entry_ID = PV.Entry_ID AND
          PT.param_name = 'RunJobsRemotely' AND
          PV.value <> _newValue AND
          MT.mgr_type_active > 0;
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

    RAISE INFO '%', _message;

EXCEPTION
    WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS
            _sqlstate = returned_sqlstate,
            _exceptionMessage = message_text,
            _exceptionContext = pg_exception_context;

    _message := 'Error enabling/disabling managers to run jobs remotely: ' || _exceptionMessage;
    _returnCode := _sqlstate;

    RAISE Warning 'Error: %', _message;
    RAISE warning '%', _exceptionContext;

    Call PostLogEntry ('Error', _message, 'EnableDisableRunJobsRemotely', 'mc');

END
$$;

COMMENT ON PROCEDURE mc.enable_disable_run_jobs_remotely IS 'EnableDisableRunJobsRemotely';

CREATE OR REPLACE PROCEDURE mc.get_default_remote_info_for_manager
(
    _managerName text,
    INOUT _remoteInfoXML text = ''
)
LANGUAGE plpgsql
AS $$
/****************************************************
**
**  Desc:
**      Gets the default remote info parameters for the given manager
**      Retrieves parameters using GetManagerParametersWork, so properly retrieves parent group parameters, if any

**      If the manager does not have parameters RunJobsRemotely and RemoteHostName defined, returns an empty string
**      Also returns an empty string if RunJobsRemotely is not True
**
**      Example value for _remoteInfoXML
**      <host>prismweb2</host><user>svc-dms</user><taskQueue>/file1/temp/DMSTasks</taskQueue><workDir>/file1/temp/DMSWorkDir</workDir><orgDB>/file1/temp/DMSOrgDBs</orgDB><privateKey>Svc-Dms.key</privateKey><passphrase>Svc-Dms.pass</passphrase>
**
**  Arguments:
**    _managerName     Manager name
**    _remoteInfoXML   Output XML if valid remote info parameters are defined, otherwise an empty string
**
**  Auth:   mem
**  Date:   05/18/2017 mem - Initial version
**          03/14/2018 mem - Use GetManagerParametersWork to lookup manager parameters, allowing for getting remote info parameters from parent groups
**          03/29/2018 mem - Return an empty string if the manager does not have parameters RunJobsRemotely and RemoteHostName defined, or if RunJobsRemotely is false
**          02/05/2020 mem - Ported to PostgreSQL
**
*****************************************************/
DECLARE
    _myRowCount int := 0;
    _managerID int := 0;
    _message text;
BEGIN

    _remoteInfoXML := '';

    SELECT mgr_id INTO _managerID
    FROM mc.t_mgrs
    WHERE mgr_name = _managerName;

    If Not Found Then
        -- Manager not found; this is not an error
        Return;
    End If;

    -----------------------------------------------
    -- Create the Temp Table to hold the manager parameters
    -----------------------------------------------

    DROP TABLE IF EXISTS Tmp_Mgr_Params;

    CREATE TEMP TABLE Tmp_Mgr_Params (
        mgr_name text NOT NULL,
        param_name text NOT NULL,
        entry_id int NOT NULL,
        type_id int NOT NULL,
        value text NOT NULL,
        mgr_id int NOT NULL,
        comment text NULL,
        last_affected timestamp NULL,
        entered_by text NULL,
        mgr_type_id int NOT NULL,
        ParentParamPointerState int,
        source text NOT NULL
    );

    -- Populate the temporary table with the manager parameters
    Call mc.get_manager_parameters_work (_managerName, 0, 50, _message => _message);

    If Not Exists ( SELECT value
                    FROM Tmp_Mgr_Params
                    WHERE mgr_name = _managerName And
                          param_name = 'RunJobsRemotely' AND
                          value = 'True' )
       OR
       Not Exists ( SELECT value
                    FROM Tmp_Mgr_Params
                    WHERE mgr_name = _managerName And
                          param_name = 'RemoteHostName' AND
                          char_length(value) > 0 )  Then

        Return;
    End If;

    -- Concatenate together the parameters to build up the XML
    --
    _remoteInfoXML := '';

    SELECT _remoteInfoXML ||
         '<host>' || value || '</host>' INTO _remoteInfoXML
    FROM Tmp_Mgr_Params
    WHERE (param_name = 'RemoteHostName' And mgr_name = _managerName);

    SELECT _remoteInfoXML ||
         '<user>' || value || '</user>' INTO _remoteInfoXML
    FROM Tmp_Mgr_Params
    WHERE (param_name = 'RemoteHostUser' And mgr_name = _managerName);

    SELECT _remoteInfoXML ||
         '<dmsPrograms>' || value || '</dmsPrograms>' INTO _remoteInfoXML
    FROM Tmp_Mgr_Params
    WHERE (param_name = 'RemoteHostDMSProgramsPath' And mgr_name = _managerName);

    SELECT _remoteInfoXML ||
         '<taskQueue>' || value || '</taskQueue>' INTO _remoteInfoXML
    FROM Tmp_Mgr_Params
    WHERE (param_name = 'RemoteTaskQueuePath' And mgr_name = _managerName);

    SELECT _remoteInfoXML ||
         '<workDir>' || value || '</workDir>' INTO _remoteInfoXML
    FROM Tmp_Mgr_Params
    WHERE (param_name = 'RemoteWorkDirPath' And mgr_name = _managerName);

    SELECT _remoteInfoXML ||
         '<orgDB>' || value || '</orgDB>' INTO _remoteInfoXML
    FROM Tmp_Mgr_Params
    WHERE (param_name = 'RemoteOrgDBPath' And mgr_name = _managerName);

    SELECT _remoteInfoXML ||
         '<privateKey>' || public.udf_get_filename(value) || '</privateKey>' INTO _remoteInfoXML
    FROM Tmp_Mgr_Params
    WHERE (param_name = 'RemoteHostPrivateKeyFile' And mgr_name = _managerName);

    SELECT _remoteInfoXML ||
         '<passphrase>' || public.udf_get_filename(value) || '</passphrase>' INTO _remoteInfoXML
    FROM Tmp_Mgr_Params
    WHERE (param_name = 'RemoteHostPassphraseFile' And mgr_name = _managerName);

END
$$;

COMMENT ON PROCEDURE mc.get_default_remote_info_for_manager IS 'GetDefaultRemoteInfoForManager';


CREATE OR REPLACE FUNCTION mc.get_manager_parameters
(
    _managerNameList text = '',
    _sortMode int = 0,
    _maxRecursion int = 10
)
RETURNS TABLE
(
    mgr_name text,
    param_name text,
    entry_id int,
    type_id int,
    value text,
    mgr_id int,
    comment text,
    last_affected timestamp,
    entered_by text,
    mgr_type_id int,
    ParentParamPointerState int,
    source text
)
LANGUAGE plpgsql
AS $$
/****************************************************
**
**  Desc:
**      Gets the parameters for the given analysis manager(s)
**      Uses MgrSettingGroupName to lookup parameters from the parent group, if any
**
**  Arguments:
**    _sortMode   0 means sort by type_id,     mgr_name
**                1 means sort by param_name,  mgr_name
**                2 means sort by mgr_name,    param_name
**                3 means sort by value,       param_name
**
**  Auth:   mem
**  Date:   05/07/2015 mem - Initial version
**          08/10/2015 mem - Add _sortMode=3
**          09/02/2016 mem - Increase the default for parameter _maxRecursion from 5 to 50
**          03/14/2018 mem - Refactor actual parameter lookup into stored procedure GetManagerParametersWork
**          02/05/2020 mem - Ported to PostgreSQL
**
*****************************************************/
DECLARE
    _message text;
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

    DROP TABLE IF EXISTS Tmp_Mgr_Params;

    CREATE TEMP TABLE Tmp_Mgr_Params (
        mgr_name text NOT NULL,
        param_name text NOT NULL,
        entry_id int NOT NULL,
        type_id int NOT NULL,
        value text NOT NULL,
        mgr_id int NOT NULL,
        comment text NULL,
        last_affected timestamp NULL,
        entered_by text NULL,
        mgr_type_id int NOT NULL,
        ParentParamPointerState int,
        source text NOT NULL
    );

    -- Populate the temporary table with the manager parameters
    Call mc.get_manager_parameters_work (_managerNameList, _sortMode, _maxRecursion, _message => _message);

    -- Return the parameters as a result set
    --
    If _sortMode = 0 Then
        RETURN QUERY
        SELECT P.mgr_name, P.param_name, P.entry_id,
               P.type_id, P.value, P.mgr_id,
               P.comment, P.last_affected, P.entered_by,
               P.mgr_type_id, P.ParentParamPointerState, P.source
        FROM Tmp_Mgr_Params P
        ORDER BY P.type_id, P.mgr_name;
        Return;
    End If;

    If _sortMode = 1 Then
        RETURN QUERY
        SELECT P.mgr_name, P.param_name, P.entry_id,
               P.type_id, P.value, P.mgr_id,
               P.comment, P.last_affected, P.entered_by,
               P.mgr_type_id, P.ParentParamPointerState, P.source
        FROM Tmp_Mgr_Params P
        ORDER BY P.param_name, P.mgr_name;
        Return;
    End If;

    If _sortMode = 2 Then
        RETURN QUERY
        SELECT P.mgr_name, P.param_name, P.entry_id,
               P.type_id, P.value, P.mgr_id,
               P.comment, P.last_affected, P.entered_by,
               P.mgr_type_id, P.ParentParamPointerState, P.source
        FROM Tmp_Mgr_Params P
        ORDER BY P.mgr_name, P.param_name;
        Return;
    End If;

    RETURN QUERY
    SELECT P.mgr_name, P.param_name, P.entry_id,
           P.type_id, P.value, P.mgr_id,
           P.comment, P.last_affected, P.entered_by,
           P.mgr_type_id, P.ParentParamPointerState, P.source
    FROM Tmp_Mgr_Params P
    ORDER BY P.value, P.param_name;

END
$$;

COMMENT ON FUNCTION mc.get_manager_parameters IS 'GetManagerParameters';

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
**  Desc:
**      Populates temporary tables with the parameters for the given analysis manager(s)
**      Uses MgrSettingGroupName to lookup parameters from the parent group, if any
**
**  Requires that the calling procedure create temporary table Tmp_Mgr_Params
**
**  Arguments:
**    _sortMode   0 means sort by ParamTypeID then mgr_name,
**                1 means param_name, then mgr_name,
**                2 means mgr_name, then param_name,
**                3 means value then param_name
**
**  Auth:   mem
**  Date:   03/14/2018 mem - Initial version (code refactored from GetManagerParameters)
**          02/05/2020 mem - Ported to PostgreSQL
**
*****************************************************/
DECLARE
    _myRowCount int := 0;
    _iterations int := 0;
BEGIN

    _message := '';

    -----------------------------------------------
    -- Create the Temp Table to hold the manager group information
    -----------------------------------------------

    DROP TABLE IF EXISTS Tmp_Manager_Group_Info;

    CREATE TEMP TABLE Tmp_Manager_Group_Info (
        mgr_name text NOT NULL,
        Group_Name text NOT NULL
    );

    -----------------------------------------------
    -- Lookup the initial manager parameters
    -----------------------------------------------
    --

    INSERT INTO Tmp_Mgr_Params(  mgr_name,
                                 param_name,
                                 entry_id,
                                 type_id,
                                 value,
                                 mgr_id,
                                 comment,
                                 last_affected,
                                 entered_by,
                                 mgr_type_id,
                                 ParentParamPointerState,
                                 source )
    SELECT mgr_name,
           param_name,
           entry_id,
           type_id,
           value,
           mgr_id,
           comment,
           last_affected,
           entered_by,
           mgr_type_id,
           CASE
               WHEN type_id = 162 THEN 1        -- param_name 'Default_AnalysisMgr_Params'
               ELSE 0
           End As ParentParamPointerState,
           mgr_name
    FROM mc.v_param_value
    WHERE (mgr_name IN (Select value From public.udf_parse_delimited_list(_managerNameList, ',')));
    --
    GET DIAGNOSTICS _myRowCount = ROW_COUNT;

    -----------------------------------------------
    -- Append parameters for parent groups, which are
    -- defined by parameter Default_AnalysisMgr_Params (type_id 162)
    -----------------------------------------------
    --

    While Exists (Select * from Tmp_Mgr_Params Where ParentParamPointerState = 1) And _iterations < _maxRecursion
    Loop
        Truncate table Tmp_Manager_Group_Info;

        INSERT INTO Tmp_Manager_Group_Info (mgr_name, Group_Name)
        SELECT mgr_name, value
        FROM Tmp_Mgr_Params
        WHERE ParentParamPointerState = 1;

        UPDATE Tmp_Mgr_Params
        Set ParentParamPointerState = 2
        WHERE ParentParamPointerState = 1;

        INSERT INTO Tmp_Mgr_Params( mgr_name,
                                     param_name,
                                     entry_id,
                                     type_id,
                                     value,
                                     mgr_id,
                                     comment,
                                     last_affected,
                                     entered_by,
                                     mgr_type_id,
                                     ParentParamPointerState,
                                     source )
        SELECT ValuesToAppend.mgr_name,
               ValuesToAppend.param_name,
               ValuesToAppend.entry_id,
               ValuesToAppend.type_id,
               ValuesToAppend.value,
               ValuesToAppend.mgr_id,
               ValuesToAppend.comment,
               ValuesToAppend.last_affected,
               ValuesToAppend.entered_by,
               ValuesToAppend.mgr_type_id,
               CASE
                   WHEN ValuesToAppend.type_id = 162 THEN 1
                   ELSE 0
               End As ParentParamPointerState,
               ValuesToAppend.source
        FROM Tmp_Mgr_Params Target
             RIGHT OUTER JOIN ( SELECT FilterQ.mgr_name,
                                       PV.param_name,
                                       PV.entry_id,
                                       PV.type_id,
                                       PV.value,
                                       PV.mgr_id,
                                       PV.comment,
                                       PV.last_affected,
                                       PV.entered_by,
                                       PV.mgr_type_id,
                                       PV.mgr_name AS source
                                FROM mc.v_param_value PV
                                     INNER JOIN ( SELECT mgr_name,
                                                         Group_Name
                                                  FROM Tmp_Manager_Group_Info ) FilterQ
                                       ON PV.mgr_name = FilterQ.Group_Name ) ValuesToAppend
               ON Target.mgr_name = ValuesToAppend.mgr_name AND
                  Target.type_id = ValuesToAppend.type_id
        WHERE (Target.type_id IS NULL Or ValuesToAppend.type_id = 162);
        --
        GET DIAGNOSTICS _myRowCount = ROW_COUNT;

        -- This is a safety check in case a manager has a Default_AnalysisMgr_Params value pointing to itself
        _iterations := _iterations + 1;

    END LOOP;

    Drop Table Tmp_Manager_Group_Info;

END
$$;


COMMENT ON PROCEDURE mc.get_manager_parameters_work IS 'GetManagerParametersWork';


CREATE OR REPLACE PROCEDURE mc.parse_manager_name_list
(
    _managerNameList text = '',
    _removeUnknownManagers int = 1,
    INOUT _message text = ''
)
LANGUAGE plpgsql
AS $$
/****************************************************
**
**  Desc:
**      Parses the list of managers in _managerNameList
**       and populates a temporary table with the manager names
**
**      The calling procedure must create a temporary table (the table can contain additional columns)
**        CREATE TEMP TABLE TmpManagerList (
**          manager_name text NOT NULL
**        )
**
**  Arguments:
**    _managerNameList          One or more manager names (comma-separated list); supports wildcards
**    _removeUnknownManagers    When 1, delete manager names that are not defined in _removeUnknownManagers
**    _message                  Output message
**
**  Auth:   mem
**  Date:   05/09/2008
**          05/14/2015 mem - Update Insert query to explicitly list field Manager_Name
**          01/28/2020 mem - Ported to PostgreSQL
**          02/04/2020 mem - Rename manager name column mgr_name
**
*****************************************************/
DECLARE
    _myRowCount int := 0;
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

    DROP TABLE IF EXISTS TmpManagerSpecList;

    CREATE TEMP TABLE TmpManagerSpecList (
        entry_id int PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
        manager_name text NOT NULL
    );

    -----------------------------------------------
    -- Parse _managerNameList
    -----------------------------------------------

    If char_length(_managerNameList) = 0 Then
        Return;
    End If;

    -- Populate TmpManagerSpecList with the data in _managerNameList
    INSERT INTO TmpManagerSpecList (manager_name)
    SELECT value
    FROM public.udf_parse_delimited_list(_managerNameList, ',');

    -- Populate TmpManagerList with the entries in TmpManagerSpecList that do not contain a % wildcard
    INSERT INTO TmpManagerList (manager_name)
    SELECT manager_name
    FROM TmpManagerSpecList
    WHERE NOT manager_name SIMILAR TO '%[%]%' AND NOT manager_name SIMILAR TO '%\[%';
    --
    GET DIAGNOSTICS _myRowCount = ROW_COUNT;

    -- Delete the non-wildcard entries from TmpManagerSpecList
    --
    DELETE FROM TmpManagerSpecList target
    WHERE NOT target.manager_name SIMILAR TO '%[%]%' AND NOT manager_name SIMILAR TO '%\[%';
    --
    GET DIAGNOSTICS _myRowCount = ROW_COUNT;

    -- Parse the entries in TmpManagerSpecList (all should have a wildcard)
    --
    For _managerFilter In
        SELECT manager_name
        FROM TmpManagerSpecList
        ORDER BY Entry_ID
    Loop
        _s := format(
                'INSERT INTO TmpManagerList (manager_name) ' ||
                'SELECT mgr_name ' ||
                'FROM mc.t_mgrs ' ||
                'WHERE mgr_name SIMILAR TO $1');

        EXECUTE _s USING _managerFilter;

        _s := regexp_replace(_s, '\$1', '''' || _managerFilter || '''');
        RAISE Info '%', _s;

    End Loop;

    If _removeUnknownManagers = 0 Then
        Return;
    End If;

    -- Delete entries from TmpManagerList that are not defined in mc.t_mgrs
    --
    DELETE FROM TmpManagerList
    WHERE NOT manager_name IN (SELECT mgr_name FROM mc.t_mgrs);
    --
    GET DIAGNOSTICS _myRowCount = ROW_COUNT;

    If _myRowCount > 0 Then
        _message := 'Found ' || _myRowCount || ' entries in _managerNameList that are not defined in mc.t_mgrs';
        RAISE INFO '%', _message;

        _message := '';
    End If;

END
$procedure$
;

COMMENT ON PROCEDURE mc.parse_manager_name_list IS 'ParseManagerNameList';

CREATE OR REPLACE PROCEDURE public.PostLogEntry
(
    _type text,
    _message text,
    _postedBy text = 'na',
    _targetSchema text = 'public',
    _duplicateEntryHoldoffHours int = 0

)
LANGUAGE plpgsql
AS $$
/****************************************************
**
**  Desc:
**      Append a log entry to T_Log_Entries, either in the public schema or the specified schema
**
**  Arguments:
**    _type                         Message type, typically Normal, Warning, Error, or Progress, but can be any text value
**    _message                      Log message
**    _postedBy                     Name of the calling procedure
**    _targetSchema                 If blank or 'public', log to public.T_Log_Entries; otherwise, log to T_Log_Entries for the given schema (assumes the table exists)
**    _duplicateEntryHoldoffHours   Set this to a value greater than 0 to prevent duplicate entries being posted within the given number of hours
**
**  Auth:   grk
**  Date:   01/26/2001
**          06/08/2006 grk - added logic to put data extraction manager stuff in analysis log
**          03/30/2009 mem - Added parameter _duplicateEntryHoldoffHours
**                         - Expanded the size of _type, _message, and _postedBy
**          07/20/2009 grk - eliminate health log (http://prismtrac.pnl.gov/trac/ticket/742)
**          09/13/2010 mem - Eliminate analysis log
**                         - Auto-update _duplicateEntryHoldoffHours to be 24 when the log type is Health or Normal and the source is the space manager
**          02/27/2017 mem - Although _message is varchar(4096), the Message column in T_Log_Entries may be shorter (512 characters in DMS); disable ANSI Warnings before inserting into the table
**          01/28/2020 mem - Ported to PostgreSQL
**
*****************************************************/
DECLARE
    _targetTableWithSchema text;
    _minimumPostingTime timestamp;
    _duplicateRowCount int := 0;
    _s text;
    _myRowCount int;
    _warningMessage text;
BEGIN

    ------------------------------------------------
    -- Validate the inputs
    ------------------------------------------------

    _targetSchema := COALESCE(_targetSchema, '');
    If (char_length(_targetSchema) = 0) Then
        _targetSchema := 'public';
    End If;

    _targetTableWithSchema := format('%I.%I', _targetSchema, 't_log_entries');

    _type := Coalesce(_type, 'Normal');
    _message := Coalesce(_message, '');
    _postedBy := Coalesce(_postedBy, 'na');

    If _postedBy ILike 'Space%' And _type::citext In ('Health', 'Normal') Then
        -- Auto-update _duplicateEntryHoldoffHours to be 24 if it is zero
        -- Otherwise we get way too many health/status log entries

        If _duplicateEntryHoldoffHours = 0 Then
            _duplicateEntryHoldoffHours := 24;
        End If;
    End If;

    _minimumPostingTime = CURRENT_TIMESTAMP - (_duplicateEntryHoldoffHours || ' hours')::INTERVAL;

    If Coalesce(_duplicateEntryHoldoffHours, 0) > 0 Then
        _s := format(
                'SELECT COUNT(*) '
                'FROM %s '
                'WHERE message = $1 AND '
                     ' type = $2 AND '
                     ' posting_time >= $3',
                _targetTableWithSchema);

        EXECUTE _s INTO _duplicateRowCount USING _message, _type, _minimumPostingTime;

    End If;

    If _duplicateRowCount > 0 THEN
        RAISE Info 'Skipping recently logged message; duplicate count: %', _duplicateRowCount;
        RETURN;
    End If;

    _s := format(
            'INSERT INTO %s (posted_by, posting_time, type, message)'
            'VALUES ( $1, CURRENT_TIMESTAMP, $2, $3)',
            _targetTableWithSchema);

    EXECUTE _s USING _postedBy, _type, _message;
    --
    GET DIAGNOSTICS _myRowCount = ROW_COUNT;

    If _myRowCount = 0 Then
        _warningMessage := 'Warning: log message not added to ' || _targetTableWithSchema;
        RAISE WARNING '%', _warningMessage;
    End If;

END
$$;

COMMENT ON PROCEDURE public.PostLogEntry IS 'PostLogEntry';


CREATE OR REPLACE PROCEDURE public.PostUsageLogEntry
(
    _postedBy text,
    _message text = '',
    _minimumUpdateInterval int = 1
)
LANGUAGE plpgsql
AS $$
/****************************************************
**
**  Desc:
**       Put new entry into T_Usage_Log and update T_Usage_Stats
**
**  Arguments:
**    _postedBy                Calling procedure name
**    _message                 Usage message
**    _minimumUpdateInterval   Set to a value greater than 0 to limit the entries to occur at most every _minimumUpdateInterval hours
**
**  Auth:   mem
**  Date:   10/22/2004
**          07/29/2005 mem - Added parameter _minimumUpdateInterval
**          03/16/2006 mem - Now updating T_Usage_Stats
**          03/17/2006 mem - Now populating Usage_Count in T_Usage_Log and changed _minimumUpdateInterval from 6 hours to 1 hour
**          05/03/2009 mem - Removed parameter _dBName
**          02/06/2020 mem - Ported to PostgreSQL
**
*****************************************************/
DECLARE
    _currentTargetTable text := 'Undefined';
    _currentOperation text := 'initializing';
    _callingUser text := session_user;
    _lastUpdated timestamp;
    _sqlstate text;
    _exceptionMessage text;
    _exceptionContext text;
BEGIN

    _currentTargetTable := 't_usage_stat';

    -- Update entry for _postedBy in t_usage_stats
    --
    If Not Exists (SELECT posted_by FROM t_usage_stats WHERE posted_by = _postedBy) THEN
        _currentOperation := 'appending to';

        INSERT INTO t_usage_stats (posted_by, last_posting_time, usage_count)
        VALUES (_postedBy, CURRENT_TIMESTAMP, 1);
    Else
        _currentOperation := 'updating';

        UPDATE t_usage_stats
        SET last_posting_time = CURRENT_TIMESTAMP, usage_count = usage_count + 1
        WHERE posted_by = _postedBy;
    End If;

    _currentTargetTable := 't_usage_log';
    _currentOperation := 'selecting';

    If _minimumUpdateInterval > 0 Then
        -- See if the last update was less than _minimumUpdateInterval hours ago

        SELECT MAX(posting_time) INTO _lastUpdated
        FROM t_usage_log
        WHERE posted_by = _postedBy AND calling_user = _callingUser;

        IF Found Then
            If CURRENT_TIMESTAMP <= _lastUpdated + _minimumUpdateInterval * INTERVAL '1 hour' Then
                -- The last usage message was posted recently
                Return;
            End If;
        End If;
    End If;

    _currentOperation := 'appending to';

    INSERT INTO t_usage_log
            (posted_by, posting_time, message, calling_user, usage_count)
    SELECT _postedBy, CURRENT_TIMESTAMP, _message, _callingUser, stats.usage_count
    FROM t_usage_stats stats
    WHERE stats.posted_by = _postedBy;

EXCEPTION
    WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS
            _sqlstate = returned_sqlstate,
            _exceptionMessage = message_text,
            _exceptionContext = pg_exception_context;

    _message := format('Error %s %s: %s',
                _currentOperation, _currentTargetTable, _exceptionMessage);

    RAISE Warning '%', _message;
    RAISE warning '%', _exceptionContext;

    Call PostLogEntry ('Error', _message, 'PostUsageLogEntry', 'public');

END
$$;

COMMENT ON PROCEDURE public.PostUsageLogEntry IS 'PostUsageLogEntry';

CREATE OR REPLACE PROCEDURE mc.report_manager_error_cleanup
(
    _managerName text,
    _state int = 0,
    _failureMsg text = '',
    INOUT _message text = '',
    INOUT _returncode text = ''
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
**          02/07/2020 mem - Ported to PostgreSQL
**
*****************************************************/
DECLARE
    _myRowCount int := 0;
    _mgrInfo record;
    _mgrID int;
    _paramID int;
    _messageType text;
    _cleanupMode text;
    _sqlstate text;
    _exceptionMessage text;
    _exceptionContext text;
BEGIN
    ---------------------------------------------------
    -- Cleanup the inputs
    ---------------------------------------------------

    _managerName := Coalesce(_managerName, '');
    _state := Coalesce(_state, 0);
    _failureMsg := Coalesce(_failureMsg, '');
    _message := '';
    _returncode := '';

    ---------------------------------------------------
    -- Confirm that the manager name is valid
    ---------------------------------------------------

    SELECT mgr_id, mgr_name INTO _mgrInfo
    FROM mc.t_mgrs
    WHERE mgr_name = _managerName;

    If Not Found Then
        _message := 'Could not find entry for manager: ' || _managerName;
        _returncode := 'U5202';
        Return;
    End If;

    _mgrID       := _mgrInfo.mgr_id;
    _managerName := _mgrInfo.mgr_name;

    ---------------------------------------------------
    -- Validate _state
    ---------------------------------------------------

    If _state < 1 Or _state > 3 Then
        _message := 'Invalid value for _state; should be 1, 2 or 3, not ' || _state;
        _returncode := 'U5203';
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

    Call PostLogEntry (_messageType, _message, 'ReportManagerErrorCleanup', 'mc');

    ---------------------------------------------------
    -- Lookup the value of ManagerErrorCleanupMode in mc.t_param_value
    ---------------------------------------------------

    SELECT PV.value INTO _cleanupMode
    FROM mc.t_param_value PV
         INNER JOIN mc.t_param_type PT
           ON PV.type_id = PT.param_id
    WHERE PT.param_name = 'ManagerErrorCleanupMode' AND
          PV.mgr_id = _mgrID;

    If Not Found Then
        -- Entry not found; make a new entry for 'ManagerErrorCleanupMode' in the mc.t_param_value table

        SELECT param_id INTO _paramID
        FROM mc.t_param_type
        WHERE param_name = 'ManagerErrorCleanupMode';

        If Found Then
            INSERT INTO mc.t_param_value (mgr_id, type_id, value)
            VALUES (_mgrID, _paramID, '0');

            _cleanupMode := '0';
        End If;
    End If;

    If Trim(_cleanupMode) = '1' Then

        -- Manager is set to auto-cleanup only once; change 'ManagerErrorCleanupMode' to 0
        --
        UPDATE mc.t_param_value
        SET value = '0'
        WHERE entry_id IN (
            SELECT PV.entry_id
            FROM mc.t_param_value PV
                 INNER JOIN mc.t_param_type PT
                   ON PV.type_id = PT.param_id
            WHERE PT.param_name = 'ManagerErrorCleanupMode' AND
                  PV.mgr_id = _mgrID);

        If Not Found Then
            _message := _message || '; Entry not found in mc.t_param_value for ManagerErrorCleanupMode; this is unexpected';
        Else
            _message := _message || '; Changed ManagerErrorCleanupMode to 0 in mc.t_param_value';
        End If;
    End If;

EXCEPTION
    WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS
            _sqlstate = returned_sqlstate,
            _exceptionMessage = message_text,
            _exceptionContext = pg_exception_context;

    _message := 'Error updating ManagerErrorCleanupMode in mc.t_param_value: ' || _exceptionMessage;
    _returnCode := _sqlstate;

    RAISE Warning 'Error: %', _message;
    RAISE warning '%', _exceptionContext;

    Call PostLogEntry ('Error', _message, 'ReportManagerErrorCleanup', 'mc');

END
$$;

COMMENT ON PROCEDURE mc.report_manager_error_cleanup IS 'ReportManagerErrorCleanup';

CREATE OR REPLACE PROCEDURE mc.set_manager_update_required
(
    _mgrList text = '',
    _showTable int = 0,
    _infoOnly int = 0,
    INOUT _message text = '',
    INOUT _returncode text = ''
)
LANGUAGE plpgsql
AS $$
/****************************************************
**
**  Desc:
**      Sets ManagerUpdateRequired to true for the given list of managers
**      If _managerList is blank, then sets it to true for all "Analysis Tool Manager" managers
**
**  Arguments:
**    _mgrList   Comma separated list of manager names; supports wildcards. If blank, selects all managers of type 11 (Analysis Tool Manager)
**
**  Auth:   mem
**  Date:   01/24/2009 mem - Initial version
**          04/17/2014 mem - Expanded _managerList to varchar(max) and added parameter _showTable
**          02/08/2020 mem - Ported to PostgreSQL
**
*****************************************************/
DECLARE
    _myRowCount int := 0;
    _mgrID int;
    _paramTypeID int;
    _previewData record;
    _countToUpdate int;
    _infoHead text;
    _infoData text;
    _sqlstate text;
    _exceptionMessage text;
    _exceptionContext text;
BEGIN
    ---------------------------------------------------
    -- Validate the inputs
    ---------------------------------------------------

    _mgrList := Coalesce(_mgrList, '');
    _showTable := Coalesce(_showTable, 0);
    _infoOnly := Coalesce(_infoOnly, 0);
    _message := '';
    _returnCode := '';

    DROP TABLE IF EXISTS TmpManagerList;

    CREATE TEMP TABLE TmpManagerList (
        manager_name citext NOT NULL,
        mgr_id int NULL
    );

    If char_length(_mgrList) > 0 AND _mgrList <> '%' Then
        ---------------------------------------------------
        -- Populate TmpManagerList with the managers in _mgrList
        ---------------------------------------------------
        --
        Call mc.parse_manager_name_list (_mgrList, _removeUnknownManagers => 1, _message => _message);

        IF NOT EXISTS (SELECT * FROM TmpManagerList) THEN
            _message := 'No valid managers were found in _mgrList';
            RAISE INFO '%', _message;
            Return;
        END IF;

        UPDATE TmpManagerList
        SET mgr_id = M.mgr_id
        FROM mc.t_mgrs M
        WHERE TmpManagerList.Manager_Name = M.mgr_name;

        DELETE FROM TmpManagerList
        WHERE mgr_id IS NULL;

    Else
        INSERT INTO TmpManagerList (mgr_id, manager_name)
        SELECT mgr_id, mgr_name
        FROM mc.t_mgrs
        WHERE mgr_type_id = 11;
    End If;

    ---------------------------------------------------
    -- Lookup the ParamID value for 'ManagerUpdateRequired'
    ---------------------------------------------------

    SELECT param_id INTO _paramTypeID
    FROM mc.t_param_type
    WHERE param_name = 'ManagerUpdateRequired';

    IF NOT FOUND THEN
        _message := 'Could not find parameter ManagerUpdateRequired in mc.t_param_type';
        _returnCode := 'U5201';
        Return;
    End If;

    ---------------------------------------------------
    -- Make sure each manager in TmpManagerList has an entry
    --  in mc.t_param_value for 'ManagerUpdateRequired'
    ---------------------------------------------------

    INSERT INTO mc.t_param_value (mgr_id, type_id, value)
    SELECT A.mgr_id, _paramTypeID, '0'
    FROM ( SELECT MgrListA.mgr_id
           FROM TmpManagerList MgrListA
         ) A
         LEFT OUTER JOIN
          ( SELECT MgrListB.mgr_id
            FROM TmpManagerList MgrListB
                 INNER JOIN mc.t_param_value PV
                   ON MgrListB.mgr_id = PV.mgr_id
            WHERE PV.type_id = _paramTypeID
         ) B
           ON A.mgr_id = B.mgr_id
    WHERE B.mgr_id IS NULL;
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

    If _infoOnly <> 0 THEN
        _infoHead := format('%-10s %-25s %-25s %-15s %-18s %-25s',
                            'Mgr_ID',
                            'Manager',
                            'Param Name',
                            'Update Required',
                            'New Update Required',
                            'Last Affected'
                        );

        RAISE INFO '%', _infoHead;

       _countToUpdate := 0;

        FOR _previewData IN
            SELECT MP.mgr_id,
                   MP.manager,
                   MP.param_name,
                   MP.value AS update_required,
                   'True' AS new_update_required,
                   MP.last_affected
            FROM mc.v_analysis_mgr_params_update_required MP
                 INNER JOIN TmpManagerList MgrList
                   ON MP.mgr_id = MgrList.mgr_id
            WHERE MP.ParamTypeID = _paramTypeID
            ORDER BY MP.manager
        LOOP

            _infoData := format('%-10s %-25s %-25s %-15s %-18s %-25s',
                                    _previewData.mgr_id,
                                    _previewData.manager,
                                    _previewData.param_name,
                                    _previewData.update_required,
                                    _previewData.new_update_required,
                                    _previewData.last_affected
                            );

            RAISE INFO '%', _infoData;

            _countToUpdate := _countToUpdate + 1;
        END LOOP;

        _message := format('Would set ManagerUpdateRequired to True for %s managers; see the Output window for details',
                            _countToUpdate);

        Return;
    End If;

    UPDATE mc.t_param_value
    SET value = 'True'
    WHERE entry_id in (
        SELECT PV.entry_id
        FROM mc.t_param_value PV
            INNER JOIN TmpManagerList MgrList
            ON PV.mgr_id = MgrList.mgr_id
        WHERE PV.type_id = _paramTypeID AND
            PV.value <> 'True');
    --
    GET DIAGNOSTICS _myRowCount = ROW_COUNT;

    If _myRowCount > 0 Then
        _message := 'Set "ManagerUpdateRequired" to True for ' || _myRowCount::text || ' manager';
        If _myRowCount > 1 Then
            _message := _message || 's';
        End If;

        RAISE INFO '%', _message;
    ELSE
        _message := 'All managers already have ManagerUpdateRequired set to True';
    End If;

    If _showTable <> 0 Then
        _infoHead := format('%-10s %-25s %-25s %-15s %-25s',
                            'Mgr_ID',
                            'Manager',
                            'Param Name',
                            'Update Required',
                            'Last Affected'
                        );

        RAISE INFO '%', _infoHead;

        FOR _previewData IN
            SELECT U.mgr_id,
                   U.manager,
                   U.param_name,
                   U.value as update_required,
                   U.last_affected
            FROM mc.v_analysis_mgr_params_update_required U
                INNER JOIN TmpManagerList MgrList
                   ON U.mgr_id = MgrList.mgr_id
            ORDER BY U.Manager
        LOOP

            _infoData := format('%-10s %-25s %-25s %-15s %-25s',
                                    _previewData.mgr_id,
                                    _previewData.manager,
                                    _previewData.param_name,
                                    _previewData.update_required,
                                    _previewData.last_affected
                            );

            RAISE INFO '%', _infoData;

        END LOOP;

         _message := _message || '; see the Output window for details';

    End If;

EXCEPTION
    WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS
            _sqlstate = returned_sqlstate,
            _exceptionMessage = message_text,
            _exceptionContext = pg_exception_context;

    _message := 'Error updating ManagerUpdateRequired for multiple managers in mc.t_param_value: ' || _exceptionMessage;
    _returnCode := _sqlstate;

    RAISE Warning 'Error: %', _message;
    RAISE warning '%', _exceptionContext;

    Call PostLogEntry ('Error', _message, 'SetManagerUpdateRequired', 'mc');

END
$$;

COMMENT ON PROCEDURE mc.set_manager_update_required IS 'SetManagerUpdateRequired';


CREATE OR REPLACE FUNCTION mc.unarchive_old_managers_and_params(
    _mgrlist text,
    _infoonly integer DEFAULT 1,
    _enablecontrolfromwebsite integer DEFAULT 0
)
RETURNS TABLE
(
    message text,
    mgr_name citext,
    control_from_website smallint,
    manager_type_id integer,
    param_name citext,
    entry_id integer,
    param_type_id integer,
    param_value citext,
    mgr_id integer,
    comment citext,
    last_affected timestamp without time zone,
    entered_by citext
)
 LANGUAGE plpgsql
/****************************************************
**
**  Desc:
**      Moves managers from mc.t_old_managers to mc.t_mgrs and
**      moves manager parameters from mc.t_param_value_old_managers to mc.t_param_value
**
**      To reverse this process, use function mc.ArchiveOldManagersAndParams
**      SELECT * FROM mc.ArchiveOldManagersAndParams('Pub-10-1', _infoOnly := 1);
**
**  Arguments:
**    _mgrList   One or more manager names (comma-separated list); supports wildcards because uses stored procedure ParseManagerNameList
**
**  Auth:   mem
**  Date:   02/25/2016 mem - Initial version
**          04/22/2016 mem - Now updating M_Comment in mc.t_mgrs
**          01/29/2020 mem - Ported to PostgreSQL
**          02/04/2020 mem - Rename columns to mgr_id and mgr_name
**
*****************************************************/
DECLARE
    _myRowCount int := 0;
    _message text;
    _newSeqValue int;
    _sqlstate text;
    _exceptionMessage text;
    _exceptionContext text;
BEGIN

    ---------------------------------------------------
    -- Validate the inputs
    ---------------------------------------------------
    --
    _mgrList := Coalesce(_mgrList, '');
    _infoOnly := Coalesce(_infoOnly, 1);
    _enableControlFromWebsite := Coalesce(_enableControlFromWebsite, 1);

    If _enableControlFromWebsite > 0 Then
        _enableControlFromWebsite := 1;
    End If;

    DROP TABLE IF EXISTS TmpManagerList;
    DROP TABLE IF EXISTS TmpWarningMessages;

    CREATE TEMP TABLE TmpManagerList (
        manager_name citext NOT NULL,
        mgr_id int NULL,
        control_from_web smallint null
    );

    CREATE TEMP TABLE TmpWarningMessages (
        entry_id int PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
        message text,
        manager_name citext
    );


    ---------------------------------------------------
    -- Populate TmpManagerList with the managers in _mgrList
    -- Using _removeUnknownManagers so that this procedure can be called repeatedly without raising an error
    ---------------------------------------------------
    --
    Call mc.parse_manager_name_list (_mgrList, _removeUnknownManagers => 0, _message => _message);

    If Not Exists (Select * from TmpManagerList) Then
        _message := '_mgrList did not match any managers in mc.t_mgrs: ';
        Raise Info 'Warning: %', _message;

        RETURN QUERY
        SELECT '_mgrList did not match any managers in mc.t_mgrs' as Message,
               _mgrList::citext as manager_name,
               0::smallint as control_from_website,
               0 as manager_type_id,
               ''::citext as param_name,
               0 as entry_id,
               0 as type_id,
               ''::citext as value,
               0 as mgr_id,
               ''::citext as comment,
               current_timestamp::timestamp as last_affected,
               ''::citext as entered_by;
        RETURN;
    End If;

    ---------------------------------------------------
    -- Validate the manager names
    ---------------------------------------------------

    UPDATE TmpManagerList
    SET mgr_id = M.mgr_id,
        control_from_web = _enableControlFromWebsite
    FROM mc.t_old_managers M
    WHERE TmpManagerList.Manager_Name = M.mgr_name;

    If Exists (Select * from TmpManagerList MgrList WHERE MgrList.mgr_id Is Null) Then
        INSERT INTO TmpWarningMessages (message, manager_name)
        SELECT 'Unknown manager (not in mc.t_old_managers)',
               MgrList.manager_name
        FROM TmpManagerList MgrList
        WHERE MgrList.mgr_id Is Null
        ORDER BY MgrList.manager_name;
    End If;

    If Exists (Select * From TmpManagerList MgrList Where MgrList.manager_name ILike '%Params%') Then
        INSERT INTO TmpWarningMessages (message, manager_name)
        SELECT 'Will not process managers with "Params" in the name (for safety)',
               MgrList.manager_name
        FROM TmpManagerList MgrList
        WHERE MgrList.manager_name ILike '%Params%'
        ORDER BY MgrList.manager_name;

        DELETE FROM TmpManagerList
        WHERE manager_name IN (SELECT WarnMsgs.manager_name FROM TmpWarningMessages WarnMsgs);
    End If;

    DELETE FROM TmpManagerList
    WHERE TmpManagerList.mgr_id Is Null OR
          TmpManagerList.control_from_web > 0;

    If Exists (Select * From TmpManagerList Src INNER JOIN mc.t_mgrs Target ON Src.mgr_id = Target.mgr_id) Then
        INSERT INTO TmpWarningMessages (message, manager_name)
        SELECT 'Manager already exists in t_mgrs; cannot restore',
               manager_name
        FROM TmpManagerList Src
             INNER JOIN mc.t_old_managers Target
               ON Src.mgr_id = Target.mgr_id;

        DELETE FROM TmpManagerList
        WHERE manager_name IN (SELECT WarnMsgs.manager_name FROM TmpWarningMessages WarnMsgs);
    End If;

    If Exists (Select * From TmpManagerList Src INNER JOIN mc.t_param_value Target ON Src.mgr_id = Target.mgr_id) Then
        INSERT INTO TmpWarningMessages (message, manager_name)
        SELECT 'Manager already has parameters in mc.t_param_value; cannot restore',
               manager_name
        FROM TmpManagerList Src
             INNER JOIN mc.t_param_value_old_managers Target
               ON Src.mgr_id = Target.mgr_id;

        DELETE FROM TmpManagerList
        WHERE manager_name IN (SELECT WarnMsgs.manager_name FROM TmpWarningMessages WarnMsgs);
    End If;

    If _infoOnly <> 0 OR NOT EXISTS (Select * From TmpManagerList) Then
        RETURN QUERY
        SELECT ' To be restored' as message,
               Src.manager_name,
               Src.control_from_web,
               PV.mgr_type_id,
               PV.param_name,
               PV.Entry_ID,
               PV.type_id,
               PV.Value,
               PV.mgr_id,
               PV.Comment,
               PV.Last_Affected,
               PV.Entered_By
        FROM TmpManagerList Src
             LEFT OUTER JOIN mc.v_old_param_value PV
               ON PV.mgr_id = Src.mgr_id
        UNION
        SELECT WarnMsgs.message,
               WarnMsgs.manager_name,
               0::smallint as control_from_website,
               0 as manager_type_id,
               ''::citext as param_name,
               0 as entry_id,
               0 as type_id,
               ''::citext as value,
               0 as mgr_id,
               ''::citext as comment,
               current_timestamp::timestamp as last_affected,
               ''::citext as entered_by
        FROM TmpWarningMessages WarnMsgs
        ORDER BY message ASC, manager_name, param_name;
        RETURN;
    End If;

    RAISE Info 'Insert into t_mgrs';

    INSERT INTO mc.t_mgrs (
                         mgr_id,
                         mgr_name,
                         mgr_type_id,
                         param_value_changed,
                         control_from_website,
                         comment )
    OVERRIDING SYSTEM VALUE
    SELECT M.mgr_id,
           M.mgr_name,
           M.mgr_type_id,
           M.param_value_changed,
           Src.control_from_web,
           M.comment
    FROM mc.t_old_managers M
         INNER JOIN TmpManagerList Src
           ON M.mgr_id = Src.mgr_id;
    --
    GET DIAGNOSTICS _myRowCount = ROW_COUNT;

    -- Set the manager ID sequence's current value to the maximum manager ID
    --
    SELECT MAX(mc.t_mgrs.mgr_id) INTO _newSeqValue
    FROM mc.t_mgrs;

    PERFORM setval('mc.t_mgrs_m_id_seq', _newSeqValue);
    RAISE INFO 'Sequence mc.t_mgrs_m_id_seq set to %', _newSeqValue;

    RAISE Info 'Insert into t_param_value';

    INSERT INTO mc.t_param_value (
             entry_id,
             type_id,
             value,
             mgr_id,
             comment,
             last_affected,
             entered_by )
    OVERRIDING SYSTEM VALUE
    SELECT PV.entry_id,
           PV.type_id,
           PV.value,
           PV.mgr_id,
           PV.comment,
           PV.last_affected,
           PV.entered_by
    FROM mc.t_param_value_old_managers PV
    WHERE PV.entry_id IN ( SELECT Max(PV.entry_ID)
                           FROM mc.t_param_value_old_managers PV
                                INNER JOIN TmpManagerList Src
                                  ON PV.mgr_id = Src.mgr_id
                           GROUP BY PV.mgr_id, PV.type_id
                         );
    --
    GET DIAGNOSTICS _myRowCount = ROW_COUNT;

    -- Set the entry_id sequence's current value to the maximum entry_id
    --
    SELECT MAX(PV.entry_id) INTO _newSeqValue
    FROM mc.t_param_value PV;

    PERFORM setval('mc.t_param_value_entry_id_seq', _newSeqValue);
    RAISE INFO 'Sequence mc.t_param_value_entry_id_seq set to %', _newSeqValue;

    DELETE FROM mc.t_param_value_old_managers
    WHERE mc.t_param_value_old_managers.mgr_id IN (SELECT MgrList.mgr_id FROM TmpManagerList MgrList);

    DELETE FROM mc.t_old_managers
    WHERE mc.t_old_managers.mgr_id IN (SELECT MgrList.mgr_id FROM TmpManagerList MgrList);

    RAISE Info 'Restore succeeded; returning results';

    RETURN QUERY
    SELECT 'Moved to mc.t_mgrs and mc.t_param_value' as Message,
           Src.Manager_Name,
           Src.control_from_web,
           OldMgrs.mgr_type_id,
           PT.param_name,
           PV.entry_id,
           PV.type_id,
           PV.value,
           PV.mgr_id,
           PV.comment,
           PV.last_affected,
           PV.entered_by
    FROM TmpManagerList Src
         LEFT OUTER JOIN mc.t_old_managers OldMgrs
           ON OldMgrs.mgr_id = Src.mgr_id
         LEFT OUTER JOIN mc.t_param_value_old_managers PV
           ON PV.mgr_id = Src.mgr_id
         LEFT OUTER JOIN mc.t_param_type PT ON
         PV.type_id = PT.param_id
    ORDER BY Src.Manager_Name, param_name;

EXCEPTION
    WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS
            _sqlstate = returned_sqlstate,
            _exceptionMessage = message_text,
            _exceptionContext = pg_exception_context;

    _message := 'Error unarchiving manager parameters for ' || _mgrList || ': ' || _exceptionMessage;

    RAISE Warning 'Error: %', _message;
    RAISE warning '%', _exceptionContext;

    Call PostLogEntry ('Error', _message, 'UnarchiveOldManagersAndParams', 'mc');

    RETURN QUERY
    SELECT _message as Message,
           ''::citext as Manager_Name,
           0::smallint as control_from_website,
           0 as manager_type_id,
           ''::citext as param_name,
           0 as entry_id,
           0 as type_id,
           ''::citext as value,
           0 as mgr_id,
           ''::citext as comment,
           current_timestamp::timestamp as last_affected,
           ''::citext as entered_by;
END
$$
;

COMMENT ON FUNCTION mc.unarchive_old_managers_and_params IS 'UnarchiveOldManagersAndParams';

CREATE OR REPLACE PROCEDURE mc.update_single_mgr_control_param
(
    _paramName text,
    _newValue text,
    _managerIDList text,
    _callingUser text = '',
    _infoOnly int = 0,
    INOUT _message text = '',
    INOUT _returnCode text = ''
)
LANGUAGE plpgsql
AS $$
/****************************************************
**
**  Desc:
**      Changes single manager params for set of given managers
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
**          02/10/2020 mem - Ported to PostgreSQL
**
*****************************************************/
DECLARE
    _myRowCount int := 0;
    _paramTypeID int;
    _previewData record;
    _infoHead text;
    _infoData text;
    _sqlstate text;
    _exceptionMessage text;
    _exceptionContext text;
BEGIN
    ---------------------------------------------------
    -- Validate the inputs
    ---------------------------------------------------

    _newValue := Coalesce(_newValue, '');
    _infoOnly := Coalesce(_infoOnly, 0);
    _message := '';
    _returnCode := '';

    ---------------------------------------------------
    -- Create a temporary table that will hold the entry_id
    -- values that need to be updated in mc.t_param_value
    ---------------------------------------------------

    DROP TABLE IF EXISTS TmpParamValueEntriesToUpdate;

    CREATE TEMP TABLE TmpParamValueEntriesToUpdate (
        entry_id int NOT NULL
    );

    CREATE UNIQUE INDEX IX_TmpParamValueEntriesToUpdate ON TmpParamValueEntriesToUpdate (entry_id);

    DROP TABLE IF EXISTS TmpMgrIDs;

    CREATE TEMP TABLE TmpMgrIDs (
        mgr_id int NOT NULL
    );

    ---------------------------------------------------
    -- Resolve _paramName to _paramTypeID
    ---------------------------------------------------

    SELECT param_id INTO _paramTypeID
    FROM mc.t_param_type
    WHERE param_name = _paramName;

    If Not Found Then
        _message := 'Error: Parameter ''' || _paramName || ''' not found in mc.t_param_type';
        Raise Warning '%', _message;
        _returnCode := 'U5309'
        Return;
    End If;


    RAISE Info 'Param type ID is %', _paramTypeID;

    ---------------------------------------------------
    -- Parse the manager ID list
    ---------------------------------------------------
    --
    INSERT INTO TmpMgrIDs (mgr_id)
    SELECT value
    FROM public.udf_parse_delimited_integer_list ( _managerIDList, ',' );
    --
    GET DIAGNOSTICS _myRowCount = ROW_COUNT;

    RAISE Info 'Inserted % manager IDs into TmpMgrIDs', _myRowCount;

    If _infoOnly <> 0 Then
        _infoHead := format('%-10s %-10s %-25s %-25s %-15s %-15s %-15s %-15s',
                            'Entry_ID',
                            'Mgr_ID',
                            'Manager',
                            'Param Name',
                            'ParamTypeID',
                            'Value',
                            'New Value',
                            'Status'
                        );

        RAISE INFO '%', _infoHead;

        FOR _previewData IN
            SELECT PV.entry_id,
                   M.mgr_id,
                   M.mgr_name,
                   PV.param_name,
                   PV.type_id,
                   PV.value,
                   _newValue AS NewValue,
                   Case When Coalesce(PV.value, '') <> _newValue Then 'Changed' Else 'Unchanged' End As Status
            FROM mc.t_mgrs M
                 INNER JOIN TmpMgrIDs
                   ON M.mgr_id = TmpMgrIDs.mgr_id
                 INNER JOIN mc.v_param_value PV
                   ON PV.mgr_id = M.mgr_id AND
                      PV.type_id = _paramTypeID
            WHERE M.control_from_website > 0
            UNION
            SELECT PV.entry_id,
                   M.mgr_id,
                   M.mgr_name,
                   PV.param_name,
                   PV.type_id,
                   PV.value,
                   '' AS NewValue,
                   'Skipping: control_from_website is 0 in mc.t_mgrs' AS  Status
            FROM mc.t_mgrs M
                 INNER JOIN TmpMgrIDs
                   ON M.mgr_id = TmpMgrIDs.mgr_id
                 INNER JOIN mc.v_param_value PV
                   ON PV.mgr_id = M.mgr_id AND
                      PV.type_id = _paramTypeID
            WHERE M.control_from_website = 0
            UNION
            SELECT NULL AS entry_id,
                   M.mgr_id,
                   M.mgr_name,
                   _paramName,
                   _paramTypeID,
                   NULL AS value,
                   _newValue AS NewValue,
                   'New'
            FROM mc.t_mgrs M
                 INNER JOIN TmpMgrIDs
                   ON M.mgr_id = TmpMgrIDs.mgr_id
                 LEFT OUTER JOIN mc.t_param_value PV
                   ON PV.mgr_id = M.mgr_id AND
                      PV.type_id = _paramTypeID
            WHERE PV.type_id IS NULL
        LOOP
            _infoData := format('%-10s %-10s %-25s %-25s %-15s %-15s %-15s %-15s',
                                    _previewData.entry_id,
                                    _previewData.mgr_id,
                                    _previewData.mgr_name,
                                    _previewData.param_name,
                                    _previewData.type_id,
                                    _previewData.value,
                                    _previewData.NewValue,
                                    _previewData.Status
                        );

            RAISE INFO '%', _infoData;

        END LOOP;

        _message := public.udf_append_to_text(_message, 'See the Output window for details');

        Return;
    End If;

    ---------------------------------------------------
    -- Add new entries for Managers in _managerIDList that
    -- don't yet have an entry in mc.t_param_value for parameter _paramName
    --
    -- Adding value '##_DummyParamValue_##' so that
    --  we'll force a call to mc.update_single_mgr_param_work
    --
    -- Intentionally not filtering on M.control_from_website > 0 here,
    -- but the query that populates TmpParamValueEntriesToUpdate does filter on that parameter
    ---------------------------------------------------

    INSERT INTO mc.t_param_value( type_id,
                                  value,
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
    WHERE PV.type_id IS NULL;
    --
    GET DIAGNOSTICS _myRowCount = ROW_COUNT;

    ---------------------------------------------------
    -- Find the entries for the Managers in _managerIDList
    -- Populate TmpParamValueEntriesToUpdate with the entries that need to be updated
    ---------------------------------------------------
    --
    INSERT INTO TmpParamValueEntriesToUpdate( entry_id )
    SELECT PV.entry_id
    FROM mc.t_param_value PV
         INNER JOIN mc.t_mgrs M
           ON PV.mgr_id = M.mgr_id
         INNER JOIN TmpMgrIDs
           ON M.mgr_id = TmpMgrIDs.mgr_id
    WHERE M.control_from_website > 0 AND
          PV.type_id = _paramTypeID AND
          Coalesce(PV.value, '') <> _newValue;
    --
    GET DIAGNOSTICS _myRowCount = ROW_COUNT;

    ---------------------------------------------------
    -- Call UpdateSingleMgrParamWork to perform the update, then call
    -- AlterEnteredByUserMultiID and AlterEventLogEntryUserMultiID for _callingUser
    ---------------------------------------------------
    --
    Call mc.update_single_mgr_param_work (_paramName, _newValue, _callingUser, _message => _message, _returnCode => _returnCode);

EXCEPTION
    WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS
            _sqlstate = returned_sqlstate,
            _exceptionMessage = message_text,
            _exceptionContext = pg_exception_context;

    _message := 'Error updating values in mc.t_param_value for the given managers: ' || _exceptionMessage;
    _returnCode := _sqlstate;

    RAISE Warning 'Error: %', _message;
    RAISE warning '%', _exceptionContext;

    Call PostLogEntry ('Error', _message, 'UpdateSingleMgrControlParam', 'mc');

END
$$;

COMMENT ON PROCEDURE mc.update_single_mgr_control_param IS 'UpdateSingleMgrControlParam';


CREATE OR REPLACE PROCEDURE mc.update_single_mgr_type_control_param
(
    _paramName text,
    _newValue text,
    _managerTypeIDList text,
    _callingUser text = '',
    INOUT _message text = '',
    INOUT _returnCode text = ''
)
LANGUAGE plpgsql
AS $$
/****************************************************
**
**  Desc:
**      Changes single manager params for set of given manager Types
**
**  Arguments:
**    _paramName          The parameter name to update
**    _newValue           The new value to assign for this parameter
**    _managerTypeIDList  Manager type IDs to update (11=Analyis Manager, 15=Capture Task Manager, etc.)
**
**  Auth:   jds
**  Date:   07/17/2007
**          07/31/2007 grk - changed for 'controlfromwebsite' no longer a parameter
**          03/30/2009 mem - Added optional parameter _callingUser; if provided, then will call AlterEnteredByUserMultiID and possibly AlterEventLogEntryUserMultiID
**          04/16/2009 mem - Now calling UpdateSingleMgrParamWork to perform the updates
**          02/15/2020 mem - Ported to PostgreSQL
**
*****************************************************/
DECLARE
    _myRowCount int := 0;
    _sqlstate text;
    _exceptionMessage text;
    _exceptionContext text;
BEGIN

    _message := '';
    _returnCode := '';

    ---------------------------------------------------
    -- Create a temporary table that will hold the entry_id
    -- values that need to be updated in mc.t_param_value
    ---------------------------------------------------
    DROP TABLE IF EXISTS TmpParamValueEntriesToUpdate;

    CREATE TEMP TABLE TmpParamValueEntriesToUpdate (
        entry_id int NOT NULL
    );

    CREATE UNIQUE INDEX IX_TmpParamValueEntriesToUpdate ON TmpParamValueEntriesToUpdate (entry_id);

    ---------------------------------------------------
    -- Find the _paramName entries for the Manager Types in _managerTypeIDList
    ---------------------------------------------------
    --
    INSERT INTO TmpParamValueEntriesToUpdate (entry_id)
    SELECT PV.entry_id
    FROM mc.t_param_value PV
         INNER JOIN mc.t_param_type PT
           ON PV.type_id = PT.param_id
         INNER JOIN mc.t_mgrs M
           ON M.mgr_id = PV.mgr_id
    WHERE PT.param_name = _paramName AND
          M.mgr_type_id IN ( SELECT value
                             FROM public.udf_parse_delimited_integer_list(_managerTypeIDList, ',')
                           ) AND
          M.control_from_website > 0;

    ---------------------------------------------------
    -- Call UpdateSingleMgrParamWork to perform the update, then call
    -- AlterEnteredByUserMultiID and AlterEventLogEntryUserMultiID for _callingUser
    ---------------------------------------------------
    --
    Call mc.UpdateSingleMgrParamWork (_paramName, _newValue, _callingUser, _message => _message, _returnCode => _returnCode);

EXCEPTION
    WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS
            _sqlstate = returned_sqlstate,
            _exceptionMessage = message_text,
            _exceptionContext = pg_exception_context;

    _message := format('Error %s %s: %s',
                _currentOperation, _currentTargetTable, _exceptionMessage);

    RAISE Warning '%', _message;
    RAISE warning '%', _exceptionContext;

    Call PostLogEntry ('Error', _message, 'UpdateSingleMgrTypeControlParam', 'public');
END
$$;

COMMENT ON PROCEDURE mc.update_single_mgr_type_control_param IS 'UpdateSingleMgrTypeControlParam';


CREATE OR REPLACE PROCEDURE mc.update_single_mgr_param_work
(
    _paramName text,
    _newValue text,
    _callingUser text = '',
    INOUT _message text = '',
    INOUT _returnCode text = ''
)
LANGUAGE plpgsql
AS $$
/****************************************************
**
**  Desc:
**      Changes single manager param for the EntryID values
**      defined in table TmpParamValueEntriesToUpdate (created by the calling procedure)
**
**  Example table creation code:
**    CREATE TEMP TABLE TmpParamValueEntriesToUpdate (entry_id int NOT NULL)
**
**  Arguments:
**    _paramName   The parameter name
**    _newValue    The new value to assign for this parameter
**
**  Auth:   mem
**  Date:   04/16/2009
**          02/10/2020 mem - Ported to PostgreSQL
**
*****************************************************/
DECLARE
    _rowCountUpdated int := 0;
    _paramTypeID int;
    _targetState int;
BEGIN

    _message := '';
    _returnCode := '';

    -- Validate that _paramName is not blank
    If Coalesce(_paramName, '') = '' Then
        _message := 'Parameter Name is empty or null';
        RAISE WARNING '%', _message;
        _returnCode := 'U5315';
        Return;
    End If;

    -- Assure that _newValue is not null
    _newValue := Coalesce(_newValue, '');

    -- Lookup the param_type_id for param _paramName
    --
    SELECT param_id INTO _paramTypeID
    FROM mc.t_param_type
    WHERE param_name = _paramName::citext;

    If Not Found Then
        _message := 'Unknown Parameter Name: ' || _paramName;
        RAISE WARNING '%', _message;
        _returnCode := 'U5316';
        Return;
    End If;

    ---------------------------------------------------
    -- Update the values defined in TmpParamValueEntriesToUpdate
    ---------------------------------------------------
    --
    UPDATE mc.t_param_value
    SET value = _newValue
    WHERE entry_id IN (SELECT entry_id FROM TmpParamValueEntriesToUpdate) AND
          Coalesce(value, '') <> _newValue;
    --
    GET DIAGNOSTICS _rowCountUpdated = ROW_COUNT;

    If _rowCountUpdated > 0 And char_length(Coalesce(_callingUser, '')) > 0 Then

        -- _callingUser is defined
        -- Items need to be updated in mc.t_param_value and possibly in mc.t_event_log

        ---------------------------------------------------
        -- Create a temporary table that will hold the entry_id
        -- values that need to be updated in mc.t_param_value
        ---------------------------------------------------

        Drop Table If Exists TmpIDUpdateList;

        CREATE TEMP TABLE TmpIDUpdateList (
            TargetID int NOT NULL
        );

        CREATE UNIQUE INDEX IX_TmpIDUpdateList ON TmpIDUpdateList (TargetID);

        -- Populate TmpIDUpdateList with entry_id values for mc.t_param_value, then call AlterEnteredByUserMultiID
        --
        INSERT INTO TmpIDUpdateList (TargetID)
        SELECT entry_id
        FROM TmpParamValueEntriesToUpdate;

        Call public.AlterEnteredByUserMultiID ('mc', 't_param_value', 'entry_id', _callingUser, _entryDateColumnName => 'last_affected', _message => _message);

        If _paramName::citext = 'mgractive' or _paramTypeID = 17 Then
            -- Triggers trig_i_t_param_value and trig_u_t_param_value make an entry in
            --  mc.t_event_log whenever mgractive (param TypeID = 17) is changed

            -- Call AlterEventLogEntryUserMultiID
            -- to alter the entered_by field in mc.t_event_log

            If _newValue::citext = 'True' Then
                _targetState := 1;
            Else
                _targetState := 0;
            End If;

            -- Populate TmpIDUpdateList with Manager ID values, then call AlterEventLogEntryUserMultiID
            Truncate Table TmpIDUpdateList;

            INSERT INTO TmpIDUpdateList (TargetID)
            SELECT PV.mgr_id
            FROM mc.t_param_value PV
            WHERE PV.entry_id IN (SELECT entry_id FROM TmpParamValueEntriesToUpdate);

            Call public.AlterEventLogEntryUserMultiID ('mc', 1, _targetState, _callingUser, _message => _message);
        End If;

    End If;

    If _message = '' Then
        If _rowCountUpdated = 0 Then
            _message := 'All ' || _rowCountUnchanged || ' row(s) in mc.t_param_value already have ' || _paramname || ' = ' || _newValue;
        Else
            _message := 'Updated ' || _rowCountUpdated || ' row(s) in mc.t_param_value to have ' || _paramname || ' = ' || _newValue;
        End If;
    End If;
END
$$;

COMMENT ON PROCEDURE mc.update_single_mgr_param_work IS 'UpdateSingleMgrParamWork';

