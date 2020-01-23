
CREATE OR REPLACE PROCEDURE mc.ArchiveOldManagersAndParams
(
    _MgrList varchar(max),
    _InfoOnly tinyint = 1,
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
**    _MgrList   One or more manager names (comma-separated list); supports wildcards because uses stored procedure ParseManagerNameList
**
**  Auth:   mem
**  Date:   05/14/2015 mem - Initial version
**          02/25/2016 mem - Add Set XACT_ABORT On
**          04/22/2016 mem - Now updating M_Comment in T_OldManagers
**
*****************************************************/
DECLARE
    _myRowCount int;
    _myError int;
    _MoveParams text;
BEGIN
    _myRowCount := 0;
    _myError := 0;

    ---------------------------------------------------
    -- Validate the inputs
    ---------------------------------------------------
    --
    _MgrList := IsNull(_MgrList, '');
    _InfoOnly := IsNull(_InfoOnly, 1);
    _message := '';

    CREATE TABLE #TmpManagerList (
        Manager_Name varchar(50) NOT NULL,
        M_ID int NULL,
        M_ControlFromWebsite tinyint null
    )

    ---------------------------------------------------
    -- Populate #TmpManagerList with the managers in _MgrList
    ---------------------------------------------------
    --

    Call ParseManagerNameList @MgrList, @RemoveUnknownManagers=0

    If Not Exists (Select * from #TmpManagerList) Then
        _message := '_MgrList was empty; no match in t_mgrs to ' || _MgrList;
        Select _Message as Warning
        Return;
    End If;

    ---------------------------------------------------
    -- Validate the manager names
    ---------------------------------------------------
    --
    UPDATE #TmpManagerList
    SET m_id = M.m_id,
        control_from_website = M.control_from_website
    FROM #TmpManagerList Target
         INNER JOIN t_mgrs M
           ON Target.Manager_Name = M.m_name
    --
    GET DIAGNOSTICS _myRowCount = ROW_COUNT;

    If Exists (Select * from #TmpManagerList where M_ID Is Null) Then
        SELECT 'Unknown manager (not in t_mgrs)' AS Warning, Manager_Name
        FROM #TmpManagerList
        ORDER BY Manager_Name
    End If;

    If Exists (Select * from #TmpManagerList WHERE NOT M_ID is Null And M_ControlFromWebsite > 0) Then
        SELECT 'Manager has M_ControlFromWebsite=1; cannot archive' AS Warning,
               Manager_Name
        FROM #TmpManagerList
        WHERE NOT M_ID IS NULL AND
              M_ControlFromWebsite > 0
        ORDER BY Manager_Name
    End If;

    If Exists (Select * From #TmpManagerList Where Manager_Name Like '%Params%') Then
        SELECT 'Will not process managers with "Params" in the name (for safety)' AS Warning,
               Manager_Name
        FROM #TmpManagerList
        WHERE Manager_Name Like '%Params%'
        ORDER BY Manager_Name

        DELETE From #TmpManagerList Where Manager_Name Like '%Params%'
    End If;

    If _InfoOnly <> 0 Then
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
        FROM #TmpManagerList Src
             LEFT OUTER JOIN V_ParamValue PV
               ON PV.MgrID = Src.M_ID
        ORDER BY Src.Manager_Name, ParamName

    Else
        DELETE FROM #TmpManagerList WHERE M_ID is Null OR M_ControlFromWebsite > 0

        _MoveParams := 'Move params transaction';
        Begin Tran _MoveParams

        INSERT INTO t_old_managers( m_id,
                                   m_name,
                                   mgr_type_id,
                                   param_value_changed,
                                   control_from_website,
                                   comment )
        SELECT M.m_id,
               M.m_name,
               M.mgr_type_id,
               M.param_value_changed,
               M.control_from_website,
               M.comment
        FROM t_mgrs M
             INNER JOIN #TmpManagerList Src
               ON M.m_id = Src.m_id
          LEFT OUTER JOIN t_old_managers Target
               ON Src.m_id = Target.m_id
        WHERE Target.m_id IS NULL
        --
        GET DIAGNOSTICS _myRowCount = ROW_COUNT;
        --
        If _myError <> 0 Then
            Rollback
            Select 'Aborted (rollback)' as Warning, _myError as ErrorCode
            Return;
        End If;

        INSERT INTO t_param_value_old_managers(
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
        FROM t_param_value PV
             INNER JOIN #TmpManagerList Src
               ON PV.mgr_id = Src.M_ID
        --
        GET DIAGNOSTICS _myRowCount = ROW_COUNT;
        --
        If _myError <> 0 Then
            Rollback
            Select 'Aborted (rollback)' as Warning, _myError as ErrorCode
            Return;
        End If;

        DELETE t_param_value
        FROM t_param_value PV
             INNER JOIN #TmpManagerList Src
               ON PV.mgr_id = Src.M_ID

        DELETE t_mgrs
        FROM t_mgrs M
             INNER JOIN #TmpManagerList Src
               ON M.m_id = Src.m_id

        Commit Tran _MoveParams

        SELECT 'Moved to t_old_managers and t_param_value_old_managers' as Message,
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
        FROM #TmpManagerList Src
             LEFT OUTER JOIN t_param_value_old_managers PV
               ON PV.mgr_id = Src.m_id
             LEFT OUTER JOIN t_param_type PT ON
             PV.type_id = PT.param_id
        ORDER BY Src.Manager_Name, param_name
    End If;

Done:
    RETURN _myError

END
$$;

CREATE OR REPLACE PROCEDURE mc.DisableAnalysisManagers
(
    _PreviewUpdates tinyint = 0,
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
**          10/09/2009 mem - Changed @ManagerTypeIDList to 11
**          06/09/2011 mem - Now calling EnableDisableAllManagers
**
*****************************************************/
DECLARE
    _myError int;
BEGIN
    Call EnableDisableAllManagers _ManagerTypeIDList='11', _ManagerNameList='', _enable=0,
                                             _PreviewUpdates=_PreviewUpdates, _message = _message output

    Return _myError

END
$$;

CREATE OR REPLACE PROCEDURE mc.DisableArchiveDependentManagers
(
    _PreviewUpdates tinyint = 0,
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
**          07/24/2008 mem - Changed @ManagerTypeIDList from '1,2,3,4,8' to '2,3,8'
**          07/24/2008 mem - Changed @ManagerTypeIDList from '2,3,8' to '8'
**                         - Note that we do not include 15=CaptureTaskManager because capture tasks can still occur when the archive is unavailable
**                         - However, you should run Stored Procedure EnableDisableArchiveStepTools in the DMS_Capture database to disable the archive-dependent step tools
**
*****************************************************/
DECLARE
    _myError int;
BEGIN
    Call EnableDisableAllManagers _ManagerTypeIDList='8', _ManagerNameList='', _enable=0,
                                             _PreviewUpdates=_PreviewUpdates, _message = _message output

    Return _myError

END
$$;

CREATE OR REPLACE PROCEDURE mc.EnableDisableAllManagers
(
    _ManagerTypeIDList text = '',
    _ManagerNameList text = '',
    _Enable tinyint = 1,
    _PreviewUpdates tinyint = 0,
    INOUT _message text=''
)
LANGUAGE plpgsql
AS $$
/****************************************************
**
**  Desc:   Enables or disables all managers, optionally filtering by manager type ID or manager name
**
**  Arguments:
**    _ManagerTypeIDList   Optional: list of manager type IDs to disable, e.g. "1, 2, 3"
**    _ManagerNameList     Optional: if defined, then only managers specified here will be enabled; supports the % wildcard
**    _Enable              1 to enable, 0 to disable
**
**  Auth:   mem
**  Date:   05/09/2008
**          06/09/2011 - Created by extending code in DisableAllManagers
**                     - Now filtering on MT_Active > 0 in T_MgrTypes
**
*****************************************************/
DECLARE
    _myRowCount int;
    _myError int;
    _MgrTypeID int;
    _Continue int;
BEGIN
    _myRowCount := 0;
    _myError := 0;

    -----------------------------------------------
    -- Validate the inputs
    -----------------------------------------------
    --
    _Enable := IsNull(_Enable, 0);
    _ManagerTypeIDList := IsNull(_ManagerTypeIDList, '');
    _ManagerNameList := IsNull(_ManagerNameList, '');
    _PreviewUpdates := IsNull(_PreviewUpdates, 0);
    _message := '';

    CREATE TABLE #TmpManagerTypeIDs (
        MgrTypeID int NOT NULL
    )

    If char_length(_ManagerTypeIDList) > 0 Then
        -- Parse _ManagerTypeIDList
        --
        INSERT INTO #TmpManagerTypeIDs (MgrTypeID)
        SELECT DISTINCT Value
        FROM dbo.udfParseDelimitedIntegerList(_ManagerTypeIDList, ',')
        ORDER BY Value
        --
        GET DIAGNOSTICS _myRowCount = ROW_COUNT;
    Else
        -- Populate #TmpManagerTypeIDs with all manager types in t_mgr_types
        --
        INSERT INTO #TmpManagerTypeIDs (MgrTypeID)
        SELECT DISTINCT mgr_type_id
        FROM t_mgr_types
        WHERE mgr_type_active > 0
        ORDER BY mgr_type_id
        --
        GET DIAGNOSTICS _myRowCount = ROW_COUNT;
    End If;

    -----------------------------------------------
    -- Loop through the manager types in #TmpManagerTypeIDs
    -- For each, call EnableDisableManagers
    -----------------------------------------------

    _MgrTypeID := 0;
    PERFORM _MgrTypeID = MIN(MgrTypeID)-1
    FROM #TmpManagerTypeIDs
    --
    GET DIAGNOSTICS _myRowCount = ROW_COUNT;

    _Continue := 1;
    While _Continue = 1 Loop
    Begin
        PERFORM _MgrTypeID = MgrTypeID
        FROM #TmpManagerTypeIDs
        WHERE MgrTypeID > _MgrTypeID
        ORDER BY MgrTypeID
        --
        GET DIAGNOSTICS _myRowCount = ROW_COUNT;

        If _myRowCount = 0 Then
            _Continue := 0;
        Else
            Call EnableDisableManagers _Enable=_Enable, _ManagerTypeID=_MgrTypeID, _ManagerNameList=_ManagerNameList, _PreviewUpdates = _PreviewUpdates, _message = _message output
        End If;
    End Loop;

Done:
    Return _myError

END
$$;

CREATE OR REPLACE PROCEDURE mc.EnableDisableManagers
(
    _Enable tinyint,
    _ManagerTypeID int=11,
    _ManagerNameList text = '',
    _PreviewUpdates tinyint = 0,
    INOUT _message text=''
)
LANGUAGE plpgsql
AS $$
/****************************************************
**
**  Desc:   Enables or disables all managers of the given type
**
**  Arguments:
**    _Enable            0 to disable, 1 to enable
**    _ManagerTypeID     Defined in table T_MgrTypes.  8=Space, 9=DataImport, 11=Analysis Tool Manager, 15=CaptureTaskManager
**    _ManagerNameList   Required when _Enable = 1.  Only managers specified here will be enabled, though you can use "All" to enable All managers.  When _Enable = 0, if this parameter is blank (or All) then all managers of the given type will be disabled; supports the % wildcard
**
**  Auth:   mem
**  Date:   07/12/2007
**          05/09/2008 mem - Added parameter @ManagerNameList
**          06/09/2011 mem - Now filtering on MT_Active > 0 in T_MgrTypes
**                         - Now allowing @ManagerNameList to be All when @Enable = 1
**          10/12/2017 mem - Allow @ManagerTypeID to be 0 if @ManagerNameList is provided
**          03/28/2018 mem - Use different messages when updating just one manager
**
*****************************************************/
DECLARE
    _myRowCount int;
    _myError int;
    _NewValue text;
    _ManagerTypeName text;
    _ActiveStateDescription text;
    _CountToUpdate int;
    _CountUnchanged int;
BEGIN
    _myRowCount := 0;
    _myError := 0;

    -----------------------------------------------
    -- Validate the inputs
    -----------------------------------------------
    --
    _ManagerNameList := IsNull(_ManagerNameList, '');
    _PreviewUpdates := IsNull(_PreviewUpdates, 0);

    If _Enable Is Null Then
        _myError := 40000;
        _message := '_Enable cannot be null';
        SELECT _message AS Message
        Return;
    End If;

    If _ManagerTypeID Is Null Then
        _myError := 40001;
        _message := '_ManagerTypeID cannot be null';
        SELECT _message AS Message
        Return;
    End If;

    If _ManagerTypeID = 0 And char_length(_ManagerNameList) > 0 And _ManagerNameList <> 'All' Then
        _ManagerTypeName := 'Any';
    Else
        -- Make sure _ManagerTypeID is valid
        _ManagerTypeName := '';
        PERFORM _ManagerTypeName = mgr_type_name
        FROM t_mgr_types
        WHERE mgr_type_id = _ManagerTypeID AND
            mgr_type_active > 0
        --
        GET DIAGNOSTICS _myRowCount = ROW_COUNT;

        If _myRowCount = 0 Then
            If Exists (SELECT * FROM t_mgr_types WHERE mgr_type_id = _ManagerTypeID AND mgr_type_active = 0) Then
                _message := '_ManagerTypeID ' || _ManagerTypeID::text || ' has mgr_type_active = 0 in t_mgr_types; unable to continue';
            Else
                _message := '_ManagerTypeID ' || _ManagerTypeID::text || ' not found in t_mgr_types';;
            End If;

            SELECT _message AS Message
            _myError := 40002;
            Return;
        End If;
    End If;

    If _Enable <> 0 AND char_length(_ManagerNameList) = 0 Then
        _message := '_ManagerNameList cannot be blank when _Enable is non-zero; to update all managers, set _ManagerNameList to All';
        SELECT _message AS Message
        _myError := 40003;
        Return;
    End If;

    -----------------------------------------------
    -- Creata a temporary table
    -----------------------------------------------

    CREATE TABLE #TmpManagerList (
        Manager_Name varchar(128) NOT NULL
    )

    If char_length(_ManagerNameList) > 0 And _ManagerNameList <> 'All' Then
        -- Populate #TmpMangerList using ParseManagerNameList

        Call ParseManagerNameList _ManagerNameList, _RemoveUnknownManagers=1, _message=_message output

        If _myError <> 0 Then
            If char_length(_message) = 0 Then
                _message := 'Error calling ParseManagerNameList: ' || _myError::text;;
            End If;

            Return;
        End If;

        If _ManagerTypeID > 0 Then
            -- Delete entries from #TmpManagerList that don't match entries in M_Name of the given type
            DELETE #TmpManagerList
            FROM #TmpManagerList U LEFT OUTER JOIN
                t_mgrs M ON M.m_name = U.Manager_Name AND M.mgr_type_id = _ManagerTypeID
            WHERE M.m_name Is Null
            --
            GET DIAGNOSTICS _myRowCount = ROW_COUNT;

            If _myRowCount > 0 Then
                _message := 'Found ' || _myRowCount::text || ' entries in _ManagerNameList that are not ' || _ManagerTypeName || ' managers';
                _message := '';
            End If;
        End If;

    Else
        -- Populate #TmpManagerList with all managers in t_mgrs
        --
        INSERT INTO #TmpManagerList (Manager_Name)
        SELECT m_name
        FROM t_mgrs
        WHERE mgr_type_id = _ManagerTypeID
        --
        GET DIAGNOSTICS _myRowCount = ROW_COUNT;
    End If;

    -- Set _NewValue based on _Enable
    If _Enable = 0 Then
        _NewValue := 'False';
        _ActiveStateDescription := 'Inactive';
    Else
        _NewValue := 'True';
        _ActiveStateDescription := 'Active';
    End If;

    -- Count the number of managers that need to be updated
    _CountToUpdate := 0;
    PERFORM _CountToUpdate = COUNT(*)
    FROM t_param_value PV
         INNER JOIN t_param_type PT
           ON PV.type_id = PT.param_id
         INNER JOIN t_mgrs M
           ON PV.mgr_id = M.m_id
         INNER JOIN t_mgr_types MT
           ON M.mgr_type_id = MT.mgr_type_id
         INNER JOIN #TmpManagerList U
           ON M.m_name = U.Manager_Name
    WHERE PT.param_name = 'mgractive' AND
          PV.value <> _NewValue AND
          MT.mgr_type_active > 0
    --
    GET DIAGNOSTICS _myRowCount = ROW_COUNT;

    -- Count the number of managers already in the target state
    _CountUnchanged := 0;
    PERFORM _CountUnchanged = COUNT(*)
    FROM t_param_value PV
         INNER JOIN t_param_type PT
           ON PV.type_id = PT.param_id
         INNER JOIN t_mgrs M
           ON PV.mgr_id = M.m_id
         INNER JOIN t_mgr_types MT
           ON M.mgr_type_id = MT.mgr_type_id
         INNER JOIN #TmpManagerList U
           ON M.m_name = U.Manager_Name
    WHERE PT.param_name = 'mgractive' AND
          PV.value = _NewValue AND
          MT.mgr_type_active > 0
    --
    GET DIAGNOSTICS _myRowCount = ROW_COUNT;

    If _CountToUpdate = 0 Then
        If _CountUnchanged = 0 Then
            If char_length(_ManagerNameList) > 0 Then
                If _ManagerTypeID = 0 Then
                    _message := 'None of the managers in _ManagerNameList was recognized';
                Else
                    _message := 'No ' || _ManagerTypeName || ' managers were found matching _ManagerNameList';;
                End If;
            Else
                _message := 'No ' || _ManagerTypeName || ' managers were found in t_mgrs';
            End If;
        Else
            If _CountUnchanged = 1 Then
                _message := 'The manager is already ' || _ActiveStateDescription;
            Else
                If _ManagerTypeID = 0 Then
                    _message := 'All ' || _CountUnchanged::text || ' managers are already ' || _ActiveStateDescription;
                Else
                    _message := 'All ' || _CountUnchanged::text || ' ' || _ManagerTypeName || ' managers are already ' || _ActiveStateDescription;;
                End If;
            End If;
        End If;

        SELECT _message AS Message

    Else
        If _PreviewUpdates <> 0 Then
            SELECT Convert(varchar(32), PV.value + '-->' + _NewValue) AS State_Change_Preview,
                   PT.param_name AS Parameter_Name,
                   M.m_name AS Manager_Name,
                   MT.mgr_type_name AS Manager_Type
            FROM t_param_value PV
                 INNER JOIN t_param_type PT
                   ON PV.type_id = PT.param_id
                 INNER JOIN t_mgrs M
                   ON PV.mgr_id = M.m_id
                 INNER JOIN t_mgr_types MT
                   ON M.mgr_type_id = MT.mgr_type_id
                 INNER JOIN #TmpManagerList U
                   ON M.m_name = U.Manager_Name
            WHERE PT.param_name = 'mgractive' AND
                  PV.value <> _NewValue AND
                  MT.mgr_type_active > 0
            --
            GET DIAGNOSTICS _myRowCount = ROW_COUNT;
        Else
            UPDATE t_param_value
            SET value = _NewValue
            FROM t_param_value PV
                 INNER JOIN t_param_type PT
                   ON PV.type_id = PT.param_id
                 INNER JOIN t_mgrs M
                   ON PV.mgr_id = M.m_id
                 INNER JOIN t_mgr_types MT
                   ON M.mgr_type_id = MT.mgr_type_id
                 INNER JOIN #TmpManagerList U
                   ON M.m_name = U.Manager_Name
            WHERE PT.param_name = 'mgractive' AND
                  PV.value <> _NewValue AND
                  MT.mgr_type_active > 0
            --
            GET DIAGNOSTICS _myRowCount = ROW_COUNT;

            If _myRowCount = 1 And _CountUnchanged = 0 Then
                _message := 'The manager is now ' || _ActiveStateDescription;
            Else
                If _ManagerTypeID = 0 Then
                    _message := 'Set ' || _myRowCount::text || ' managers to state ' || _ActiveStateDescription;
                Else
                    _message := 'Set ' || _myRowCount::text || ' ' || _ManagerTypeName || ' managers to state ' || _ActiveStateDescription;;
                End If;

                If _CountUnchanged <> 0 Then
                    _message := _message || ' (' || _CountUnchanged::text || ' managers were already ' || _ActiveStateDescription || ')';;
                End If;
            End If;

            SELECT _message AS Message
        End If;
    End If;

Done:
    Return _myError

END
$$;

CREATE OR REPLACE PROCEDURE mc.PostLogEntry()
LANGUAGE plpgsql
AS $$
/****************************************************
**
**  Desc:   Put new entry into the main log table
**
*
**  Auth:   grk
**  Date:   10/31/2001
**          02/17/2005 mem - Added parameter @duplicateEntryHoldoffHours
**          05/31/2007 mem - Expanded the size of @type, @message, and @postedBy
**
*****************************************************/
DECLARE
    _duplicateRowCount int;
BEGIN
    _type varchar(128),
    _message varchar(4096),
    _postedBy varchar(128)= 'na',
    _duplicateEntryHoldoffHours int = 0            -- Set this to a value greater than 0 to prevent duplicate entries being posted within the given number of hours

    _duplicateRowCount := 0;

    If IsNull(_duplicateEntryHoldoffHours, 0) > 0 Then
        PERFORM _duplicateRowCount = COUNT(*)
        FROM t_log_entries
        WHERE message = _message AND type = _type AND posting_time >= (GetDate() - _duplicateEntryHoldoffHours)
    End If;

    If _duplicateRowCount = 0 Then
        INSERT INTO t_log_entries
            (posted_by, posting_time, type, message)
        VALUES ( _postedBy, GETDATE(), _type, _message)
        --
        if __rowcount <> 1 Then
            RAISERROR ('Update was unsuccessful for t_log_entries table', 10, 1)
            return 51191
        End If;
    End If;

    return 0

END
$$;

CREATE OR REPLACE PROCEDURE mc.PostUsageLogEntry
(
    _postedBy text,
    _message text = '',
    _MinimumUpdateInterval int = 1
)
LANGUAGE plpgsql
AS $$
/****************************************************
**
**  Desc:   Put new entry into T_Usage_Log and update T_Usage_Stats
**
**  Arguments:
**    _MinimumUpdateInterval   Set to a value greater than 0 to limit the entries to occur at most every _MinimumUpdateInterval hours
**
**  Auth:   mem
**  Date:   10/22/2004
**          07/29/2005 mem - Added parameter @MinimumUpdateInterval
**          03/16/2006 mem - Now updating T_Usage_Stats
**          03/17/2006 mem - Now populating Usage_Count in T_Usage_Log and changed @MinimumUpdateInterval from 6 hours to 1 hour
**          05/03/2009 mem - Removed parameter @DBName
**
*****************************************************/
DECLARE
    _myRowCount int;
    _myError int;
    _CallingUser text;
    _PostEntry tinyint;
    _LastUpdated text;
BEGIN
    _myRowCount := 0;
    _myError := 0;

    _CallingUser := SUSER_SNAME();

    _PostEntry := 1;

    -- Update entry for _postedBy in t_usage_stats
    If Not Exists (SELECT posted_by FROM t_usage_stats WHERE posted_by = _postedBy) Then
        INSERT INTO t_usage_stats (posted_by, last_posting_time, usage_count);
    End If;
        VALUES (_postedBy, GetDate(), 1)
    Else
        UPDATE t_usage_stats
        SET last_posting_time = GetDate(), usage_count = usage_count + 1
        WHERE posted_by = _postedBy

    if _MinimumUpdateInterval > 0 Then
        -- See if the last update was less than _MinimumUpdateInterval hours ago

        _LastUpdated := '1/1/1900';

        PERFORM _LastUpdated = MAX(posting_time)
        FROM t_usage_log
        WHERE posted_by = _postedBy AND calling_user = _CallingUser
        --
        GET DIAGNOSTICS _myRowCount = ROW_COUNT;

        IF _myRowCount = 1 Then
            If GetDate() <= DateAdd(hour, _MinimumUpdateInterval, IsNull(_LastUpdated, '1/1/1900')) Then
                _PostEntry := 0;;
            End If;
        End If;
    End If;

    If _PostEntry = 1 Then
        INSERT INTO t_usage_log
                (posted_by, posting_time, message, calling_user, usage_count)
        SELECT _postedBy, GetDate(), _message, _CallingUser, S.usage_count
        FROM t_usage_stats S
        WHERE S.posted_by = _postedBy
        --
        GET DIAGNOSTICS _myRowCount = ROW_COUNT;
        --
        if _myRowCount <> 1 Or _myError <> 0 Then
            _message := 'Update was unsuccessful for t_usage_log table: _myRowCount = ' || _myRowCount::text || '; _myError = ' || _myError::text;
            execute PostLogEntry 'Error', _message, 'PostUsageLogEntry'
        End If;
    End If;

    RETURN 0

END
$$;

CREATE OR REPLACE PROCEDURE mc.ReportManagerErrorCleanup
(
    _ManagerName text,
    _State int = 0,
    _FailureMsg text = '',
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
**    _State   1 = Cleanup Attempt start, 2 = Cleanup Successful, 3 = Cleanup Failed
**
**  Auth:   mem
**  Date:   09/10/2009 mem - Initial version
**
*****************************************************/
DECLARE
    _myError int;
    _myRowCount int;
    _MgrID int;
    _MgrNameLocal text;
    _ParamID int;
    _MessageType text;
    _CleanupMode text;
BEGIN
    _myError := 0;
    _myRowCount := 0;

    ---------------------------------------------------
    -- Cleanup the inputs
    ---------------------------------------------------

    _ManagerName := IsNull(_ManagerName, '');
    _State := IsNull(_State, 0);
    _FailureMsg := IsNull(_FailureMsg, '');
    _message := '';

    ---------------------------------------------------
    -- Confirm that the manager name is valid
    ---------------------------------------------------

    PERFORM _MgrID = m_id,
            _MgrNameLocal = m_name
    FROM t_mgrs
    WHERE (m_name = _ManagerName)
    --
    GET DIAGNOSTICS _myRowCount = ROW_COUNT;

    if _myRowCount <> 1 Then
        _myError := 52002;
        _message := 'Could not find entry for manager: ' || _ManagerName;
        Return;
    End If;

    _ManagerName := _MgrNameLocal;

    ---------------------------------------------------
    -- Validate _State
    ---------------------------------------------------

    If _State < 1 or _State > 3 Then
        _myError := 52003;
        _message := 'Invalid value for _State; should be 1, 2 or 3';
        Return;
    End If;

    ---------------------------------------------------
    -- Log this cleanup event
    ---------------------------------------------------

    _MessageType := 'Error';
    _Message := 'Unknown _State value';

    If _State = 1 Then
        _MessageType := 'Normal';
        _Message := 'Manager ' || _ManagerName || ' is attempting auto error cleanup';
    End If;

    If _State = 2 Then
        _MessageType := 'Normal';
        _Message := 'Automated error cleanup succeeded for ' || _ManagerName;
    End If;

    If _State = 3 Then
        _MessageType := 'Normal';
        _Message := 'Automated error cleanup failed for ' || _ManagerName;
        If _FailureMsg <> '' Then
            _message := _message || '; ' || _FailureMsg;;
        End If;
    End If;

    Call PostLogEntry @MessageType, @Message, 'ReportManagerErrorCleanup'

    ---------------------------------------------------
    -- Lookup the value of ManagerErrorCleanupMode in t_param_value
    ---------------------------------------------------

    _CleanupMode := '0';

    PERFORM _CleanupMode = t_param_value.value
    FROM t_param_value
         INNER JOIN t_param_type
           ON t_param_value.type_id = t_param_type.param_id
    WHERE (t_param_type.param_name = 'ManagerErrorCleanupMode') AND
          (t_param_value.mgr_id = _MgrID)
    --
    GET DIAGNOSTICS _myRowCount = ROW_COUNT;

    If _myRowCount = 0 Then
        -- Entry not found; make a new entry for 'ManagerErrorCleanupMode' in the t_param_value table
        _ParamID := 0;

        PERFORM _ParamID = param_id
        FROM t_param_type
        WHERE (param_name = 'ManagerErrorCleanupMode')
        --
        GET DIAGNOSTICS _myRowCount = ROW_COUNT;

        If _ParamID > 0 Then
            INSERT INTO t_param_value (mgr_id, type_id, value)
            VALUES (_MgrID, _ParamID, '0')

            _CleanupMode := '0';
        End If;
    End If;

    If LTrim(RTrim(_CleanupMode)) = '1' Then
        -- Manager is set to auto-cleanup only once; change 'ManagerErrorCleanupMode' to 0
        UPDATE t_param_value
        SET value = '0'
        FROM t_param_value
             INNER JOIN t_param_type
               ON t_param_value.type_id = t_param_type.param_id
        WHERE (t_param_type.param_name = 'ManagerErrorCleanupMode') AND
              (t_param_value.mgr_id = _MgrID)
        --
        GET DIAGNOSTICS _myRowCount = ROW_COUNT;

        if _myError <> 0 Then
            _Message := 'Error setting ManagerErrorCleanupMode to 0 in t_param_value for manager ' || _ManagerName;
            Call PostLogEntry 'Error', @message, 'ReportManagerErrorCleanup'
        Else
            If _myRowCount = 0 Then
                _message := _Message || '; Entry not found in t_param_value for ManagerErrorCleanupMode; this is unexpected';
            Else
                _message := _Message || '; Decremented ManagerErrorCleanupMode to 0 in t_param_value';;
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

CREATE OR REPLACE PROCEDURE mc.SetManagerErrorCleanupMode
(
    _ManagerList varchar(max) = '',
    _CleanupMode tinyint = 1,
    _showTable tinyint = 1,
    _infoOnly tinyint = 0,
    INOUT _message text = ''
)
LANGUAGE plpgsql
AS $$
/****************************************************
**
**  Desc:
**      Sets ManagerErrorCleanupMode to @CleanupMode for the given list of managers
**      If @ManagerList is blank, then sets it to @CleanupMode for all "Analysis Tool Manager" managers
**
**  Arguments:
**    _CleanupMode   0 = No auto cleanup, 1 = Attempt auto cleanup once, 2 = Auto cleanup always
**
**  Auth:   mem
**  Date:   09/10/2009 mem - Initial version
**          09/29/2014 mem - Expanded @ManagerList to varchar(max) and added parameters @showTable and @infoOnly
**                         - Fixed where clause bug in final update query
**
*****************************************************/
DECLARE
    _myError int;
    _myRowCount int;
    _mgrID int;
    _ParamID int;
    _CleanupModeString text;
BEGIN
    _myError := 0;
    _myRowCount := 0;

    ---------------------------------------------------
    -- Validate the inputs
    ---------------------------------------------------

    _ManagerList := IsNull(_ManagerList, '');
    _CleanupMode := IsNull(_CleanupMode, 1);
    _showTable := IsNull(_showTable, 1);
    _infoOnly := IsNull(_infoOnly, 0);
    _message := '';

    If _CleanupMode < 0 Then
        _CleanupMode := 0;;
    End If;

    If _CleanupMode > 2 Then
        _CleanupMode := 2;;
    End If;

    CREATE TABLE #TmpManagerList (
        ManagerName varchar(128) NOT NULL,
        MgrID int NULL
    )

    ---------------------------------------------------
    -- Confirm that the manager names are valid
    ---------------------------------------------------

    If char_length(_ManagerList) > 0 Then
        INSERT INTO #TmpManagerList (ManagerName);
    End If;
        SELECT Value
        FROM dbo.udfParseDelimitedList(_ManagerList, ',')
        WHERE char_length(IsNull(Value, '')) > 0
    Else
        INSERT INTO #TmpManagerList (ManagerName)
        SELECT m_name
        FROM t_mgrs
        WHERE (mgr_type_id = 11)

    UPDATE #TmpManagerList
    SET MgrID = t_mgrs.m_id
    FROM #TmpManagerList INNER JOIN t_mgrs
            ON t_mgrs.m_name = #TmpManagerList.ManagerName
    --
    GET DIAGNOSTICS _myRowCount = ROW_COUNT;

    DELETE FROM #TmpManagerList
    WHERE MgrID IS NULL
    --
    GET DIAGNOSTICS _myRowCount = ROW_COUNT;

    If _myRowCount <> 0 Then
        _message := 'Removed ' || _myRowCount::text || ' invalid manager';
        If _myRowCount > 1 Then
            _message := _message || 's';;
        End If;

        _message := _message || ' from #TmpManagerList';
        RAISE INFO '%', _message;
    End If;

    ---------------------------------------------------
    -- Lookup the ParamID value for 'ManagerErrorCleanupMode'
    ---------------------------------------------------

    _ParamID := 0;
    --
    PERFORM _ParamID = param_id
    FROM t_param_type
    WHERE (param_name = 'ManagerErrorCleanupMode')

    ---------------------------------------------------
    -- Make sure each manager in #TmpManagerList has an entry
    --  in t_param_value for 'ManagerErrorCleanupMode'
    ---------------------------------------------------

    INSERT INTO t_param_value (mgr_id, type_id, value)
    SELECT A.mgr_id, _ParamID, '0'
    FROM ( SELECT mgr_id
           FROM #TmpManagerList
         ) A
         LEFT OUTER JOIN
          ( SELECT #TmpManagerList.mgr_id
            FROM #TmpManagerList
                 INNER JOIN t_param_value
                   ON #TmpManagerList.mgr_id = t_param_value.mgr_id
            WHERE t_param_value.type_id = _ParamID
         ) B
           ON A.mgr_id = B.mgr_id
    WHERE B.mgr_id IS NULL
    --
    GET DIAGNOSTICS _myRowCount = ROW_COUNT;

    If _myRowCount <> 0 Then
        _message := 'Added entry for "ManagerErrorCleanupMode" to t_param_value for ' || _myRowCount::text || ' manager';
        If _myRowCount > 1 Then
            _message := _message || 's';;
        End If;

        RAISE INFO '%', _message;
    End If;

    ---------------------------------------------------
    -- Update the 'ManagerErrorCleanupMode' entry for each manager in #TmpManagerList
    ---------------------------------------------------

    _CleanupModeString := _CleanupMode::text;

    If _infoOnly <> 0 Then
        SELECT MP.*, _CleanupMode As NewCleanupMode
        FROM V_AnalysisMgrParams_ActiveAndDebugLevel MP
            INNER JOIN #TmpManagerList
            ON MP.MgrID = #TmpManagerList.MgrID
        WHERE MP.ParamTypeID = 120
        ORDER BY MP.Manager
    Else

        UPDATE t_param_value
        SET value = _CleanupModeString
        FROM t_param_value
            INNER JOIN #TmpManagerList
            ON t_param_value.mgr_id = #TmpManagerList.mgr_id
        WHERE t_param_value.type_id = _ParamID AND
            t_param_value.value <> _CleanupModeString
        --
        GET DIAGNOSTICS _myRowCount = ROW_COUNT;

        If _myRowCount <> 0 Then
            _message := 'Set "ManagerErrorCleanupMode" to ' || _CleanupModeString || ' for ' || _myRowCount::text || ' manager';
            If _myRowCount > 1 Then
                _message := _message || 's';;
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
            INNER JOIN #TmpManagerList
            ON MP.MgrID = #TmpManagerList.MgrID
        WHERE MP.ParamTypeID = 120
        ORDER BY MP.Manager
    End If;

    ---------------------------------------------------
    -- Exit the procedure
    ---------------------------------------------------
Done:
    return _myError

END
$$;
