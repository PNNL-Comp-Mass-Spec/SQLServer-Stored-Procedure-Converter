
CREATE OR REPLACE PROCEDURE mc.ArchiveOldManagersAndParams
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
**       One or more manager names (comma-separated list); supports wildcards because uses stored procedure ParseManagerNameList
**
**  Auth:   mem
**  Date:   05/14/2015 mem - Initial version
**          02/25/2016 mem - Add Set XACT_ABORT On
**          04/22/2016 mem - Now updating M_Comment in T_OldManagers
**          02/04/2020 mem - Ported to PostgreSQL
**
*****************************************************/
DECLARE
    _myRowCount int;
    _myError int;
    _moveParams text := 'Move params transaction';
BEGIN
    _myRowCount := 0;
    _myError := 0;

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

    Call ParseManagerNameList _mgrList, _removeUnknownManagers := 0

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
    ** WHERE #TmpManagerList.Primary_Key_ID = #TmpManagerListAliased.Primary_Key_ID
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
        ** WHERE mc.t_param_value.Primary_Key_ID = T_ParamValueAliased.Primary_Key_ID
        **
        ** Delete queries must also include the USING keyword
        ** Alternatively, the more standard approach is to rearrange the query to be similar to
        ** DELETE FROM target WHERE id in (SELECT id from ...)
        ********************************************************************************/

                               ToDo: Fix this query

             INNER JOIN TmpManagerList Src
               ON PV.MgrID = Src.M_ID

        DELETE mc.t_mgrs
        FROM mc.t_mgrs M

        /********************************************************************************
        ** This DELETE query includes the target table name in the FROM clause
        ** The WHERE clause needs to have a self join to the target table, for example:
        ** WHERE mc.t_mgrs.Primary_Key_ID = T_MgrsAliased.Primary_Key_ID
        **
        ** Delete queries must also include the USING keyword
        ** Alternatively, the more standard approach is to rearrange the query to be similar to
        ** DELETE FROM target WHERE id in (SELECT id from ...)
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

END
$$;

COMMENT ON PROCEDURE mc.ArchiveOldManagersAndParams IS 'ArchiveOldManagersAndParams';

CREATE OR REPLACE PROCEDURE mc.DisableAnalysisManagers
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
**          02/04/2020 mem - Ported to PostgreSQL
**
*****************************************************/
DECLARE
    _myError int;
BEGIN
    Call EnableDisableAllManagers _managerTypeIDList := '11', _managerNameList := '', _enable := 0,
                                             _previewUpdates=@PreviewUpdates, _message = _message output

    Return _myError

END
$$;

COMMENT ON PROCEDURE mc.DisableAnalysisManagers IS 'DisableAnalysisManagers';

CREATE OR REPLACE PROCEDURE mc.DisableArchiveDependentManagers
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
**          02/04/2020 mem - Ported to PostgreSQL
**
*****************************************************/
DECLARE
    _myError int;
BEGIN
    Call EnableDisableAllManagers _managerTypeIDList := '8', _managerNameList := '', _enable := 0,
                                             _previewUpdates=@PreviewUpdates, _message = _message output

    Return _myError

END
$$;

COMMENT ON PROCEDURE mc.DisableArchiveDependentManagers IS 'DisableArchiveDependentManagers';

CREATE OR REPLACE PROCEDURE mc.EnableDisableAllManagers
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
**       Optional: list of manager type IDs to disable, e.g. "1, 2, 3"
**       Optional: if defined, then only managers specified here will be enabled; supports the % wildcard
**       1 to enable, 0 to disable
**
**  Auth:   mem
**  Date:   05/09/2008
**          06/09/2011 - Created by extending code in DisableAllManagers
**                     - Now filtering on MT_Active > 0 in T_MgrTypes
**          02/04/2020 mem - Ported to PostgreSQL
**
*****************************************************/
DECLARE
    _myRowCount int;
    _myError int;
    _mgrTypeID int;
    _continue int;
BEGIN
    _myRowCount := 0;
    _myError := 0;

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
        FROM public.udf_parse_delimited_integer_list(_managerTypeIDList, ',')
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
        SELECT MgrTypeID INTO _mgrTypeID
        FROM TmpManagerTypeIDs
        WHERE MgrTypeID > _mgrTypeID
        ORDER BY MgrTypeID
        --
        GET DIAGNOSTICS _myRowCount = ROW_COUNT;

        If _myRowCount = 0 Then
            _continue := 0;
        Else
            Call EnableDisableManagers _enable := @Enable, _managerTypeID := @MgrTypeID, _managerNameList := @ManagerNameList, _previewUpdates := _previewUpdates, _message := _message output
        End If;
    End Loop;

Done:
    Return _myError

END
$$;

COMMENT ON PROCEDURE mc.EnableDisableAllManagers IS 'EnableDisableAllManagers';

CREATE OR REPLACE PROCEDURE mc.EnableDisableManagers
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
**       0 to disable, 1 to enable
**       Defined in table T_MgrTypes.  8=Space, 9=DataImport, 11=Analysis Tool Manager, 15=CaptureTaskManager
**       Required when _enable = 1.  Only managers specified here will be enabled, though you can use "All" to enable All managers.  When _enable = 0, if this parameter is blank (or All) then all managers of the given type will be disabled; supports the % wildcard
**
**  Auth:   mem
**  Date:   07/12/2007
**          05/09/2008 mem - Added parameter _managerNameList
**          06/09/2011 mem - Now filtering on MT_Active > 0 in T_MgrTypes
**                         - Now allowing _managerNameList to be All when _enable = 1
**          10/12/2017 mem - Allow _managerTypeID to be 0 if _managerNameList is provided
**          03/28/2018 mem - Use different messages when updating just one manager
**          02/04/2020 mem - Ported to PostgreSQL
**
*****************************************************/
DECLARE
    _myRowCount int;
    _myError int;
    _newValue text;
    _managerTypeName text;
    _activeStateDescription text;
    _countToUpdate int;
    _countUnchanged int;
BEGIN
    _myRowCount := 0;
    _myError := 0;

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

        Call ParseManagerNameList _managerNameList, _removeUnknownManagers := 1, _message := @message output

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
            ** WHERE #TmpManagerList.Primary_Key_ID = #TmpManagerListAliased.Primary_Key_ID
            **
            ** Delete queries must also include the USING keyword
            ** Alternatively, the more standard approach is to rearrange the query to be similar to
            ** DELETE FROM target WHERE id in (SELECT id from ...)
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
            SELECT Convert(text, PV.value + '-->' + _newValue) AS State_Change_Preview,
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
            ** WHERE mc.t_param_value.Primary_Key_ID = T_ParamValueAliased.Primary_Key_ID
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

END
$$;

COMMENT ON PROCEDURE mc.EnableDisableManagers IS 'EnableDisableManagers';

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
**          02/17/2005 mem - Added parameter _duplicateEntryHoldoffHours
**          05/31/2007 mem - Expanded the size of _type, _message, and _postedBy
**          02/04/2020 mem - Ported to PostgreSQL
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
            (posted_by, posting_time, type, message)
        VALUES ( _postedBy, CURRENT_TIMESTAMP, _type, _message)
        --
        if @@rowcount <> 1 Then
            RAISERROR ('Update was unsuccessful for mc.t_log_entries table', 10, 1)
            return 51191
        End If;
    End If;

    return 0

END
$$;

COMMENT ON PROCEDURE mc.PostLogEntry IS 'PostLogEntry';

CREATE OR REPLACE PROCEDURE mc.PostUsageLogEntry
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
**       Set to a value greater than 0 to limit the entries to occur at most every _minimumUpdateInterval hours
**
**  Auth:   mem
**  Date:   10/22/2004
**          07/29/2005 mem - Added parameter _minimumUpdateInterval
**          03/16/2006 mem - Now updating T_Usage_Stats
**          03/17/2006 mem - Now populating Usage_Count in T_Usage_Log and changed _minimumUpdateInterval from 6 hours to 1 hour
**          05/03/2009 mem - Removed parameter _dBName
**          02/04/2020 mem - Ported to PostgreSQL
**
*****************************************************/
DECLARE
    _myRowCount int;
    _myError int;
    _callingUser text;
    _postEntry int;
    _lastUpdated text;
BEGIN
    _myRowCount := 0;
    _myError := 0;

    _callingUser := session_user;

    _postEntry := 1;

    -- Update entry for _postedBy in mc.t_usage_stats
    If Not Exists (SELECT posted_by FROM mc.t_usage_stats WHERE posted_by = _postedBy) Then
        INSERT INTO mc.t_usage_stats (posted_by, last_posting_time, usage_count);
    End If;
        VALUES (_postedBy, CURRENT_TIMESTAMP, 1)
    Else
        UPDATE mc.t_usage_stats
        SET last_posting_time = CURRENT_TIMESTAMP, usage_count = usage_count + 1
        WHERE posted_by = _postedBy

    if _minimumUpdateInterval > 0 Then
        -- See if the last update was less than _minimumUpdateInterval hours ago

        _lastUpdated := '1/1/1900';

        SELECT MAX(posting_time) INTO _lastUpdated
        FROM mc.t_usage_log
        WHERE posted_by = _postedBy AND calling_user = _callingUser
        --
        GET DIAGNOSTICS _myRowCount = ROW_COUNT;

        IF _myRowCount = 1 Then
            If CURRENT_TIMESTAMP <= DateAdd(hour, _minimumUpdateInterval, Coalesce(_lastUpdated, '1/1/1900')) Then
                _postEntry := 0;
            End If;
        End If;
    End If;

    If _postEntry = 1 Then
        INSERT INTO mc.t_usage_log
                (posted_by, posting_time, message, calling_user, usage_count)
        SELECT _postedBy, CURRENT_TIMESTAMP, _message, _callingUser, S.usage_count
        FROM mc.t_usage_stats S
        WHERE S.posted_by = _postedBy
        --
        GET DIAGNOSTICS _myRowCount = ROW_COUNT;
        --
        if _myRowCount <> 1 Or _myError <> 0 Then
            _message := 'Update was unsuccessful for mc.t_usage_log table: _myRowCount = ' || _myRowCount::text || '; _myError = ' || _myError::text;
            execute PostLogEntry 'Error', _message, 'PostUsageLogEntry'
        End If;
    End If;

    RETURN 0

END
$$;

COMMENT ON PROCEDURE mc.PostUsageLogEntry IS 'PostUsageLogEntry';

CREATE OR REPLACE PROCEDURE mc.ReportManagerErrorCleanup
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
**       1 = Cleanup Attempt start, 2 = Cleanup Successful, 3 = Cleanup Failed
**
**  Auth:   mem
**  Date:   09/10/2009 mem - Initial version
**          02/04/2020 mem - Ported to PostgreSQL
**
*****************************************************/
DECLARE
    _myError int;
    _myRowCount int;
    _mgrID int;
    _mgrNameLocal text;
    _paramID int;
    _messageType text;
    _cleanupMode text;
BEGIN
    _myError := 0;
    _myRowCount := 0;

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

    Call PostLogEntry _messageType, _message, 'ReportManagerErrorCleanup'

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

    If LTrim(RTrim(_cleanupMode)) = '1' Then
        -- Manager is set to auto-cleanup only once; change 'ManagerErrorCleanupMode' to 0
        UPDATE mc.t_param_value
        SET value = '0'
        FROM mc.t_param_value

        /********************************************************************************
        ** This UPDATE query includes the target table name in the FROM clause
        ** The WHERE clause needs to have a self join to the target table, for example:
        ** WHERE mc.t_param_value.Primary_Key_ID = T_ParamValueAliased.Primary_Key_ID
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
            Call PostLogEntry 'Error', _message, 'ReportManagerErrorCleanup'
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

COMMENT ON PROCEDURE mc.ReportManagerErrorCleanup IS 'ReportManagerErrorCleanup';

CREATE OR REPLACE PROCEDURE mc.SetManagerErrorCleanupMode
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
**       0 = No auto cleanup, 1 = Attempt auto cleanup once, 2 = Auto cleanup always
**
**  Auth:   mem
**  Date:   09/10/2009 mem - Initial version
**          09/29/2014 mem - Expanded _managerList to varchar(max) and added parameters _showTable and _infoOnly
**                         - Fixed where clause bug in final update query
**          02/04/2020 mem - Ported to PostgreSQL
**
*****************************************************/
DECLARE
    _myError int;
    _myRowCount int;
    _mgrID int;
    _paramID int;
    _cleanupModeString text;
BEGIN
    _myError := 0;
    _myRowCount := 0;

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
        FROM public.udf_parse_delimited_list(_managerList, ',')
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
    ** WHERE #TmpManagerList.Primary_Key_ID = #TmpManagerListAliased.Primary_Key_ID
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
        ** WHERE mc.t_param_value.Primary_Key_ID = T_ParamValueAliased.Primary_Key_ID
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

END
$$;

COMMENT ON PROCEDURE mc.SetManagerErrorCleanupMode IS 'SetManagerErrorCleanupMode';
