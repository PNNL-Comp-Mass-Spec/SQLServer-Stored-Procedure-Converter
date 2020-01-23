USE [Manager_Control]
GO

/****** Object:  StoredProcedure [dbo].[ArchiveOldManagersAndParams]    Script Date: 1/15/2020 8:33:53 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[ArchiveOldManagersAndParams]
/****************************************************
**
**	Desc:	Moves managers from T_Mgrs to T_OldManagers
**			and moves manager parameters from T_ParamValue to T_ParamValue_OldManagers
**
**			To reverse this process, use procedure UnarchiveOldManagersAndParams
**
**	Return values: 0: success, otherwise, error code
**
**	Auth:	mem
**	Date:	05/14/2015 mem - Initial version
**			02/25/2016 mem - Add Set XACT_ABORT On
**			04/22/2016 mem - Now updating M_Comment in T_OldManagers
**
*****************************************************/
(
	@MgrList varchar(max),	-- One or more manager names (comma-separated list); supports wildcards because uses stored procedure ParseManagerNameList
	@InfoOnly tinyint = 1,
	@message varchar(512)='' output
)
As
	Set XACT_ABORT, NoCount On

	declare @myRowCount int
	declare @myError int
	set @myRowCount = 0
	set @myError = 0

	---------------------------------------------------
	-- Validate the inputs
	---------------------------------------------------
	--
	Set @MgrList = IsNull(@MgrList, '')
	Set @InfoOnly = IsNull(@InfoOnly, 1)
	Set @message = ''

	CREATE TABLE #TmpManagerList (
		Manager_Name varchar(50) NOT NULL,
		M_ID int NULL,
		M_ControlFromWebsite tinyint null
	)

	---------------------------------------------------
	-- Populate #TmpManagerList with the managers in @MgrList
	---------------------------------------------------
	--

	exec ParseManagerNameList @MgrList, @RemoveUnknownManagers=0

	If Not Exists (Select * from #TmpManagerList)
	Begin
		Set @message = '@MgrList was empty; no match in T_Mgrs to ' + @MgrList
		Select @Message as Warning
		Goto done
	End

	---------------------------------------------------
	-- Validate the manager names
	---------------------------------------------------
	--
	UPDATE #TmpManagerList
	SET M_ID = M.M_ID,
	    M_ControlFromWebsite = M.M_ControlFromWebsite
	FROM #TmpManagerList Target
	     INNER JOIN T_Mgrs M
	       ON Target.Manager_Name = M.M_Name
	--
	SELECT @myError = @@error, @myRowCount = @@rowcount

	If Exists (Select * from #TmpManagerList where M_ID Is Null)
	Begin
		SELECT 'Unknown manager (not in T_Mgrs)' AS Warning, Manager_Name
		FROM #TmpManagerList
		ORDER BY Manager_Name
	End

	If Exists (Select * from #TmpManagerList WHERE NOT M_ID is Null And M_ControlFromWebsite > 0)
	Begin
		SELECT 'Manager has M_ControlFromWebsite=1; cannot archive' AS Warning,
		       Manager_Name
		FROM #TmpManagerList
		WHERE NOT M_ID IS NULL AND
		      M_ControlFromWebsite > 0
		ORDER BY Manager_Name
	End

	If Exists (Select * From #TmpManagerList Where Manager_Name Like '%Params%')
	Begin
		SELECT 'Will not process managers with "Params" in the name (for safety)' AS Warning,
		       Manager_Name
		FROM #TmpManagerList
		WHERE Manager_Name Like '%Params%'
		ORDER BY Manager_Name

		DELETE From #TmpManagerList Where Manager_Name Like '%Params%'
	End

	If @InfoOnly <> 0
	Begin
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

	End
	Else
	Begin
		DELETE FROM #TmpManagerList WHERE M_ID is Null OR M_ControlFromWebsite > 0

		Declare @MoveParams varchar(24) = 'Move params transaction'
		Begin Tran @MoveParams


		INSERT INTO T_OldManagers( M_ID,
		                           M_Name,
		                           M_TypeID,
		                           M_ParmValueChanged,
		                           M_ControlFromWebsite,
		                           M_Comment )
		SELECT M.M_ID,
		       M.M_Name,
		       M.M_TypeID,
		       M.M_ParmValueChanged,
		       M.M_ControlFromWebsite,
		       M.M_Comment
		FROM T_Mgrs M
		     INNER JOIN #TmpManagerList Src
		       ON M.M_ID = Src.M_ID
		  LEFT OUTER JOIN T_OldManagers Target
		       ON Src.M_ID = Target.M_ID
		WHERE Target.M_ID IS NULL
		--
		SELECT @myError = @@error, @myRowCount = @@rowcount
		--
		If @myError <> 0
		Begin
			Rollback
			Select 'Aborted (rollback)' as Warning, @myError as ErrorCode
			Goto Done
		End


		INSERT INTO T_ParamValue_OldManagers(
		         Entry_ID,
		         TypeID,
		         [Value],
		         MgrID,
		         [Comment],
		         Last_Affected,
		         Entered_By )
		SELECT PV.Entry_ID,
		       PV.TypeID,
		       PV.[Value],
		       PV.MgrID,
		       PV.[Comment],
		       PV.Last_Affected,
		       PV.Entered_By
		FROM T_ParamValue PV
		     INNER JOIN #TmpManagerList Src
		       ON PV.MgrID = Src.M_ID
		--
		SELECT @myError = @@error, @myRowCount = @@rowcount
		--
		If @myError <> 0
		Begin
			Rollback
			Select 'Aborted (rollback)' as Warning, @myError as ErrorCode
			Goto Done
		End

		DELETE T_ParamValue
		FROM T_ParamValue PV
		     INNER JOIN #TmpManagerList Src
		       ON PV.MgrID = Src.M_ID

		DELETE T_Mgrs
		FROM T_Mgrs M
		     INNER JOIN #TmpManagerList Src
		       ON M.M_ID = Src.M_ID

		Commit Tran @MoveParams

		SELECT 'Moved to T_OldManagers and T_ParamValue_OldManagers' as Message,
		       Src.Manager_Name,
		       Src.M_ControlFromWebsite,
		       PT.ParamName,
		       PV.Entry_ID,
		       PV.TypeID,
		       PV.[Value],
		       PV.MgrID,
		       PV.[Comment],
		       PV.Last_Affected,
		       PV.Entered_By
		FROM #TmpManagerList Src
		     LEFT OUTER JOIN T_ParamValue_OldManagers PV
		       ON PV.MgrID = Src.M_ID
		     LEFT OUTER JOIN T_ParamType PT ON
		     PV.TypeID = PT.ParamID
		ORDER BY Src.Manager_Name, ParamName
	End


Done:
	RETURN @myError
GO

/****** Object:  StoredProcedure [dbo].[DisableAnalysisManagers]    Script Date: 1/15/2020 8:33:53 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE PROCEDURE [dbo].[DisableAnalysisManagers]
/****************************************************
**
**	Desc:	Disables all analysis managers
**
**	Return values: 0: success, otherwise, error code
**
**	Auth:	mem
**	Date:	05/09/2008
**			10/09/2009 mem - Changed @ManagerTypeIDList to 11
**			06/09/2011 mem - Now calling EnableDisableAllManagers
**
*****************************************************/
(
	@PreviewUpdates tinyint = 0,
	@message varchar(512)='' output
)
As
	Set NoCount On

	Declare @myError int

	exec @myerror = EnableDisableAllManagers @ManagerTypeIDList='11', @ManagerNameList='', @enable=0,
	                                         @PreviewUpdates=@PreviewUpdates, @message = @message output

	Return @myError


GO

GRANT EXECUTE ON [dbo].[DisableAnalysisManagers] TO [Mgr_Config_Admin] AS [dbo]
GO

/****** Object:  StoredProcedure [dbo].[DisableArchiveDependentManagers]    Script Date: 1/15/2020 8:33:53 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[DisableArchiveDependentManagers]
/****************************************************
**
**	Desc:	Disables managers that rely on the NWFS archive
**
**	Return values: 0: success, otherwise, error code
**
**	Auth:	mem
**	Date:	05/09/2008
**			07/24/2008 mem - Changed @ManagerTypeIDList from '1,2,3,4,8' to '2,3,8'
**			07/24/2008 mem - Changed @ManagerTypeIDList from '2,3,8' to '8'
**						   - Note that we do not include 15=CaptureTaskManager because capture tasks can still occur when the archive is unavailable
**						   - However, you should run Stored Procedure EnableDisableArchiveStepTools in the DMS_Capture database to disable the archive-dependent step tools
**
*****************************************************/
(
	@PreviewUpdates tinyint = 0,
	@message varchar(512)='' output
)
As
	Set NoCount On

	Declare @myError int

	exec @myerror = EnableDisableAllManagers @ManagerTypeIDList='8', @ManagerNameList='', @enable=0,
	                                         @PreviewUpdates=@PreviewUpdates, @message = @message output


	Return @myError

GO

GRANT EXECUTE ON [dbo].[DisableArchiveDependentManagers] TO [Mgr_Config_Admin] AS [dbo]
GO

/****** Object:  StoredProcedure [dbo].[EnableDisableAllManagers]    Script Date: 1/15/2020 8:33:53 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[EnableDisableAllManagers]
/****************************************************
**
**	Desc:	Enables or disables all managers, optionally filtering by manager type ID or manager name
**
**	Return values: 0: success, otherwise, error code
**
**	Auth:	mem
**	Date:	05/09/2008
**			06/09/2011 - Created by extending code in DisableAllManagers
**					   - Now filtering on MT_Active > 0 in T_MgrTypes
**
*****************************************************/
(
	@ManagerTypeIDList varchar(1024) = '',	-- Optional: list of manager type IDs to disable, e.g. "1, 2, 3"
	@ManagerNameList varchar(4000) = '',	-- Optional: if defined, then only managers specified here will be enabled; supports the % wildcard
	@Enable tinyint = 1,					-- 1 to enable, 0 to disable
	@PreviewUpdates tinyint = 0,
	@message varchar(512)='' output
)
As
	Set NoCount On

	declare @myRowCount int
	declare @myError int
	set @myRowCount = 0
	set @myError = 0

	Declare @MgrTypeID int
	Declare @Continue int

	-----------------------------------------------
	-- Validate the inputs
	-----------------------------------------------
	--
	Set @Enable = IsNull(@Enable, 0)
	Set @ManagerTypeIDList = IsNull(@ManagerTypeIDList, '')
	Set @ManagerNameList = IsNull(@ManagerNameList, '')
	Set @PreviewUpdates = IsNull(@PreviewUpdates, 0)
	Set @message = ''

	CREATE TABLE #TmpManagerTypeIDs (
		MgrTypeID int NOT NULL
	)

	If Len(@ManagerTypeIDList) > 0
	Begin
		-- Parse @ManagerTypeIDList
		--
		INSERT INTO #TmpManagerTypeIDs (MgrTypeID)
		SELECT DISTINCT Value
		FROM dbo.udfParseDelimitedIntegerList(@ManagerTypeIDList, ',')
		ORDER BY Value
		--
		SELECT @myError = @@error, @myRowCount = @@rowcount
	End
	Else
	Begin
		-- Populate #TmpManagerTypeIDs with all manager types in T_MgrTypes
		--
		INSERT INTO #TmpManagerTypeIDs (MgrTypeID)
		SELECT DISTINCT MT_TypeID
		FROM T_MgrTypes
		WHERE MT_Active > 0
		ORDER BY MT_TypeID
		--
		SELECT @myError = @@error, @myRowCount = @@rowcount
	End

	-----------------------------------------------
	-- Loop through the manager types in #TmpManagerTypeIDs
	-- For each, call EnableDisableManagers
	-----------------------------------------------

	Set @MgrTypeID = 0
	SELECT @MgrTypeID = MIN(MgrTypeID)-1
	FROM #TmpManagerTypeIDs
	--
	SELECT @myError = @@error, @myRowCount = @@rowcount

	Set @Continue = 1
	While @Continue = 1
	Begin
		SELECT TOP 1 @MgrTypeID = MgrTypeID
		FROM #TmpManagerTypeIDs
		WHERE MgrTypeID > @MgrTypeID
		ORDER BY MgrTypeID
		--
		SELECT @myError = @@error, @myRowCount = @@rowcount

		If @myRowCount = 0
			Set @Continue = 0
		Else
		Begin
			exec @myError = EnableDisableManagers @Enable=@Enable, @ManagerTypeID=@MgrTypeID, @ManagerNameList=@ManagerNameList, @PreviewUpdates = @PreviewUpdates, @message = @message output
		End
	End

Done:
	Return @myError

GO

GRANT EXECUTE ON [dbo].[EnableDisableAllManagers] TO [Mgr_Config_Admin] AS [dbo]
GO

/****** Object:  StoredProcedure [dbo].[EnableDisableManagers]    Script Date: 1/15/2020 8:33:53 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE PROCEDURE [dbo].[EnableDisableManagers]
/****************************************************
**
**  Desc:  Enables or disables all managers of the given type
**
**  Return values: 0: success, otherwise, error code
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
(
    @Enable tinyint,                        -- 0 to disable, 1 to enable
    @ManagerTypeID int=11,                  -- Defined in table T_MgrTypes.  8=Space, 9=DataImport, 11=Analysis Tool Manager, 15=CaptureTaskManager
    @ManagerNameList varchar(4000) = '',    -- Required when @Enable = 1.  Only managers specified here will be enabled, though you can use "All" to enable All managers.  When @Enable = 0, if this parameter is blank (or All) then all managers of the given type will be disabled; supports the % wildcard
    @PreviewUpdates tinyint = 0,
    @message varchar(512)='' output
)
As
    Set NoCount On

    declare @myRowCount int
    declare @myError int
    set @myRowCount = 0
    set @myError = 0

    Declare @NewValue varchar(32)
    Declare @ManagerTypeName varchar(128)
    Declare @ActiveStateDescription varchar(16)
    Declare @CountToUpdate int
    Declare @CountUnchanged int

    -----------------------------------------------
    -- Validate the inputs
    -----------------------------------------------
    --
    Set @ManagerNameList = IsNull(@ManagerNameList, '')
    Set @PreviewUpdates = IsNull(@PreviewUpdates, 0)

    If @Enable Is Null
    Begin
        set @myError  = 40000
        Set @message = '@Enable cannot be null'
        SELECT @message AS Message
        Goto Done
    End

    If @ManagerTypeID Is Null
    Begin
        set @myError = 40001
        Set @message = '@ManagerTypeID cannot be null'
        SELECT @message AS Message
        Goto Done
    End

    If @ManagerTypeID = 0 And Len(@ManagerNameList) > 0 And @ManagerNameList <> 'All'
    Begin
        Set @ManagerTypeName = 'Any'
    End
    Else
    Begin
        -- Make sure @ManagerTypeID is valid
        Set @ManagerTypeName = ''
        SELECT @ManagerTypeName = MT_TypeName
        FROM T_MgrTypes
        WHERE MT_TypeID = @ManagerTypeID AND
            MT_Active > 0
        --
        SELECT @myError = @@error, @myRowCount = @@rowcount

        If @myRowCount = 0
        Begin
            If Exists (SELECT * FROM T_MgrTypes WHERE MT_TypeID = @ManagerTypeID AND MT_Active = 0)
                Set @message = '@ManagerTypeID ' + Convert(varchar(12), @ManagerTypeID) + ' has MT_Active = 0 in T_MgrTypes; unable to continue'
            Else
                Set @message = '@ManagerTypeID ' + Convert(varchar(12), @ManagerTypeID) + ' not found in T_MgrTypes'

            SELECT @message AS Message
            set @myError  = 40002
            Goto Done
        End
    End

    If @Enable <> 0 AND Len(@ManagerNameList) = 0
    Begin
        Set @message = '@ManagerNameList cannot be blank when @Enable is non-zero; to update all managers, set @ManagerNameList to All'
        SELECT @message AS Message
        set @myError  = 40003
        Goto Done
    End

    -----------------------------------------------
    -- Creata a temporary table
    -----------------------------------------------

    CREATE TABLE #TmpManagerList (
        Manager_Name varchar(128) NOT NULL
    )

    If Len(@ManagerNameList) > 0 And @ManagerNameList <> 'All'
    Begin
        -- Populate #TmpMangerList using ParseManagerNameList

        Exec @myError = ParseManagerNameList @ManagerNameList, @RemoveUnknownManagers=1, @message=@message output

        If @myError <> 0
        Begin
            If Len(@message) = 0
                Set @message = 'Error calling ParseManagerNameList: ' + Convert(varchar(12), @myError)

            Goto Done
        End

        If @ManagerTypeID > 0
        Begin
            -- Delete entries from #TmpManagerList that don't match entries in M_Name of the given type
            DELETE #TmpManagerList
            FROM #TmpManagerList U LEFT OUTER JOIN
                T_Mgrs M ON M.M_Name = U.Manager_Name AND M.M_TypeID = @ManagerTypeID
            WHERE M.M_Name Is Null
            --
            SELECT @myError = @@error, @myRowCount = @@rowcount

            If @myRowCount > 0
            Begin
                Set @message = 'Found ' + convert(varchar(12), @myRowCount) + ' entries in @ManagerNameList that are not ' + @ManagerTypeName + ' managers'
                Set @message = ''
            End
        End

    End
    Else
    Begin
        -- Populate #TmpManagerList with all managers in T_Mgrs
        --
        INSERT INTO #TmpManagerList (Manager_Name)
        SELECT M_Name
        FROM T_Mgrs
        WHERE M_TypeID = @ManagerTypeID
        --
        SELECT @myError = @@error, @myRowCount = @@rowcount
    End


    -- Set @NewValue based on @Enable
    If @Enable = 0
    Begin
        Set @NewValue = 'False'
        Set @ActiveStateDescription = 'Inactive'
    End
    Else
    Begin
        Set @NewValue = 'True'
        Set @ActiveStateDescription = 'Active'
    End

    -- Count the number of managers that need to be updated
    Set @CountToUpdate = 0
    SELECT @CountToUpdate = COUNT(*)
    FROM T_ParamValue PV
         INNER JOIN T_ParamType PT
           ON PV.TypeID = PT.ParamID
         INNER JOIN T_Mgrs M
           ON PV.MgrID = M.M_ID
         INNER JOIN T_MgrTypes MT
           ON M.M_TypeID = MT.MT_TypeID
         INNER JOIN #TmpManagerList U
           ON M.M_Name = U.Manager_Name
    WHERE PT.ParamName = 'mgractive' AND
          PV.Value <> @NewValue AND
          MT.MT_Active > 0
    --
    SELECT @myError = @@error, @myRowCount = @@rowcount


    -- Count the number of managers already in the target state
    Set @CountUnchanged = 0
    SELECT @CountUnchanged = COUNT(*)
    FROM T_ParamValue PV
         INNER JOIN T_ParamType PT
           ON PV.TypeID = PT.ParamID
         INNER JOIN T_Mgrs M
           ON PV.MgrID = M.M_ID
         INNER JOIN T_MgrTypes MT
           ON M.M_TypeID = MT.MT_TypeID
         INNER JOIN #TmpManagerList U
           ON M.M_Name = U.Manager_Name
    WHERE PT.ParamName = 'mgractive' AND
          PV.Value = @NewValue AND
          MT.MT_Active > 0
    --
    SELECT @myError = @@error, @myRowCount = @@rowcount


    If @CountToUpdate = 0
    Begin
        If @CountUnchanged = 0
        Begin
            If Len(@ManagerNameList) > 0
            Begin
                If @ManagerTypeID = 0
                    Set @message = 'None of the managers in @ManagerNameList was recognized'
                Else
                    Set @message = 'No ' + @ManagerTypeName + ' managers were found matching @ManagerNameList'
            End
            Else
            Begin
                Set @message = 'No ' + @ManagerTypeName + ' managers were found in T_Mgrs'
            End
        End
        Else
        Begin
            If @CountUnchanged = 1
            Begin
                Set @message = 'The manager is already ' + @ActiveStateDescription
            End
            Else
            Begin
                If @ManagerTypeID = 0
                    Set @message = 'All ' + Convert(varchar(12), @CountUnchanged) + ' managers are already ' + @ActiveStateDescription
                Else
                    Set @message = 'All ' + Convert(varchar(12), @CountUnchanged) + ' ' + @ManagerTypeName + ' managers are already ' + @ActiveStateDescription
            End
        End

        SELECT @message AS Message

    End
    Else
    Begin
        If @PreviewUpdates <> 0
        Begin
            SELECT Convert(varchar(32), PV.Value + '-->' + @NewValue) AS State_Change_Preview,
                   PT.ParamName AS Parameter_Name,
                   M.M_Name AS Manager_Name,
                   MT.MT_TypeName AS Manager_Type
            FROM T_ParamValue PV
                 INNER JOIN T_ParamType PT
                   ON PV.TypeID = PT.ParamID
                 INNER JOIN T_Mgrs M
                   ON PV.MgrID = M.M_ID
                 INNER JOIN T_MgrTypes MT
                   ON M.M_TypeID = MT.MT_TypeID
                 INNER JOIN #TmpManagerList U
                   ON M.M_Name = U.Manager_Name
            WHERE PT.ParamName = 'mgractive' AND
                  PV.Value <> @NewValue AND
                  MT.MT_Active > 0
            --
            SELECT @myError = @@error, @myRowCount = @@rowcount
        End
        Else
        Begin
            UPDATE T_ParamValue
            SET VALUE = @NewValue
            FROM T_ParamValue PV
                 INNER JOIN T_ParamType PT
                   ON PV.TypeID = PT.ParamID
                 INNER JOIN T_Mgrs M
                   ON PV.MgrID = M.M_ID
                 INNER JOIN T_MgrTypes MT
                   ON M.M_TypeID = MT.MT_TypeID
                 INNER JOIN #TmpManagerList U
                   ON M.M_Name = U.Manager_Name
            WHERE PT.ParamName = 'mgractive' AND
                  PV.Value <> @NewValue AND
                  MT.MT_Active > 0
            --
            SELECT @myError = @@error, @myRowCount = @@rowcount

            If @myRowCount = 1 And @CountUnchanged = 0
            Begin
                Set @message = 'The manager is now ' + @ActiveStateDescription
            End
            Else
            Begin
                If @ManagerTypeID = 0
                    Set @message = 'Set ' + Convert(varchar(12), @myRowCount) + ' managers to state ' + @ActiveStateDescription
                Else
                    Set @message = 'Set ' + Convert(varchar(12), @myRowCount) + ' ' + @ManagerTypeName + ' managers to state ' + @ActiveStateDescription

                If @CountUnchanged <> 0
                    Set @message = @message + ' (' + Convert(varchar(12), @CountUnchanged) + ' managers were already ' + @ActiveStateDescription + ')'
            End

            SELECT @message AS Message
        End
    End

Done:
    Return @myError

GO

GRANT EXECUTE ON [dbo].[EnableDisableManagers] TO [Mgr_Config_Admin] AS [dbo]
GO

/****** Object:  StoredProcedure [dbo].[PostLogEntry]    Script Date: 1/15/2020 8:33:53 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE Procedure [dbo].[PostLogEntry]
/****************************************************
**
**	Desc: Put new entry into the main log table
**
**	Return values: 0: success, otherwise, error code
*
**	Auth:	grk
**	Date:	10/31/2001
**			02/17/2005 mem - Added parameter @duplicateEntryHoldoffHours
**			05/31/2007 mem - Expanded the size of @type, @message, and @postedBy
**
*****************************************************/
	@type varchar(128),
	@message varchar(4096),
	@postedBy varchar(128)= 'na',
	@duplicateEntryHoldoffHours int = 0			-- Set this to a value greater than 0 to prevent duplicate entries being posted within the given number of hours
As

	Declare @duplicateRowCount int
	Set @duplicateRowCount = 0

	If IsNull(@duplicateEntryHoldoffHours, 0) > 0
	Begin
		SELECT @duplicateRowCount = COUNT(*)
		FROM T_Log_Entries
		WHERE Message = @message AND Type = @type AND Posting_Time >= (GetDate() - @duplicateEntryHoldoffHours)
	End

	If @duplicateRowCount = 0
	Begin
		INSERT INTO T_Log_Entries
			(posted_by, posting_time, type, message)
		VALUES ( @postedBy, GETDATE(), @type, @message)
		--
		if @@rowcount <> 1
		begin
			RAISERROR ('Update was unsuccessful for T_Log_Entries table', 10, 1)
			return 51191
		end
	End

	return 0

GO

GRANT EXECUTE ON [dbo].[PostLogEntry] TO [DMS_Analysis_Job_Runner] AS [dbo]
GO

GRANT EXECUTE ON [dbo].[PostLogEntry] TO [DMS_SP_User] AS [dbo]
GO

GRANT EXECUTE ON [dbo].[PostLogEntry] TO [Mgr_Config_Admin] AS [dbo]
GO

GRANT EXECUTE ON [dbo].[PostLogEntry] TO [svc-dms] AS [dbo]
GO

/****** Object:  StoredProcedure [dbo].[PostUsageLogEntry]    Script Date: 1/15/2020 8:33:53 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[PostUsageLogEntry]
/****************************************************
**
**	Desc: Put new entry into T_Usage_Log and update T_Usage_Stats
**
**	Return values: 0: success, otherwise, error code
**
**	Parameters:
**
**	Auth:	mem
**	Date:	10/22/2004
**			07/29/2005 mem - Added parameter @MinimumUpdateInterval
**			03/16/2006 mem - Now updating T_Usage_Stats
**			03/17/2006 mem - Now populating Usage_Count in T_Usage_Log and changed @MinimumUpdateInterval from 6 hours to 1 hour
**			05/03/2009 mem - Removed parameter @DBName
**
*****************************************************/
(
	@postedBy varchar(255),
	@message varchar(500) = '',
	@MinimumUpdateInterval int = 1			-- Set to a value greater than 0 to limit the entries to occur at most every @MinimumUpdateInterval hours
)
As
	set nocount on

	declare @myRowCount int
	declare @myError int
	set @myRowCount = 0
	set @myError = 0

	Declare @CallingUser varchar(128)
	Set @CallingUser = SUSER_SNAME()

	declare @PostEntry tinyint
	Set @PostEntry = 1

	Declare @LastUpdated varchar(64)

	-- Update entry for @postedBy in T_Usage_Stats
	If Not Exists (SELECT Posted_By FROM T_Usage_Stats WHERE Posted_By = @postedBy)
		INSERT INTO T_Usage_Stats (Posted_By, Last_Posting_Time, Usage_Count)
		VALUES (@postedBy, GetDate(), 1)
	Else
		UPDATE T_Usage_Stats
		SET Last_Posting_Time = GetDate(), Usage_Count = Usage_Count + 1
		WHERE Posted_By = @postedBy


	if @MinimumUpdateInterval > 0
	Begin
		-- See if the last update was less than @MinimumUpdateInterval hours ago

		Set @LastUpdated = '1/1/1900'

		SELECT @LastUpdated = MAX(Posting_time)
		FROM T_Usage_Log
		WHERE Posted_By = @postedBy AND Calling_User = @CallingUser
		--
		SELECT @myError = @@error, @myRowCount = @@rowcount

		IF @myRowCount = 1
		Begin
			If GetDate() <= DateAdd(hour, @MinimumUpdateInterval, IsNull(@LastUpdated, '1/1/1900'))
				Set @PostEntry = 0
		End
	End


    If @PostEntry = 1
    Begin
		INSERT INTO T_Usage_Log
				(Posted_By, Posting_Time, Message, Calling_User, Usage_Count)
		SELECT @postedBy, GetDate(), @message, @CallingUser, S.Usage_Count
		FROM T_Usage_Stats S
		WHERE S.Posted_By = @postedBy
		--
		SELECT @myError = @@error, @myRowCount = @@rowcount
		--
		if @myRowCount <> 1 Or @myError <> 0
		begin
			Set @message = 'Update was unsuccessful for T_Usage_Log table: @myRowCount = ' + Convert(varchar(19), @myRowCount) + '; @myError = ' + Convert(varchar(19), @myError)
			execute PostLogEntry 'Error', @message, 'PostUsageLogEntry'
		end
	End

	RETURN 0
GO

/****** Object:  StoredProcedure [dbo].[ReportManagerErrorCleanup]    Script Date: 1/15/2020 8:33:53 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[ReportManagerErrorCleanup]
/****************************************************
**
**	Desc:
**		Reports that the manager tried to auto-cleanup
**		when there is a flag file or non-empty working directory
**
**	Auth:	mem
**	Date:	09/10/2009 mem - Initial version
**
*****************************************************/
(
	@ManagerName varchar(128),
	@State int = 0,					-- 1 = Cleanup Attempt start, 2 = Cleanup Successful, 3 = Cleanup Failed
	@FailureMsg varchar(512) = '',
	@message varchar(512) = '' output
)
AS
	set nocount on

	declare @myError int
	declare @myRowCount int
	set @myError = 0
	set @myRowCount = 0

	Declare @MgrID int
	Declare @MgrNameLocal varchar(128)
	Declare @ParamID int

	Declare @MessageType varchar(64)

	Declare @CleanupMode varchar(256)

	---------------------------------------------------
	-- Cleanup the inputs
	---------------------------------------------------

	Set @ManagerName = IsNull(@ManagerName, '')
	Set @State = IsNull(@State, 0)
	Set @FailureMsg = IsNull(@FailureMsg, '')
	Set @message = ''

	---------------------------------------------------
	-- Confirm that the manager name is valid
	---------------------------------------------------

	SELECT  @MgrID = M_ID,
			@MgrNameLocal = M_Name
	FROM T_Mgrs
	WHERE (M_Name = @ManagerName)
	--
	SELECT @myError = @@error, @myRowCount = @@rowcount

	if @myRowCount <> 1
	begin
		set @myError = 52002
		set @message = 'Could not find entry for manager: ' + @ManagerName
		goto Done
	end

	Set @ManagerName = @MgrNameLocal

	---------------------------------------------------
	-- Validate @State
	---------------------------------------------------

	If @State < 1 or @State > 3
	Begin
		set @myError = 52003
		set @message = 'Invalid value for @State; should be 1, 2 or 3'
		goto Done
	End

	---------------------------------------------------
	-- Log this cleanup event
	---------------------------------------------------

	Set @MessageType = 'Error'
	Set @Message = 'Unknown @State value'

	If @State = 1
	Begin
		Set @MessageType = 'Normal'
		Set @Message = 'Manager ' + @ManagerName + ' is attempting auto error cleanup'
	End

	If @State = 2
	Begin
		Set @MessageType = 'Normal'
		Set @Message = 'Automated error cleanup succeeded for ' + @ManagerName
	End

	If @State = 3
	Begin
		Set @MessageType = 'Normal'
		Set @Message = 'Automated error cleanup failed for ' + @ManagerName
		If @FailureMsg <> ''
			Set @message = @message + '; ' + @FailureMsg
	End

	Exec PostLogEntry @MessageType, @Message, 'ReportManagerErrorCleanup'

	---------------------------------------------------
	-- Lookup the value of ManagerErrorCleanupMode in T_ParamValue
	---------------------------------------------------

	Set @CleanupMode = '0'

	SELECT @CleanupMode = T_ParamValue.Value
	FROM T_ParamValue
	     INNER JOIN T_ParamType
	       ON T_ParamValue.TypeID = T_ParamType.ParamID
	WHERE (T_ParamType.ParamName = 'ManagerErrorCleanupMode') AND
	      (T_ParamValue.MgrID = @MgrID)
	--
	SELECT @myError = @@error, @myRowCount = @@rowcount

	If @myRowCount = 0
	Begin
		-- Entry not found; make a new entry for 'ManagerErrorCleanupMode' in the T_ParamValue table
		Set @ParamID = 0

		SELECT @ParamID = ParamID
		FROM T_ParamType
		WHERE (ParamName = 'ManagerErrorCleanupMode')
		--
		SELECT @myError = @@error, @myRowCount = @@rowcount

		If @ParamID > 0
		Begin
			INSERT INTO T_ParamValue (MgrID, TypeID, Value)
			VALUES (@MgrID, @ParamID, '0')

			Set @CleanupMode = '0'
		End
	End

	If LTrim(RTrim(@CleanupMode)) = '1'
	Begin
		-- Manager is set to auto-cleanup only once; change 'ManagerErrorCleanupMode' to 0
		UPDATE T_ParamValue
		SET Value = '0'
		FROM T_ParamValue
		     INNER JOIN T_ParamType
		       ON T_ParamValue.TypeID = T_ParamType.ParamID
		WHERE (T_ParamType.ParamName = 'ManagerErrorCleanupMode') AND
		      (T_ParamValue.MgrID = @MgrID)
		--
		SELECT @myError = @@error, @myRowCount = @@rowcount

		if @myError <> 0
		Begin
			Set @Message = 'Error setting ManagerErrorCleanupMode to 0 in T_ParamValue for manager ' + @ManagerName
			Exec PostLogEntry 'Error', @message, 'ReportManagerErrorCleanup'
		End
		Else
		Begin
			If @myRowCount = 0
				Set @message = @Message + '; Entry not found in T_ParamValue for ManagerErrorCleanupMode; this is unexpected'
			Else
				Set @message = @Message + '; Decremented ManagerErrorCleanupMode to 0 in T_ParamValue'
		End
	End


	---------------------------------------------------
	-- Exit the procedure
	---------------------------------------------------
Done:
	return @myError

GO

GRANT EXECUTE ON [dbo].[ReportManagerErrorCleanup] TO [DMS_Analysis_Job_Runner] AS [dbo]
GO

GRANT EXECUTE ON [dbo].[ReportManagerErrorCleanup] TO [Mgr_Config_Admin] AS [dbo]
GO

GRANT EXECUTE ON [dbo].[ReportManagerErrorCleanup] TO [svc-dms] AS [dbo]
GO

/****** Object:  StoredProcedure [dbo].[SetManagerErrorCleanupMode]    Script Date: 1/15/2020 8:33:53 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[SetManagerErrorCleanupMode]
/****************************************************
**
**	Desc:
**		Sets ManagerErrorCleanupMode to @CleanupMode for the given list of managers
**		If @ManagerList is blank, then sets it to @CleanupMode for all "Analysis Tool Manager" managers
**
**	Auth:	mem
**	Date:	09/10/2009 mem - Initial version
**			09/29/2014 mem - Expanded @ManagerList to varchar(max) and added parameters @showTable and @infoOnly
**			               - Fixed where clause bug in final update query
**
*****************************************************/
(
	@ManagerList varchar(max) = '',
	@CleanupMode tinyint = 1,				-- 0 = No auto cleanup, 1 = Attempt auto cleanup once, 2 = Auto cleanup always
	@showTable tinyint = 1,
	@infoOnly tinyint = 0,
	@message varchar(512) = '' output
)
AS
	set nocount on

	declare @myError int
	declare @myRowCount int
	set @myError = 0
	set @myRowCount = 0

	Declare @mgrID int
	Declare @ParamID int
	Declare @CleanupModeString varchar(12)

	---------------------------------------------------
	-- Validate the inputs
	---------------------------------------------------

	Set @ManagerList = IsNull(@ManagerList, '')
	Set @CleanupMode = IsNull(@CleanupMode, 1)
	Set @showTable = IsNull(@showTable, 1)
	Set @infoOnly = IsNull(@infoOnly, 0)
	Set @message = ''

	If @CleanupMode < 0
		Set @CleanupMode = 0

	If @CleanupMode > 2
		Set @CleanupMode = 2

	CREATE TABLE #TmpManagerList (
		ManagerName varchar(128) NOT NULL,
		MgrID int NULL
	)

	---------------------------------------------------
	-- Confirm that the manager names are valid
	---------------------------------------------------

	If Len(@ManagerList) > 0
		INSERT INTO #TmpManagerList (ManagerName)
		SELECT Value
		FROM dbo.udfParseDelimitedList(@ManagerList, ',')
		WHERE Len(IsNull(Value, '')) > 0
	Else
		INSERT INTO #TmpManagerList (ManagerName)
		SELECT M_Name
		FROM T_Mgrs
		WHERE (M_TypeID = 11)

	UPDATE #TmpManagerList
	SET MgrID = T_Mgrs.M_ID
	FROM #TmpManagerList INNER JOIN T_Mgrs
	        ON T_Mgrs.M_Name = #TmpManagerList.ManagerName
	--
	SELECT @myError = @@error, @myRowCount = @@rowcount


	DELETE FROM #TmpManagerList
	WHERE MgrID IS NULL
	--
	SELECT @myError = @@error, @myRowCount = @@rowcount

	If @myRowCount <> 0
	Begin
		Set @message = 'Removed ' + Convert(varchar(12), @myRowCount) + ' invalid manager'
		If @myRowCount > 1
			Set @message = @message + 's'

		Set @message = @message + ' from #TmpManagerList'
		Print @message
	End

	---------------------------------------------------
	-- Lookup the ParamID value for 'ManagerErrorCleanupMode'
	---------------------------------------------------

	Set @ParamID = 0
	--
	SELECT @ParamID = ParamID
	FROM T_ParamType
	WHERE (ParamName = 'ManagerErrorCleanupMode')

	---------------------------------------------------
	-- Make sure each manager in #TmpManagerList has an entry
	--  in T_ParamValue for 'ManagerErrorCleanupMode'
	---------------------------------------------------

	INSERT INTO T_ParamValue (MgrID, TypeID, Value)
	SELECT A.MgrID, @ParamID, '0'
	FROM ( SELECT MgrID
	       FROM #TmpManagerList
	     ) A
	     LEFT OUTER JOIN
	      ( SELECT #TmpManagerList.MgrID
	        FROM #TmpManagerList
	             INNER JOIN T_ParamValue
	               ON #TmpManagerList.MgrID = T_ParamValue.MgrID
	        WHERE T_ParamValue.TypeID = @ParamID
	     ) B
	       ON A.MgrID = B.MgrID
	WHERE B.MgrID IS NULL
	--
	SELECT @myError = @@error, @myRowCount = @@rowcount

	If @myRowCount <> 0
	Begin
		Set @message = 'Added entry for "ManagerErrorCleanupMode" to T_ParamValue for ' + Convert(varchar(12), @myRowCount) + ' manager'
		If @myRowCount > 1
			Set @message = @message + 's'

		Print @message
	End

	---------------------------------------------------
	-- Update the 'ManagerErrorCleanupMode' entry for each manager in #TmpManagerList
	---------------------------------------------------

	Set @CleanupModeString = Convert(varchar(12), @CleanupMode)

	If @infoOnly <> 0
	Begin
		SELECT MP.*, @CleanupMode As NewCleanupMode
		FROM V_AnalysisMgrParams_ActiveAndDebugLevel MP
			INNER JOIN #TmpManagerList
			ON MP.MgrID = #TmpManagerList.MgrID
		WHERE MP.ParamTypeID = 120
		ORDER BY MP.Manager
	End
	Else
	Begin

		UPDATE T_ParamValue
		SET Value = @CleanupModeString
		FROM T_ParamValue
			INNER JOIN #TmpManagerList
			ON T_ParamValue.MgrID = #TmpManagerList.MgrID
		WHERE T_ParamValue.TypeID = @ParamID AND
			T_ParamValue.Value <> @CleanupModeString
		--
		SELECT @myError = @@error, @myRowCount = @@rowcount

		If @myRowCount <> 0
		Begin
			Set @message = 'Set "ManagerErrorCleanupMode" to ' + @CleanupModeString + ' for ' + Convert(varchar(12), @myRowCount) + ' manager'
			If @myRowCount > 1
				Set @message = @message + 's'

			Print @message
		End
	End

	---------------------------------------------------
	-- Show the new values
	---------------------------------------------------

	If @infoOnly = 0 And @showTable <> 0
	Begin
		SELECT MP.*
		FROM V_AnalysisMgrParams_ActiveAndDebugLevel MP
			INNER JOIN #TmpManagerList
			ON MP.MgrID = #TmpManagerList.MgrID
		WHERE MP.ParamTypeID = 120
		ORDER BY MP.Manager
	End

	---------------------------------------------------
	-- Exit the procedure
	---------------------------------------------------
Done:
	return @myError
GO

GRANT EXECUTE ON [dbo].[SetManagerErrorCleanupMode] TO [Mgr_Config_Admin] AS [dbo]
GO

GRANT EXECUTE ON [dbo].[SetManagerErrorCleanupMode] TO [MTUser] AS [dbo]
GO

/****** Object:  StoredProcedure [dbo].[UnarchiveOldManagersAndParams]    Script Date: 1/15/2020 8:33:53 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[UnarchiveOldManagersAndParams]
/****************************************************
**
**	Desc:	Moves managers from T_OldManagers to T_Mgrs
**			and moves manager parameters from T_ParamValue_OldManagers to T_ParamValue
**
**			To reverse this process, use procedure UnarchiveOldManagersAndParams
**
**	Return values: 0: success, otherwise, error code
**
**	Auth:	mem
**	Date:	02/25/2016 mem - Initial version
**			04/22/2016 mem - Now updating M_Comment in T_Mgrs
**
*****************************************************/
(
	@MgrList varchar(max),	-- One or more manager names (comma-separated list); supports wildcards because uses stored procedure ParseManagerNameList
	@InfoOnly tinyint = 1,
	@EnableControlFromWebsite tinyint = 0,
	@message varchar(512)='' output
)
As
	Set XACT_ABORT, NoCount On

	declare @myRowCount int
	declare @myError int
	set @myRowCount = 0
	set @myError = 0

	---------------------------------------------------
	-- Validate the inputs
	---------------------------------------------------
	--
	Set @MgrList = IsNull(@MgrList, '')
	Set @InfoOnly = IsNull(@InfoOnly, 1)
	Set @EnableControlFromWebsite = IsNull(@EnableControlFromWebsite, 1)
	Set @message = ''

	If @EnableControlFromWebsite > 0
		Set @EnableControlFromWebsite= 1

	CREATE TABLE #TmpManagerList (
		Manager_Name varchar(50) NOT NULL,
		M_ID int NULL
	)

	---------------------------------------------------
	-- Populate #TmpManagerList with the managers in @MgrList
	---------------------------------------------------
	--

	exec ParseManagerNameList @MgrList, @RemoveUnknownManagers=0

	If Not Exists (Select * from #TmpManagerList)
	Begin
		Set @message = '@MgrList was empty'
		Select @Message as Warning
		Goto done
	End

	---------------------------------------------------
	-- Validate the manager names
	---------------------------------------------------
	--
	UPDATE #TmpManagerList
	SET M_ID = M.M_ID
	FROM #TmpManagerList Target
	     INNER JOIN T_OldManagers M
	       ON Target.Manager_Name = M.M_Name
	--
	SELECT @myError = @@error, @myRowCount = @@rowcount

	If Exists (Select * from #TmpManagerList where M_ID Is Null)
	Begin
		SELECT 'Unknown manager (not in T_OldManagers)' AS Warning, Manager_Name
		FROM #TmpManagerList
		WHERE M_ID  Is Null
		ORDER BY Manager_Name
	End

	If Exists (Select * From #TmpManagerList Where Manager_Name Like '%Params%')
	Begin
		SELECT 'Will not process managers with "Params" in the name (for safety)' AS Warning,
		       Manager_Name
		FROM #TmpManagerList
		WHERE Manager_Name Like '%Params%'
		ORDER BY Manager_Name
		--
		DELETE From #TmpManagerList Where Manager_Name Like '%Params%'
	End

	If Exists (Select * FROM #TmpManagerList Where Manager_Name IN (Select M_Name From T_Mgrs))
	Begin
		SELECT DISTINCT 'Will not process managers with existing entries in T_Mgrs' AS Warning,
		                Manager_Name
		FROM #TmpManagerList Src
		WHERE Manager_Name IN (Select M_Name From T_Mgrs)
		ORDER BY Manager_Name
		--
		DELETE From #TmpManagerList Where Manager_Name IN (Select M_Name From T_Mgrs)
	End

	If Exists (Select * FROM #TmpManagerList Where M_ID IN (Select Distinct MgrID From T_ParamValue))
	Begin
		SELECT DISTINCT 'Will not process managers with existing entries in T_ParamValue' AS Warning,
		                Manager_Name
		FROM #TmpManagerList Src
		WHERE M_ID IN (Select Distinct MgrID From T_ParamValue)
		ORDER BY Manager_Name
		--
		DELETE From #TmpManagerList Where M_ID IN (Select Distinct MgrID From T_ParamValue)
	End

	If @InfoOnly <> 0
	Begin
		SELECT Src.Manager_Name,
		       @EnableControlFromWebsite AS M_ControlFromWebsite,
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
		     LEFT OUTER JOIN V_OldParamValue PV
		       ON PV.MgrID = Src.M_ID
		ORDER BY Src.Manager_Name, ParamName

	End
	Else
	Begin
		DELETE FROM #TmpManagerList WHERE M_ID is Null

		Declare @MoveParams varchar(24) = 'Move params transaction'
		Begin Tran @MoveParams

		SET IDENTITY_INSERT T_Mgrs ON

		INSERT INTO T_Mgrs ( M_ID,
		                     M_Name,
		                     M_TypeID,
		                     M_ParmValueChanged,
		                     M_ControlFromWebsite,
		                     M_Comment )
		SELECT M.M_ID,
		       M.M_Name,
		       M.M_TypeID,
		       M.M_ParmValueChanged,
		       @EnableControlFromWebsite,
		       M.M_Comment
		FROM T_OldManagers M
		     INNER JOIN #TmpManagerList Src
		       ON M.M_ID = Src.M_ID
		  LEFT OUTER JOIN T_Mgrs Target
		   ON Src.M_ID = Target.M_ID
		WHERE Target.M_ID IS NULL
		--
		SELECT @myError = @@error, @myRowCount = @@rowcount
		SET IDENTITY_INSERT T_Mgrs Off
		--
		If @myError <> 0
		Begin
			Rollback
			Select 'Aborted (rollback) due to insert error for T_Mgrs' as Warning, @myError as ErrorCode
			Goto Done
		End

		SET IDENTITY_INSERT T_ParamValue On

		INSERT INTO T_ParamValue (
		         Entry_ID,
		         TypeID,
		         [Value],
		         MgrID,
		         [Comment],
		         Last_Affected,
		         Entered_By )
		SELECT PV.Entry_ID,
		       PV.TypeID,
		       PV.[Value],
		       PV.MgrID,
		       PV.[Comment],
		       PV.Last_Affected,
		       PV.Entered_By
		FROM T_ParamValue_OldManagers PV
		     INNER JOIN #TmpManagerList Src
		       ON PV.MgrID = Src.M_ID
		--
		SELECT @myError = @@error, @myRowCount = @@rowcount
		SET IDENTITY_INSERT T_ParamValue On
		--
		If @myError <> 0
		Begin
			Rollback
			Select 'Aborted (rollback) due to insert error for T_ParamValue_OldManagers' as Warning, @myError as ErrorCode
			Goto Done
		End

		DELETE T_ParamValue_OldManagers
		FROM T_ParamValue_OldManagers PV
		     INNER JOIN #TmpManagerList Src
		       ON PV.MgrID = Src.M_ID

		DELETE T_OldManagers
		FROM T_OldManagers M
		     INNER JOIN #TmpManagerList Src
		       ON M.M_ID = Src.M_ID

		Commit Tran @MoveParams

		SELECT 'Moved to T_Managers and T_ParamValue' as Message,
		       Src.Manager_Name,
		       @EnableControlFromWebsite AS M_ControlFromWebsite,
		       PT.ParamName,
		       PV.Entry_ID,
		       PV.TypeID,
		       PV.[Value],
		       PV.MgrID,
		       PV.[Comment],
		       PV.Last_Affected,
		       PV.Entered_By
		FROM #TmpManagerList Src
		     LEFT OUTER JOIN T_ParamValue PV
		       ON PV.MgrID = Src.M_ID
		     LEFT OUTER JOIN T_ParamType PT ON
		     PV.TypeID = PT.ParamID
		ORDER BY Src.Manager_Name, ParamName
	End


Done:
	RETURN @myError
GO
