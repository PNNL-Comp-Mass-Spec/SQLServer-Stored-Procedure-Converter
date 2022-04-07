USE [Manager_Control]
GO

/****** Object:  StoredProcedure [dbo].[AckManagerUpdateRequired]    Script Date: 1/15/2020 8:33:53 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[AckManagerUpdateRequired]
/****************************************************
**
**	Desc:
**		Acknowledges that a manager has seen that
**		ManagerUpdateRequired is True in the manager control DB
**
**		This SP will thus set ManagerUpdateRequired to False for this manager
**
**	Auth:	mem
**	Date:	01/16/2009 mem - Initial version
**			09/09/2009 mem - Added support for 'ManagerUpdateRequired' already being False
**
*****************************************************/
(
	@managerName varchar(128),
	@message varchar(512) = '' output
)
AS
	set nocount on

	declare @myError int
	declare @myRowCount int
	set @myError = 0
	set @myRowCount = 0

	set @message = ''

	Declare @mgrID int
	Declare @ParamID int

	---------------------------------------------------
	-- Confirm that the manager name is valid
	---------------------------------------------------

	SELECT @mgrID = M_ID
	FROM T_Mgrs
	WHERE (M_Name = @managerName)
	--
	SELECT @myError = @@error, @myRowCount = @@rowcount

	if @myRowCount <> 1
	begin
		set @myError = 52002
		set @message = 'Could not find entry for manager: ' + @managername
		goto Done
	end

	---------------------------------------------------
	-- Update the 'ManagerUpdateRequired' entry for this manager
	---------------------------------------------------

	UPDATE T_ParamValue
	SET Value = 'False'
	FROM T_ParamType
	     INNER JOIN T_ParamValue
	       ON T_ParamType.ParamID = T_ParamValue.TypeID
	WHERE (T_ParamType.ParamName = 'ManagerUpdateRequired') AND
	      (T_ParamValue.MgrID = @mgrID) AND
	      Value <> 'False'
	--
	SELECT @myError = @@error, @myRowCount = @@rowcount

	If @myRowCount > 0
		Set @message = 'Acknowledged that update is required'
	Else
	Begin
		-- No rows were updated; may need to make a new entry for 'ManagerUpdateRequired' in the T_ParamValue table
		Set @ParamID = 0

		SELECT @ParamID = ParamID
		FROM T_ParamType
		WHERE (ParamName = 'ManagerUpdateRequired')
		--
		SELECT @myError = @@error, @myRowCount = @@rowcount

		If @ParamID > 0
		Begin
			If Exists (SELECT * FROM T_ParamValue WHERE MgrID = @mgrID AND TypeID = @ParamID)
				Set @message = 'ManagerUpdateRequired was already acknowledged in T_ParamValue'
			Else
			Begin
				INSERT INTO T_ParamValue (MgrID, TypeID, Value)
				VALUES (@mgrID, @ParamID, 'False')

				Set @message = 'Acknowledged that update is required (added new entry to T_ParamValue)'
			End
		End
	End

	---------------------------------------------------
	-- Exit the procedure
	---------------------------------------------------
Done:
	return @myError

GO

GRANT EXECUTE ON [dbo].[AckManagerUpdateRequired] TO [DMS_Analysis_Job_Runner] AS [dbo]
GO

GRANT EXECUTE ON [dbo].[AckManagerUpdateRequired] TO [Mgr_Config_Admin] AS [dbo]
GO

GRANT EXECUTE ON [dbo].[AckManagerUpdateRequired] TO [svc-dms] AS [dbo]
GO

/****** Object:  StoredProcedure [dbo].[AlterEnteredByUser]    Script Date: 1/15/2020 8:33:53 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

create PROCEDURE [dbo].[AlterEnteredByUser]
/****************************************************
**
**	Desc:	Updates the Entered_By column for the specified row in the given table to be @NewUser
**
**			If @ApplyTimeFilter is non-zero, then only matches entries made within the last
**			  @EntryTimeWindowSeconds seconds
**
**			Use @infoOnly = 1 to preview updates
**
**	Return values: 0: success, otherwise, error code
**
**	Auth:	mem
**	Date:	03/25/2008 mem - Initial version (Ticket: #644)
**			05/23/2008 mem - Expanded @EntryDescription to varchar(512)
**
*****************************************************/
(
	@TargetTableName varchar(128),
	@TargetIDColumnName varchar(128),
	@TargetID int,
	@NewUser varchar(128),
	@ApplyTimeFilter tinyint = 1,		-- If 1, then filters by the current date and time; if 0, looks for the most recent matching entry
	@EntryTimeWindowSeconds int = 15,	-- Only used if @ApplyTimeFilter = 1
	@EntryDateColumnName varchar(128) = 'Entered',
	@EnteredByColumnName varchar(128) = 'Entered_By',
	@message varchar(512) = '' output,
	@infoOnly tinyint = 0,
	@PreviewSql tinyint = 0
)
As
	Set nocount on

	Declare @myRowCount int
	Declare @myError int
	Set @myRowCount = 0
	Set @myError = 0

	Declare @EntryDateStart datetime
	Declare @EntryDateEnd datetime

	Declare @EntryDescription varchar(512)
	Declare @EntryIndex int
	Declare @MatchIndex int

	Declare @EnteredBy varchar(255)
	Declare @EnteredByNew varchar(255)
	Set @EnteredByNew = ''

	Declare @CurrentTime datetime
	Set @CurrentTime = GetDate()

	declare @S nvarchar(3000)

	declare @EntryFilterSql nvarchar(512)
	Set @EntryFilterSql = ''

	declare @ParamDef nvarchar(512)
	declare @result int
	declare @TargetIDMatch int

	------------------------------------------------
	-- Validate the inputs
	------------------------------------------------

	Set @NewUser = IsNull(@NewUser, '')
	Set @ApplyTimeFilter = IsNull(@ApplyTimeFilter, 0)
	Set @EntryTimeWindowSeconds = IsNull(@EntryTimeWindowSeconds, 15)
	Set @message = ''
	Set @infoOnly = IsNull(@infoOnly, 0)
	Set @PreviewSql = IsNull(@PreviewSql, 0)

	If @TargetTableName Is Null Or @TargetIDColumnName Is Null Or @TargetID Is Null
	Begin
		Set @message = '@TargetTableName and @TargetIDColumnName and @TargetID must be defined; unable to continue'
		Set @myError = 50201
		Goto done
	End

	If Len(@NewUser) = 0
	Begin
		Set @message = '@NewUser is empty; unable to continue'
		Set @myError = 50202
		Goto done
	End

	Set @EntryDescription = 'ID ' + Convert(varchar(12), @TargetID) + ' in table ' + @TargetTableName + ' (column ' + @TargetIDColumnName + ')'

	Set @S = ''
	Set @S = @S + '	SELECT @TargetIDMatch = [' + @TargetIDColumnName + '],'
	Set @S = @S +        ' @EnteredBy = [' + @EnteredByColumnName + ']'
	Set @S = @S + ' FROM [' + @TargetTableName + ']'
	Set @S = @S + ' WHERE [' + @TargetIDColumnName + '] = ' + Convert(varchar(12), @TargetID)

	If @ApplyTimeFilter <> 0 And IsNull(@EntryTimeWindowSeconds, 0) >= 1
	Begin
		------------------------------------------------
		-- Filter using the current date/time
		------------------------------------------------
		--
		Set @EntryDateStart = DateAdd(second, -@EntryTimeWindowSeconds, @CurrentTime)
		Set @EntryDateEnd = DateAdd(second, 1, @CurrentTime)

		If @infoOnly <> 0
			Print 'Filtering on entries dated between ' + Convert(varchar(64), @EntryDateStart, 120) + ' and ' + Convert(varchar(64), @EntryDateEnd, 120) + ' (Window = ' + Convert(varchar(12), @EntryTimeWindowSeconds) + ' seconds)'

		Set @EntryFilterSql = ' [' + @EntryDateColumnName + '] Between ''' + Convert(varchar(64), @EntryDateStart, 120) + ''' And ''' + Convert(varchar(64), @EntryDateEnd, 120) + ''''
		Set @S = @S + ' AND ' + @EntryFilterSql

		Set @EntryDescription = @EntryDescription + ' with ' + @EntryFilterSql
	End

	Set @ParamDef = '@TargetIDMatch int output, @EnteredBy varchar(128) output'

	If @PreviewSql <> 0
	Begin
		Print @S
		Set @EnteredBy = suser_sname() + '_Simulated'
	End
	Else
		Exec @result = sp_executesql @S, @ParamDef, @TargetIDMatch = @TargetIDMatch output, @EnteredBy = @EnteredBy output
	--
	SELECT @myError = @@error, @myRowCount = @@rowcount


	If @myError <> 0
	Begin
		Set @message = 'Error looking for ' + @EntryDescription
		Goto done
	End

	If @PreviewSql = 0 AND (@myRowCount <= 0 Or @TargetIDMatch <> @TargetID)
		Set @message = 'Match not found for ' + @EntryDescription
	Else
	Begin
		-- Confirm that @EnteredBy doesn't already contain @NewUser
		-- If it does, then there's no need to update it

		Set @MatchIndex = CharIndex(@NewUser, @EnteredBy)
		If @MatchIndex > 0
		Begin
			Set @message = 'Entry ' + @EntryDescription + ' is already attributed to ' + @NewUser + ': "' + @EnteredBy + '"'
			Goto Done
		End

		-- Look for a semicolon in @EnteredBy

		Set @MatchIndex = CharIndex(';', @EnteredBy)

		If @MatchIndex > 0
			Set @EnteredByNew = @NewUser + ' (via ' + SubString(@EnteredBy, 1, @MatchIndex-1) + ')' + SubString(@EnteredBy, @MatchIndex, Len(@EnteredBy))
		Else
			Set @EnteredByNew = @NewUser + ' (via ' + @EnteredBy + ')'

		If Len(IsNull(@EnteredByNew, '')) > 0
		Begin

			If @infoOnly = 0
			Begin
				Set @S = ''
				Set @S = @S + ' UPDATE [' + @TargetTableName + ']'
				Set @S = @S + ' SET [' + @EnteredByColumnName + '] = ''' + @EnteredByNew + ''''
				Set @S = @S + ' WHERE [' + @TargetIDColumnName + '] = ' + Convert(varchar(12), @TargetID)

				If Len(@EntryFilterSql) > 0
					Set @S = @S + ' AND ' + @EntryFilterSql

				If @PreviewSql <> 0
					Print @S
				Else
					Exec (@S)
				--
				SELECT @myError = @@error, @myRowCount = @@rowcount

				If @myError <> 0
				Begin
					Set @message = 'Error updating ' + @EntryDescription
					Exec PostLogEntry 'Error', @message, 'AlterEventLogEntryUser'
					Goto Done
				End
				Else
					Set @message = 'Updated ' + @EntryDescription + ' to indicate "' + @EnteredByNew + '"'
			End
			Else
			Begin
				Set @S = ''
				Set @S = @S + ' SELECT *, ''' + @EnteredByNew + ''' AS Entered_By_New'
				Set @S = @S + ' FROM [' + @TargetTableName + ']'
				Set @S = @S + ' WHERE [' + @TargetIDColumnName + '] = ' + Convert(varchar(12), @TargetID)

				If Len(@EntryFilterSql) > 0
					Set @S = @S + ' AND ' + @EntryFilterSql

				If @PreviewSql <> 0
					Print @S
				Else
					Exec (@S)
				--
				SELECT @myError = @@error, @myRowCount = @@rowcount

				Set @message = 'Would update ' + @EntryDescription + ' to indicate "' + @EnteredByNew + '"'
			End

		End
		Else
			Set @Message = 'Match not found; unable to continue'

	End

Done:
	return @myError
GO

/****** Object:  StoredProcedure [dbo].[AlterEnteredByUserMultiID]    Script Date: 1/15/2020 8:33:53 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

create PROCEDURE [dbo].[AlterEnteredByUserMultiID]
/****************************************************
**
**	Desc:	Calls AlterEnteredByUser for each entry in #TmpIDUpdateList
**
**			The calling procedure must create and populate temporary table #TmpIDUpdateList:
**				CREATE TABLE #TmpIDUpdateList (
**					TargetID int NOT NULL
**				)
**
**			Increased performance can be obtained by adding an index to the table; thus
**			it is advisable that the calling procedure also create this index:
**				CREATE CLUSTERED INDEX #IX_TmpIDUpdateList ON #TmpIDUpdateList (TargetID)
**
**	Return values: 0: success, otherwise, error code
**
**	Auth:	mem
**	Date:	03/28/2008 mem - Initial version (Ticket: #644)
**			05/23/2008 mem - Expanded @EntryDescription to varchar(512)
**
*****************************************************/
(
	@TargetTableName varchar(128),
	@TargetIDColumnName varchar(128),
	@NewUser varchar(128),
	@ApplyTimeFilter tinyint = 1,		-- If 1, then filters by the current date and time; if 0, looks for the most recent matching entry
	@EntryTimeWindowSeconds int = 15,	-- Only used if @ApplyTimeFilter = 1
	@EntryDateColumnName varchar(128) = 'Entered',
	@EnteredByColumnName varchar(128) = 'Entered_By',
	@message varchar(512) = '' output,
	@infoOnly tinyint = 0,
	@PreviewSql tinyint = 0
)
As
	Set nocount on

	Declare @myRowCount int
	Declare @myError int
	Set @myRowCount = 0
	Set @myError = 0

	Declare @EntryDateStart datetime
	Declare @EntryDateEnd datetime

	Declare @EntryDescription varchar(512)
	Declare @EntryIndex int
	Declare @MatchIndex int

	Declare @EnteredBy varchar(255)
	Declare @EnteredByNew varchar(255)
	Set @EnteredByNew = ''

	Declare @CurrentTime datetime
	Set @CurrentTime = GetDate()

	Declare @TargetID int
	Declare @CountUpdated int
	Declare @Continue tinyint

	Declare @StartTime datetime
	Declare @EntryTimeWindowSecondsCurrent int
	Declare @ElapsedSeconds int

	------------------------------------------------
	-- Validate the inputs
	------------------------------------------------

	Set @NewUser = IsNull(@NewUser, '')
	Set @ApplyTimeFilter = IsNull(@ApplyTimeFilter, 0)
	Set @EntryTimeWindowSeconds = IsNull(@EntryTimeWindowSeconds, 15)
	Set @message = ''
	Set @infoOnly = IsNull(@infoOnly, 0)
	Set @PreviewSql = IsNull(@PreviewSql, 0)

	If @TargetTableName Is Null Or @TargetIDColumnName Is Null
	Begin
		Set @message = '@TargetTableName and @TargetIDColumnName must be defined; unable to continue'
		Set @myError = 50201
		Goto done
	End

	If Len(@NewUser) = 0
	Begin
		Set @message = '@NewUser is empty; unable to continue'
		Set @myError = 50202
		Goto done
	End

	-- Make sure #TmpIDUpdateList is not empty
	SELECT @myRowCount = COUNT(*)
	FROM #TmpIDUpdateList

	If @myRowCount <= 0
	Begin
		Set @message = '#TmpIDUpdateList is empty; nothing to do'
		Goto done
	End

	------------------------------------------------
	-- Initialize @EntryTimeWindowSecondsCurrent
	-- This variable will be automatically increased
	--  if too much time elapses
	------------------------------------------------
	--
	Set @StartTime = GetDate()
	Set @EntryTimeWindowSecondsCurrent = @EntryTimeWindowSeconds

	------------------------------------------------
	-- Determine the minimum value in #TmpIDUpdateList
	------------------------------------------------

	SELECT @TargetID = Min(TargetID)-1
	FROM #TmpIDUpdateList
	--
	SELECT @myError = @@error, @myRowCount = @@rowcount

	Set @TargetID = IsNull(@TargetID, -1)

	------------------------------------------------
	-- Parse the values in #TmpIDUpdateList
	-- Call AlterEnteredByUser for each
	------------------------------------------------

	Set @CountUpdated = 0
	Set @Continue = 1

	While @Continue = 1
	Begin
		SELECT TOP 1 @TargetID = TargetID
		FROM #TmpIDUpdateList
		WHERE TargetID > @TargetID
		ORDER BY TargetID
		--
		SELECT @myError = @@error, @myRowCount = @@rowcount

		If @myRowCount = 0
			Set @continue = 0
		Else
		Begin
			Exec @myError = AlterEnteredByUser
								@TargetTableName,
								@TargetIDColumnName,
								@TargetID,
								@NewUser,
								@ApplyTimeFilter,
								@EntryTimeWindowSecondsCurrent,
								@EntryDateColumnName,
								@EnteredByColumnName,
								@message output,
								@infoOnly,
								@PreviewSql

			If @myError <> 0
				Goto Done

			Set @CountUpdated = @CountUpdated + 1
			If @CountUpdated % 5 = 0
			Begin
				Set @ElapsedSeconds = DateDiff(second, @StartTime, GetDate())

				If @ElapsedSeconds * 2 > @EntryTimeWindowSecondsCurrent
					Set @EntryTimeWindowSecondsCurrent = @ElapsedSeconds * 4
			End
		End
	End

Done:
	return @myError
GO

/****** Object:  StoredProcedure [dbo].[AlterEventLogEntryUser]    Script Date: 1/15/2020 8:33:53 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[AlterEventLogEntryUser]
/****************************************************
**
**	Desc:	Updates the user associated with a given event log entry to be @NewUser
**
**			If @ApplyTimeFilter is non-zero, then only matches entries made within the last
**			  @EntryTimeWindowSeconds seconds
**
**			Use @infoOnly = 1 to preview updates
**
**	Return values: 0: success, otherwise, error code
**
**	Auth:	mem
**	Date:	02/29/2008 mem - Initial version (Ticket: #644)
**			05/23/2008 mem - Expanded @EntryDescription to varchar(512)
**			03/30/2009 mem - Ported to the Manager Control DB
**
*****************************************************/
(
	@TargetType smallint,				-- 1=Manager Enable/Disable
	@TargetID int,
	@TargetState int,
	@NewUser varchar(128),
	@ApplyTimeFilter tinyint = 1,		-- If 1, then filters by the current date and time; if 0, looks for the most recent matching entry
	@EntryTimeWindowSeconds int = 15,	-- Only used if @ApplyTimeFilter = 1
	@message varchar(512) = '' output,
	@infoOnly tinyint = 0
)
As
	Set nocount on

	Declare @myRowCount int
	Declare @myError int
	Set @myRowCount = 0
	Set @myError = 0

	Declare @EntryDateStart datetime
	Declare @EntryDateEnd datetime

	Declare @EntryDescription varchar(512)
	Declare @EventID int
	Declare @MatchIndex int

	Declare @EnteredBy varchar(255)
	Declare @EnteredByNew varchar(255)
	Set @EnteredByNew = ''

	Declare @CurrentTime datetime
	Set @CurrentTime = GetDate()

	------------------------------------------------
	-- Validate the inputs
	------------------------------------------------

	Set @NewUser = IsNull(@NewUser, '')
	Set @ApplyTimeFilter = IsNull(@ApplyTimeFilter, 0)
	Set @EntryTimeWindowSeconds = IsNull(@EntryTimeWindowSeconds, 15)
	Set @message = ''
	Set @infoOnly = IsNull(@infoOnly, 0)

	If @TargetType Is Null Or @TargetID Is Null Or @TargetState Is Null
	Begin
		Set @message = '@TargetType and @TargetID and @TargetState must be defined; unable to continue'
		Set @myError = 50201
		Goto done
	End

	If Len(@NewUser) = 0
	Begin
		Set @message = '@NewUser is empty; unable to continue'
		Set @myError = 50202
		Goto done
	End

	Set @EntryDescription = 'ID ' + Convert(varchar(12), @TargetID) + ' (type ' + Convert(varchar(12), @TargetType) + ') with state ' + Convert(varchar(12), @TargetState)
	If @ApplyTimeFilter <> 0 And IsNull(@EntryTimeWindowSeconds, 0) >= 1
	Begin
		------------------------------------------------
		-- Filter using the current date/time
		------------------------------------------------
		--
		Set @EntryDateStart = DateAdd(second, -@EntryTimeWindowSeconds, @CurrentTime)
		Set @EntryDateEnd = DateAdd(second, 1, @CurrentTime)

		If @infoOnly <> 0
			Print 'Filtering on entries dated between ' + Convert(varchar(64), @EntryDateStart, 120) + ' and ' + Convert(varchar(64), @EntryDateEnd, 120) + ' (Window = ' + Convert(varchar(12), @EntryTimeWindowSeconds) + ' seconds)'

		SELECT @EventID = EL.Event_ID,
			   @EnteredBy = EL.Entered_By
		FROM T_Event_Log EL INNER JOIN
				(SELECT MAX(Event_ID) AS Event_ID
				 FROM dbo.T_Event_Log
				 WHERE Target_Type = @TargetType AND
				       Target_ID = @TargetID AND
					   Target_State = @TargetState AND
					   Entered Between @EntryDateStart And @EntryDateEnd
				) LookupQ ON EL.Event_ID = LookupQ.Event_ID
		--
		SELECT @myError = @@error, @myRowCount = @@rowcount

		Set @EntryDescription = @EntryDescription + ' and Entry Time between ' + Convert(varchar(64), @EntryDateStart, 120) + ' and ' + Convert(varchar(64), @EntryDateEnd, 120)
	End
	Else
	Begin
		------------------------------------------------
		-- Do not filter by time
		------------------------------------------------
		--
		SELECT @EventID = EL.Event_ID,
			   @EnteredBy = EL.Entered_By
		FROM T_Event_Log EL INNER JOIN
				(SELECT MAX(Event_ID) AS Event_ID
				 FROM dbo.T_Event_Log
				 WHERE Target_Type = @TargetType AND
				       Target_ID = @TargetID AND
					   Target_State = @TargetState
				) LookupQ ON EL.Event_ID = LookupQ.Event_ID
		--
		SELECT @myError = @@error, @myRowCount = @@rowcount
	End

	If @myError <> 0
	Begin
		Set @message = 'Error looking for ' + @EntryDescription
		Goto done
	End

	If @myRowCount <= 0
		Set @message = 'Match not found for ' + @EntryDescription
	Else
	Begin
		-- Confirm that @EnteredBy doesn't already contain @NewUser
		-- If it does, then there's no need to update it

		Set @MatchIndex = CharIndex(@NewUser, @EnteredBy)
		If @MatchIndex > 0
		Begin
			Set @message = 'Entry ' + @EntryDescription + ' is already attributed to ' + @NewUser + ': "' + @EnteredBy + '"'
			Goto Done
		End

		-- Look for a semicolon in @EnteredBy

		Set @MatchIndex = CharIndex(';', @EnteredBy)

		If @MatchIndex > 0
			Set @EnteredByNew = @NewUser + ' (via ' + SubString(@EnteredBy, 1, @MatchIndex-1) + ')' + SubString(@EnteredBy, @MatchIndex, Len(@EnteredBy))
		Else
			Set @EnteredByNew = @NewUser + ' (via ' + @EnteredBy + ')'

		If Len(IsNull(@EnteredByNew, '')) > 0
		Begin

			If @infoOnly = 0
			Begin
				UPDATE T_Event_Log
				SET Entered_By = @EnteredByNew
				WHERE Event_ID = @EventID
				--
				SELECT @myError = @@error, @myRowCount = @@rowcount

				If @myError <> 0
				Begin
					Set @message = 'Error updating ' + @EntryDescription
					Exec PostLogEntry 'Error', @message, 'AlterEventLogEntryUser'
					Goto Done
				End
				Else
					Set @message = 'Updated ' + @EntryDescription + ' to indicate "' + @EnteredByNew + '"'
			End
			Else
			Begin
				SELECT Event_ID, Target_Type, Target_ID, Target_State,
					   Prev_Target_State, Entered,
					   Entered_By AS Entered_By_Old,
					   @EnteredByNew AS Entered_By_New
				FROM T_Event_Log
				WHERE Event_ID = @EventID
				--
				SELECT @myError = @@error, @myRowCount = @@rowcount

				Set @message = 'Would update ' + @EntryDescription + ' to indicate "' + @EnteredByNew + '"'
			End

		End
		Else
			Set @Message = 'Match not found; unable to continue'

	End

Done:
	return @myError
GO

/****** Object:  StoredProcedure [dbo].[AlterEventLogEntryUserMultiID]    Script Date: 1/15/2020 8:33:53 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[AlterEventLogEntryUserMultiID]
/****************************************************
**
**	Desc:	Calls AlterEventLogEntryUser for each entry in #TmpIDUpdateList
**
**			The calling procedure must create and populate temporary table #TmpIDUpdateList:
**				CREATE TABLE #TmpIDUpdateList (
**					TargetID int NOT NULL
**				)
**
**			Increased performance can be obtained by adding an index to the table; thus
**			it is advisable that the calling procedure also create this index:
**				CREATE CLUSTERED INDEX #IX_TmpIDUpdateList ON #TmpIDUpdateList (TargetID)
**
**	Return values: 0: success, otherwise, error code
**
**	Auth:	mem
**	Date:	02/29/2008 mem - Initial version (Ticket: #644)
**			05/23/2008 mem - Expanded @EntryDescription to varchar(512)
**			03/30/2009 mem - Ported to the Manager Control DB
**
*****************************************************/
(
	@TargetType smallint,				-- 1=Manager Enable/Disable
	@TargetState int,
	@NewUser varchar(128),
	@ApplyTimeFilter tinyint = 1,		-- If 1, then filters by the current date and time; if 0, looks for the most recent matching entry
	@EntryTimeWindowSeconds int = 15,	-- Only used if @ApplyTimeFilter = 1
	@message varchar(512) = '' output,
	@infoOnly tinyint = 0
)
As
	Set nocount on

	Declare @myRowCount int
	Declare @myError int
	Set @myRowCount = 0
	Set @myError = 0

	Declare @EntryDateStart datetime
	Declare @EntryDateEnd datetime

	Declare @EntryDescription varchar(512)
	Declare @EntryIndex int
	Declare @MatchIndex int

	Declare @EnteredBy varchar(255)
	Declare @EnteredByNew varchar(255)
	Set @EnteredByNew = ''

	Declare @CurrentTime datetime
	Set @CurrentTime = GetDate()

	Declare @TargetID int
	Declare @CountUpdated int
	Declare @Continue tinyint

	Declare @StartTime datetime
	Declare @EntryTimeWindowSecondsCurrent int
	Declare @ElapsedSeconds int

	------------------------------------------------
	-- Validate the inputs
	------------------------------------------------

	Set @NewUser = IsNull(@NewUser, '')
	Set @ApplyTimeFilter = IsNull(@ApplyTimeFilter, 0)
	Set @EntryTimeWindowSeconds = IsNull(@EntryTimeWindowSeconds, 15)
	Set @message = ''
	Set @infoOnly = IsNull(@infoOnly, 0)

	If @TargetType Is Null Or @TargetState Is Null
	Begin
		Set @message = '@TargetType and @TargetState must be defined; unable to continue'
		Set @myError = 50201
		Goto done
	End

	If Len(@NewUser) = 0
	Begin
		Set @message = '@NewUser is empty; unable to continue'
		Set @myError = 50202
		Goto done
	End

	-- Make sure #TmpIDUpdateList is not empty
	SELECT @myRowCount = COUNT(*)
	FROM #TmpIDUpdateList

	If @myRowCount <= 0
	Begin
		Set @message = '#TmpIDUpdateList is empty; nothing to do'
		Goto done
	End

	------------------------------------------------
	-- Initialize @EntryTimeWindowSecondsCurrent
	-- This variable will be automatically increased
	--  if too much time elapses
	------------------------------------------------
	--
	Set @StartTime = GetDate()
	Set @EntryTimeWindowSecondsCurrent = @EntryTimeWindowSeconds

	------------------------------------------------
	-- Determine the minimum value in #TmpIDUpdateList
	------------------------------------------------

	SELECT @TargetID = Min(TargetID)-1
	FROM #TmpIDUpdateList
	--
	SELECT @myError = @@error, @myRowCount = @@rowcount

	Set @TargetID = IsNull(@TargetID, -1)

	------------------------------------------------
	-- Parse the values in #TmpIDUpdateList
	-- Call AlterEventLogEntryUser for each
	------------------------------------------------

	Set @CountUpdated = 0
	Set @Continue = 1

	While @Continue = 1
	Begin
		SELECT TOP 1 @TargetID = TargetID
		FROM #TmpIDUpdateList
		WHERE TargetID > @TargetID
		ORDER BY TargetID
		--
		SELECT @myError = @@error, @myRowCount = @@rowcount

		If @myRowCount = 0
			Set @continue = 0
		Else
		Begin
			Exec @myError = AlterEventLogEntryUser
								@TargetType,
								@TargetID,
								@TargetState,
								@NewUser,
								@ApplyTimeFilter,
								@EntryTimeWindowSeconds,
								@message output,
								@infoOnly

			If @myError <> 0
				Goto Done

			Set @CountUpdated = @CountUpdated + 1
			If @CountUpdated % 5 = 0
			Begin
				Set @ElapsedSeconds = DateDiff(second, @StartTime, GetDate())

				If @ElapsedSeconds * 2 > @EntryTimeWindowSecondsCurrent
					Set @EntryTimeWindowSecondsCurrent = @ElapsedSeconds * 4
			End
		End
	End

Done:
	return @myError
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

/****** Object:  StoredProcedure [dbo].[CacheServerUsersAndPermissions]    Script Date: 1/15/2020 8:33:53 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE Procedure [dbo].[CacheServerUsersAndPermissions]
/****************************************************
**
**	Desc:
**		Caches server logins, database logins and roles, and database objects in the T_Auth tables
**
**		If the tables already exist, updates the information using Merge statements
**
**	Return values: 0 if no error; otherwise error code
**
**	Auth:	mem
**	Date:	03/11/2016 mem - Initial version
**
*****************************************************/
(
	@databaseList nvarchar(2000) = 'DMS5, DMS_Capture, DMS_Data_Package,DMSHistoricLog,Ontology_Lookup', 	-- List of database names to parse for database logins and roles, plus database permissions
	@infoOnly tinyint = 1,
	@previewSql tinyint = 0,
	@message varchar(255) = '' OUTPUT
)
AS
	Set XACT_ABORT, nocount on

	declare @myRowCount int
	declare @myError int
	set @myRowCount = 0
	set @myError = 0

	Declare @CurrentLocation varchar(128) = 'Initializing'

	Declare @S nvarchar(4000)
	Declare @Params nvarchar(256)

	---------------------------------------------------
	-- Validate the inputs
	---------------------------------------------------

	Set @databaseList = IsNull(@databaseList, '')
	Set @infoOnly = IsNull(@infoOnly, 1)
	Set @previewSql = IsNull(@previewSql, 0)
	Set @message = ''

	---------------------------------------------------
	-- Create several temporary tables
	---------------------------------------------------

	CREATE TABLE #Tmp_DatabaseNames (
		Entry_ID int identity(1,1) not null,
		Database_Name nvarchar(128) not null,
		IsValid tinyint not null,
		Database_ID int null
	)

	CREATE TABLE #Tmp_Auth_Server_Logins (
		LoginName nvarchar(128) NOT NULL,
		User_Type_Desc varchar(32) NOT NULL,
		Server_Roles nvarchar(max) NULL,
		Principal_ID int NULL
	)

	CREATE TABLE #Tmp_Auth_Database_LoginsAndRoles(
		Database_ID int NOT NULL,
		Database_Name nvarchar(128) NOT NULL,
		Principal_ID int NOT NULL,
		UserName nvarchar(128) NOT NULL,
		LoginName nvarchar(128) NULL,
		User_Type char(1) NOT NULL,
		User_Type_Desc nvarchar(60) NULL,
		Database_Roles nvarchar(2000) NULL
	)

	CREATE TABLE #Tmp_Auth_Database_Permissions(
		Database_ID int NOT NULL,
		Database_Name nvarchar(128) NOT NULL,
		Principal_ID int NOT NULL,
		Role_Or_User nvarchar(128) NOT NULL,
		User_Type char(1) NOT NULL,
		User_Type_Desc nvarchar(60) NULL,
		Permission nvarchar(128) NOT NULL,
		Object_Names nvarchar(max) NULL	,
		Sort_Order int NOT NULL
	)

	BEGIN TRY


		If @InfoOnly <> 0
		Begin

			---------------------------------------------------
			-- Create the tracking tables if missing
			---------------------------------------------------
			--
			Set @CurrentLocation = 'Creating missing database tables'

			If Not Exists (Select * From sys.Tables where Name = 'T_Auth_Database_LoginsAndRoles')
			Begin
				CREATE TABLE [dbo].[T_Auth_Database_LoginsAndRoles](
					[Database_ID] [int] NOT NULL,
					[Database_Name] [nvarchar](128) NOT NULL,
					[Principal_ID] [int] NOT NULL,
					[UserName] [nvarchar](128) NOT NULL,
					[LoginName] [nvarchar](128) NULL,
					[User_Type] [char](1) NOT NULL,
					[User_Type_Desc] [nvarchar](60) NULL,
					[Database_Roles] [nvarchar](2000) NULL,
					[Entered] [datetime] NOT NULL,
					[Last_Affected] [datetime] NOT NULL,
					[Enabled] [tinyint] NOT NULL,
				 CONSTRAINT [PK_T_Auth_Database_LoginsAndRoles] PRIMARY KEY CLUSTERED (
					[Database_ID] ASC,
					[Principal_ID] ASC )
				)

				ALTER TABLE [dbo].[T_Auth_Database_LoginsAndRoles] ADD  CONSTRAINT [DF_T_Auth_Database_LoginsAndRoles_Entered]  DEFAULT (getdate()) FOR [Entered]
				ALTER TABLE [dbo].[T_Auth_Database_LoginsAndRoles] ADD  CONSTRAINT [DF_T_Auth_Database_LoginsAndRoles_Last_Affected]  DEFAULT (getdate()) FOR [Last_Affected]
				ALTER TABLE [dbo].[T_Auth_Database_LoginsAndRoles] ADD  CONSTRAINT [DF_T_Auth_Database_LoginsAndRoles_Enabled]  DEFAULT ((1)) FOR [Enabled]

			End

			If Not Exists (Select * From sys.Tables where Name = 'T_Auth_Database_Permissions')
			Begin

				CREATE TABLE [dbo].[T_Auth_Database_Permissions](
					[Database_ID] [int] NOT NULL,
					[Database_Name] [nvarchar](128) NOT NULL,
					[Principal_ID] [int] NOT NULL,
					[Role_Or_User] [nvarchar](128) NOT NULL,
					[User_Type] [char](1) NOT NULL,
					[User_Type_Desc] [nvarchar](60) NULL,
					[Permission] [nvarchar](128) NOT NULL,
					[Object_Names] [nvarchar](max) NULL,
					[Sort_Order] [int] NOT NULL,
					[Entered] [datetime] NOT NULL,
					[Last_Affected] [datetime] NOT NULL,
					[Enabled] [tinyint] NOT NULL,
				 CONSTRAINT [PK_T_Auth_Database_Permissions] PRIMARY KEY CLUSTERED
				(
					[Database_ID] ASC,
					[Principal_ID] ASC,
					[Permission] ASC )
				)

				ALTER TABLE [dbo].[T_Auth_Database_Permissions] ADD  CONSTRAINT [DF_T_Auth_Database_Permissions_Entered]  DEFAULT (getdate()) FOR [Entered]
				ALTER TABLE [dbo].[T_Auth_Database_Permissions] ADD  CONSTRAINT [DF_T_Auth_Database_Permissions_Last_Affected]  DEFAULT (getdate()) FOR [Last_Affected]
				ALTER TABLE [dbo].[T_Auth_Database_Permissions] ADD  CONSTRAINT [DF_T_Auth_Database_Permissions_Enabled]  DEFAULT ((1)) FOR [Enabled]
			End

			If Not Exists (Select * From sys.Tables where Name = 'T_Auth_Server_Logins')
			Begin
				CREATE TABLE [dbo].[T_Auth_Server_Logins](
					[LoginName] [nvarchar](128) NOT NULL,
					[User_Type_Desc] [varchar](32) NOT NULL,
					[Server_Roles] [nvarchar](max) NULL,
					[Principal_ID] [int] NULL,
					[Entered] [datetime] NULL,
					[Last_Affected] [datetime] NULL,
					[Enabled] [tinyint] NOT NULL,
				 CONSTRAINT [PK_T_Auth_Server_Logins] PRIMARY KEY CLUSTERED
				(
					[LoginName] ASC)
				)

				ALTER TABLE [dbo].[T_Auth_Server_Logins] ADD  CONSTRAINT [DF_T_Auth_Server_Logins_Entered]  DEFAULT (getdate()) FOR [Entered]
				ALTER TABLE [dbo].[T_Auth_Server_Logins] ADD  CONSTRAINT [DF_T_Auth_Server_Logins_Last_Affected]  DEFAULT (getdate()) FOR [Last_Affected]
				ALTER TABLE [dbo].[T_Auth_Server_Logins] ADD  CONSTRAINT [DF_T_Auth_Server_Logins_Enabled]  DEFAULT ((1)) FOR [Enabled]

			End

		End


		---------------------------------------------------
		-- Preview or update the server logins
		---------------------------------------------------

		Set @CurrentLocation = 'Finding server logins'

		;
		Set @S = ''
		Set @S = @S + ' WITH UserRoleNames (sid, Server_Role) AS ('
		Set @S = @S + '   SELECT sid, CASE WHEN sysadmin > 0      THEN Cast(''sysadmin'' AS varchar(15))      ELSE Cast('''' AS varchar(15)) END AS Server_Role FROM sys.syslogins UNION'
		Set @S = @S + '   SELECT sid, CASE WHEN securityadmin > 0 THEN Cast(''securityadmin'' AS varchar(15)) ELSE Cast('''' AS varchar(15)) END AS Server_Role FROM sys.syslogins UNION'
		Set @S = @S + '   SELECT sid, CASE WHEN serveradmin > 0   THEN Cast(''serveradmin'' AS varchar(15))   ELSE Cast('''' AS varchar(15)) END AS Server_Role FROM sys.syslogins UNION'
		Set @S = @S + '   SELECT sid, CASE WHEN setupadmin > 0    THEN Cast(''setupadmin'' AS varchar(15))    ELSE Cast('''' AS varchar(15)) END AS Server_Role FROM sys.syslogins UNION'
		Set @S = @S + '   SELECT sid, CASE WHEN processadmin > 0  THEN Cast(''processadmin'' AS varchar(15))  ELSE Cast('''' AS varchar(15)) END AS Server_Role FROM sys.syslogins UNION'
		Set @S = @S + '   SELECT sid, CASE WHEN diskadmin > 0     THEN Cast(''diskadmin'' AS varchar(15))     ELSE Cast('''' AS varchar(15)) END AS Server_Role FROM sys.syslogins UNION'
		Set @S = @S + '   SELECT sid, CASE WHEN dbcreator > 0     THEN Cast(''dbcreator'' AS varchar(15))     ELSE Cast('''' AS varchar(15)) END AS Server_Role FROM sys.syslogins UNION'
		Set @S = @S + '   SELECT sid, CASE WHEN bulkadmin > 0     THEN Cast(''bulkadmin'' AS varchar(15))     ELSE Cast('''' AS varchar(15)) END AS Server_Role FROM sys.syslogins '
		Set @S = @S + ' ),'
		Set @S = @S + ' UserRoleList (sid, Server_Roles) AS ('
		Set @S = @S + ' SELECT sid,  (STUFF(( SELECT CAST('', '' + Server_Role AS varchar(256))'
		Set @S = @S +				  ' FROM UserRoleNames AS ObjectSource'
		Set @S = @S +				  ' WHERE (UserRoleNames.sid = ObjectSource.sid )'
		Set @S = @S +				  ' ORDER BY Server_Role'
		Set @S = @S +				  ' FOR XML PATH ( '''' ) ), 1, 2, '''')) AS Server_Roles'
		Set @S = @S + ' FROM UserRoleNames'
		Set @S = @S + ' GROUP BY sid)'
		Set @S = @S + ' INSERT INTO #Tmp_Auth_Server_Logins (LoginName, User_Type_Desc, Server_Roles, Principal_ID)'
		Set @S = @S + ' SELECT LoginName, User_Type_Desc,'
		Set @S = @S +        ' CASE WHEN UserRoleList.Server_Roles LIKE '', %'' THEN Substring(UserRoleList.Server_Roles, 3, 100)'
		Set @S = @S +        ' ELSE UserRoleList.Server_Roles'
		Set @S = @S +        ' END AS Server_Roles,'
		Set @S = @S +        ' Principal_ID'
		Set @S = @S + ' FROM (SELECT name AS LoginName,'
		Set @S = @S +          ' default_database_name AS Default_DB,'
		Set @S = @S +          ' principal_id AS [Principal_ID],'
		Set @S = @S +          ' Cast(''SQL_USER'' AS varchar(32)) AS User_Type_Desc,'
		Set @S = @S +          ' sid'
		Set @S = @S +       ' FROM sys.sql_logins'
		Set @S = @S +       ' WHERE is_disabled = 0'
		Set @S = @S +       ' UNION'
		Set @S = @S +       ' SELECT L.loginname,'
		Set @S = @S +              ' L.dbname,'
		Set @S = @S +              ' NULL AS Principal_ID,'
		Set @S = @S +              ' CASE WHEN L.isntname = 0 THEN ''SQL_USER'' '
		Set @S = @S +              ' ELSE CASE WHEN L.isntgroup = 1 THEN ''WINDOWS_GROUP'' '
		Set @S = @S +                        ' WHEN L.isntuser = 1  THEN ''WINDOWS_USER'' '
		Set @S = @S +                        ' ELSE ''Unknown_Type'' '
		Set @S = @S +                   ' END'
		Set @S = @S +              ' END AS User_Type_Desc,'
		Set @S = @S +              ' sid'
		Set @S = @S +       ' FROM sys.syslogins AS L'
		Set @S = @S +       ' WHERE NOT L.sid IN ( SELECT sid FROM sys.sql_logins ) AND'
		Set @S = @S +             ' NOT L.name LIKE ''##MS%'' ) UnionQ'
		Set @S = @S + ' INNER JOIN UserRoleList'
		Set @S = @S +   ' ON UnionQ.sid = UserRoleList.sid'
		Set @S = @S + ' ORDER BY UnionQ.User_Type_Desc, UnionQ.LoginName'

		If @previewSql <> 0
			Print @S
		Else
		Begin -- <a1>
			exec sp_executesql @S

			If @infoOnly <> 0
			Begin
				SELECT *
				FROM #Tmp_Auth_Server_Logins
				ORDER BY User_Type_Desc, LoginName
			End
			Else
			Begin -- <b1>

				---------------------------------------------------
				-- Merge #Tmp_Auth_Server_Logins into T_Auth_Server_Logins
				---------------------------------------------------
				--
				Set @CurrentLocation = 'Merge #Tmp_Auth_Server_Logins into T_Auth_Server_Logins'

				MERGE dbo.T_Auth_Server_Logins AS t
				USING (SELECT LoginName,
				              User_Type_Desc,
				              Server_Roles,
				              Principal_ID
				       FROM #Tmp_Auth_Server_Logins) as s
				ON ( t.LoginName = s.LoginName)
				WHEN MATCHED AND (
					t.User_Type_Desc <> s.User_Type_Desc OR
					t.Enabled = 0 OR
					ISNULL( NULLIF(t.Server_Roles, s.Server_Roles),
							NULLIF(s.Server_Roles, t.Server_Roles)) IS NOT NULL OR
					ISNULL( NULLIF(t.Principal_ID, s.Principal_ID),
							NULLIF(s.Principal_ID, t.Principal_ID)) IS NOT NULL
					)
				THEN UPDATE SET
					User_Type_Desc = s.User_Type_Desc,
					Server_Roles = s.Server_Roles,
					Principal_ID = s.Principal_ID,
					Last_Affected = GetDate(),
					Enabled = 1
				WHEN NOT MATCHED BY TARGET THEN
					INSERT(LoginName, User_Type_Desc, Server_Roles, Principal_ID, Entered, Last_Affected, Enabled)
					VALUES(s.LoginName, s.User_Type_Desc, s.Server_Roles, s.Principal_ID, GetDate(), GetDate(), 1)
				WHEN NOT MATCHED BY SOURCE THEN
				    UPDATE SET
					Enabled = 0
				;

			End -- </b1>
		End -- </a1>


		---------------------------------------------------
		-- Populate #Tmp_DatabaseNames with the database names
		---------------------------------------------------

		Set @CurrentLocation = 'Parsing database name list'


		Declare @Delim char(1) = ','

		-- The following generates a Tally table with 256 rows
		-- then uses that table to split @databaseList on commas
		-- We could alternatively have used dbo.udfParseDelimitedList() but wanted to keep this procedure self-contained
		--
		;
		WITH
		  Pass0 as (select 1 as C union all select 1),          -- 2 rows
		  Pass1 as (select 1 as C from Pass0 as A, Pass0 as B), -- 4 rows
		  Pass2 as (select 1 as C from Pass1 as A, Pass1 as B), -- 16 rows
		  Pass3 as (select 1 as C from Pass2 as A, Pass2 as B), -- 256 rows
		  Tally as (select row_number() over(order by C) as Number from Pass3)
		INSERT INTO #Tmp_DatabaseNames( Database_Name,
		                                IsValid )
		SELECT [Value] AS Database_Name,
		       0 AS IsValid
		FROM ( SELECT rowNum,
		              Row_Number() OVER ( Partition BY [Value] ORDER BY rowNum ) AS valueNum,
		              [Value]
		       FROM ( SELECT Row_Number() OVER ( ORDER BY CHARINDEX(@Delim, @databaseList + @Delim) ) AS rowNum,
		                     LTRIM(RTRIM(SUBSTRING(
		                       @databaseList,
		                       Tally.[Number],
		                       CHARINDEX(@Delim, @databaseList + @Delim, Tally.[Number]) - [Number]))) AS [Value]
		              FROM Tally
		              WHERE Tally.[Number] <= LEN(@databaseList) AND
		                    SUBSTRING(@Delim + @databaseList, Tally.[Number], LEN(@Delim)) = @Delim ) AS x
		      ) SplitQ
		WHERE valueNum = 1
		ORDER BY rowNum;


		---------------------------------------------------
		-- Validate the database names
		---------------------------------------------------
		--
		Set @CurrentLocation = 'Validating database names'

		-- Make sure none of the names are surrounded with square brackets
		--
		UPDATE #Tmp_DatabaseNames
		SET Database_Name = Substring(Database_Name, 2, Len(Database_Name)-2)
		WHERE Database_Name Like '[[]%]'
		--
		Select @myRowCount = @@RowCount, @myError = @@Error


		UPDATE #Tmp_DatabaseNames
		SET IsValid = 1,
		    Database_ID = SystemDBs.Database_ID,
		    Database_Name = SystemDBs.name
		FROM #Tmp_DatabaseNames
		     INNER JOIN sys.databases SystemDBs
		       ON #Tmp_DatabaseNames.Database_Name = SystemDBs.name
		--
		Select @myRowCount = @@RowCount, @myError = @@Error


		If Exists (Select * From #Tmp_DatabaseNames Where IsValid = 0)
		Begin
			Set @message = 'One or more invalid databases: '

			SELECT @message = @message + Database_Name + ', '
			FROM #Tmp_DatabaseNames
			WHERE IsValid = 0

			Set @message = Substring(@message, 1, Len(@message) - 2)
			Print @message

			If @infoOnly <> 0
				SELECT @message as Warning

			Delete From #Tmp_DatabaseNames Where IsValid = 0
		End

		If Not Exists (Select * From #Tmp_DatabaseNames)
		Begin
			If @message = ''
			Begin
				Set @message = 'Database list was empty'
				Print @message

				If @infoOnly <> 0
					SELECT @message as Warning

			End

			Goto Done
		End

		---------------------------------------------------
		-- Iterate through the database list
		---------------------------------------------------
		--

		Declare @entryID int = 0
		Declare @continue tinyint = 1

		Declare @DatabaseID int
		Declare @DatabaseName nvarchar(128)

		While @continue > 0
		Begin -- <a2>

			SELECT TOP 1
				@DatabaseID = Database_ID,
				@DatabaseName = Database_Name,
				@EntryID = Entry_ID
			FROM #Tmp_DatabaseNames
			WHERE Entry_ID > @EntryID
			ORDER BY Entry_ID
			--
			Select @myRowCount = @@RowCount, @myError = @@Error


			If @myRowCount = 0
			Begin
				Set @Continue = 0
			End
			Else
			Begin -- <b2>

				---------------------------------------------------
				-- Store the database logins and roles in #Tmp_Auth_Database_LoginsAndRoles
				---------------------------------------------------
				--
				Set @CurrentLocation = 'Populating #Tmp_Auth_Database_LoginsAndRoles for database ' + @DatabaseName + ' (ID ' + Cast(@DatabaseID as varchar(12)) + ')'

				Set @S = ''
				Set @S = @S + ' WITH RoleMembers (member_principal_id, role_principal_id) AS ('
				Set @S = @S + '   SELECT rm1.member_principal_id, rm1.role_principal_id'
				Set @S = @S +   ' FROM [' + @DatabaseName + '].sys.database_role_members rm1 ( NOLOCK )'
				Set @S = @S +   ' UNION ALL'
				Set @S = @S +   ' SELECT d.member_principal_id, rm.role_principal_id'
				Set @S = @S +   ' FROM [' + @DatabaseName + '].sys.database_role_members rm ( NOLOCK )'
				Set @S = @S +   '   INNER JOIN RoleMembers AS d'
				Set @S = @S +     '   ON rm.member_principal_id = d.role_principal_id'
				Set @S = @S + ' ),'
				Set @S = @S + ' UserRoleQuery AS ('
				Set @S = @S +   ' SELECT DISTINCT mp.name AS database_user,'
				Set @S = @S +     ' rp.name AS database_role,'
				Set @S = @S +     ' drm.member_principal_id'
				Set @S = @S +   ' FROM RoleMembers drm'
				Set @S = @S +     ' INNER JOIN [' + @DatabaseName + '].sys.database_principals rp'
				Set @S = @S +       ' ON (drm.role_principal_id = rp.principal_id)'
				Set @S = @S +     ' INNER JOIN [' + @DatabaseName + '].sys.database_principals mp'
				Set @S = @S +       ' ON (drm.member_principal_id = mp.principal_id)'
				Set @S = @S + ' )'
				Set @S = @S + ' INSERT INTO #Tmp_Auth_Database_LoginsAndRoles ('
				Set @S = @S +   ' Database_ID, Database_Name, Principal_ID, UserName, LoginName, User_Type, User_Type_Desc, Database_Roles)'
				Set @S = @S + ' SELECT ' + Cast(@DatabaseID as varchar(12)) + ', '
				Set @S = @S +       ' ''' + @DatabaseName + ''', '
				Set @S = @S +       ' dbp.Principal_ID,'
				Set @S = @S +       ' dbp.name AS UserName,'
				Set @S = @S +       ' [' + @DatabaseName + '].sys.syslogins.LoginName,'
				Set @S = @S +       ' dbp.[type] AS User_Type,'
				Set @S = @S +       ' dbp.type_desc AS User_Type_Desc,'
				Set @S = @S +       ' RoleListByUser.Database_Roles'
				Set @S = @S + ' FROM [' + @DatabaseName + '].sys.database_principals dbp '
				Set @S = @S +      ' LEFT OUTER JOIN [' + @DatabaseName + '].sys.syslogins'
				Set @S = @S +         ' ON dbp.sid = [' + @DatabaseName + '].sys.syslogins.sid'
				Set @S = @S +      ' LEFT OUTER JOIN ( SELECT UserRoleQuery.database_user,'
				Set @S = @S +                               ' UserRoleQuery.member_principal_id,'
				Set @S = @S +                               ' (STUFF(( SELECT CAST('', '' + database_role AS varchar(256))'
				Set @S = @S +                                        ' FROM UserRoleQuery AS UserRoleQuery2'
				Set @S = @S +                                        ' WHERE UserRoleQuery.database_user = UserRoleQuery2.database_user'
				Set @S = @S +                                       '  ORDER BY database_role'
				Set @S = @S +                                ' FOR XML PATH ( '''' ) ), 1, 2, '''')) AS Database_Roles'
				Set @S = @S +                        ' FROM UserRoleQuery'
				Set @S = @S +                        ' GROUP BY UserRoleQuery.database_user, UserRoleQuery.member_principal_id ) AS RoleListByUser'
				Set @S = @S +        ' ON dbp.principal_id = RoleListByUser.member_principal_id'
				Set @S = @S + ' WHERE NOT dbp.[type] IN (''R'') AND'
				Set @S = @S +       ' NOT dbp.name IN (''INFORMATION_SCHEMA'', ''guest'', ''sys'')'
				Set @S = @S + ' GROUP BY dbp.principal_id, [' + @DatabaseName + '].sys.syslogins.loginname, dbp.name, dbp.[type], dbp.type_desc, RoleListByUser.Database_Roles'
				Set @S = @S + ' ORDER BY dbp.name'

				If @previewSql <> 0
					Print @S
				Else
				Begin -- <c>

					Truncate Table #Tmp_Auth_Database_LoginsAndRoles
					exec sp_executesql @S

					If @infoOnly <> 0
					Begin
						SELECT *
						FROM #Tmp_Auth_Database_LoginsAndRoles
						ORDER BY UserName
					End
					Else
					Begin -- <d>

						---------------------------------------------------
						-- Delete invalid rows from T_Auth_Database_LoginsAndRoles
						---------------------------------------------------
						--
						Set @CurrentLocation = 'Deleting invalid rows in T_Auth_Database_LoginsAndRoles for database ' + @DatabaseName + ' (ID ' + Cast(@DatabaseID as varchar(12)) + ')'

						If Exists (
							SELECT *
							FROM T_Auth_Database_LoginsAndRoles
							WHERE Database_Name = @DatabaseName AND Database_ID <> @DatabaseID OR
							      Database_Name <> @DatabaseName AND Database_ID = @DatabaseID
							)
						Begin

							DELETE FROM T_Auth_Database_LoginsAndRoles
							WHERE Database_Name = @DatabaseName AND Database_ID <> @DatabaseID OR
							      Database_Name <> @DatabaseName AND Database_ID = @DatabaseID
							--
							Select @myRowCount = @@RowCount, @myError = @@Error

							Set @message = 'Deleted ' + Cast(@myRowCount as varchar(12)) + ' rows from T_Auth_Database_LoginsAndRoles ' +
							              ' that were for database ' + @DatabaseName + ' yet did not have database ID ' + Cast(@DatabaseID as varchar(12))

							Exec PostLogEntry 'Warning', @message, 'CacheServerUsersAndPermissions'

						End

						---------------------------------------------------
						-- Merge #Tmp_Auth_Database_LoginsAndRoles into T_Auth_Database_LoginsAndRoles
						---------------------------------------------------
						--
						Set @CurrentLocation = 'Merge #Tmp_Auth_Database_LoginsAndRoles into T_Auth_Database_LoginsAndRoles for database ' + @DatabaseName + ' (ID ' + Cast(@DatabaseID as varchar(12)) + ')'

						MERGE dbo.T_Auth_Database_LoginsAndRoles AS t
						USING (SELECT Database_ID,
									  Database_Name,
									  Principal_ID,
									  UserName,
									  LoginName,
									  User_Type,
									  User_Type_Desc,
									  Database_Roles
								FROM #Tmp_Auth_Database_LoginsAndRoles) as s
						ON ( t.Database_ID = s.Database_ID AND t.Principal_ID = s.Principal_ID)
						WHEN MATCHED AND (
						    t.UserName <> s.UserName OR
						    t.User_Type <> s.User_Type OR
						    t.Enabled = 0 OR
						    ISNULL( NULLIF(t.LoginName, s.LoginName),
						            NULLIF(s.LoginName, t.LoginName)) IS NOT NULL OR
						    ISNULL( NULLIF(t.User_Type_Desc, s.User_Type_Desc),
						            NULLIF(s.User_Type_Desc, t.User_Type_Desc)) IS NOT NULL OR
						    ISNULL( NULLIF(t.Database_Roles, s.Database_Roles),
						            NULLIF(s.Database_Roles, t.Database_Roles)) IS NOT NULL
						    )
						THEN UPDATE SET
						    Database_Name = s.Database_Name,
						    UserName = s.UserName,
						    LoginName = s.LoginName,
						    User_Type = s.User_Type,
						    User_Type_Desc = s.User_Type_Desc,
						    Database_Roles = s.Database_Roles,
						    Last_Affected = GetDate(),
						    Enabled = 1
						WHEN NOT MATCHED BY TARGET THEN
						    INSERT(Database_ID, Database_Name, Principal_ID, UserName, LoginName,
						           User_Type, User_Type_Desc, Database_Roles,
						           Entered, Last_Affected, Enabled)
						    VALUES(s.Database_ID, s.Database_Name, s.Principal_ID, s.UserName, s.LoginName,
						           s.User_Type, s.User_Type_Desc, s.Database_Roles,
						           GetDate(), GetDate(), 1)
						;

						-- Update extra rows to have Enabled = 0
						--
						UPDATE T_Auth_Database_LoginsAndRoles
						SET Enabled = 0
						FROM T_Auth_Database_LoginsAndRoles target
						     LEFT OUTER JOIN #Tmp_Auth_Database_LoginsAndRoles source
						       ON target.Database_ID = source.Database_ID AND
						          target.Principal_ID = source.Principal_ID
						WHERE target.Database_ID = @DatabaseID AND
						      source.Database_ID IS NULL
						--
						Select @myRowCount = @@RowCount, @myError = @@Error

					End -- </d>
				End -- </c>

				---------------------------------------------------
				-- Store the database permissions in #Tmp_Auth_Database_Permissions
				---------------------------------------------------
				--
				Set @CurrentLocation = 'Populating #Tmp_Auth_Database_Permissions for database ' + @DatabaseName + ' (ID ' + Cast(@DatabaseID as varchar(12)) + ')'

				Set @S = ''
				Set @S = @S + ' WITH SourceData (Principal_ID, User_Type, User_Type_Desc, Role_Or_User, Permission, ObjectName, Sort_Order)'
				Set @S = @S + ' AS ('
				Set @S = @S +   ' SELECT p.principal_id,'
				Set @S = @S +          ' p.type,'
				Set @S = @S +          ' p.type_desc,'
				Set @S = @S +          ' p.name,'
				Set @S = @S +          ' d.permission_name,'
				Set @S = @S +          ' o.name,'
				Set @S = @S +          ' CASE WHEN d.permission_name = ''EXECUTE'' THEN 1'
				Set @S = @S +               ' WHEN d.permission_name = ''SELECT'' THEN 2'
				Set @S = @S +               ' WHEN d.permission_name = ''INSERT'' THEN 3'
				Set @S = @S +               ' WHEN d.permission_name = ''UPDATE'' THEN 4'
				Set @S = @S +               ' WHEN d.permission_name = ''DELETE'' THEN 5'
				Set @S = @S +               ' ELSE 5'
				Set @S = @S +          ' END AS Sort_Order'
				Set @S = @S +   ' FROM [' + @DatabaseName + '].sys.database_principals AS p'
				Set @S = @S +     ' INNER JOIN [' + @DatabaseName + '].sys.database_permissions AS d'
				Set @S = @S +       ' ON d.grantee_principal_id = p.principal_id'
				Set @S = @S +     ' INNER JOIN [' + @DatabaseName + '].sys.objects AS o'
				Set @S = @S +       ' ON o.object_id = d.major_id'
				Set @S = @S +   ' WHERE NOT (p.name = ''public'' AND (o.name LIKE ''dt[_]%'' OR o.name IN (''dtproperties''))) AND'
				Set @S = @S +   ' NOT d.permission_name IN (''view definition'', ''alter'', ''REFERENCES'') AND'
				Set @S = @S +   ' NOT o.name IN (''fn_diagramobjects'', ''sp_alterdiagram'', ''sp_creatediagram'', ''sp_dropdiagram'', ''sp_helpdiagramdefinition'', ''sp_helpdiagrams'', ''sp_renamediagram'')'
				Set @S = @S +   ' )'
				Set @S = @S +   ' INSERT INTO #Tmp_Auth_Database_Permissions(Database_ID, Database_Name, Principal_ID, Role_Or_User, User_Type, User_Type_Desc, Permission, Object_Names, Sort_Order)'
				Set @S = @S + ' SELECT ' + Cast(@DatabaseID as varchar(12)) + ', '
				Set @S = @S +       ' ''' + @DatabaseName + ''', '
				Set @S = @S +       ' Principal_ID,'
				Set @S = @S +       ' Role_Or_User,'
				Set @S = @S +       ' User_Type,'
				Set @S = @S +       ' User_Type_Desc,'
				Set @S = @S +       ' Permission,'
				Set @S = @S +       ' (STUFF(( SELECT CAST('', '' + ObjectName AS varchar(256))'
				Set @S = @S +                ' FROM SourceData AS ObjectSource'
				Set @S = @S +                ' WHERE (SourceData.Role_Or_User = ObjectSource.Role_Or_User AND'
				Set @S = @S +                       ' SourceData.Permission = ObjectSource.Permission)'
				Set @S = @S +                ' ORDER BY ObjectName'
				Set @S = @S +        ' FOR XML PATH ( '''' ) ), 1, 2, '''')) AS Object_Names,'
				Set @S = @S +        ' Sort_Order'
				Set @S = @S + ' FROM SourceData'
				Set @S = @S + ' GROUP BY Principal_ID, User_Type, User_Type_Desc, Role_Or_User, Permission, Sort_Order'
				Set @S = @S + ' ORDER BY Role_Or_User, Sort_Order;'

				If @previewSql <> 0
					Print @S
				Else
				Begin -- <e>

					Truncate Table #Tmp_Auth_Database_Permissions
					exec sp_executesql @S

					If @infoOnly <> 0
					Begin
						SELECT *
						FROM #Tmp_Auth_Database_Permissions
						ORDER BY Role_Or_User, Sort_Order
					End
					Else
					Begin -- <f>

						---------------------------------------------------
						-- Delete invalid rows from T_Auth_Database_Permissions
						---------------------------------------------------
						--
						Set @CurrentLocation = 'Deleting invalid rows in T_Auth_Database_Permissions for database ' + @DatabaseName + ' (ID ' + Cast(@DatabaseID as varchar(12)) + ')'

						If Exists (
							SELECT *
							FROM T_Auth_Database_Permissions
							WHERE Database_Name = @DatabaseName AND Database_ID <> @DatabaseID OR
							      Database_Name <> @DatabaseName AND Database_ID = @DatabaseID
							)
						Begin

							DELETE FROM T_Auth_Database_Permissions
							WHERE Database_Name = @DatabaseName AND Database_ID <> @DatabaseID OR
							      Database_Name <> @DatabaseName AND Database_ID = @DatabaseID
							--
							Select @myRowCount = @@RowCount, @myError = @@Error

							Set @message = 'Deleted ' + Cast(@myRowCount as varchar(12)) + ' rows from T_Auth_Database_Permissions ' +
							              ' that were for database ' + @DatabaseName + ' yet did not have database ID ' + Cast(@DatabaseID as varchar(12))

							Exec PostLogEntry 'Warning', @message, 'CacheServerUsersAndPermissions'

						End

						---------------------------------------------------
						-- Merge #Tmp_Auth_Database_Permissions into T_Auth_Database_Permissions
						---------------------------------------------------
						--
						Set @CurrentLocation = 'Merge #Tmp_Auth_Database_Permissions into T_Auth_Database_Permissions for database ' + @DatabaseName + ' (ID ' + Cast(@DatabaseID as varchar(12)) + ')'

						MERGE dbo.T_Auth_Database_Permissions AS t
						USING (SELECT Database_ID,
									  Database_Name,
									  Principal_ID,
									  Role_Or_User,
									  User_Type,
									  User_Type_Desc,
									  Permission,
									  Object_Names,
									  Sort_Order FROM #Tmp_Auth_Database_Permissions) as s
						ON ( t.Database_ID = s.Database_ID AND t.Permission = s.Permission AND t.Principal_ID = s.Principal_ID)
						WHEN MATCHED AND (
						    t.Role_Or_User <> s.Role_Or_User OR
						    t.User_Type <> s.User_Type OR
						    t.Enabled = 0 OR
						    t.Sort_Order <> s.Sort_Order OR
						    ISNULL( NULLIF(t.User_Type_Desc, s.User_Type_Desc),
						            NULLIF(s.User_Type_Desc, t.User_Type_Desc)) IS NOT NULL OR
						    ISNULL( NULLIF(t.Object_Names, s.Object_Names),
						            NULLIF(s.Object_Names, t.Object_Names)) IS NOT NULL
						    )
						THEN UPDATE SET
						    Database_Name = s.Database_Name,
						    Role_Or_User = s.Role_Or_User,
						    User_Type = s.User_Type,
						    User_Type_Desc = s.User_Type_Desc,
						    Object_Names = s.Object_Names,
						    Sort_Order = s.Sort_Order,
						    Last_Affected = GetDate(),
						    Enabled = 1
						WHEN NOT MATCHED BY TARGET THEN
						    INSERT(Database_ID, Database_Name, Principal_ID, Role_Or_User,
						           User_Type, User_Type_Desc, Permission, Object_Names,
						           Sort_Order, Entered, Last_Affected, Enabled)
						    VALUES(s.Database_ID, s.Database_Name, s.Principal_ID, s.Role_Or_User,
						           s.User_Type, s.User_Type_Desc, s.Permission, s.Object_Names,
						           s.Sort_Order, GetDate(), GetDate(), 1)
						;

						-- Update extra rows to have Enabled = 0
						--
						UPDATE T_Auth_Database_Permissions
						SET Enabled = 0
						FROM T_Auth_Database_Permissions target
						     LEFT OUTER JOIN #Tmp_Auth_Database_Permissions source
						       ON target.Database_ID = source.Database_ID AND
						          target.Principal_ID = source.Principal_ID
						WHERE target.Database_ID = @DatabaseID AND
						      source.Database_ID IS NULL
						--
						Select @myRowCount = @@RowCount, @myError = @@Error

					End -- </f>
				End -- </e>

			End -- </b2>
		End -- </a2>

		If @infoOnly = 0
		Begin
			Print 'View the cached data with:'
			Print 'SELECT * FROM T_Auth_Server_Logins ORDER BY User_Type_Desc, LoginName'
			Print 'SELECT * FROM T_Auth_Database_LoginsAndRoles ORDER BY Database_Name, UserName'
			Print 'SELECT * FROM T_Auth_Database_Permissions ORDER BY Database_Name, Role_Or_User, Sort_Order'
		End

	END TRY
	BEGIN CATCH
		-- Error caught
		If @@TranCount > 0
			Rollback

		Declare @CallingProcName varchar(128) = IsNull(ERROR_PROCEDURE(), 'CacheServerUsersAndPermissions')
				exec LocalErrorHandler  @CallingProcName, @CurrentLocation, @LogError = 0,
										@ErrorNum = @myError output, @message = @message output

		Set @message = 'Exception: ' + @message
		print @message
		Goto Done
	END CATCH

Done:

	Return @myError

GO

/****** Object:  StoredProcedure [dbo].[CheckAccessPermission]    Script Date: 1/15/2020 8:33:53 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

create PROCEDURE [dbo].[CheckAccessPermission]
/****************************************************
**
**	Desc:
**  Does current user have permission to execute
**  given stored procedure
**
**	Return values: 0: no, >0: yes
**
**	Parameters:
**
**		Auth: grk
**		Date: 02/08/2005
**
*****************************************************/
@sprocName varchar(128)
AS
	SET NOCOUNT ON
	declare @result int
	set @result = 0

	select @result = (PERMISSIONS(OBJECT_ID(@sprocName)) & 0x20)

	RETURN @result
GO

GRANT EXECUTE ON [dbo].[CheckAccessPermission] TO [Mgr_Config_Admin] AS [dbo]
GO

/****** Object:  StoredProcedure [dbo].[CheckForParamChanged]    Script Date: 1/15/2020 8:33:53 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[CheckForParamChanged]
/****************************************************
**
**	Desc:
**    Checks whether or not the manager needs to
**    update its local copy of its parameters
**
**	Return values:
**     0: Parameters haven't changed
**    -1: Parameters have changed
**     n: Error code
**
**	Parameters:
**
**	Auth:	grk
**	Date:	06/05/2007
**			06/12/2007 dac - Modified numeric return values to remove duplicates
**			05/04/2009 mem - Added call to PostUsageLogEntry to gauge the frequency that this stored procedure is called
**
*****************************************************/
(
	@managerName varchar(50),
	@message varchar(512) output
)
AS
	set nocount on

	declare @myError int
	set @myError = 0

	declare @myRowCount int
	set @myRowCount = 0

	set @message = ''

	---------------------------------------------------
	-- Check param changed flag for manager
	---------------------------------------------------
	declare @pvc tinyint
	set @pvc = 0
	--
	SELECT @pvc = M_ParmValueChanged
	FROM T_Mgrs
	WHERE (M_Name = @managerName)
	--
	SELECT @myError = @@error, @myRowCount = @@rowcount
	--
	if @myError <> 0
	begin
		set @myError = 52001
		set @message = 'Error checking param changed flag'
		goto DONE
	end
	--
	if @myRowCount <> 1
	begin
		set @myError = 52002
		set @message = 'Could not find entry for manager, name = ' + @managername
		goto DONE
	end

	---------------------------------------------------
	-- No further action required if flag was not set
	---------------------------------------------------
	--
	if @pvc = 0 goto DONE

	---------------------------------------------------
	-- Flag was set: Clear flag and set return code
	---------------------------------------------------
	--
	UPDATE T_Mgrs
	SET M_ParmValueChanged = 0
	WHERE (M_Name = @managerName)
	--
	SELECT @myError = @@error, @myRowCount = @@rowcount
	--
	if @myError <> 0
	begin
		set @myError = 52003
		set @message = 'Error resetting param changed flag'
		goto DONE
	end

	set @myError = -1

	---------------------------------------------------
	--
	---------------------------------------------------
Done:

	Declare @UsageMessage varchar(512)
	Set @UsageMessage = 'Manager: ' + @managerName
	Exec PostUsageLogEntry 'CheckForParamChanged', @UsageMessage, @MinimumUpdateInterval=0

	return @myError
GO

GRANT EXECUTE ON [dbo].[CheckForParamChanged] TO [DMS_Analysis_Job_Runner] AS [dbo]
GO

GRANT EXECUTE ON [dbo].[CheckForParamChanged] TO [Mgr_Config_Admin] AS [dbo]
GO

GRANT EXECUTE ON [dbo].[CheckForParamChanged] TO [svc-dms] AS [dbo]
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

/****** Object:  StoredProcedure [dbo].[DisableSequestClusters]    Script Date: 1/15/2020 8:33:53 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE PROCEDURE [dbo].[DisableSequestClusters]
/****************************************************
**
**	Desc:	Disables the Sequest Clusters
**
**	Return values: 0: success, otherwise, error code
**
**	Auth:	mem
**	Date:	07/24/2008
**			10/09/2009 mem - Changed @ManagerTypeIDList to 11
**
*****************************************************/
(
	@PreviewUpdates tinyint = 0,
	@message varchar(512)='' output
)
As
	Set NoCount On

	Declare @myError int

	exec @myerror = EnableDisableAllManagers @ManagerTypeIDList='11', @ManagerNameList='%SeqCluster%', @enable=0,
	                                         @PreviewUpdates=@PreviewUpdates, @message = @message output

	Return @myError


GO

GRANT EXECUTE ON [dbo].[DisableSequestClusters] TO [Mgr_Config_Admin] AS [dbo]
GO

/****** Object:  StoredProcedure [dbo].[DuplicateManagerParameter]    Script Date: 1/15/2020 8:33:53 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE Procedure [dbo].[DuplicateManagerParameter]
/****************************************************
**
**	Desc:	Duplicates an existing parameter for all managers,
**			creating a new entry with a new TypeID value
**
**	Example usage:
**	  exec DuplicateManagerParameter 157, 172, @ParamValueSearchText='msfileinfoscanner', @ParamValueReplaceText='AgilentToUimfConverter', @InfoOnly=1
**
**    exec DuplicateManagerParameter 179, 182, @ParamValueSearchText='PbfGen', @ParamValueReplaceText='ProMex', @InfoOnly=1
**
**	Auth:	mem
**	Date:	08/26/2013 mem - Initial release
**
*****************************************************/
(
	@SourceParamTypeID int,
	@NewParamTypeID int,
	@ParamValueOverride varchar(255) = null,		-- Optional: New parameter value; ignored if @ParamValueSearchText is defined
	@CommentOverride varchar(255) = null,
	@ParamValueSearchText varchar(255) = null,		-- Optional: text to search for in the source parameter value
	@ParamValueReplaceText varchar(255) = null,		-- Optional: replacement text (ignored if @ParamValueReplaceText is null)
	@InfoOnly tinyint = 1
)
As
	set nocount on

	declare @myError int
	declare @myRowCount int
	set @myError = 0
	set @myRowCount = 0

	---------------------------------------------------
	-- Validate input fields
	---------------------------------------------------

	set @InfoOnly = IsNull(@InfoOnly, 1)

	If @SourceParamTypeID Is Null
	Begin
		Print '@SourceParamTypeID cannot be null; unable to continue'
		return 52000
	End

	If @NewParamTypeID Is Null
	Begin
		Print '@NewParamTypeID cannot be null; unable to continue'
		return 52001
	End

	If Not @ParamValueSearchText Is Null AND @ParamValueReplaceText Is Null
	Begin
		Print '@ParamValueReplaceText cannot be null when @ParamValueSearchText is defined; unable to continue'
		return 52002
	End

	---------------------------------------------------
	-- Make sure the soure parameter exists
	---------------------------------------------------

	If Not Exists (Select * From T_ParamValue Where TypeID = @SourceParamTypeID)
	Begin
		Print '@SourceParamTypeID ' + Convert(varchar(12), @SourceParamTypeID) + ' not found in T_ParamValue; unable to continue'
		return 52003
	End

	If Exists (Select * From T_ParamValue Where TypeID = @NewParamTypeID)
	Begin
		Print '@NewParamTypeID ' + Convert(varchar(12), @NewParamTypeID) + ' already exists in T_ParamValue; unable to continue'
		return 52004
	End

	If Not Exists (Select * From T_ParamType Where ParamID = @NewParamTypeID)
	Begin
		Print '@NewParamTypeID ' + Convert(varchar(12), @NewParamTypeID) + ' not found in T_ParamType; unable to continue'
		return 52005
	End


	If Not @ParamValueSearchText Is Null
	Begin
		If @InfoOnly <> 0
			SELECT @NewParamTypeID AS TypeID,
				REPLACE([Value], @ParamValueSearchText, @ParamValueReplaceText) AS [Value],
				MgrID,
				IsNull(@CommentOverride, '') AS [Comment]
			FROM T_ParamValue
			WHERE (TypeID = @SourceParamTypeID)
		Else
			INSERT INTO T_ParamValue( TypeID,
									[Value],
									MgrID,
									[Comment] )
			SELECT @NewParamTypeID AS TypeID,
				REPLACE([Value], @ParamValueSearchText, @ParamValueReplaceText) AS [Value],
				MgrID,
				IsNull(@CommentOverride, '') AS [Comment]
			FROM T_ParamValue
			WHERE (TypeID = @SourceParamTypeID)
		--
		SELECT @myError = @@error, @myRowCount = @@rowcount
	End
	Else
	Begin
		If @InfoOnly <> 0
			SELECT @NewParamTypeID AS TypeID,
			       IsNull(@ParamValueOverride, [Value]) AS [Value],
			       MgrID,
			       IsNull(@CommentOverride, '') AS [Comment]
			FROM T_ParamValue
			WHERE (TypeID = @SourceParamTypeID)
		Else
			INSERT INTO T_ParamValue( TypeID,
			                          [Value],
			                          MgrID,
			                          [Comment] )
			SELECT @NewParamTypeID AS TypeID,
			       IsNull(@ParamValueOverride, [Value]) AS [Value],
			       MgrID,
			       IsNull(@CommentOverride, '') AS [Comment]
			FROM T_ParamValue
			WHERE (TypeID = @SourceParamTypeID)
		--
		SELECT @myError = @@error, @myRowCount = @@rowcount
	End

	return 0
GO

/****** Object:  StoredProcedure [dbo].[DuplicateManagerParameters]    Script Date: 1/15/2020 8:33:53 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE Procedure [dbo].[DuplicateManagerParameters]
/****************************************************
**
**	Desc:	Duplicates the parameters for a given manager
**			to create new parameters for a new manager
**
**	Example usage:
**	  exec DuplicateManagerParameter 157, 172
**
**	Auth:	mem
**	Date:	10/10/2014 mem - Initial release
**
*****************************************************/
(
	@SourceMgrID int,
	@TargetMgrID int,
	@MergeSourceWithTarget tinyint = 0,			-- When 0, then the target manager cannot have any parameters; if 1, then will add missing parameters to the target manager
	@InfoOnly tinyint = 0
)
As
	set nocount on

	declare @myError int
	declare @myRowCount int
	set @myError = 0
	set @myRowCount = 0

	---------------------------------------------------
	-- Validate input fields
	---------------------------------------------------

	set @InfoOnly = IsNull(@InfoOnly, 1)

	If @SourceMgrID Is Null
	Begin
		Print '@SourceMgrID cannot be null; unable to continue'
		return 52000
	End

	If @TargetMgrID Is Null
	Begin
		Print '@TargetMgrID cannot be null; unable to continue'
		return 52001
	End

	Set @MergeSourceWithTarget = IsNull(@MergeSourceWithTarget, 0)

	---------------------------------------------------
	-- Make sure the source and target managers exist
	---------------------------------------------------

	If Not Exists (Select * From T_Mgrs Where M_ID = @SourceMgrID)
	Begin
		Print '@SourceMgrID ' + Convert(varchar(12), @SourceMgrID) + ' not found in T_Mgrs; unable to continue'
		return 52003
	End

	If Not Exists (Select * From T_Mgrs Where M_ID = @TargetMgrID)
	Begin
		Print '@TargetMgrID ' + Convert(varchar(12), @TargetMgrID) + ' not found in T_Mgrs; unable to continue'
		return 52004
	End

	If @MergeSourceWithTarget = 0
	Begin
		-- Make sure the target manager does not have any parameters
		--
		If Exists (SELECT * FROM T_ParamValue WHERE MgrID = @TargetMgrID)
		Begin
			Print '@TargetMgrID ' + Convert(varchar(12), @TargetMgrID) + ' has existing parameters in T_ParamValue; aborting since @MergeSourceWithTarget = 0'
			return 52005
		End
	End

	If @InfoOnly <> 0
	Begin
			SELECT Source.TypeID,
			       Source.Value,
			       @TargetMgrID AS MgrID,
			       Source.Comment
			FROM T_ParamValue AS Source
			     LEFT OUTER JOIN ( SELECT TypeID
			                       FROM T_ParamValue
			                       WHERE MgrID = @TargetMgrID ) AS ExistingParams
			       ON Source.TypeID = ExistingParams.TypeID
			WHERE MgrID = @SourceMgrID AND
			      ExistingParams.TypeID IS NULL

	End
	Else
	Begin
		INSERT INTO T_ParamValue (TypeID, Value, MgrID, Comment)
		SELECT Source.TypeID,
		       Source.Value,
		       @TargetMgrID AS MgrID,
		       Source.Comment
		FROM T_ParamValue AS Source
		     LEFT OUTER JOIN ( SELECT TypeID
		                       FROM T_ParamValue
		                       WHERE MgrID = @TargetMgrID ) AS ExistingParams
		       ON Source.TypeID = ExistingParams.TypeID
		WHERE MgrID = @SourceMgrID AND
		      ExistingParams.TypeID IS NULL
		--
		SELECT @myError = @@error, @myRowCount = @@rowcount
	End

	return 0
GO

/****** Object:  StoredProcedure [dbo].[EnableArchiveDependentManagers]    Script Date: 1/15/2020 8:33:53 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[EnableArchiveDependentManagers]
/****************************************************
**
**	Desc:	Disables managers that rely on the NWFS archive
**
**	Return values: 0: success, otherwise, error code
**
**	Auth:	mem
**	Date:	06/09/2011 mem - Initial Version
**
*****************************************************/
(
	@PreviewUpdates tinyint = 0,
	@message varchar(512)='' output
)
As
	Set NoCount On

	Declare @myError int

	exec @myerror = EnableDisableAllManagers @ManagerTypeIDList='8,15', @ManagerNameList='All', @enable=1,
	                                         @PreviewUpdates=@PreviewUpdates, @message = @message output


	Return @myError

GO

GRANT EXECUTE ON [dbo].[EnableArchiveDependentManagers] TO [Mgr_Config_Admin] AS [dbo]
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

/****** Object:  StoredProcedure [dbo].[EnableDisableRunJobsRemotely]    Script Date: 1/15/2020 8:33:53 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE PROCEDURE [dbo].[EnableDisableRunJobsRemotely]
/****************************************************
**
**  Desc:   Enables or disables a manager to run jobs remotely
**
**  Return values: 0: success, otherwise, error code
**
**  Auth:   mem
**  Date:   03/28/2018 mem - Initial version
**          03/29/2018 mem - Add parameter @addMgrParamsIfMissing
**
*****************************************************/
(
    @enable tinyint,                        -- 0 to disable running jobs remotely, 1 to enable running jobs remotely
    @managerNameList varchar(4000) = '',    -- Manager(s) to update; supports % for wildcards
    @previewUpdates tinyint = 0,
    @addMgrParamsIfMissing tinyint = 0,      -- When 1, if manger(s) are missing parameters RunJobsRemotely or RemoteHostName, will auto-add those parameters
    @message varchar(512) = '' output
)
As
    Set NoCount On

    declare @myRowCount int
    declare @myError int
    set @myRowCount = 0
    set @myError = 0

    Declare @NewValue varchar(32)
    Declare @ActiveStateDescription varchar(32)
    Declare @CountToUpdate int
    Declare @CountUnchanged int

    -----------------------------------------------
    -- Validate the inputs
    -----------------------------------------------
    --
    Set @managerNameList = IsNull(@managerNameList, '')
    Set @previewUpdates = IsNull(@previewUpdates, 0)
    Set @addMgrParamsIfMissing = IsNull(@addMgrParamsIfMissing, 0)

    If @enable Is Null
    Begin
        set @myError  = 40000
        Set @message = '@enable cannot be null'
        SELECT @message AS Message
        Goto Done
    End

    If Len(@managerNameList) = 0
    Begin
        set @myError  = 40003
        Set @message = '@managerNameList cannot be blank'
        SELECT @message AS Message
        Goto Done
    End

    -----------------------------------------------
    -- Creata a temporary table
    -----------------------------------------------

    CREATE TABLE #TmpManagerList (
        Manager_Name varchar(128) NOT NULL
    )

    -- Populate #TmpMangerList using ParseManagerNameList
    --
    Exec @myError = ParseManagerNameList @managerNameList, @RemoveUnknownManagers=1, @message=@message output

    If @myError <> 0
    Begin
        If Len(@message) = 0
            Set @message = 'Error calling ParseManagerNameList: ' + Convert(varchar(12), @myError)

        Goto Done
    End

    -- Set @NewValue based on @enable
    If @enable = 0
    Begin
        Set @NewValue = 'False'
        Set @ActiveStateDescription = 'run jobs locally'
    End
    Else
    Begin
        Set @NewValue = 'True'
        Set @ActiveStateDescription = 'run jobs remotely'
    End

    If Exists (Select * From #TmpManagerList Where Manager_Name = 'Default_AnalysisMgr_Params')
    Begin
        Delete From #TmpManagerList Where Manager_Name = 'Default_AnalysisMgr_Params'

        Set @message = 'For safety, not updating RunJobsRemotely for manager Default_AnalysisMgr_Params'

        If Exists (Select * From #TmpManagerList)
        Begin
            -- #TmpManagerList contains other managers; update them
            Print @message
        End
        Else
        Begin
            -- #TmpManagerList is now empty; abort
            SELECT @message AS Message
            Goto Done
        End
    End

    If @addMgrParamsIfMissing > 0
    Begin -- <a>
        Declare @mgrName varchar(128) = ''
        Declare @mgrId int = 0
        Declare @paramTypeId int = 0
        Declare @continue tinyint = 1

        While @continue > 0
        Begin -- <b>
            SELECT TOP 1 @mgrName = #TmpManagerList.Manager_Name,
                         @mgrId = T_Mgrs.M_ID
            FROM #TmpManagerList
                 INNER JOIN T_Mgrs
                   ON #TmpManagerList.Manager_Name = T_Mgrs.M_Name
            WHERE Manager_Name > @mgrName
            ORDER BY Manager_Name
            --
            SELECT @myError = @@error, @myRowCount = @@rowcount

            If @myRowCount = 0
                Set @continue = 0
            Else
            Begin -- <c>
                If Not Exists (SELECT * FROM V_MgrParams Where ParameterName = 'RunJobsRemotely' And ManagerName = @mgrName)
                Begin -- <d1>
                    Set @paramTypeId = null
                    SELECT @paramTypeId = ParamID
                    FROM [T_ParamType]
                    Where ParamName = 'RunJobsRemotely'

                    If IsNull(@paramTypeId, 0) = 0
                    Begin
                        Print 'Error: could not find parameter "RunJobsRemotely" in [T_ParamType]'
                    End
                    Else
                    Begin
                        If @previewUpdates > 0
                        Begin
                            Print 'Create parameter RunJobsRemotely for Manager ' + @mgrName + ', value ' + @newValue

                            -- Actually do go ahead and create the parameter, but use a value of False even if @newValue is True
                            -- We need to do this so the managers are included in the query below with PT.ParamName = 'RunJobsRemotely'
                            Insert Into T_ParamValue (MgrID, TypeID, Value)
                            Values (@mgrId, @paramTypeId, 'False')
                        End
                        Else
                        Begin
                            Insert Into T_ParamValue (MgrID, TypeID, Value)
                            Values (@mgrId, @paramTypeId, @newValue)
                        End
                    End
                End -- </d1>

                If Not Exists (SELECT * FROM V_MgrParams Where ParameterName = 'RemoteHostName' And ManagerName = @mgrName)
                Begin -- <d2>
                    Set @paramTypeId = null
                    SELECT @paramTypeId = ParamID
                    FROM [T_ParamType]
                    Where ParamName = 'RemoteHostName'

                    If IsNull(@paramTypeId, 0) = 0
                    Begin
                        Print 'Error: could not find parameter "RemoteHostName" in [T_ParamType]'
                    End
                    Else
                    Begin
                        If @previewUpdates > 0
                        Begin
                            Print 'Create parameter RemoteHostName for Manager ' + @mgrName + ', value PrismWeb2'
                        End
                        Else
                        Begin
                            Insert Into T_ParamValue (MgrID, TypeID, Value)
                            Values (@mgrId, @paramTypeId, 'PrismWeb2')
                        End
                    End
                End -- </d1>
            End -- </c>
        End -- </b>
    End -- </a>

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
    WHERE PT.ParamName = 'RunJobsRemotely' AND
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
    WHERE PT.ParamName = 'RunJobsRemotely' AND
          PV.Value = @NewValue AND
          MT.MT_Active > 0
    --
    SELECT @myError = @@error, @myRowCount = @@rowcount


    If @CountToUpdate = 0
    Begin
        If @CountUnchanged = 0
        Begin
            Set @message = 'No managers were found matching @managerNameList'
        End
        Else
        Begin
            If @CountUnchanged = 1
                Set @message = 'The manager is already set to ' + @ActiveStateDescription
            Else
                Set @message = 'All ' + Convert(varchar(12), @CountUnchanged) + ' managers are already set to ' + @ActiveStateDescription
        End

        SELECT @message AS Message
    End
    Else
    Begin
        If @previewUpdates <> 0
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
            WHERE PT.ParamName = 'RunJobsRemotely' AND
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
            WHERE PT.ParamName = 'RunJobsRemotely' AND
                  PV.Value <> @NewValue AND
                  MT.MT_Active > 0
            --
            SELECT @myError = @@error, @myRowCount = @@rowcount

            If @myRowCount = 1 And @CountUnchanged = 0
            Begin
                Set @message = 'Configured the manager to ' + @ActiveStateDescription
            End
            Else
            Begin
                Set @message = 'Configured ' + Convert(varchar(12), @myRowCount) + ' managers to ' + @ActiveStateDescription

                If @CountUnchanged <> 0
                    Set @message = @message + ' (' + Convert(varchar(12), @CountUnchanged) + ' managers were already set to ' + @ActiveStateDescription + ')'
            End

            SELECT @message AS Message
        End
    End

Done:
    Return @myError

GO

/****** Object:  StoredProcedure [dbo].[GetDefaultRemoteInfoForManager]    Script Date: 1/15/2020 8:33:53 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE PROCEDURE [dbo].[GetDefaultRemoteInfoForManager]
/****************************************************
**
**  Desc:   Gets the default remote info parameters for the given manager
**          Retrieves parameters using GetManagerParametersWork, so properly retrieves parent group parameters, if any
**          If the manager does not have parameters RunJobsRemotely and RemoteHostName defined, returns an empty string
**          Also returns an empty string if RunJobsRemotely is not True
**
**          Example value for @remoteInfoXML
**          <host>prismweb2</host><user>svc-dms</user><taskQueue>/file1/temp/DMSTasks</taskQueue><workDir>/file1/temp/DMSWorkDir</workDir><orgDB>/file1/temp/DMSOrgDBs</orgDB><privateKey>Svc-Dms.key</privateKey><passphrase>Svc-Dms.pass</passphrase>
**
**  Return values: 0: success, otherwise, error code
**
**  Auth:   mem
**  Date:   05/18/2017 mem - Initial version
**          03/14/2018 mem - Use GetManagerParametersWork to lookup manager parameters, allowing for getting remote info parameters from parent groups
**          03/29/2018 mem - Return an empty string if the manager does not have parameters RunJobsRemotely and RemoteHostName defined, or if RunJobsRemotely is false
**
*****************************************************/
(
    @managerName varchar(128),            -- Manager name
    @remoteInfoXML varchar(900) Output    -- Output XML if valid remote info parameters are defined, otherwise an empty string
)
As
    Set NoCount On

    Declare @myRowCount int
    Declare @myError int
    Set @myRowCount = 0
    Set @myError = 0

    Declare @managerID int = 0
    Set @remoteInfoXML = ''

    SELECT @managerID = M_ID
    FROM T_Mgrs
    WHERE M_Name = @managerName
    --
    SELECT @myError = @@error, @myRowCount = @@rowcount

    If @myRowCount = 0
    Begin
        -- Manager not found
        Goto Done
    End

    -----------------------------------------------
    -- Create the Temp Table to hold the manager parameters
    -----------------------------------------------

    CREATE TABLE #Tmp_Mgr_Params (
        M_Name varchar(50) NOT NULL,
        ParamName varchar(50) NOT NULL,
        Entry_ID int NOT NULL,
        TypeID int NOT NULL,
        Value varchar(128) NOT NULL,
        MgrID int NOT NULL,
        Comment varchar(255) NULL,
        Last_Affected datetime NULL,
        Entered_By varchar(128) NULL,
        M_TypeID int NOT NULL,
        ParentParamPointerState tinyint,
        Source varchar(50) NOT NULL
    )

    -- Populate the temporary table with the manager parameters
    Exec @myError = GetManagerParametersWork @managerName, 0, 50

    If Not Exists ( SELECT [Value]
                    FROM #Tmp_Mgr_Params
                    WHERE M_Name = @managerName And
                          ParamName = 'RunJobsRemotely' AND
                          Value = 'True' )
       OR
       Not Exists ( SELECT [Value]
                    FROM #Tmp_Mgr_Params
                    WHERE M_Name = @managerName And
                          ParamName = 'RemoteHostName' AND
                          Len(Value) > 0 )
    Begin
        Set @remoteInfoXML = ''
    End
    Else
    Begin
        SELECT @remoteInfoXML = @remoteInfoXML + SourceQ.[Value]
        FROM (SELECT 1 AS Sort,
                     '<host>' + [Value] + '</host>' AS [Value]
              FROM #Tmp_Mgr_Params
              WHERE (ParamName = 'RemoteHostName' And M_Name = @managerName)
              UNION
              SELECT 2 AS Sort,
                     '<user>' + [Value] + '</user>' AS [Value]
              FROM #Tmp_Mgr_Params
              WHERE (ParamName = 'RemoteHostUser' And M_Name = @managerName)
              UNION
              SELECT 3 AS Sort,
                     '<dmsPrograms>' + [Value] + '</dmsPrograms>' AS [Value]
              FROM #Tmp_Mgr_Params
              WHERE (ParamName = 'RemoteHostDMSProgramsPath' And M_Name = @managerName)
              UNION
              SELECT 4 AS Sort,
                     '<taskQueue>' + [Value] + '</taskQueue>' AS [Value]
              FROM #Tmp_Mgr_Params
              WHERE (ParamName = 'RemoteTaskQueuePath' And M_Name = @managerName)
              UNION
              SELECT 5 AS Sort,
                     '<workDir>' + [Value] + '</workDir>' AS [Value]
              FROM #Tmp_Mgr_Params
              WHERE (ParamName = 'RemoteWorkDirPath' And M_Name = @managerName)
              UNION
              SELECT 6 AS Sort,
                     '<orgDB>' + [Value] + '</orgDB>' AS [Value]
              FROM #Tmp_Mgr_Params
              WHERE (ParamName = 'RemoteOrgDBPath' And M_Name = @managerName)
              UNION
              SELECT 7 AS Sort,
                     '<privateKey>' + dbo.udfGetFilename([Value]) + '</privateKey>' AS [Value]
              FROM #Tmp_Mgr_Params
              WHERE (ParamName = 'RemoteHostPrivateKeyFile' And M_Name = @managerName)
              UNION
              SELECT 8 AS Sort,
                     '<passphrase>' + dbo.udfGetFilename([Value]) + '</passphrase>' AS [Value]
              FROM #Tmp_Mgr_Params
              WHERE (ParamName = 'RemoteHostPassphraseFile' And M_Name = @managerName)
              ) SourceQ
        ORDER BY SourceQ.Sort
    End

Done:
    Return @myError

GO

GRANT EXECUTE ON [dbo].[GetDefaultRemoteInfoForManager] TO [MTUser] AS [dbo]
GO

/****** Object:  StoredProcedure [dbo].[GetManagerParameters]    Script Date: 1/15/2020 8:33:53 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE PROCEDURE [dbo].[GetManagerParameters]
/****************************************************
**
**  Desc:   Gets the parameters for the given analysis manager(s)
**          Uses MgrSettingGroupName to lookup parameters from the parent group, if any
**
**  Return values: 0: success, otherwise, error code
**
**  Auth:   mem
**  Date:   05/07/2015 mem - Initial version
**          08/10/2015 mem - Add @SortMode=3
**          09/02/2016 mem - Increase the default for parameter @MaxRecursion from 5 to 50
**          03/14/2018 mem - Refactor actual parameter lookup into stored procedure GetManagerParametersWork
**
*****************************************************/
(
    @ManagerNameList varchar(4000) = '',
    @SortMode tinyint = 0,                    -- 0 means sort by ParamTypeID then MgrName, 1 means ParamName, then MgrName, 2 means MgrName, then ParamName, 3 means Value then ParamName
    @MaxRecursion tinyint = 50,
    @message varchar(512) = '' output
)
As
    Set NoCount On

    Declare @myRowCount int
    Declare @myError int
    Set @myRowCount = 0
    Set @myError = 0

    -----------------------------------------------
    -- Validate the inputs
    -----------------------------------------------
    --
    Set @ManagerNameList = IsNull(@ManagerNameList, '')

    Set @SortMode = IsNull(@SortMode, 0)

    If @MaxRecursion > 10
        Set @MaxRecursion = 10

    -----------------------------------------------
    -- Create the Temp Table to hold the manager parameters
    -----------------------------------------------

    CREATE TABLE #Tmp_Mgr_Params (
        M_Name varchar(50) NOT NULL,
        ParamName varchar(50) NOT NULL,
        Entry_ID int NOT NULL,
        TypeID int NOT NULL,
        Value varchar(128) NOT NULL,
        MgrID int NOT NULL,
        Comment varchar(255) NULL,
        Last_Affected datetime NULL,
        Entered_By varchar(128) NULL,
        M_TypeID int NOT NULL,
        ParentParamPointerState tinyint,
        Source varchar(50) NOT NULL
    )

    -- Populate the temporary table with the manager parameters
    Exec @myError = GetManagerParametersWork @ManagerNameList, @SortMode, @MaxRecursion, @message = @message Output

    -- Return the parameters as a result set

    If @SortMode = 0
        SELECT *
        FROM #Tmp_Mgr_Params
        ORDER BY TypeID, M_Name

    If @SortMode = 1
        SELECT *
        FROM #Tmp_Mgr_Params
        ORDER BY ParamName, M_Name

    If @SortMode = 2
        SELECT *
        FROM #Tmp_Mgr_Params
        ORDER BY M_Name, ParamName

    If @SortMode Not In (0,1,2)
        SELECT *
        FROM #Tmp_Mgr_Params
        ORDER BY Value, ParamName

     Drop Table #Tmp_Mgr_Params

Done:
    Return @myError

GO

/****** Object:  StoredProcedure [dbo].[GetManagerParametersWork]    Script Date: 1/15/2020 8:33:53 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE PROCEDURE [dbo].[GetManagerParametersWork]
/****************************************************
**
**  Desc:   Populates temporary tables with the parameters for the given analysis manager(s)
**          Uses MgrSettingGroupName to lookup parameters from the parent group, if any
**
**  Requires that the calling procedure create temporary table #Tmp_Mgr_Params
**
**  Auth:   mem
**  Date:   03/14/2018 mem - Initial version (code refactored from GetManagerParameters)
**
*****************************************************/
(
    @ManagerNameList varchar(4000) = '',
    @SortMode tinyint = 0,                    -- 0 means sort by ParamTypeID then MgrName, 1 means ParamName, then MgrName, 2 means MgrName, then ParamName, 3 means Value then ParamName
    @MaxRecursion tinyint = 50,
    @message varchar(512)='' output
)
As
    Set NoCount On

    Declare @myRowCount int
    Declare @myError int
    Set @myRowCount = 0
    Set @myError = 0

    -----------------------------------------------
    -- Create the Temp Table to hold the manager group information
    -----------------------------------------------

    CREATE TABLE #Tmp_Manager_Group_Info (
        M_Name varchar(50) NOT NULL,
        Group_Name varchar(128) NOT NULL
    )

    -----------------------------------------------
    -- Lookup the initial manager parameters
    -----------------------------------------------
    --

    INSERT INTO #Tmp_Mgr_Params( M_Name,
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
    WHERE (M_Name IN (Select Value From dbo.udfParseDelimitedList(@ManagerNameList, ',')))
    --
    SELECT @myError = @@error, @myRowCount = @@rowcount


    -----------------------------------------------
    -- Append parameters for parent groups, which are
    -- defined by parameter Default_AnalysisMgr_Params (TypeID 162)
    -----------------------------------------------
    --
    Declare @iterations tinyint = 0

    While Exists (Select * from #Tmp_Mgr_Params Where ParentParamPointerState = 1) And @iterations < @MaxRecursion
    Begin
        Truncate table #Tmp_Manager_Group_Info

        INSERT INTO #Tmp_Manager_Group_Info (M_Name, Group_Name)
        SELECT M_Name, Value
        FROM #Tmp_Mgr_Params
        WHERE (ParentParamPointerState = 1)

        UPDATE #Tmp_Mgr_Params
        Set ParentParamPointerState = 2
        WHERE (ParentParamPointerState = 1)

        INSERT INTO #Tmp_Mgr_Params( M_Name,
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
               End As ParentParamPointerState,
               ValuesToAppend.Source
        FROM #Tmp_Mgr_Params Target
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
                                                  FROM #Tmp_Manager_Group_Info ) FilterQ
                                       ON PV.M_Name = FilterQ.Group_Name ) ValuesToAppend
               ON Target.M_Name = ValuesToAppend.M_Name AND
                  Target.TypeID = ValuesToAppend.TypeID
        WHERE (Target.TypeID IS NULL Or ValuesToAppend.typeID = 162)
        --
        SELECT @myError = @@error, @myRowCount = @@rowcount

        -- This is a safety check in case a manager has a Default_AnalysisMgr_Params value pointing to itself
        Set @iterations = @iterations + 1

    End

    Drop Table #Tmp_Manager_Group_Info

Done:
    Return @myError

GO

/****** Object:  StoredProcedure [dbo].[LocalErrorHandler]    Script Date: 1/15/2020 8:33:53 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[LocalErrorHandler]
/****************************************************
**
**	Desc:	This procedure should be called from within a Try...Catch block
**			It will generate an error description and optionally log the error
**			It also returns the Error Severity and Error Number via output parameters
**
**	Return values: 0: success, otherwise, error code
**
**	Auth:	mem
**	Date:	11/30/2006
**			01/03/2008 mem - Added parameter @duplicateEntryHoldoffHours
**			02/23/2016 mem - Add set XACT_ABORT on
**
*****************************************************/
(
	@CallingProcName varchar(128)='',			-- Optionally provide the calling procedure name; if not provided then uses ERROR_PROCEDURE()
	@CallingProcLocation varchar(128)='',		-- Custom description of the location within the calling procedure within which the error occurred
	@LogError tinyint = 0,						-- Set to 1 to log the error in T_Log_Entries
	@DisplayError tinyint = 0,					-- Set to 1 to display the error via SELECT @message
	@LogWarningErrorList varchar(512) = '1205',	-- Comma separated list of errors that should be treated as warnings if logging to T_Log_Entries
	@ErrorSeverity int=0 output,
	@ErrorNum int=0 output,
	@message varchar(512)='' output,			-- Populated with a description of the error
	@duplicateEntryHoldoffHours int = 0			-- Set this to a value greater than 0 to prevent duplicate entries being posted within the given number of hours
)
As
	Set XACT_ABORT, nocount on

	declare @myRowCount int
	declare @myError int
	set @myRowCount = 0
	set @myError = 0

	declare @CurrentLocation varchar(128)
	Set @CurrentLocation = 'Start'

	Declare @ErrorState int
	Declare @ErrorProc varchar(256)
	Declare @ErrorLine int
	Declare @ErrorMessage varchar(256)
	Declare @LogErrorType varchar(64)

	Begin Try
		Set @CurrentLocation = 'Validating the inputs'

		-- Validate the inputs
		Set @CallingProcName = IsNull(@CallingProcName, '')
		Set @CallingProcLocation = IsNull(@CallingProcLocation, '')
		Set @LogError = IsNull(@LogError, 0)
		Set @DisplayError = IsNull(@DisplayError, 0)
		Set @ErrorSeverity = 0
		Set @ErrorNum = 0
		Set @message = ''

		-- Lookup current error information
		Set @CurrentLocation = 'Populating the error tracking variables'
		SELECT
			@ErrorSeverity = IsNull(ERROR_SEVERITY(), 0),
			@ErrorNum = IsNull(ERROR_NUMBER(), 0),
			@ErrorState = IsNull(ERROR_STATE(), 0),
			@ErrorProc = IsNull(ERROR_PROCEDURE(), ''),
			@ErrorLine = IsNull(ERROR_LINE(), 0),
			@ErrorMessage = IsNull(ERROR_MESSAGE(), '')

		-- Generate the error description
		Set @CurrentLocation = 'Generating the error description'
		If Len(IsNull(@ErrorProc, '')) = 0
		Begin
			-- Update @ErrorProc using @CallingProcName
			If len(@CallingProcName) = 0
				Set @CallingProcName = 'Unknown Procedure'

			Set @ErrorProc = @CallingProcName
		End

		-- Update @CallingProcName using @ErrorProc (required for calling PostLogEntry)
		Set @CallingProcName = @ErrorProc

		If @ErrorNum = 0 And Len(@ErrorMessage) = 0
			Set @message = 'No Error for ' + @ErrorProc
		Else
		Begin
			Set @message = 'Error caught in ' + @ErrorProc
			If Len(@CallingProcLocation) > 0
				Set @message = @message + ' at "' + @CallingProcLocation + '"'
			Set @message = @message + ': ' + @ErrorMessage + '; Severity ' + Convert(varchar(12), @ErrorSeverity) + '; Error ' + Convert(varchar(12), @ErrorNum) + '; Line ' + Convert(varchar(12), @ErrorLine)
		End

		If @LogError <> 0
		Begin
			Set @CurrentLocation = 'Examining @LogWarningErrorList'
			If Exists (SELECT Value FROM dbo.udfParseDelimitedIntegerList(@LogWarningErrorList, ',') WHERE Value = @ErrorNum)
				Set @LogErrorType = 'Warning'
			Else
				Set @LogErrorType = 'Error'

			Set @CurrentLocation = 'Calling PostLogEntry'
			execute PostLogEntry @LogErrorType, @message, @CallingProcName, @duplicateEntryHoldoffHours
		End

		If @DisplayError <> 0
			SELECT @message as Error_Description

	End Try
	Begin Catch
		Set @message = 'Error ' + @CurrentLocation + ' in LocalErrorHandler: ' + IsNull(ERROR_MESSAGE(), '?') + '; Error ' + Convert(varchar(12), IsNull(ERROR_NUMBER(), 0))
		Set @myError = ERROR_NUMBER()
		SELECT @message as Error_Description
	End Catch

	RETURN @myError
GO

GRANT EXECUTE ON [dbo].[LocalErrorHandler] TO [DMS_SP_User] AS [dbo]
GO

GRANT EXECUTE ON [dbo].[LocalErrorHandler] TO [Mgr_Config_Admin] AS [dbo]
GO

/****** Object:  StoredProcedure [dbo].[ParseManagerNameList]    Script Date: 1/15/2020 8:33:53 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE PROCEDURE [dbo].[ParseManagerNameList]
/****************************************************
**
**	Desc:	Parses the list of managers in @ManagerNameList and populates
**			a temporary tables with the manager names
**
**			If @RemoveUnknownManagers = 1, then deletes manager names that are not defined in T_Mgrs
**
**			The calling procedure must create the following temporary table:
**			CREATE TABLE #TmpManagerList (
**				Manager_Name varchar(128) NOT NULL
**			)
**
**	Return values: 0: success, otherwise, error code
**
**	Auth:	mem
**	Date:	05/09/2008
**			05/14/2015 mem - Update Insert query to explicitly list field Manager_Name
**
*****************************************************/
(
	@ManagerNameList varchar(4000) = '',
	@RemoveUnknownManagers tinyint = 1,
	@message varchar(512)='' output
)
As
	Set NoCount On

	declare @myRowCount int
	declare @myError int
	set @myRowCount = 0
	set @myError = 0

	Declare @EntryID int
	Declare @Continue int

	Declare @ManagerFilter varchar(128)
	Declare @S varchar(4000)

	-----------------------------------------------
	-- Validate the inputs
	-----------------------------------------------
	--
	Set @ManagerNameList = IsNull(@ManagerNameList, '')
	Set @RemoveUnknownManagers = IsNull(@RemoveUnknownManagers, 1)
	Set @message = ''

	-----------------------------------------------
	-- Creata a temporary table
	-----------------------------------------------

	CREATE TABLE #TmpMangerSpecList (
		Entry_ID int Identity (1,1),
		Manager_Name varchar(128) NOT NULL
	)

	-----------------------------------------------
	-- Parse @ManagerNameList
	-----------------------------------------------

	If Len(@ManagerNameList) > 0
	Begin -- <a>

		-- Populate #TmpMangerSpecList with the data in @ManagerNameList
		INSERT INTO #TmpMangerSpecList (Manager_Name)
		SELECT Value
		FROM dbo.udfParseDelimitedList(@ManagerNameList, ',')
		--
		SELECT @myError = @@error, @myRowCount = @@rowcount


		-- Populate #TmpManagerList with the entries in #TmpMangerSpecList that do not contain a % wildcard
		INSERT INTO #TmpManagerList (Manager_Name)
		SELECT Manager_Name
		FROM #TmpMangerSpecList
		WHERE NOT Manager_Name LIKE '%[%]%'
		--
		SELECT @myError = @@error, @myRowCount = @@rowcount

		-- Delete the non-wildcard entries from #TmpMangerSpecList
		DELETE FROM #TmpMangerSpecList
		WHERE NOT Manager_Name LIKE '%[%]%'


		-- Parse the entries in #TmpMangerSpecList (all should have a wildcard)
		Set @EntryID = 0

		Set @Continue = 1
		While @Continue = 1
		Begin -- <b1>
			SELECT TOP 1 @EntryID = Entry_ID,
						 @ManagerFilter = Manager_Name
			FROM #TmpMangerSpecList
			WHERE Entry_ID > @EntryID
			ORDER BY Entry_ID
			--
			SELECT @myError = @@error, @myRowCount = @@rowcount

			If @myRowCount = 0
				Set @Continue = 0
			Else
			Begin -- <c>
				Set @S = ''
				Set @S = @S + ' INSERT INTO #TmpManagerList (Manager_Name)'
				Set @S = @S + ' SELECT M_Name'
				Set @S = @S + ' FROM T_Mgrs'
				Set @S = @S + ' WHERE M_Name LIKE ''' + @ManagerFilter + ''''

				Exec (@S)
				--
				SELECT @myError = @@error, @myRowCount = @@rowcount

			End -- </c>

		End -- </b1>

		If @RemoveUnknownManagers <> 0
		Begin -- <b2>
			-- Delete entries from #TmpManagerList that don't match entries in M_Name of the given type
			DELETE #TmpManagerList
			FROM #TmpManagerList U LEFT OUTER JOIN
				T_Mgrs M ON M.M_Name = U.Manager_Name
			WHERE M.M_Name Is Null
			--
			SELECT @myError = @@error, @myRowCount = @@rowcount

			If @myRowCount > 0
			Begin
				Set @message = 'Found ' + convert(varchar(12), @myRowCount) + ' entries in @ManagerNameList that are not defined in T_Mgrs'
				Print @message

				Set @message = ''
			End

		End -- </b2>

	End -- </a>

	Return @myError

GO

GRANT EXECUTE ON [dbo].[ParseManagerNameList] TO [Mgr_Config_Admin] AS [dbo]
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

/****** Object:  StoredProcedure [dbo].[RebuildFragmentedIndices]    Script Date: 1/15/2020 8:33:53 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE PROCEDURE [dbo].[RebuildFragmentedIndices]
/****************************************************
**
**	Desc:
**		Reindexes fragmented indices in the database
**
**	Return values: 0:  success, otherwise, error code
**
**	Parameters:
**
**	Auth:	mem
**	Date:	11/12/2007
**			10/15/2012 mem - Added spaces prior to printing debug messages
**			10/18/2012 mem - Added parameter @VerifyUpdateEnabled
**
*****************************************************/
(
	@MaxFragmentation int = 15,
	@TrivialPageCount int = 12,
	@VerifyUpdateEnabled tinyint = 1,		-- When non-zero, then calls VerifyUpdateEnabled to assure that database updating is enabled
	@infoOnly tinyint = 1,
	@message varchar(1024) = '' output
)
As
	set nocount on

	Declare @myError int
	Declare @myRowcount int
	set @myRowcount = 0
	set @myError = 0

	Declare @objectid int
	Declare @indexid int
	Declare @partitioncount bigint
	Declare @schemaname nvarchar(130)
	Declare @objectname nvarchar(130)
	Declare @indexname nvarchar(130)
	Declare @partitionnum bigint
	Declare @partitions bigint
	Declare @frag float
	Declare @command nvarchar(4000)
	Declare @HasBlobColumn int

	Declare @StartTime datetime
	Declare @continue int
	Declare @UniqueID int

	Declare @IndexCountProcessed int
	Set @IndexCountProcessed = 0

	Declare @UpdateEnabled tinyint

	---------------------------------------
	-- Validate the inputs
	---------------------------------------
	--
	Set @MaxFragmentation = IsNull(@MaxFragmentation, 15)
	Set @TrivialPageCount = IsNull(@TrivialPageCount, 12)
	Set @VerifyUpdateEnabled = IsNull(@VerifyUpdateEnabled, 1)
	Set @infoOnly = IsNull(@infoOnly, 1)
	Set @message = ''

	---------------------------------------
	-- Create a table to track the indices to process
	---------------------------------------
	--
	CREATE TABLE dbo.#TmpIndicesToProcess(
		[UniqueID] int Identity(1,1) NOT NULL,
		[objectid] [int] NULL,
		[indexid] [int] NULL,
		[partitionnum] [int] NULL,
		[frag] [float] NULL
	) ON [PRIMARY]

	---------------------------------------
	-- Conditionally select tables and indexes from the sys.dm_db_index_physical_stats function
	-- and convert object and index IDs to names.
	---------------------------------------
	--
	INSERT INTO #TmpIndicesToProcess (objectid, indexid, partitionnum, frag)
	SELECT object_id,
	       index_id,
	       partition_number,
	       avg_fragmentation_in_percent
	FROM sys.dm_db_index_physical_stats ( DB_ID(), NULL, NULL, NULL, 'LIMITED' )
	WHERE avg_fragmentation_in_percent > @MaxFragmentation
	  AND index_id > 0 -- cannot defrag a heap
	  AND page_count > @TrivialPageCount -- ignore trivial sized indexes
 	ORDER BY avg_fragmentation_in_percent Desc
	--
	SELECT @myError = @@error, @myRowCount = @@rowcount

	If @myRowCount = 0
	Begin
		Set @Message = 'All database indices have fragmentation levels below ' + convert(varchar(12), @MaxFragmentation) + '%'
		If @infoOnly <> 0
			Print '  ' + @message
		Goto Done
	End

	---------------------------------------
	-- Loop through #TmpIndicesToProcess and process the indices
	---------------------------------------
	--
	Set @StartTime = GetDate()
	Set @continue = 1
	Set @UniqueID = -1

	While @continue = 1
	Begin -- <a>
		SELECT TOP 1 @UniqueID = UniqueiD,
		             @objectid = objectid,
		             @indexid = indexid,
		             @partitionnum = partitionnum,
		             @frag = frag
		FROM #TmpIndicesToProcess
		WHERE UniqueID > @UniqueID
		ORDER BY UniqueID
		--
		SELECT @myError = @@error, @myRowCount = @@rowcount

		If @myRowCount = 0
			Set @continue = 0
		Else
		Begin -- <b>

	        Set @HasBlobColumn = 0 -- reinitialize
			SELECT @objectname = QUOTENAME(o.name),
			       @schemaname = QUOTENAME(s.name)
			FROM sys.objects AS o
			     JOIN sys.schemas AS s
			       ON s.schema_id = o.schema_id
			WHERE o.object_id = @objectid

			SELECT @indexname = QUOTENAME(name)
			FROM sys.indexes
			WHERE object_id = @objectid AND
			      index_id = @indexid


			SELECT @partitioncount = count(*)
			FROM sys.partitions
			WHERE object_id = @objectid AND
			      index_id = @indexid

	        -- Check for BLOB columns
	        If @indexid = 1 -- only check here for clustered indexes ANY blob column on the table counts
			Begin
	            SELECT @HasBlobColumn = CASE
	                                        WHEN max(so.object_ID) IS NULL THEN 0
	                                        ELSE 1
	                                    END
	            FROM sys.objects SO
	                 INNER JOIN sys.columns SC
	                   ON SO.Object_id = SC.object_id
	                 INNER JOIN sys.types ST
	                   ON SC.system_type_id = ST.system_type_id
	               AND
	            ST.name IN ('text', 'ntext', 'image', 'varchar(max)', 'nvarchar(max)', 'varbinary(max)', 'xml')
	            WHERE SO.Object_ID = @objectID
			End
	        Else -- nonclustered. Only need to check if indexed column is a BLOB
			Begin
	            SELECT @HasBlobColumn = CASE
	                                        WHEN max(so.object_ID) IS NULL THEN 0
	                                        ELSE 1
	                                    END
	            FROM sys.objects SO
	                 INNER JOIN sys.index_columns SIC
	                   ON SO.Object_ID = SIC.object_id
	                 INNER JOIN sys.Indexes SI
	                   ON SO.Object_ID = SI.Object_ID AND
	                      SIC.index_id = SI.index_id
	                 INNER JOIN sys.columns SC
	                   ON SO.Object_id = SC.object_id AND
	                      SIC.Column_id = SC.column_id
	                 INNER JOIN sys.types ST
	                   ON SC.system_type_id = ST.system_type_id
	                      AND ST.name IN ('text', 'ntext', 'image', 'varchar(max)', 'nvarchar(max)', 'varbinary(max)', 'xml')
	            WHERE SO.Object_ID = @objectID
			End

	        SET @command = N'ALTER INDEX ' + @indexname + N' ON ' + @schemaname + N'.' + @objectname + N' REBUILD'

	        if @HasBlobColumn = 1
	            Set @command = @command + N' WITH( SORT_IN_TEMPDB = ON) '
	        else
	            Set @command = @command + N' WITH( ONLINE = OFF, SORT_IN_TEMPDB = ON) '

			IF @partitioncount > 1
				SET @command = @command + N' PARTITION=' + CAST(@partitionnum AS nvarchar(10))

			Set @message = 'Fragmentation = ' + Convert(varchar(12), convert(decimal(9,1), @frag)) + '%; '
			Set @message = @message + 'Executing: ' + @command + ' Has Blob = ' + convert(nvarchar(2),@HasBlobColumn)

			if @InfoOnly <> 0
				Print '  ' + @message
			Else
			Begin
				EXEC (@command)

				Set @message = 'Reindexed ' + @indexname + ' due to Fragmentation = ' + Convert(varchar(12), Convert(decimal(9,1), @frag)) + '%; '
				Exec PostLogEntry 'Normal', @message, 'RebuildFragmentedIndices'

				Set @IndexCountProcessed = @IndexCountProcessed + 1

				If @VerifyUpdateEnabled <> 0
				Begin
					-- Validate that updating is enabled, abort if not enabled
					If Exists (select * from sys.objects where name = 'VerifyUpdateEnabled')
					Begin
						exec VerifyUpdateEnabled @CallingFunctionDescription = 'RebuildFragmentedIndices', @AllowPausing = 1, @UpdateEnabled = @UpdateEnabled output, @message = @message output
						If @UpdateEnabled = 0
							Goto Done
					End
				End
			End

		End -- </b>
	End -- </a>

	If @IndexCountProcessed > 0
	Begin
		---------------------------------------
		-- Log the reindex
		---------------------------------------

		Set @message = 'Reindexed ' + Convert(varchar(12), @IndexCountProcessed) + ' indices in ' + convert(varchar(12), Convert(decimal(9,1), DateDiff(second, @StartTime, GetDate()) / 60.0)) + ' minutes'
		Exec PostLogEntry 'Normal', @message, 'RebuildFragmentedIndices'
	End

Done:

	-- Drop the temporary table.
	DROP TABLE #TmpIndicesToProcess

	Return @myError

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

/****** Object:  StoredProcedure [dbo].[SetManagerParams]    Script Date: 1/15/2020 8:33:53 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[SetManagerParams]
/****************************************************
**
**	Desc:
**    Sets the values of parameters given in XML format
**    for the given manager
**
**	Return values: 0: success, otherwise, error code
**
**	Parameters:
**
**		Auth: grk
**		Date: 04/27/2007
**		      05/02/2007 grk - added translation table
**		      05/02/2007 grk - fixed too-narrow variables in OPENXML
**		      05/02/2007 grk - fixed sloppy final update statement
**		      05/02/2007 dac - added translation for bionet password
**
*****************************************************/
(
	@managerName varchar(128),
	@xmlDoc nvarchar(3500),
	@mode varchar(24) = 'InfoOnly',
    @message varchar(512) output
)
As
	set nocount on

	declare @myError int
	set @myError = 0

	declare @myRowCount int
	set @myRowCount = 0

	set @message = ''


	---------------------------------------------------
	-- Resolve manager name to manager ID
	---------------------------------------------------
	declare @mgrID int
	set @mgrID = 0
	--
	SELECT @mgrID = M_ID
	FROM T_Mgrs
	WHERE (M_Name = @managerName)
    --
    SELECT @myError = @@error, @myRowCount = @@rowcount
    --
    if @myError <> 0
    begin
      set @message = 'Error trying to resolve manager name to ID'
      goto DONE
    end
    --
    if @mgrID = 0
    begin
      set @message = 'Could not find manager ID'
		set @myError = 51000
      goto DONE
    end

	---------------------------------------------------
	--  Create temporary table to hold list of parameters
	---------------------------------------------------

 	CREATE TABLE #TDS (
		paramID int NULL,
		paramName varchar(128),
		paramValue varchar(255)
	)
	--
	SELECT @myError = @@error, @myRowCount = @@rowcount
	--
	if @myError <> 0
	begin
		set @message = 'Failed to create temporary parameter table'
		goto DONE
	end
	---------------------------------------------------
	-- Parse the XML input
	---------------------------------------------------
	DECLARE @hDoc int
	EXEC sp_xml_preparedocument @hDoc OUTPUT, @xmlDoc

 	---------------------------------------------------
	-- Populate table from XML parameter description
	-- Using OPENXML in a SELECT statement to read data from XML file
	---------------------------------------------------

	INSERT INTO #TDS
	(paramName, paramValue)
	SELECT * FROM OPENXML(@hDoc, N'//section/item')  with ([key] varchar(128), value varchar(128))
	--
	SELECT @myError = @@error, @myRowCount = @@rowcount
	--
	if @myError <> 0
	begin
		set @message = 'Error populating temporary parameter table'
		goto DONE
	end

	-- Remove the internal representation of the XML document.
	EXEC sp_xml_removedocument @hDoc


	---------------------------------------------------
	-- FUTURE: translate parameter names that have changed
	---------------------------------------------------

	UPDATE #TDS SET paramName = 'maxrepetitions' WHERE paramName = 'maxjobcount'
	UPDATE #TDS SET paramName = 'bionetpwd' WHERE paramName = 'bionetmisc'
--	UPDATE #TDS SET paramName = '' WHERE paramName = ''

	---------------------------------------------------
	-- Get parameter IDs for parameters
	---------------------------------------------------

	UPDATE T
	SET T.paramID = T_ParamType.ParamID
	FROM #TDS T INNER JOIN
	T_ParamType ON T_ParamType.ParamName = T.paramName
	--
	SELECT @myError = @@error, @myRowCount = @@rowcount
	--
	if @myError <> 0
	begin
		set @message = 'Error getting parameter IDs'
		goto DONE
	end

	---------------------------------------------------
	-- Trap "information only mode" here
	---------------------------------------------------
	if @mode = 'InfoOnly'
	begin
		select * from #TDS
		goto DONE
	end

	---------------------------------------------------
	-- FUTURE: check for parameters that didn't get IDs
	---------------------------------------------------
	-- for now, just remove them from table
	--
	DELETE FROM #TDS
	WHERE #TDS.paramID is NULL
	--
	SELECT @myError = @@error, @myRowCount = @@rowcount
	--
	if @myError <> 0
	begin
		set @message = 'Cleaning up parameter IDs'
		goto DONE
	end

	---------------------------------------------------
	-- Insert paramters that aren't already in table
	---------------------------------------------------

	INSERT INTO T_ParamValue
		(MgrID, TypeID, Value)
	SELECT @mgrID, #TDS.paramID, #TDS.paramValue
	FROM #TDS
	WHERE #TDS.paramID NOT IN (SELECT TypeID FROM T_ParamValue WHERE MgrID = @mgrID)

	--
	SELECT @myError = @@error, @myRowCount = @@rowcount
	--
	if @myError <> 0
	begin
		set @message = 'Error inserting new parameteres'
		goto DONE
	end


	---------------------------------------------------
	-- Update parameters
	---------------------------------------------------

	UPDATE M
	SET M.Value = T.paramValue
	FROM T_ParamValue M INNER JOIN
	#TDS T ON T.paramID = M.TypeID AND M.MgrID = @mgrID
	--
	SELECT @myError = @@error, @myRowCount = @@rowcount
	--
	if @myError <> 0
	begin
		set @message = 'Error updating parameteres'
		goto DONE
	end

	---------------------------------------------------
	--
	---------------------------------------------------
	---------------------------------------------------
	--
	---------------------------------------------------
DONE:
	return @myError
GO

GRANT EXECUTE ON [dbo].[SetManagerParams] TO [Mgr_Config_Admin] AS [dbo]
GO

/****** Object:  StoredProcedure [dbo].[SetManagerUpdateRequired]    Script Date: 1/15/2020 8:33:53 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[SetManagerUpdateRequired]
/****************************************************
**
**	Desc:
**		Sets ManagerUpdateRequired to true for the given list of managers
**		If @ManagerList is blank, then sets it to true for all "Analysis Tool Manager" managers
**
**	Auth:	mem
**	Date:	01/24/2009 mem - Initial version
**			04/17/2014 mem - Expanded @ManagerList to varchar(max) and added parameter @showTable
**
*****************************************************/
(
	@ManagerList varchar(max) = '',
	@showTable tinyint = 0,
	@message varchar(512) = '' output
)
AS
	set nocount on

	declare @myError int
	declare @myRowCount int
	set @myError = 0
	set @myRowCount = 0

	Set @showTable = IsNull(@showTable, 0)
	Set @message = ''


	Declare @mgrID int
	Declare @ParamID int

	CREATE TABLE #TmpManagerList (
		ManagerName varchar(128) NOT NULL,
		MgrID int NULL
	)

	---------------------------------------------------
	-- Confirm that the manager name is valid
	---------------------------------------------------

	Set @ManagerList = IsNull(@ManagerList, '')

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
	-- Lookup the ParamID value for 'ManagerUpdateRequired'
	---------------------------------------------------

	Set @ParamID = 0
	--
	SELECT @ParamID = ParamID
	FROM T_ParamType
	WHERE (ParamName = 'ManagerUpdateRequired')

	---------------------------------------------------
	-- Make sure each manager in #TmpManagerList has an entry
	--  in T_ParamValue for 'ManagerUpdateRequired'
	---------------------------------------------------

	INSERT INTO T_ParamValue (MgrID, TypeID, Value)
	SELECT A.MgrID, @ParamID, 'False'
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
		Set @message = 'Added entry for "ManagerUpdateRequired" to T_ParamValue for ' + Convert(varchar(12), @myRowCount) + ' manager'
		If @myRowCount > 1
			Set @message = @message + 's'

		Print @message
	End

	---------------------------------------------------
	-- Update the 'ManagerUpdateRequired' entry for each manager in #TmpManagerList
	---------------------------------------------------

	UPDATE T_ParamValue
	SET VALUE = 'True'
	FROM T_ParamValue
	     INNER JOIN #TmpManagerList
	       ON T_ParamValue.MgrID = #TmpManagerList.MgrID
	WHERE (T_ParamValue.TypeID = @ParamID) AND
	      T_ParamValue.Value <> 'True'
	--
	SELECT @myError = @@error, @myRowCount = @@rowcount

	If @myRowCount <> 0
	Begin
		Set @message = 'Set "ManagerUpdateRequired" to True for ' + Convert(varchar(12), @myRowCount) + ' manager'
		If @myRowCount > 1
			Set @message = @message + 's'

		Print @message
	End

	If @showTable <> 0
	Begin
		SELECT U.*
		FROM V_AnalysisMgrParams_UpdateRequired U
		     INNER JOIN #TmpManagerList L
		       ON U.MgrID = L.MgrId
		ORDER BY Manager DESC
	End

	---------------------------------------------------
	-- Exit the procedure
	---------------------------------------------------
Done:
	return @myError

GO

GRANT EXECUTE ON [dbo].[SetManagerUpdateRequired] TO [Mgr_Config_Admin] AS [dbo]
GO

GRANT EXECUTE ON [dbo].[SetManagerUpdateRequired] TO [MTUser] AS [dbo]
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

/****** Object:  StoredProcedure [dbo].[UpdateManagerControlParams]    Script Date: 1/15/2020 8:33:53 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE Procedure [dbo].[UpdateManagerControlParams]
/****************************************************
**
**	Desc:
**	Changes manager params for set of given managers
**
**
**	Return values: 0: success, otherwise, error code
**
**	Parameters:
**
**	Auth:	jds
**	Date:	06/20/2007
**			07/27/2007 jds - Added support for parameters that do not exist for a manager
**			07/31/2007 grk - Factored out param change logic into 'SetParamForManagerList'
**			03/28/2008 jds - Renamed Paramx variables to ParamValx for clarity
**			04/16/2009 mem - Added optional parameter @callingUser; if provided, then SetParamForManagerList will populate field Entered_By with this name
**
*****************************************************/
(
	@mode varchar(32),					-- Unused in this procedure
	@paramVal1 varchar(512),			-- New value to assign for parameter #1
	@param1Type varchar(50),			-- Parameter name #1
	@paramVal2 varchar(512),			-- New value to assign for parameter #2
	@param2Type varchar(50),			-- Parameter name #2
	@paramVal3 varchar(512),			-- etc.
	@param3Type varchar(50),
	@paramVal4 varchar(512),
	@param4Type varchar(50),
	@paramVal5 varchar(512),
	@param5Type varchar(50),
	@managerIDList varchar(2048),		-- manager ID values (numbers, not manager names)
	@callingUser varchar(128) = ''
)
As
	set nocount on

	declare @myError int
	declare @myRowCount int
	set @myError = 0
	set @myRowCount = 0

	declare @message varchar(512)
	set @message = ''

	---------------------------------------------------
	-- Get list of managers to be updated
	---------------------------------------------------
	-- temp table to hold list
	--
	Create table #ManagerIDList(
		ID int
	)
	--
	SELECT @myError = @@error, @myRowCount = @@rowcount
	--
	if @myError <> 0
	begin
		RAISERROR ('Error trying to create temp table', 10, 1)
		return 51090
	end

	--Insert IDs into temp table
	--
	INSERT INTO #ManagerIDList
	SELECT Item FROM MakeTableFromList(@managerIDList)
	--
	SELECT @myError = @@error, @myRowCount = @@rowcount
	--
	if @myError <> 0
	begin
		RAISERROR ('Error trying to populate temp table', 10, 1)
		return 51091
	end

	-- remove managers that are not enabled for update
	--
	DELETE FROM #ManagerIDList
	WHERE ID IN
	(
		SELECT M_ID
		FROM T_Mgrs
		WHERE (M_ControlFromWebsite = 0)
	)
	--
	SELECT @myError = @@error, @myRowCount = @@rowcount
	--
	if @myError <> 0
	begin
		RAISERROR ('Error trying to remove disabled managers from temp table', 10, 1)
		return 51092
	end

	---------------------------------------------------
	-- Call SetParamForManagerList to update the managers
	---------------------------------------------------

	if IsNull(@param1Type, '') <> ''
	begin
		exec @myError = SetParamForManagerList @paramVal1, @param1Type, @message output, @callingUser
		if @myError <> 0
		begin
			RAISERROR (@message, 10, 1)
			return @myError
		end
	end
	--
	if IsNull(@param2Type, '') <> ''
	begin
		exec @myError = SetParamForManagerList @paramVal2, @param2Type, @message output, @callingUser
		if @myError <> 0
		begin
			RAISERROR (@message, 10, 1)
			return @myError
		end
	end
	--
	if IsNull(@param3Type, '') <> ''
	begin
		exec @myError = SetParamForManagerList @paramVal3, @param3Type, @message output, @callingUser
		if @myError <> 0
		begin
			RAISERROR (@message, 10, 1)
			return @myError
		end
	end
	--
	if IsNull(@param4Type, '') <> ''
	begin
		exec @myError = SetParamForManagerList @paramVal4, @param4Type, @message output, @callingUser
		if @myError <> 0
		begin
			RAISERROR (@message, 10, 1)
			return @myError
		end
	end
	--
	if IsNull(@param5Type, '') <> ''
	begin
		exec @myError = SetParamForManagerList @paramVal5, @param5Type, @message output, @callingUser
		if @myError <> 0
		begin
			RAISERROR (@message, 10, 1)
			return @myError
		end
	end

	return @myError
GO

GRANT EXECUTE ON [dbo].[UpdateManagerControlParams] TO [Mgr_Config_Admin] AS [dbo]
GO

/****** Object:  StoredProcedure [dbo].[UpdateSingleMgrControlParam]    Script Date: 1/15/2020 8:33:53 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE Procedure [dbo].[UpdateSingleMgrControlParam]
/****************************************************
**
**	Desc:
**	Changes single manager params for set of given managers
**
**
**	Return values: 0: success, otherwise, error code
**
**	Parameters:
**
**	Auth:	jds
**	Date:	06/20/2007
**			07/31/2007 grk - changed for 'controlfromwebsite' no longer a parameter
**			04/16/2009 mem - Added optional parameter @callingUser; if provided, then UpdateSingleMgrParamWork will populate field Entered_By with this name
**			04/08/2011 mem - Will now add parameter @paramValue to managers that don't yet have the parameter defined
**			04/21/2011 mem - Expanded @managerIDList to varchar(8000)
**			05/11/2011 mem - Fixed bug reporting error resolving @paramValue to @ParamTypeID
**			04/29/2015 mem - Now parsing @managerIDList using udfParseDelimitedIntegerList
**						   - Added parameter @infoOnly
**						   - Renamed the first parameter from @paramValue to @paramName
**
*****************************************************/
(
	@paramName varchar(32),			-- The parameter name
	@newValue varchar(128),				-- The new value to assign for this parameter
	@managerIDList varchar(8000),		-- manager ID values (numbers, not manager names)
	@callingUser varchar(128) = '',
	@infoOnly tinyint = 0
)
As

	set nocount on

	declare @myError int
	declare @myRowCount int
	set @myError = 0
	set @myRowCount = 0

	Declare @ParamTypeID int
	Declare @message varchar(512) = ''

	---------------------------------------------------
	-- Validate the inputs
	---------------------------------------------------

	-- Assure that @newValue is not null
	Set @newValue = IsNull(@newValue, '')
	Set @infoOnly = IsNull(@infoOnly, 0)

	---------------------------------------------------
	-- Create a temporary table that will hold the Entry_ID
	-- values that need to be updated in T_ParamValue
	---------------------------------------------------
	CREATE TABLE #TmpParamValueEntriesToUpdate (
		EntryID int NOT NULL
	)

	CREATE UNIQUE CLUSTERED INDEX #IX_TmpParamValueEntriesToUpdate ON #TmpParamValueEntriesToUpdate (EntryID)

	CREATE TABLE #TmpMgrIDs (
		MgrID varchar(12) NOT NULL
	)

	---------------------------------------------------
	-- Resolve @paramName to @ParamTypeID
	---------------------------------------------------

	Set @ParamTypeID = -1

	SELECT @ParamTypeID = ParamID
	FROM T_ParamType
	WHERE ParamName = @paramName
	--
	SELECT @myError = @@error, @myRowCount = @@rowcount

	If @myRowCount = 0
	Begin
		Set @message = 'Error: Parameter ''' + @paramName + ''' not found in T_ParamType'
		RAISERROR (@message, 10, 1)
		Set @message = ''
		return 51309
	End

	---------------------------------------------------
	-- Parse the manager ID list
	---------------------------------------------------
	--
	INSERT INTO #TmpMgrIDs (MgrID)
	SELECT Cast(Value as varchar(12))
	FROM dbo.udfParseDelimitedIntegerList ( @managerIDList, ',' )

	If @infoOnly <> 0
	Begin


		SELECT PV.Entry_ID,
		       M.M_ID,
		       M.M_Name,
		       PV.ParamName,
		       PV.TypeID,
		       PV.[Value],
		       @newValue AS NewValue,
		       Case When IsNull(PV.[Value], '') <> @newValue Then 'Changed' Else 'Unchanged' End As Status
		FROM T_Mgrs M
		     INNER JOIN #TmpMgrIDs
		       ON M.M_ID = #TmpMgrIDs.MgrID
		     INNER JOIN V_ParamValue PV
		       ON PV.MgrID = M.M_ID AND
		          PV.TypeID = @ParamTypeID
		WHERE M_ControlFromWebsite > 0
		UNION
		SELECT NULL AS Entry_ID,
		       M.M_ID,
		       M.M_Name,
		       @paramName,
		       @ParamTypeID,
		       NULL AS [Value],
		       @newValue AS NewValue,
		       'New'
		FROM T_Mgrs M
		     INNER JOIN #TmpMgrIDs
		       ON M.M_ID = #TmpMgrIDs.MgrID
		     LEFT OUTER JOIN T_ParamValue PV
		       ON PV.MgrID = M.M_ID AND
		          PV.TypeID = @ParamTypeID
		WHERE PV.TypeID IS NULL

	End
	Else
	Begin

		---------------------------------------------------
		-- Add new entries for Managers in @managerIDList that
		-- don't yet have an entry in T_ParamValue for parameter @paramName
		--
		-- Adding value '##_DummyParamValue_##' so that
		--  we'll force a call to UpdateSingleMgrParamWork
		---------------------------------------------------

		INSERT INTO T_ParamValue( TypeID,
		                          [Value],
		                          MgrID )
		SELECT @ParamTypeID,
		       '##_DummyParamValue_##',
		       #TmpMgrIDs.MgrID
		FROM T_Mgrs M
		     INNER JOIN #TmpMgrIDs
		       ON M.M_ID = #TmpMgrIDs.MgrID
		     LEFT OUTER JOIN T_ParamValue PV
		       ON PV.MgrID = M.M_ID AND
		          PV.TypeID = @ParamTypeID
		WHERE PV.TypeID IS NULL
		--
		SELECT @myError = @@error, @myRowCount = @@rowcount

		---------------------------------------------------
		-- Find the entries for the Managers in @managerIDList
		-- Populate #TmpParamValueEntriesToUpdate with the entries that need to be updated
		---------------------------------------------------
		--
		INSERT INTO #TmpParamValueEntriesToUpdate( EntryID )
		SELECT PV.Entry_ID
		FROM T_ParamValue PV
		     INNER JOIN T_Mgrs M
		       ON PV.MgrID = M.M_ID
		     INNER JOIN #TmpMgrIDs
		       ON M.M_ID = #TmpMgrIDs.MgrID
		WHERE M_ControlFromWebsite > 0 AND
		      PV.TypeID = @ParamTypeID AND
		      IsNull(PV.[Value], '') <> @newValue
		--
		SELECT @myError = @@error, @myRowCount = @@rowcount
		--
		if @myError <> 0
		begin
			RAISERROR ('Error finding Manager params to update', 10, 1)
			return 51309
		end

		---------------------------------------------------
		-- Call UpdateSingleMgrParamWork to perform the update, then call
		-- AlterEnteredByUserMultiID and AlterEventLogEntryUserMultiID for @callingUser
		---------------------------------------------------
		--
		exec @myError = UpdateSingleMgrParamWork @paramName, @newValue, @callingUser

	End

	return @myError
GO

GRANT EXECUTE ON [dbo].[UpdateSingleMgrControlParam] TO [Mgr_Config_Admin] AS [dbo]
GO

/****** Object:  StoredProcedure [dbo].[UpdateSingleMgrParamWork]    Script Date: 1/15/2020 8:33:53 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE Procedure [dbo].[UpdateSingleMgrParamWork]
/****************************************************
**
**	Desc:
**	Changes single manager param for the EntryID values
**  defined in table #TmpParamValueEntriesToUpdate (created by the calling procedure)
**
**	Example table creation code:
**	  CREATE TABLE #TmpParamValueEntriesToUpdate (EntryID int NOT NULL)
**
**	Return values: 0: success, otherwise, error code
**
**	Parameters:
**
**	Auth:	mem
**	Date:	04/16/2009
**
*****************************************************/
(
	@paramName varchar(32),				-- The parameter name
	@newValue varchar(128),				-- The new value to assign for this parameter
	@callingUser varchar(128) = ''
)
As

	declare @myError int
	declare @myRowCount int
	set @myError = 0
	set @myRowCount = 0

	Declare @ParamID int
	Declare @TargetState int

	Declare @message varchar(512)
	Set @message = ''

	-- Validate that @paramName is not blank
	If IsNull(@paramName, '') = ''
	Begin
		Set @message = 'Parameter Name is empty or null'
		RAISERROR (@message, 10, 1)
		return 51315
	End

	-- Assure that @newValue is not null
	Set @newValue = IsNull(@newValue, '')


	-- Lookup the ParamID for param @paramName
	Set @ParamID = 0
	SELECT @ParamID = ParamID
	FROM T_ParamType
	WHERE (ParamName = @paramName)
	--
	SELECT @myError = @@error, @myRowCount = @@rowcount

	If @myRowCount = 0
	Begin
		Set @message = 'Unknown Parameter Name: ' + @paramName
		RAISERROR (@message, 10, 1)
		return 51316
	End

	---------------------------------------------------
	-- Update the values defined in #TmpParamValueEntriesToUpdate
	---------------------------------------------------
	--
	UPDATE T_ParamValue
	SET [Value] = @newValue
	WHERE Entry_ID IN (SELECT EntryID FROM #TmpParamValueEntriesToUpdate) AND
	      IsNull([Value], '') <> @newValue
	--
	SELECT @myError = @@error, @myRowCount = @@rowcount
	--
	if @myError <> 0
	begin
		Set @message = 'Error trying to update Manager params'
		RAISERROR (@message, 10, 1)
		return 51310
	end


 	If @myRowCount > 0 And Len(@callingUser) > 0
	Begin
		-- @callingUser is defined
		-- Items need to be updated in T_ParamValue and possibly in T_Event_Log

		---------------------------------------------------
		-- Create a temporary table that will hold the Entry_ID
		-- values that need to be updated in T_ParamValue
		---------------------------------------------------
		CREATE TABLE #TmpIDUpdateList (
			TargetID int NOT NULL
		)

		CREATE UNIQUE CLUSTERED INDEX #IX_TmpIDUpdateList ON #TmpIDUpdateList (TargetID)

		-- Populate #TmpIDUpdateList with Entry_ID values for T_ParamValue, then call AlterEnteredByUserMultiID
		--
		INSERT INTO #TmpIDUpdateList (TargetID)
		SELECT EntryID
		FROM #TmpParamValueEntriesToUpdate

		Exec AlterEnteredByUserMultiID 'T_ParamValue', 'Entry_ID', @CallingUser, @EntryDateColumnName = 'Last_Affected'


		If @paramName = 'mgractive' or @ParamID = 17
		Begin
			-- Triggers trig_i_T_ParamValue and trig_u_T_ParamValue make an entry in
			--  T_Event_Log whenever mgractive (param TypeID = 17) is changed

			-- Call AlterEventLogEntryUserMultiID
			-- to alter the Entered_By field in T_Event_Log

			If @newValue = 'True'
				Set @TargetState = 1
			else
				Set @TargetState = 0

			-- Populate #TmpIDUpdateList with Manager ID values, then call AlterEventLogEntryUserMultiID
			Truncate Table #TmpIDUpdateList

			INSERT INTO #TmpIDUpdateList (TargetID)
			SELECT MgrID
			FROM T_ParamValue
			WHERE Entry_ID IN (SELECT EntryID FROM #TmpParamValueEntriesToUpdate)

			Exec AlterEventLogEntryUserMultiID 1, @TargetState, @callingUser
		End

	End

	Return @myError
GO

/****** Object:  StoredProcedure [dbo].[UpdateSingleMgrTypeControlParam]    Script Date: 1/15/2020 8:33:53 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE Procedure [dbo].[UpdateSingleMgrTypeControlParam]
/****************************************************
**
**	Desc:
**	Changes single manager params for set of given manager Types
**
**
**	Return values: 0: success, otherwise, error code
**
**	Parameters:
**
**	Auth:	jds
**	Date:	07/17/2007
**			07/31/2007 grk - changed for 'controlfromwebsite' no longer a parameter
**			03/30/2009 mem - Added optional parameter @callingUser; if provided, then will call AlterEnteredByUserMultiID and possibly AlterEventLogEntryUserMultiID
**			04/16/2009 mem - Now calling UpdateSingleMgrParamWork to perform the updates
**
*****************************************************/
(
	@paramValue varchar(32),			-- The parameter name
	@newValue varchar(128),				-- The new value to assign for this parameter
	@managerTypeIDList varchar(2048),
	@callingUser varchar(128) = ''
)
As

	declare @myError int
	declare @myRowCount int
	set @myError = 0
	set @myRowCount = 0

	---------------------------------------------------
	-- Create a temporary table that will hold the Entry_ID
	-- values that need to be updated in T_ParamValue
	---------------------------------------------------
	CREATE TABLE #TmpParamValueEntriesToUpdate (
		EntryID int NOT NULL
	)

	CREATE UNIQUE CLUSTERED INDEX #IX_TmpParamValueEntriesToUpdate ON #TmpParamValueEntriesToUpdate (EntryID)


	---------------------------------------------------
	-- Find the @paramValue entries for the Manager Types in @managerTypeIDList
	---------------------------------------------------
	--
	INSERT INTO #TmpParamValueEntriesToUpdate (EntryID)
	SELECT T_ParamValue.Entry_ID
	FROM T_ParamValue
	     INNER JOIN T_ParamType
	       ON dbo.T_ParamValue.TypeID = dbo.T_ParamType.ParamID
	     INNER JOIN T_Mgrs
	       ON MgrID = M_ID
	WHERE ParamName = @paramValue AND
	      M_TypeID IN ( SELECT Item
	                    FROM MakeTableFromList ( @managerTypeIDList )
	                  ) AND
	      MgrID IN ( SELECT M_ID
	                 FROM T_Mgrs
	                 WHERE M_ControlFromWebsite > 0
	                 )
	--
	SELECT @myError = @@error, @myRowCount = @@rowcount
	--
	if @myError <> 0
	begin
		RAISERROR ('Error finding Manager params to update', 10, 1)
		return 51309
	end

	---------------------------------------------------
	-- Call UpdateSingleMgrParamWork to perform the update, then call
	-- AlterEnteredByUserMultiID and AlterEventLogEntryUserMultiID for @callingUser
	---------------------------------------------------
	--
	exec @myError = UpdateSingleMgrParamWork @paramValue, @newValue, @callingUser

	return @myError
GO

GRANT EXECUTE ON [dbo].[UpdateSingleMgrTypeControlParam] TO [Mgr_Config_Admin] AS [dbo]
GO

/****** Object:  StoredProcedure [dbo].[UpdateUserPermissions]    Script Date: 1/15/2020 8:33:53 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE PROCEDURE [dbo].[UpdateUserPermissions]
/****************************************************
**
**	Desc: Updates user permissions in the current DB
**
**	Return values: 0: success, otherwise, error code
**
**	Parameters:
**
**	Auth:	mem
**	Date:	07/31/2012 mem - Initial Version
**			08/08/2012 mem - Added permissions for DMSWebUser
**
*****************************************************/
AS
	Set NoCount On


	if exists (select * from sys.schemas where name = 'DMSReader')
		drop schema DMSReader
	if exists (select * from sys.sysusers where name = 'DMSReader')
		drop user DMSReader
	create user DMSReader for login DMSReader
	exec sp_addrolemember 'db_datareader', 'DMSReader'


	if exists (select * from sys.schemas where name = 'DMSWebUser')
		drop schema DMSWebUser
	if exists (select * from sys.sysusers where name = 'DMSWebUser')
		drop user DMSWebUser
	create user DMSWebUser for login DMSWebUser
	exec sp_addrolemember 'db_datareader', 'DMSWebUser'
	exec sp_addrolemember 'DMS_SP_User', 'DMSWebUser'
	exec sp_addrolemember 'Mgr_Config_Admin', 'DMSWebUser'


	if exists (select * from sys.schemas where name = 'MTUser')
		drop schema MTUser
	if exists (select * from sys.sysusers where name = 'MTUser')
		drop user MTUser
	create user MTUser for login MTUser
	exec sp_addrolemember 'db_datareader', 'MTUser'
	exec sp_addrolemember 'DMS_SP_User', 'MTUser'

	grant showplan to DMSReader
	grant showplan to DMSWebUser
	grant showplan to MTUser


	GRANT EXECUTE ON [dbo].[AckManagerUpdateRequired] TO [Mgr_Config_Admin] AS [dbo]
	GRANT EXECUTE ON [dbo].[AddManagers] TO [Mgr_Config_Admin] AS [dbo]
	GRANT EXECUTE ON [dbo].[AddUpdateManager] TO [Mgr_Config_Admin] AS [dbo]
	GRANT EXECUTE ON [dbo].[AddUpdateManagerParamDefaults] TO [Mgr_Config_Admin] AS [dbo]
	GRANT EXECUTE ON [dbo].[AddUpdateManagerParams] TO [Mgr_Config_Admin] AS [dbo]
	GRANT EXECUTE ON [dbo].[AddUpdateManagerState] TO [Mgr_Config_Admin] AS [dbo]
	GRANT EXECUTE ON [dbo].[AddUpdateManagerType] TO [Mgr_Config_Admin] AS [dbo]
	GRANT EXECUTE ON [dbo].[AddUpdateMgrTypeControlParams] TO [Mgr_Config_Admin] AS [dbo]
	GRANT EXECUTE ON [dbo].[AddUpdateParamByManagerType] TO [Mgr_Config_Admin] AS [dbo]
	GRANT EXECUTE ON [dbo].[AddUpdateParamType] TO [Mgr_Config_Admin] AS [dbo]
	GRANT EXECUTE ON [dbo].[CheckAccessPermission] TO [Mgr_Config_Admin] AS [dbo]
	GRANT EXECUTE ON [dbo].[CheckForParamChanged]  TO [Mgr_Config_Admin] AS [dbo]
	GRANT EXECUTE ON [dbo].[DisableAnalysisManagers] TO [Mgr_Config_Admin] AS [dbo]
	GRANT EXECUTE ON [dbo].[DisableArchiveDependentManagers] TO [Mgr_Config_Admin] AS [dbo]
	GRANT EXECUTE ON [dbo].[DisableSequestClusters] TO [Mgr_Config_Admin] AS [dbo]
	GRANT EXECUTE ON [dbo].[EnableArchiveDependentManagers] TO [Mgr_Config_Admin] AS [dbo]
	GRANT EXECUTE ON [dbo].[EnableDisableAllManagers] TO [Mgr_Config_Admin] AS [dbo]
	GRANT EXECUTE ON [dbo].[EnableDisableManagers] TO [Mgr_Config_Admin] AS [dbo]
	GRANT EXECUTE ON [dbo].[LocalErrorHandler] TO [Mgr_Config_Admin] AS [dbo]
	GRANT EXECUTE ON [dbo].[NextField] TO [Mgr_Config_Admin] AS [dbo]
	GRANT EXECUTE ON [dbo].[ParseManagerNameList] TO [Mgr_Config_Admin] AS [dbo]
	GRANT EXECUTE ON [dbo].[PostLogEntry] TO [Mgr_Config_Admin] AS [dbo]
	GRANT EXECUTE ON [dbo].[ReportManagerErrorCleanup]  TO [Mgr_Config_Admin] AS [dbo]
	GRANT EXECUTE ON [dbo].[SelectManagerControlParams] TO [Mgr_Config_Admin] AS [dbo]
	GRANT EXECUTE ON [dbo].[SetManagerErrorCleanupMode] TO [Mgr_Config_Admin] AS [dbo]
	GRANT EXECUTE ON [dbo].[SetManagerParams] TO [Mgr_Config_Admin] AS [dbo]
	GRANT EXECUTE ON [dbo].[SetManagerUpdateRequired] TO [Mgr_Config_Admin] AS [dbo]
	GRANT EXECUTE ON [dbo].[SetParamForManagerList] TO [Mgr_Config_Admin] AS [dbo]
	GRANT EXECUTE ON [dbo].[UpdateManagerControlParams] TO [Mgr_Config_Admin] AS [dbo]
	GRANT EXECUTE ON [dbo].[UpdateSingleMgrControlParam] TO [Mgr_Config_Admin] AS [dbo]
	GRANT EXECUTE ON [dbo].[UpdateSingleMgrTypeControlParam] TO [Mgr_Config_Admin] AS [dbo]

	GRANT INSERT ON [dbo].[V_MgrState] TO [DMSWebUser]
	GRANT SELECT ON [dbo].[V_MgrState] TO [DMSWebUser]
	GRANT UPDATE ON [dbo].[V_MgrState] TO [DMSWebUser]
	GRANT UPDATE ON [dbo].[T_ParamValue] ([Entered_By]) TO [DMSWebUser] AS [dbo]
	GRANT UPDATE ON [dbo].[T_ParamValue] ([Last_Affected]) TO [DMSWebUser] AS [dbo]

	Return 0

GO

/****** Object:  StoredProcedure [dbo].[VerifySPAuthorized]    Script Date: 1/15/2020 8:33:53 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE Procedure [dbo].[VerifySPAuthorized]
/****************************************************
**
**	Desc:
**		Verifies that a user can execute the given stored procedure from the given remote host
**		Authorization is controlled via table T_SP_Authorization
**		The HostName column is either a specific host name, or * for any host
**
**	Returns:
**		1 if authorized, or 0 if not authorized
**
**		If authorized, @message is empty; otherwise it will be of the form:
**		'User PNL\Username cannot execute procedure ProcedureName from host HostName
**
**	Auth:	mem
**	Date:	06/16/2017 mem - Initial version
**			01/05/2018 mem - Include username and hostname in RAISERROR message
**
*****************************************************/
(
	@procedureName nvarchar(128),
	@raiseError tinyint = 0,
	@infoOnly tinyint = 0,
	@message varchar(255) = '' output
)
AS
	Set nocount on

	---------------------------------------------------
	-- Validate inputs
	---------------------------------------------------

	Set @procedureName = IsNull(@procedureName, '')
	Set @raiseError = IsNull(@raiseError, 0)
	Set @infoOnly = IsNull(@infoOnly, 0)

	---------------------------------------------------
	-- Determine host name and login name
	---------------------------------------------------

	DECLARE @clientHostName nvarchar(128)
	DECLARE @loginName nvarchar(128)

	SELECT @clientHostName = sess.host_name,
	       @loginName = sess.login_name
	FROM sys.dm_exec_sessions sess
	WHERE sess.session_ID = @@SPID

	Declare @authorized tinyint = 0

	If Exists (
		SELECT *
		FROM T_SP_Authorization
		WHERE ProcedureName = @procedureName AND
		      LoginName = @loginName AND
		      (HostName = @clientHostName Or HostName = '*'))
	Begin
		Set @authorized = 1

		If @infoOnly > 0
		Begin
			SELECT 'Yes' AS Authorized, @procedureName AS StoredProcedure, @loginName AS LoginName, @clientHostName AS HostName
		End
	End
	Else
	Begin
		If Exists (
			SELECT *
			FROM T_SP_Authorization
			WHERE ProcedureName = '*' AND
				LoginName = @loginName AND
				(HostName = @clientHostName Or HostName = '*'))
		Begin
			Set @authorized = 1

			If @infoOnly > 0
			Begin
				SELECT 'Yes ' AS Authorized, @procedureName + ' (Global)' AS StoredProcedure, @loginName AS LoginName, @clientHostName AS HostName
			End
		End
	End

	If @authorized = 0
	Begin
		If @infoOnly > 0
		Begin
			SELECT 'No' AS Authorized, @procedureName AS StoredProcedure, @loginName AS LoginName, @clientHostName AS HostName
		End
		Else
		Begin
			If @raiseError > 0
			Begin
				Set @message = 'User ' + @loginName + ' cannot execute procedure ' + @procedureName + ' from host ' + @clientHostName
				Exec PostLogEntry 'Error', @message, 'VerifySPAuthorized'

				Declare @msg varchar(128) = 'Access denied for current user (' + @loginName + ' on host ' + @clientHostName + ')'
				RAISERROR (@msg, 11, 4)
			End
		End
	End

	-----------------------------------------------
	-- Exit
	-----------------------------------------------

	return @authorized

GO
