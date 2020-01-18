using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using PRISM;

namespace SQLServer_Stored_Procedure_Converter
{
    class StoredProcedureConverter : EventNotifier
    {
        #region "Constants and Enums"

        private enum ControlBlockTypes
        {
            If = 0,
            While = 1
        }

        #endregion

        #region "Member variables"

        /// <summary>
        /// This is used to match Desc, Auth, or Date keywords in a comment block
        /// It captures both the keyword and any text after the keyword
        /// For example, given: Auth:	mem
        /// The Label group will have "Auth" and the Value group will have "mem"
        /// </summary>
        private readonly Regex mCommentBlockLabelMatcher = new Regex(
            @"^\*\*\s+(?<Label>Desc|Auth|Date):\s*(?<Value>.*)",
            RegexOptions.Compiled | RegexOptions.IgnoreCase);

        /// <summary>
        /// This is used to match varchar(10) or longer
        /// </summary>
        private readonly Regex mVarcharMatcher = new Regex(
            @"n*varchar\(\d{2,}\)",
            RegexOptions.Compiled | RegexOptions.IgnoreCase);

        /// <summary>
        /// This finds leading whitespace
        /// </summary>
        private readonly Regex mLeadingWhitespaceMatcher = new Regex(@"^\s+", RegexOptions.Compiled);

        /// <summary>
        /// This finds lines like:
        /// Set NoCount On
        /// Set XACT_ABORT, NoCount on
        /// </summary>
        private readonly Regex mSetNoCountMatcher = new Regex(
            @"^\s+Set.+(XACT_ABORT|NoCount).+On$",
            RegexOptions.Compiled | RegexOptions.IgnoreCase);

        /// <summary>
        /// This finds variable assignment statements, looking for Set followed by a variable name and an equals sign
        /// Although there is typically a value after the equals sign, this is not a requirement (the value could be on the next line)
        /// </summary>
        private readonly Regex mSetStatementMatcher = new Regex(
            @"^(?<LeadingWhitespace>\s*)Set\s+[@_](?<VariableName>[^\s]+)\s*=\s*(?<AssignedValue>.*)",
            RegexOptions.Compiled | RegexOptions.IgnoreCase);

        /// <summary>
        /// This is used to switch from + to || for string concatenation
        /// It matches single quoted text followed by a plus sign (including '' +)
        /// </summary>
        /// <remarks>This Regex uses positive lookbehind to find the quoted text before the plus sign</remarks>
        private readonly Regex mConcatenationReplacerA = new Regex(
            @"(?<='[^']*'\s*)\+",
            RegexOptions.Compiled | RegexOptions.IgnoreCase);

        /// <summary>
        /// This is used to switch from + to || for string concatenation
        /// It matches single quoted text preceded by a plus sign (including + '')
        /// </summary>
        /// <remarks>This Regex uses positive lookahead to find the quoted text after the plus sign</remarks>
        private readonly Regex mConcatenationReplacerB = new Regex(
            @"\+(?=\s*'[^']*')",
            RegexOptions.Compiled | RegexOptions.IgnoreCase);

        /// <summary>
        /// This is used to switch from + to || for string concatenation
        /// It matches single quoted text preceded by a plus sign (including + '')
        /// </summary>
        private readonly Regex mLenFunctionUpdater = new Regex(
            @"\bLen\s*\(",
            RegexOptions.Compiled | RegexOptions.IgnoreCase);

        /// <summary>
        /// This is used to switch from + to || for string concatenation
        /// It matches single quoted text preceded by a plus sign (including + '')
        /// </summary>
        private readonly Regex mCharIndexUpdater = new Regex(
            @"CharIndex\s*\(\s*(?<TextToFind>[^)]+)\s*,\s*(?<TextToSearch>[^)]+)\s*\)",
            RegexOptions.Compiled | RegexOptions.IgnoreCase);

        /// <summary>
        /// This is used to switch from Convert(DataType, @Variable) to  _Variable::DataType
        /// </summary>
        private readonly Regex mConvertDataTypeUpdater = new Regex(
            @"Convert\s*\(\s*(?<DataType>[^,]+)+\s*,\s*[@_](?<VariableName>[^\s]+)\s*\)",
            RegexOptions.Compiled | RegexOptions.IgnoreCase);

        /// <summary>
        /// This is used to find a SQL Server variable name (starts with @)
        /// </summary>
        private readonly Regex mVariableNameMatcher = new Regex(
            @"(?<VariableName>@[^\s]+)",
            RegexOptions.Compiled | RegexOptions.IgnoreCase);


        /// <summary>
        /// Options
        /// </summary>
        private readonly StoredProcedureConverterOptions mOptions;

        #endregion

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="options"></param>
        public StoredProcedureConverter(StoredProcedureConverterOptions options)
        {
            mOptions = options;
        }

        /// <summary>
        /// Add a line to the procedure body, replacing tabs with spaces
        /// </summary>f
        /// <param name="procedureBody"></param>
        /// <param name="dataLine"></param>
        private void AppendLine(ICollection<string> procedureBody, string dataLine)
        {
            procedureBody.Add(ReplaceTabs(dataLine));
        }

        /// <summary>
        /// If the line contains a comment, add it to the body
        /// </summary>
        /// <param name="procedureBody"></param>
        /// <param name="dataLine"></param>
        private void AppendLineComment(ICollection<string> procedureBody, string dataLine)
        {
            if (string.IsNullOrWhiteSpace(dataLine))
                return;

            var commentIndex = dataLine.IndexOf("--", StringComparison.Ordinal);
            if (commentIndex < 0)
                return;

            var leadingWhitespace = GetLeadingWhitespace(dataLine);

            AppendLine(procedureBody, leadingWhitespace + dataLine.Substring(commentIndex));
        }

        private string GetLeadingWhitespace(string dataLine)
        {
            var match = mLeadingWhitespaceMatcher.Match(dataLine);
            return !match.Success ? string.Empty : match.Value;
        }

        public bool ProcessFile(string inputFilePath)
        {
            try
            {
                var inputFile = new FileInfo(inputFilePath);
                if (!inputFile.Exists)
                {
                    OnWarningEvent("File not found: " + inputFilePath);
                    if (!inputFilePath.Equals(inputFile.FullName))
                    {
                        OnStatusEvent(" ... " + inputFile.FullName);
                    }

                    return false;
                }

                if (string.IsNullOrWhiteSpace(mOptions.OutputFilePath))
                {
                    mOptions.OutputFilePath = mOptions.GetDefaultOutputFilePath();
                }

                var outputFile = new FileInfo(mOptions.OutputFilePath);
                if (outputFile.Directory == null)
                {
                    OnWarningEvent("Unable to determine the parent directory of the output file: " + mOptions.OutputFilePath);
                    return false;
                }

                if (!outputFile.Directory.Exists)
                {
                    outputFile.Directory.Create();
                }

                var success = ProcessFile(inputFile, outputFile);

                return success;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in ProcessFile", ex);
                return false;
            }
        }

        private bool ProcessFile(FileSystemInfo inputFile, FileSystemInfo outputFile)
        {
            try
            {
                // This extracts a procedure name between the second pair of square brackets
                // For example, given: [dbo].[PostLogEntry]
                // ProcedureName will be PostLogEntry
                var procedureNameMatcher = new Regex(
                    @"\[[^\]]+\]\.\[(?<ProcedureName>[^\]]+)\]",
                    RegexOptions.Compiled | RegexOptions.IgnoreCase);

                // This looks for variable declaration statements, where a value is assigned to the variable
                var declareAndAssignMatcher = new Regex(
                    @"^(?<LeadingWhitespace>\s*)Declare\s+@(?<VariableName>[^\s]+)(?<DataType>[^=]+)\s*=\s*(?<AssignedValue>.+)",
                    RegexOptions.Compiled | RegexOptions.IgnoreCase);

                // This looks for variable declaration statements where no value is assigned
                var declareMatcher = new Regex(
                    @"^(?<LeadingWhitespace>\s*)Declare\s+@(?<VariableName>[^\s]+)(?<DataType>[^=]+)",
                    RegexOptions.Compiled | RegexOptions.IgnoreCase);

                // This looks for lines that start with End
                var endStatementMatcher = new Regex(
                    @"^(?<LeadingWhitespace>\s*)End\b(?<ExtraInfo>.*)",
                    RegexOptions.Compiled | RegexOptions.IgnoreCase);

                // This looks for lines of the form
                // Print @variable
                var printVariableMatcher = new Regex(
                    @"^(?<LeadingWhitespace>\s*)Print\b\s+@(?<VariableName>[^\s]+)",
                    RegexOptions.Compiled | RegexOptions.IgnoreCase);

                // This is used to change
                // SELECT @myRowCount = @@rowcount statements to
                // GET DIAGNOSTICS _rowcount = ROW_COUNT;
                var selectRowCountMatcher = new Regex(
                    @"^(?<LeadingWhitespace>\s*)SELECT.+@(?<VariableName>[^\s]+)\s*=\s*@@rowcount",
                    RegexOptions.Compiled | RegexOptions.IgnoreCase);


                // This is used to update SELECT statements that assign a value to a variable
                var selectAssignVariableMatcher = new Regex(
                    @"^(?<LeadingWhitespace>\s*)SELECT.+@(?<VariableName>[^\s]+)\s*=(?<SourceColumn>.+)",
                    RegexOptions.Compiled | RegexOptions.IgnoreCase);

                // Define the schema name
                var schemaName = string.IsNullOrWhiteSpace(mOptions.SchemaName) ? "public" : mOptions.SchemaName;

                var foundStartOfProcedureCommentBlock = false;
                var foundEndOfProcedureCommentBlock = false;

                var foundArgumentListStart = false;
                var foundArgumentListEnd = false;

                // This queue tracks lines read from the input file; it is first in, first out (FIFO)
                var cachedLines = new Queue<string>();

                var skipNextLineIfGo = false;

                // This stack tracks nested if and while blocks; it is last in, first out (LIFO)
                var controlBlockStack = new Stack<ControlBlockTypes>();

                var storedProcedureInfo = new StoredProcedureDDL(string.Empty);

                using (var reader = new StreamReader(new FileStream(inputFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                using (var writer = new StreamWriter(new FileStream(outputFile.FullName, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    while (!reader.EndOfStream)
                    {
                        string dataLine;
                        if (cachedLines.Count > 0)
                        {
                            dataLine = cachedLines.Dequeue();
                        }
                        else
                        {
                            dataLine = reader.ReadLine();
                        }

                        var trimmedLine = dataLine.Trim();

                        // Skip lines that are null, but don't skip blank lines
                        if (dataLine == null)
                            continue;

                        if (skipNextLineIfGo && dataLine.StartsWith("GO", StringComparison.OrdinalIgnoreCase))
                        {
                            SkipNextLineIfBlank(reader, cachedLines);
                            skipNextLineIfGo = false;
                            continue;
                        }

                        if (SkipLine(dataLine, out skipNextLineIfGo))
                            continue;

                        if (dataLine.StartsWith("CREATE PROCEDURE", StringComparison.OrdinalIgnoreCase) ||
                            dataLine.StartsWith("CREATE FUNCTION", StringComparison.OrdinalIgnoreCase))
                        {
                            if (!string.IsNullOrWhiteSpace(storedProcedureInfo.ProcedureName))
                            {
                                if (storedProcedureInfo.IsFunction)
                                    OnStatusEvent("Writing function " + storedProcedureInfo.ProcedureName);
                                else
                                    OnStatusEvent("Writing stored procedure " + storedProcedureInfo.ProcedureName);

                                // Write out the the previous procedure (or function)
                                storedProcedureInfo.ToWriterForPostgres(writer);
                            }

                            // Reset the tracking variables
                            foundStartOfProcedureCommentBlock = false;
                            foundEndOfProcedureCommentBlock = false;
                            foundArgumentListStart = false;
                            foundArgumentListEnd = false;

                            skipNextLineIfGo = false;
                            controlBlockStack.Clear();

                            var isFunction = dataLine.StartsWith("CREATE FUNCTION", StringComparison.OrdinalIgnoreCase);

                            var createKeywords = isFunction ? "CREATE FUNCTION" : "CREATE PROCEDURE";

                            var matchedName = procedureNameMatcher.Match(dataLine);
                            string procedureNameWithSchema;
                            if (matchedName.Success)
                            {
                                procedureNameWithSchema = schemaName + "." + matchedName.Groups["ProcedureName"].Value;
                            }
                            else
                            {
                                procedureNameWithSchema = schemaName + "." + dataLine.Substring(createKeywords.Length + 1);
                            }

                            storedProcedureInfo.Reset(procedureNameWithSchema, isFunction);
                            continue;
                        }

                        if (!foundStartOfProcedureCommentBlock && dataLine.StartsWith("/*****************"))
                        {
                            foundStartOfProcedureCommentBlock = true;
                            storedProcedureInfo.ProcedureCommentBlock.Add(ReplaceTabs(dataLine));
                            continue;
                        }

                        if (foundStartOfProcedureCommentBlock && !foundEndOfProcedureCommentBlock && dataLine.EndsWith("*****************/"))
                        {
                            foundEndOfProcedureCommentBlock = true;
                            storedProcedureInfo.ProcedureCommentBlock.Add(ReplaceTabs(dataLine));
                            continue;
                        }

                        if (foundStartOfProcedureCommentBlock && !foundEndOfProcedureCommentBlock)
                        {
                            if (dataLine.IndexOf("Return values: 0: success, otherwise, error code", StringComparison.OrdinalIgnoreCase) > 0 ||
                                dataLine.IndexOf("Return values: 0 if no error; otherwise error code", StringComparison.OrdinalIgnoreCase) > 0)
                            {
                                // Skip this line that we traditionally have included as boilerplate
                                ReadAndCacheLines(reader, cachedLines, 1);
                                if (cachedLines.Count > 0 && cachedLines.First().Trim().Equals("**"))
                                {
                                    // The next line is just "**"
                                    // Skip it too
                                    cachedLines.Dequeue();
                                }

                                continue;
                            }

                            StoreProcedureCommentLine(storedProcedureInfo, dataLine);
                            continue;
                        }

                        if (!foundArgumentListStart && dataLine.StartsWith("("))
                        {
                            foundArgumentListStart = true;
                            continue;
                        }

                        if (foundArgumentListStart && !foundArgumentListEnd && dataLine.StartsWith(")"))
                        {
                            foundArgumentListEnd = true;
                            continue;
                        }

                        if (foundArgumentListStart && !foundArgumentListEnd)
                        {
                            // Inside the argument list
                            StoreProcedureArgument(storedProcedureInfo, dataLine);
                            continue;
                        }

                        // The ReplaceText function performs a case-insensitive search/replace
                        // It supports RegEx
                        dataLine = ReplaceText(dataLine, "\bIsNull\b", "Coalesce");

                        var declareAndAssignMatch = declareAndAssignMatcher.Match(dataLine);
                        if (declareAndAssignMatch.Success)
                        {
                            StoreVariableToDeclare(storedProcedureInfo, declareAndAssignMatch);
                            StoreSetStatement(storedProcedureInfo.ProcedureBody, declareAndAssignMatch);
                            continue;
                        }

                        var declareMatch = declareMatcher.Match(dataLine);
                        if (declareMatch.Success)
                        {
                            StoreVariableToDeclare(storedProcedureInfo, declareMatch);
                            continue;
                        }

                        var assignVariableMatch = mSetStatementMatcher.Match(dataLine);
                        if (assignVariableMatch.Success)
                        {
                            StoreSetStatement(storedProcedureInfo.ProcedureBody, assignVariableMatch);
                            continue;
                        }

                        var printVariableMatch = printVariableMatcher.Match(dataLine);
                        if (printVariableMatch.Success)
                        {
                            StorePrintVariable(storedProcedureInfo.ProcedureBody, printVariableMatch);
                            continue;
                        }

                        var selectRowcountMatch = selectRowCountMatcher.Match(dataLine);
                        if (selectRowcountMatch.Success)
                        {
                            StoreSelectRowCount(storedProcedureInfo.ProcedureBody, selectRowcountMatch);
                            continue;
                        }

                        var selectAssignVariableMatch = selectAssignVariableMatcher.Match(dataLine);
                        if (selectAssignVariableMatch.Success)
                        {
                            StoreSelectAssignVariable(storedProcedureInfo.ProcedureBody, selectAssignVariableMatch);
                            continue;
                        }

                        var endMatch = endStatementMatcher.Match(dataLine);
                        if (endMatch.Success && controlBlockStack.Count > 0)
                        {
                            var leadingWhitespace = endMatch.Groups["LeadingWhitespace"].Value;

                            var extraInfo = endMatch.Groups["ExtraInfo"].Value;

                            var controlBlock = controlBlockStack.Pop();
                            switch (controlBlock)
                            {
                                case ControlBlockTypes.If:
                                    // If the next line is ELSE, skip this END statement
                                    ReadAndCacheLines(reader, cachedLines, 1);
                                    if (cachedLines.Count > 0 && cachedLines.First().Trim().StartsWith("Else", StringComparison.OrdinalIgnoreCase))
                                    {
                                        var elseLine = ReplaceText(cachedLines.Dequeue(), "else", "Else");
                                        UpdateAndAppendLine(storedProcedureInfo.ProcedureBody, elseLine);

                                        ReadAndCacheLines(reader, cachedLines, 1);

                                        // Look for Begin on the next line
                                        // If found, push ControlBlockTypes.If onto the stack
                                        // Skip the line if no comment; otherwise, write the comment
                                        if (NextCachedLineIsBegin(cachedLines, storedProcedureInfo.ProcedureBody, controlBlockStack))
                                        {
                                            continue;
                                        }

                                        if (cachedLines.Count > 0)
                                        {
                                            // The next line does not start Begin
                                            //   1) Write the next line (rename variables and change = to := if necessary)
                                            //   2) Write End If;
                                            var nextLine = cachedLines.Dequeue();
                                            UpdateAndAppendLine(storedProcedureInfo.ProcedureBody, nextLine + ";");

                                            AppendLine(storedProcedureInfo.ProcedureBody, leadingWhitespace + "End If;");
                                        }

                                        continue;
                                    }

                                    AppendLine(storedProcedureInfo.ProcedureBody, leadingWhitespace + "End If;" + extraInfo);
                                    continue;

                                case ControlBlockTypes.While:
                                    AppendLine(storedProcedureInfo.ProcedureBody, leadingWhitespace + "End Loop;" + extraInfo);
                                    continue;
                            }
                        }

                        if (trimmedLine.StartsWith("If ", StringComparison.OrdinalIgnoreCase))
                        {
                            // If statement
                            // Change to "If ... Then"
                            // This change assumes the If condition does not span multiple lines

                            var updatedLine = UpdateFunctionNames(UpdateVariablePrefix(dataLine));
                            AppendLine(storedProcedureInfo.ProcedureBody, updatedLine + " Then");

                            // Peek at the next two or three lines to determine what to do
                            // The following logic does not support "ELSE IF" code; that will need to be manually updated

                            ReadAndCacheLines(reader, cachedLines, 3);
                            if (cachedLines.Count == 0)
                                continue;

                            // Look for Begin on the next line
                            // If found, push ControlBlockTypes.If onto the stack
                            // Skip the line if no comment; otherwise, write the comment
                            if (NextCachedLineIsBegin(cachedLines, storedProcedureInfo.ProcedureBody, controlBlockStack))
                            {
                                continue;
                            }

                            var leadingWhitespace = GetLeadingWhitespace(dataLine);

                            if (cachedLines.Count > 1 && cachedLines.Take(2).Last().Trim().StartsWith("Else", StringComparison.OrdinalIgnoreCase))
                            {
                                // The line after the next line starts with Else:
                                //   1) Write out the next line (rename variables and change = to := if necessary)
                                //   2) Write Else

                                var lineBeforeElse = cachedLines.Dequeue();
                                UpdateAndAppendLine(storedProcedureInfo.ProcedureBody, lineBeforeElse);

                                var elseLine = ReplaceText(cachedLines.Dequeue(), "else", "Else");
                                UpdateAndAppendLine(storedProcedureInfo.ProcedureBody, elseLine);

                                // Look for Begin on the next line
                                // If found, push ControlBlockTypes.If onto the stack
                                // Skip the line if no comment; otherwise, write the comment
                                if (NextCachedLineIsBegin(cachedLines, storedProcedureInfo.ProcedureBody, controlBlockStack))
                                {
                                    continue;
                                }
                            }

                            // The line after the next line does not start with Else or Begin
                            //   1) Write the next line (rename variables and change = to := if necessary)
                            //   2) Write End If;
                            var nextLine = cachedLines.Dequeue();
                            UpdateAndAppendLine(storedProcedureInfo.ProcedureBody, nextLine + ";");

                            AppendLine(storedProcedureInfo.ProcedureBody, leadingWhitespace + "End If;");

                            continue;
                        }

                        if (trimmedLine.StartsWith("While ", StringComparison.OrdinalIgnoreCase))
                        {
                            // While statement
                            // Change to "While ... Loop"

                            var updatedLine = UpdateFunctionNames(UpdateVariablePrefix(dataLine));
                            AppendLine(storedProcedureInfo.ProcedureBody, updatedLine + " Loop");

                            // Assume the next line starts with Begin
                            controlBlockStack.Push(ControlBlockTypes.While);

                            continue;
                        }

                        if (trimmedLine.Equals("Goto done", StringComparison.OrdinalIgnoreCase))
                        {
                            var leadingWhitespace = GetLeadingWhitespace(dataLine);
                            AppendLine(storedProcedureInfo.ProcedureBody, leadingWhitespace + "Return;");

                            continue;
                        }

                        if (trimmedLine.StartsWith("exec @myError = ", StringComparison.OrdinalIgnoreCase))
                        {
                            var leadingWhitespace = GetLeadingWhitespace(dataLine);
                            var updatedLine = "Call " + UpdateVariablePrefix(trimmedLine.Substring("exec @myError = ".Length));
                            AppendLine(storedProcedureInfo.ProcedureBody, leadingWhitespace + updatedLine);
                            continue;
                        }

                        // Normal line of code (or whitespace); append it to the body
                        UpdateAndAppendLine(storedProcedureInfo.ProcedureBody, dataLine);
                    }

                }

                return true;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in ProcessFile", ex);
                return false;
            }
        }

        private bool NextCachedLineIsBegin(Queue<string> cachedLines, ICollection<string> procedureBody, Stack<ControlBlockTypes> controlBlockStack)
        {
            if (cachedLines.Count == 0)
                return false;

            if (!cachedLines.First().Trim().StartsWith("Begin", StringComparison.OrdinalIgnoreCase))
                return false;

            // The next line starts with Begin
            // Push "if" onto controlBlockStack
            // Skip the line if no comment; otherwise, write the comment

            controlBlockStack.Push(ControlBlockTypes.If);
            var beginStatement = ReplaceText(cachedLines.Dequeue(), "begin", "Begin");

            AppendLineComment(procedureBody, beginStatement);
            return true;

        }

        private void ReadAndCacheLines(StreamReader reader, Queue<string> cachedLines, int countToRead)
        {
            for (var i = 0; i < countToRead; i++)
            {
                if (reader.EndOfStream)
                    return;

                var dataLine = reader.ReadLine();
                cachedLines.Enqueue(dataLine);
            }
        }

        private string ReformatSetStatement(Match assignVariableMatch)
        {
            var assignedValue = assignVariableMatch.Groups["AssignedValue"].Value;

            var commentIndex = assignedValue.IndexOf(" --", StringComparison.Ordinal);

            string assignedValueClean;
            string commentText;
            if (commentIndex > 0)
            {
                assignedValueClean = assignedValue.Substring(0, commentIndex);
                commentText = assignedValue.Substring(commentIndex);
            }
            else
            {
                assignedValueClean = assignedValue;
                commentText = string.Empty;
            }

            var assignedValueUpdatedVariables = UpdateVariablePrefix(assignedValueClean);
            var assignedValueUpdatedOperators = UpdateConcatenationOperator(assignedValueUpdatedVariables);
            var assignedValueUpdatedFunctions = UpdateFunctionNames(assignedValueUpdatedOperators);
            var assignedValueToUse = VarcharToText(assignedValueUpdatedFunctions);

            var updatedLine = string.Format("{0}_{1} := {2};{3}",
                assignVariableMatch.Groups["LeadingWhitespace"].Value,
                assignVariableMatch.Groups["VariableName"].Value,
                assignedValueToUse,
                commentText);

            return updatedLine;
        }

        /// <summary>
        /// Case insensitive search/replace
        /// </summary>
        /// <param name="dataLine"></param>
        /// <param name="textToFind"></param>
        /// <param name="replacementText"></param>
        /// <returns>Updated text</returns>
        /// <remarks>The text to find is treated as RegEx, so be careful with symbols</remarks>
        private string ReplaceText(string dataLine, string textToFind, string replacementText)
        {
            return Regex.Replace(dataLine, textToFind, replacementText, RegexOptions.IgnoreCase);
        }

        private string ReplaceTabs(string dataLine)
        {
            return dataLine.Replace("\t", "    ").TrimEnd();
        }

        private bool SkipLine(string dataLine, out bool skipNextLineIfGo)
        {

            if (dataLine.Equals("AS", StringComparison.OrdinalIgnoreCase) ||
                dataLine.Equals("GO", StringComparison.OrdinalIgnoreCase) ||
                mSetNoCountMatcher.Match(dataLine).Success ||
                dataLine.StartsWith("/****** Object: ", StringComparison.OrdinalIgnoreCase))
            {
                skipNextLineIfGo = false;
                return true;
            }

            if (dataLine.StartsWith("SET ANSI_NULLS ON", StringComparison.OrdinalIgnoreCase) ||
                dataLine.StartsWith("SET QUOTED_IDENTIFIER ON", StringComparison.OrdinalIgnoreCase) ||
                dataLine.StartsWith("GRANT EXECUTE", StringComparison.OrdinalIgnoreCase))
            {
                skipNextLineIfGo = true;
                return true;
            }

            skipNextLineIfGo = false;
            return false;
        }

        private void SkipNextLineIfBlank(StreamReader reader, Queue<string> cachedLines)
        {
            ReadAndCacheLines(reader, cachedLines, 1);
            if (cachedLines.Count > 0 && string.IsNullOrWhiteSpace(cachedLines.First()))
            {
                cachedLines.Dequeue();
            }
        }

        private void StorePrintVariable(ICollection<string> procedureBody, Match printMatch)
        {
            var updatedVariableName = "_" + printMatch.Groups["VariableName"].Value;

            var raiseInfoLine = string.Format("{0}RAISE INFO '%', {1};",
                printMatch.Groups["LeadingWhitespace"].Value,
                updatedVariableName);

            AppendLine(procedureBody, raiseInfoLine);
        }

        private void StoreProcedureArgument(StoredProcedureDDL storedProcedureInfo, string dataLine)
        {

            if (string.IsNullOrWhiteSpace(dataLine))
                return;

            var argumentNameMatch = mVariableNameMatcher.Match(dataLine);

            string argumentName;

            if (argumentNameMatch.Success)
            {
                var argumentNameSqlServer = argumentNameMatch.Groups["VariableName"].Value;
                argumentName = UpdateVariablePrefix(argumentNameSqlServer);
            }
            else
            {
                // This shouldn't normally happen
                argumentName = dataLine;
            }

            var updatedArgumentLine = UpdateVariablePrefix(dataLine).Trim();
            if (updatedArgumentLine.IndexOf("varchar", StringComparison.OrdinalIgnoreCase) > 0)
            {
                // Change any instance of varchar(10) or larger to text
                updatedArgumentLine = VarcharToText(updatedArgumentLine);
            }

            if (updatedArgumentLine.IndexOf(" output,", StringComparison.OrdinalIgnoreCase) > 0)
            {
                updatedArgumentLine = "OUT " + updatedArgumentLine.Replace(" output,", ",").Trim();
            }
            else if (updatedArgumentLine.EndsWith("output"))
            {
                updatedArgumentLine = "OUT " + updatedArgumentLine.Substring(0, updatedArgumentLine.Length - "output".Length).Trim();
            }

            // Look for a comment after the argument declaration
            var commentIndex = updatedArgumentLine.IndexOf("--", StringComparison.Ordinal);
            if (commentIndex > 0 && commentIndex < updatedArgumentLine.Length - 1)
            {
                // When extracting the comment, do not include the leading "--"
                var argumentComment = updatedArgumentLine.Substring(commentIndex + 2).Trim();

                var argumentNameAndComment = new KeyValuePair<string, string>(argumentName, argumentComment);

                storedProcedureInfo.ProcedureArgumentComments.Add(argumentNameAndComment);

                updatedArgumentLine = updatedArgumentLine.Substring(0, commentIndex).Trim();
            }

            storedProcedureInfo.ProcedureArguments.Add(ReplaceTabs(updatedArgumentLine));
        }

        private void StoreProcedureCommentLine(StoredProcedureDDL storedProcedureInfo, string dataLine)
        {
            // Replace tabs with spaces
            // However, handle spaces in the stored procedure comment block specially

            var labelMatch = mCommentBlockLabelMatcher.Match(dataLine);
            if (!labelMatch.Success)
            {
                if (dataLine.StartsWith("**\t") && dataLine.Length > 3)
                {
                    storedProcedureInfo.ProcedureCommentBlock.Add("**  " + ReplaceTabs(dataLine.Substring(3)));
                }
                else
                {
                    storedProcedureInfo.ProcedureCommentBlock.Add(ReplaceTabs(dataLine));
                }

                return;
            }

            string updatedDataLine;
            if (labelMatch.Groups["Value"].Value.Length > 0)
            {
                updatedDataLine = string.Format(
                    "**  {0}:   {1}",
                    labelMatch.Groups["Label"].Value,
                    labelMatch.Groups["Value"].Value);
            }
            else
            {
                updatedDataLine = string.Format(
                    "**  {0}:",
                    labelMatch.Groups["Label"].Value);
            }

            storedProcedureInfo.ProcedureCommentBlock.Add(ReplaceTabs(updatedDataLine));
        }

        private void StoreSelectAssignVariable(ICollection<string> procedureBody, Match selectAssignVariableMatch)
        {
            var updatedVariableName = "_" + selectAssignVariableMatch.Groups["VariableName"].Value;

            var updatedLine = string.Format("{0}PERFORM {1} ={2}",
                selectAssignVariableMatch.Groups["LeadingWhitespace"].Value,
                updatedVariableName,
                selectAssignVariableMatch.Groups["SourceColumn"].Value
                );

            AppendLine(procedureBody, updatedLine);
        }

        private void StoreSelectRowCount(ICollection<string> procedureBody, Match selectRowCountMatch)
        {
            var updatedVariableName = "_" + selectRowCountMatch.Groups["VariableName"].Value;

            var raiseInfoLine = string.Format("{0}GET DIAGNOSTICS {1} = ROW_COUNT;",
                selectRowCountMatch.Groups["LeadingWhitespace"].Value,
                updatedVariableName);

            AppendLine(procedureBody, raiseInfoLine);
        }

        private void StoreSetStatement(ICollection<string> procedureBody, Match assignVariableMatch)
        {
            var updatedLine = ReformatSetStatement(assignVariableMatch);
            AppendLine(procedureBody, updatedLine);
        }

        private void StoreVariableToDeclare(StoredProcedureDDL storedProcedureInfo, Match declareMatch)
        {
            var variableDeclaration = string.Format("_{0}{1}",
                declareMatch.Groups["VariableName"].Value,
                VarcharToText(declareMatch.Groups["DataType"].Value));

            storedProcedureInfo.LocalVariablesToDeclare.Add(ReplaceTabs(variableDeclaration));
        }

        private void UpdateAndAppendLine(ICollection<string> procedureBody, string dataLine)
        {
            dataLine = UpdateVariablePrefix(dataLine);
            dataLine = UpdateSetStatement(dataLine);
            dataLine = UpdatePrintStatement(dataLine);
            dataLine = UpdateFunctionNames(dataLine);
            AppendLine(procedureBody, dataLine);
        }

        private string UpdateFunctionNames(string dataLine)
        {
            dataLine = mLenFunctionUpdater.Replace(dataLine, "char_length(");

            var charIndexMatch = mCharIndexUpdater.Match(dataLine);
            if (charIndexMatch.Success)
            {
                var textToFind = charIndexMatch.Groups["TextToFind"].Value;
                var textToSearch = charIndexMatch.Groups["TextToSearch"].Value;

                var updatedFunctionCall = string.Format("position({0} in {1})", textToFind, textToSearch);

                dataLine = mCharIndexUpdater.Replace(dataLine, updatedFunctionCall);
            }

            if (dataLine.Contains("Convert("))
                Console.WriteLine("Check this code");

            var convertMatch = mConvertDataTypeUpdater.Match(dataLine);
            if (convertMatch.Success)
            {
                var dataType = VarcharToText(convertMatch.Groups["DataType"].Value);
                var variableName = convertMatch.Groups["VariableName"].Value;

                var castCommand = string.Format("_{0}::{1}", variableName, dataType);

                dataLine = mConvertDataTypeUpdater.Replace(dataLine, castCommand);
            }

            return dataLine;
        }

        private string UpdateConcatenationOperator(string dataLine)
        {
            var updatedLineA = mConcatenationReplacerA.Replace(dataLine, "||");
            var updatedLineB = mConcatenationReplacerB.Replace(updatedLineA, "||");
            return updatedLineB;
        }

        private string UpdatePrintStatement(string dataLine)
        {
            if (!dataLine.Trim().StartsWith("Print", StringComparison.OrdinalIgnoreCase))
                return dataLine;

            var printIndex = dataLine.IndexOf("Print", StringComparison.OrdinalIgnoreCase);
            var leadingWhitespace = GetLeadingWhitespace(dataLine);

            var updatedLine = leadingWhitespace + "RAISE INFO" + dataLine.Substring(printIndex + "Print".Length) + ";";
            return updatedLine;
        }
        private string UpdateSetStatement(string dataLine)
        {
            var assignVariableMatch = mSetStatementMatcher.Match(dataLine);
            if (!assignVariableMatch.Success)
                return dataLine;

            var updatedLine = ReformatSetStatement(assignVariableMatch);
            return updatedLine;
        }

        private string UpdateVariablePrefix(string dataLine)
        {
            return dataLine.Replace('@', '_');
        }

        private string VarcharToText(string dataLine)
        {
            return mVarcharMatcher.Replace(dataLine, "text");
        }

    }
}
