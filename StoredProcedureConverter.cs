using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using PRISM;
using TableColumnNameMapContainer;

namespace SQLServer_Stored_Procedure_Converter
{
    internal class StoredProcedureConverter : EventNotifier
    {
        // ReSharper disable once CommentTypo

        // Ignore Spelling: auth, bs, dbo, desc, lookbehind, mem, myemsl, regex, tmp
        // Ignore Spelling: smallint, tinyint, varchar

        private enum ControlBlockTypes
        {
            If = 0,
            While = 1
        }

        /// <summary>
        /// This is used when backtracking and forward tracking to find lines of code that should be processed as a block
        /// </summary>
        private static readonly Regex mBlockBoundaryMatcher = new(
            @"^\s*(Begin|End|If|Else)\b",
            RegexOptions.Compiled | RegexOptions.IgnoreCase);

        /// <summary>
        /// This is used to match Desc, Auth, or Date keywords in a comment block
        /// It captures both the keyword and any text after the keyword
        /// For example, given: Auth:	mem
        /// The Label group will have "Auth" and the Value group will have "mem"
        /// </summary>
        private readonly Regex mCommentBlockLabelMatcher = new(
            @"^\*\*\s+(?<Label>Desc|Auth|Date):\s*(?<Value>.*)",
            RegexOptions.Compiled | RegexOptions.IgnoreCase);

        private readonly Regex mExecStoreReturnMatcher = new(
            @"exec\s+(?<TargetVariable>@[a-z]+) *=(?<TargetProcedureAndParams>.*)",
            RegexOptions.Compiled | RegexOptions.IgnoreCase);

        /// <summary>
        /// This is used to match varchar(10) or longer
        /// </summary>
        private readonly Regex mVarcharMatcher = new(
            @"n*varchar\((\d{2,}|max)\)",
            RegexOptions.Compiled | RegexOptions.IgnoreCase);

        private readonly Regex mLeadingTabMatcher = new(@"^\t+", RegexOptions.Compiled);

        /// <summary>
        /// This finds leading whitespace (spaces and tabs)
        /// </summary>
        private readonly Regex mLeadingWhitespaceMatcher = new(@"^\s+", RegexOptions.Compiled);

        /// <summary>
        /// This finds lines like:
        /// Set NoCount On
        /// Set NoCount Off
        /// Set XACT_ABORT, NoCount on
        /// </summary>
        private readonly Regex mSetNoCountMatcher = new(
            @"^\s*Set +(XACT_ABORT|NoCount) +(On|Off) *$",
            RegexOptions.Compiled | RegexOptions.IgnoreCase);

        /// <summary>
        /// This finds variable assignment statements, looking for Set followed by a variable name and an equals sign
        /// Although there is typically a value after the equals sign, this is not a requirement (the value could be on the next line)
        /// </summary>
        private readonly Regex mSetStatementMatcher = new(
            @"^(?<LeadingWhitespace>\s*)Set\s+[@_](?<VariableName>[^\s]+)\s*=\s*(?<AssignedValue>.*)",
            RegexOptions.Compiled | RegexOptions.IgnoreCase);

        /// <summary>
        /// This is used to switch from + to || for string concatenation
        /// It matches single quoted text followed by a plus sign (including '' +)
        /// </summary>
        /// <remarks>This Regex uses positive lookbehind to find the quoted text before the plus sign</remarks>
        private readonly Regex mConcatenationReplacerA = new(
            @"(?<='[^']*'\s*)\+",
            RegexOptions.Compiled | RegexOptions.IgnoreCase);

        /// <summary>
        /// This is used to switch from + to || for string concatenation
        /// It matches single quoted text preceded by a plus sign (including + '')
        /// </summary>
        /// <remarks>This Regex uses positive lookahead to find the quoted text after the plus sign</remarks>
        private readonly Regex mConcatenationReplacerB = new(
            @"\+(?=\s*'[^']*')",
            RegexOptions.Compiled | RegexOptions.IgnoreCase);

        /// <summary>
        /// This is used to switch from + to || for string concatenation
        /// It matches single quoted text preceded by a plus sign (including + '')
        /// </summary>
        private readonly Regex mLenFunctionUpdater = new(
            @"\bLen\s*\(",
            RegexOptions.Compiled | RegexOptions.IgnoreCase);

        /// <summary>
        /// This is used to switch from CharIndex('text', 'TextToSearch') to
        /// position('Text' in 'TextToSearch')
        /// </summary>
        private readonly Regex mCharIndexUpdater = new(
            @"CharIndex\s*\(\s*(?<TextToFind>[^)]+)\s*,\s*(?<TextToSearch>[^)]+)\s*\)",
            RegexOptions.Compiled | RegexOptions.IgnoreCase);

        /// <summary>
        /// This is used to switch from Convert(DataType, @Variable) to  _Variable::DataType
        /// </summary>
        private readonly Regex mConvertDataTypeUpdater = new(
            @"Convert\s*\(\s*(?<DataType>[^,]+)+\s*,\s*[@_](?<VariableName>[^\s]+)\s*\)",
            RegexOptions.Compiled | RegexOptions.IgnoreCase);

        /// <summary>
        /// This is used to find SQL Server variable names (which start with @)
        /// It uses negative look behind to avoid matching @@error
        /// </summary>
        private readonly Regex mVariableNameMatcher = new(
            @"(?<!@)@(?<FirstCharacter>[a-z0-9_])(?<RemainingCharacters>[^\s]+)",
            RegexOptions.Compiled | RegexOptions.IgnoreCase);

        /// <summary>
        /// This is used to find text that starts with @ plus the next letter, number, or underscore
        /// It uses negative look behind to avoid matching @@error
        /// </summary>
        private readonly Regex mVariableStartMatcher = new(
            "(?<!@)@(?<FirstCharacter>[a-z0-9_])",
            RegexOptions.Compiled | RegexOptions.IgnoreCase);

        /// <summary>
        /// This is used to find fields declared as Identity(1,1)
        /// </summary>
        private readonly Regex mIdentityFieldMatcher = new(
            @"(Identity\s*\(1,1\)\s*NOT NULL|Not Null Identity\s*\(1,1\)|Identity\s*\(1,1\))",
            RegexOptions.Compiled | RegexOptions.IgnoreCase);

        /// <summary>
        /// This is used to find cases where the LIKE keyword is followed by text in quotes where a square bracket is used to denote a character class
        /// </summary>
        private readonly Regex mLikeCharacterClassMatcher = new(
            @"\bLIKE(?<ComparisonSpec>\s*'.*\[[^]]+\].*')",
            RegexOptions.Compiled | RegexOptions.IgnoreCase);

        /// <summary>
        /// This is used to find UPDATE or DELETE queries
        /// </summary>
        private readonly Regex mUpdateOrDeleteQueryMatcher = new(
            @"^\s*(?<QueryType>UPDATE|DELETE)\s+(?<TargetTable>[^ ]+)",
            RegexOptions.Compiled | RegexOptions.IgnoreCase);

        /// <summary>
        /// Options
        /// </summary>
        private readonly StoredProcedureConverterOptions mOptions;

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
        /// </summary>
        /// <param name="procedureBody"></param>
        /// <param name="dataLine"></param>
        private void AppendLine(ICollection<string> procedureBody, string dataLine)
        {
            var updatedLine = ReplaceTabs(dataLine);
            if (string.IsNullOrWhiteSpace(updatedLine) &&
                string.IsNullOrWhiteSpace(procedureBody.LastOrDefault()))
            {
                // Prevent two blank lines in a row
                return;
            }

            while (updatedLine != null && updatedLine.EndsWith(";;"))
            {
                updatedLine = updatedLine.Substring(0, updatedLine.Length - 1);
            }

            procedureBody.Add(updatedLine);
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

        private void AppendProcedureToWriter(
            StreamWriter writer,
            StoredProcedureDDL storedProcedureInfo,
            Dictionary<string, WordReplacer> tableNameMap,
            Dictionary<string, Dictionary<string, WordReplacer>> columnNameMap,
            bool updateSchemaOnTables)
        {
            var procedureNameWithoutSchema = StoredProcedureDDL.GetNameWithoutSchema(storedProcedureInfo.ProcedureName);

            if (mOptions.StoredProcedureNamesToSkip.Contains(procedureNameWithoutSchema))
            {
                OnStatusEvent("Skipping " + storedProcedureInfo.ProcedureName);
                return;
            }

            // Write out the previous procedure (or function)
            if (storedProcedureInfo.IsFunction)
                OnStatusEvent("Writing function " + storedProcedureInfo.ProcedureName);
            else
                OnStatusEvent("Writing stored procedure " + storedProcedureInfo.ProcedureName);

            UpdateTableAndColumnNames(storedProcedureInfo.ProcedureBody, tableNameMap, columnNameMap, updateSchemaOnTables);

            storedProcedureInfo.ToWriterForPostgres(writer);
        }

        /// <summary>
        /// Convert the object name to snake_case
        /// </summary>
        /// <param name="objectName"></param>
        public static string ConvertNameToSnakeCase(string objectName)
        {
            return NameUpdater.ConvertNameToSnakeCase(objectName);
        }

        /// <summary>
        /// Examine the cached lines to find lines of code related to the line at the given index
        /// </summary>
        /// <param name="cachedLines">Cached SQL code</param>
        /// <param name="index">Index in cachedLines to start at when finding the lines to add to the block</param>
        /// <param name="updatedLineIndices">
        /// Tracks the indexes of lines that have been updated;
        /// this is used to assure we don't backtrack into a region that has already been processed
        /// </param>
        /// <param name="blockStartIndex">The index in cachedLines of the first line in the returned block of text</param>
        /// <returns>Lines of related SQL code, adjacent to the line at cachedLines[index]</returns>
        private List<string> FindCurrentBlock(
            IReadOnlyList<string> cachedLines,
            int index,
            IEnumerable<int> updatedLineIndices,
            out int blockStartIndex)
        {
            // Backtrack to find the start of this block
            blockStartIndex = index;
            var blockEndIndex = index;

            var minimumIndex = Math.Max(0, updatedLineIndices.LastOrDefault());

            var onlyIncludeCommentLines = cachedLines[index].Trim().StartsWith("--");

            while (blockStartIndex > minimumIndex)
            {
                var previousLine = cachedLines[blockStartIndex - 1].Trim();

                if (onlyIncludeCommentLines)
                {
                    if (!previousLine.Trim().StartsWith("--"))
                        break;
                }
                else
                {
                    if (IsBlockBoundary(previousLine))
                        break;
                }

                blockStartIndex--;
            }

            // Forward track to find the end of this block
            var stopEndIndex = cachedLines.Count - 1;
            while (blockEndIndex < stopEndIndex)
            {
                var nextLine = cachedLines[blockEndIndex + 1].Trim();

                if (onlyIncludeCommentLines)
                {
                    if (!nextLine.Trim().StartsWith("--"))
                        break;
                }
                else
                {
                    if (IsBlockBoundary(nextLine))
                        break;
                }

                blockEndIndex++;
            }

            var currentBlock = new List<string>();
            for (var i = blockStartIndex; i <= blockEndIndex; i++)
            {
                currentBlock.Add(cachedLines[i]);
            }

            return currentBlock;
        }

        private string GetLeadingWhitespace(string dataLine)
        {
            var match = mLeadingWhitespaceMatcher.Match(dataLine);
            return !match.Success ? string.Empty : match.Value;
        }

        private static bool IsBlankOrComment(string dataLine)
        {
            return string.IsNullOrWhiteSpace(dataLine) || dataLine.Trim().StartsWith("--");
        }

        /// <summary>
        /// Return true if the line is whitespace, or starts with --, Begin, If, or Else
        /// </summary>
        /// <param name="dataLine"></param>
        private static bool IsBlockBoundary(string dataLine)
        {
            return
                string.IsNullOrWhiteSpace(dataLine) ||
                dataLine.Trim().StartsWith("--") ||
                mBlockBoundaryMatcher.IsMatch(dataLine);
        }

        /// <summary>
        /// Load the column name map file, if defined
        /// It is a tab-delimited file with five columns, created by sqlserver2pgsql.pl or by the PgSqlViewCreatorHelper
        /// Columns:
        /// SourceTable  SourceName  Schema  NewTable  NewName
        /// </summary>
        /// <param name="tableNameMap">
        /// Dictionary where keys are the original (source) table names
        /// and values are WordReplacer classes that track the new table names and new column names in PostgreSQL
        /// </param>
        /// <param name="columnNameMap">
        /// Dictionary where keys are new table names
        /// and values are a Dictionary of mappings of original column names to new column names in PostgreSQL;
        /// names should not have double quotes around them
        /// </param>
        private bool LoadColumnNameMapFile(
            out Dictionary<string, WordReplacer> tableNameMap,
            out Dictionary<string, Dictionary<string, WordReplacer>> columnNameMap)
        {
            if (string.IsNullOrWhiteSpace(mOptions.ColumnNameMapFile))
            {
                tableNameMap = new Dictionary<string, WordReplacer>();
                columnNameMap = new Dictionary<string, Dictionary<string, WordReplacer>>();
                return true;
            }

            var mapFile = new FileInfo(mOptions.ColumnNameMapFile);
            if (!mapFile.Exists)
            {
                OnErrorEvent("Column name map file not found: " + mapFile.FullName);
                tableNameMap = new Dictionary<string, WordReplacer>();
                columnNameMap = new Dictionary<string, Dictionary<string, WordReplacer>>();
                return false;
            }

            var mapReader = new NameMapReader();
            RegisterEvents(mapReader);

            const string defaultSchema = "public";

            return mapReader.LoadSqlServerToPgSqlColumnMapFile(
                mapFile,
                defaultSchema,
                false,
                out tableNameMap,
                out columnNameMap);
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

                return ProcessFile(inputFile, outputFile);
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

                // This looks for lines that start with whitespace then (
                var argumentListStartMatcher = new Regex(@"^\s*\(", RegexOptions.Compiled);

                // This looks for lines that start with whitespace then )
                var argumentListEndMatcher = new Regex(@"^\s*\)", RegexOptions.Compiled);

                // This looks for lines of the form
                // ** Copyright 2005, Battelle Memorial Institute
                var copyrightMatcher = new Regex(@"\*\* Copyright 20\d\d, Battelle Memorial Institute", RegexOptions.Compiled | RegexOptions.IgnoreCase);

                // This looks for lines that start with ()
                var emptyArgumentListMatcher = new Regex(@"^\s*\(\s*\)", RegexOptions.Compiled);

                // This looks for the Return type of functions
                var returnTypeMatcher = new Regex(
                    @"^\s*RETURNS\s+(?<ReturnType>.+)",
                    RegexOptions.Compiled | RegexOptions.IgnoreCase);

                // This looks for functions where the return type is specified on the next line
                var returnTypeMatcherNoType = new Regex(
                    @"^\s*RETURNS\s*$",
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
                    @"^(?<LeadingWhitespace>\s*)SELECT.+@(?<VariableName>[^\s]+)\s*=\s*(?<SourceColumn>.+)",
                    RegexOptions.Compiled | RegexOptions.IgnoreCase);

                // This is used to find SELECT statements that use TOP N to limit the number of rows retrieved
                var selectTopMatcher = new Regex(
                    @"^(?<LeadingWhitespace>\s*)SELECT\s+TOP\s+(?<RowCount>\d+)",
                    RegexOptions.Compiled | RegexOptions.IgnoreCase);

                var createTempTableMatcher = new Regex(
                    @"^(?<LeadingWhitespace>\s+)CREATE TABLE #(?<TempTableName>[^\s]+)(?<ExtraInfo>.*)",
                    RegexOptions.Compiled | RegexOptions.IgnoreCase);

                var mapFileSuccess = LoadColumnNameMapFile(out var tableNameMap, out var columnNameMap);

                if (!mapFileSuccess)
                    return false;

                // Define the schema name
                var schemaName = string.IsNullOrWhiteSpace(mOptions.SchemaName) ? "public" : mOptions.SchemaName;

                var foundStartOfProcedureCommentBlock = false;
                var foundEndOfProcedureCommentBlock = false;

                var foundArgumentListStart = false;
                var foundArgumentListEnd = false;

                // This queue tracks lines read from the input file; it is first in, first out (FIFO)
                var cachedLines = new Queue<string>();

                var tempTableDropStatements = new List<string>();

                var updateSchemaOnTables =
                    !string.IsNullOrWhiteSpace(mOptions.SchemaName) &&
                    !mOptions.SchemaName.Equals("public", StringComparison.OrdinalIgnoreCase);

                var skipNextLineIfGo = false;
                var insideDateBlock = false;

                var trimmedLine = string.Empty;

                var mostRecentUpdateOrDeleteType = string.Empty;
                var mostRecentUpdateOrDeleteTable = string.Empty;
                var limitRowCountDDL = string.Empty;

                // This stack tracks nested if and while blocks; it is last in, first out (LIFO)
                var controlBlockStack = new Stack<ControlBlockTypes>();

                var storedProcedureInfo = new StoredProcedureDDL(mOptions, this, string.Empty);

                using var reader = new StreamReader(new FileStream(inputFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite));
                using var writer = new StreamWriter(new FileStream(outputFile.FullName, FileMode.Create, FileAccess.Write, FileShare.Read));

                while (!reader.EndOfStream)
                {
                    var dataLine = cachedLines.Count > 0 ? cachedLines.Dequeue() : reader.ReadLine();

                    // Skip lines that are null, but don't skip blank lines
                    if (dataLine == null)
                        continue;

                    // Skip Copyright lines
                    if (dataLine.Trim().Equals("** Pacific Northwest National Laboratory, Richland, WA"))
                        continue;

                    if (copyrightMatcher.IsMatch(dataLine))
                        continue;

                    // Replace smart quotes with straight quotes
                    dataLine = PunctuationUpdater.ProcessLine(dataLine);

                    var previousTrimmedLine = string.Copy(trimmedLine);

                    // Note that .Trim() removes leading and trailing spaces and tabs
                    trimmedLine = dataLine.Trim();

                    if (trimmedLine.Contains("Custom SQL to find"))
                        Console.WriteLine("Check this code");

                    if (!string.IsNullOrWhiteSpace(limitRowCountDDL) && (trimmedLine.Length == 0 || trimmedLine.StartsWith("--")))
                    {
                        AppendLine(storedProcedureInfo.ProcedureBody, limitRowCountDDL);
                        limitRowCountDDL = string.Empty;
                    }

                    // Skip lines that assign 0 to @myError
                    if (trimmedLine.Equals("Set @myError = 0", StringComparison.OrdinalIgnoreCase))
                        continue;

                    // If the previous line was "Declare @myRowCount" or "Declare @myError", skip lines that assign 0 to @myRowCount
                    if (trimmedLine.Equals("Set @myRowCount = 0", StringComparison.OrdinalIgnoreCase) &&
                        (previousTrimmedLine.StartsWith("Declare @myRowCount", StringComparison.OrdinalIgnoreCase) ||
                         previousTrimmedLine.StartsWith("Declare @myError", StringComparison.OrdinalIgnoreCase) ||
                         previousTrimmedLine.StartsWith("Set @myError = 0", StringComparison.OrdinalIgnoreCase)))
                    {
                        continue;
                    }

                    if (skipNextLineIfGo && trimmedLine.Equals("GO", StringComparison.OrdinalIgnoreCase))
                    {
                        SkipNextLineIfBlank(reader, cachedLines);
                        skipNextLineIfGo = false;
                        continue;
                    }

                    if (trimmedLine.Equals("GO", StringComparison.OrdinalIgnoreCase) && tempTableDropStatements.Count > 0)
                    {
                        storedProcedureInfo.ProcedureBody.AddRange(tempTableDropStatements);
                        tempTableDropStatements.Clear();
                    }

                    if (!string.IsNullOrWhiteSpace(mostRecentUpdateOrDeleteTable) && string.IsNullOrWhiteSpace(trimmedLine))
                    {
                        mostRecentUpdateOrDeleteTable = string.Empty;
                    }

                    if (SkipLine(trimmedLine, out skipNextLineIfGo))
                        continue;

                    if (trimmedLine.StartsWith("CREATE PROCEDURE", StringComparison.OrdinalIgnoreCase) ||
                        trimmedLine.StartsWith("CREATE FUNCTION", StringComparison.OrdinalIgnoreCase))
                    {
                        if (!string.IsNullOrWhiteSpace(storedProcedureInfo.ProcedureName))
                        {
                            AppendProcedureToWriter(writer, storedProcedureInfo, tableNameMap, columnNameMap, updateSchemaOnTables);
                        }

                        // Reset the tracking variables
                        foundStartOfProcedureCommentBlock = false;
                        foundEndOfProcedureCommentBlock = false;
                        foundArgumentListStart = false;
                        foundArgumentListEnd = false;

                        skipNextLineIfGo = false;
                        controlBlockStack.Clear();

                        var isFunction = trimmedLine.StartsWith("CREATE FUNCTION", StringComparison.OrdinalIgnoreCase);

                        var createKeywords = isFunction ? "CREATE FUNCTION" : "CREATE PROCEDURE";

                        var matchedName = procedureNameMatcher.Match(trimmedLine);
                        string procedureNameWithSchema;

                        if (matchedName.Success)
                        {
                            procedureNameWithSchema = schemaName + "." + matchedName.Groups["ProcedureName"].Value;
                        }
                        else
                        {
                            procedureNameWithSchema = schemaName + "." + trimmedLine.Substring(createKeywords.Length + 1);
                        }

                        storedProcedureInfo.Reset(procedureNameWithSchema, isFunction);
                        continue;
                    }

                    if (!foundStartOfProcedureCommentBlock && trimmedLine.StartsWith("/*****************"))
                    {
                        foundStartOfProcedureCommentBlock = true;
                        insideDateBlock = false;
                        storedProcedureInfo.ProcedureCommentBlock.Add(ReplaceTabs(dataLine));
                        continue;
                    }

                    if (foundStartOfProcedureCommentBlock && !foundEndOfProcedureCommentBlock && trimmedLine.EndsWith("*****************/"))
                    {
                        foundEndOfProcedureCommentBlock = true;
                        if (insideDateBlock)
                        {
                            storedProcedureInfo.ProcedureCommentBlock.Add(string.Format(
                                "**          {0:MM/dd/yyyy} mem - Ported to PostgreSQL",
                                DateTime.Now));
                            insideDateBlock = false;
                        }

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
                            if (cachedLines.Count > 0 && cachedLines.Peek().Trim().Equals("**"))
                            {
                                // The next line is just "**"
                                // Skip it too
                                cachedLines.Dequeue();
                            }

                            continue;
                        }

                        if (dataLine.IndexOf("Parameters:", StringComparison.OrdinalIgnoreCase) > 1)
                        {
                            // Skip lines of the form "**  Parameters:" if the next line is blank
                            var lineAfterAsterisks = dataLine.Substring(2).Trim();
                            if (lineAfterAsterisks.Equals("Parameters:", StringComparison.OrdinalIgnoreCase))
                            {
                                ReadAndCacheLines(reader, cachedLines, 1);
                                if (cachedLines.Count > 0 && cachedLines.Peek().Trim().Equals("**"))
                                {
                                    // The next line is just "**"
                                    // Skip this line and the next one
                                    cachedLines.Dequeue();
                                    continue;
                                }
                            }
                        }

                        if (insideDateBlock && trimmedLine.Equals("**"))
                        {
                            storedProcedureInfo.ProcedureCommentBlock.Add(string.Format(
                                "**          {0:MM/dd/yyyy} mem - Ported to PostgreSQL",
                                DateTime.Now));
                            insideDateBlock = false;
                        }

                        StoreProcedureCommentLine(storedProcedureInfo, dataLine, out var startOfDateBlock);
                        if (startOfDateBlock)
                        {
                            insideDateBlock = true;
                        }

                        continue;
                    }

                    if (!foundArgumentListStart && argumentListStartMatcher.IsMatch(dataLine))
                    {
                        foundArgumentListStart = true;

                        if (emptyArgumentListMatcher.IsMatch(dataLine))
                            foundArgumentListEnd = true;

                        continue;
                    }

                    if (foundArgumentListStart && !foundArgumentListEnd && argumentListEndMatcher.IsMatch(dataLine))
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

                    if (returnTypeMatcher.IsMatch(dataLine))
                    {
                        var match = returnTypeMatcher.Match(dataLine);

                        storedProcedureInfo.FunctionReturnType = match.Groups["ReturnType"].Value;
                        continue;
                    }

                    if (returnTypeMatcherNoType.IsMatch(dataLine))
                    {
                        // The return type is on the next line
                        storedProcedureInfo.FunctionReturnType = cachedLines.Count > 0 ? cachedLines.Dequeue() : reader.ReadLine();
                        continue;
                    }

                    // Perform some standard text replacements using ReplaceText
                    // It performs a case-insensitive search/replace and it supports Regex

                    dataLine = ReplaceText(dataLine, @"\bIsNull\b", "Coalesce");

                    dataLine = ReplaceText(dataLine, @"\bDatetime\b", "timestamp");

                    dataLine = ReplaceText(dataLine, @"\bGetDate\b\s*\(\)", "CURRENT_TIMESTAMP");

                    // Stored procedures with smallint parameters are harder to call, since you have to explicitly cast numbers to ::smallint
                    // Thus, replace both tinyint and smallint with int (aka integer or int4)
                    dataLine = ReplaceText(dataLine, @"\b(tinyint|smallint)\b", "int");

                    // ReSharper disable CommentTypo

                    // This matches user_name(), suser_name(), or suser_sname()
                    dataLine = ReplaceText(dataLine, @"\bs*user_s*name\b\s*\(\)", "session_user");

                    // ReSharper restore CommentTypo

                    dataLine = ReplaceText(dataLine, "(dbo.)*AlterEnteredByUserMultiID", "public.alter_entered_by_user_multi_id");
                    dataLine = ReplaceText(dataLine, "(dbo.)*AlterEnteredByUser", "public.alter_entered_by_user");
                    dataLine = ReplaceText(dataLine, "(dbo.)*AlterEventLogEntryUserMultiID", "public.alter_event_log_entry_user_multi_id");
                    dataLine = ReplaceText(dataLine, "(dbo.)*AlterEventLogEntryUser", "public.alter_event_log_entry_user");

                    dataLine = ReplaceText(dataLine, "(dbo.)*udfParseDelimitedIntegerList", "public.parse_delimited_integer_list");
                    dataLine = ReplaceText(dataLine, "(dbo.)*udfParseDelimitedListOrdered", "public.parse_delimited_list_ordered");
                    dataLine = ReplaceText(dataLine, "(dbo.)*udfParseDelimitedList", "public.parse_delimited_list");

                    dataLine = ReplaceText(dataLine, "(dbo.)*udfCombinePaths", "public.combine_paths");
                    dataLine = ReplaceText(dataLine, "(dbo.)*udfGetFilename", "public.get_filename");
                    dataLine = ReplaceText(dataLine, "(dbo.)*udfTimeStampText", "public.timestamp_text");

                    dataLine = ReplaceText(dataLine, "(dbo.)*udfWhitespaceChars", "public.has_whitespace_chars");

                    // ReSharper disable once StringLiteralTypo
                    dataLine = ReplaceText(dataLine, "(dbo.)*MakeTableFromListDelim", "public.parse_delimited_list");
                    dataLine = ReplaceText(dataLine, "(dbo.)*MakeTableFromList", "public.parse_delimited_list");

                    dataLine = ReplaceLeadingTabs(dataLine);

                    if (dataLine.IndexOf("LTrim(RTrim", StringComparison.OrdinalIgnoreCase) >= 0)
                    {
                        dataLine = ReplaceText(dataLine, @"LTrim\(RTrim", "Trim");

                        var trimIndex = dataLine.IndexOf("Trim(", StringComparison.OrdinalIgnoreCase);

                        // Replace the next occurrence of )) with )
                        var parenthesesIndex = dataLine.IndexOf("))", Math.Max(0, trimIndex), StringComparison.OrdinalIgnoreCase);

                        if (parenthesesIndex > 0)
                        {
                            dataLine = dataLine.Substring(0, parenthesesIndex) + dataLine.Substring(parenthesesIndex + 1);
                        }
                    }

                    var createTempTableMatch = createTempTableMatcher.Match(dataLine);

                    if (createTempTableMatch.Success)
                    {
                        dataLine = string.Format(
                            "{0}CREATE TEMP TABLE {1}{2}",
                            createTempTableMatch.Groups["LeadingWhitespace"],
                            createTempTableMatch.Groups["TempTableName"],
                            createTempTableMatch.Groups["ExtraInfo"]
                        );

                        tempTableDropStatements.Add("    DROP TABLE " + createTempTableMatch.Groups["TempTableName"]);
                    }

                    dataLine = ReplaceText(dataLine, "#Tmp", "Tmp");
                    dataLine = ReplaceText(dataLine, "#IX", "IX");

                    dataLine = ReplaceText(dataLine, "(dbo.)*AppendToText", "public.append_to_text");

                    var declareAndAssignMatch = declareAndAssignMatcher.Match(dataLine);
                    if (declareAndAssignMatch.Success)
                    {
                        StoreVariableToDeclare(storedProcedureInfo, declareAndAssignMatch);
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

                    var selectTopMatch = selectTopMatcher.Match(dataLine);
                    if (selectTopMatch.Success)
                    {
                        // Cache DDL to be written to the output file when the next blank line or line that starts with a comment is found
                        // For example:
                        //     LIMIT 1;
                        limitRowCountDDL = string.Format("{0}LIMIT {1};",
                            selectTopMatch.Groups["LeadingWhitespace"].Value,
                            selectTopMatch.Groups["RowCount"].Value);

                        AppendLine(storedProcedureInfo.ProcedureBody,
                            string.Format("{0}-- Moved to bottom of query: TOP {1}",
                                selectTopMatch.Groups["LeadingWhitespace"].Value,
                                selectTopMatch.Groups["RowCount"].Value));
                    }

                    var selectAssignVariableMatch = selectAssignVariableMatcher.Match(dataLine);
                    if (selectAssignVariableMatch.Success)
                    {
                        StoreSelectAssignVariable(storedProcedureInfo.ProcedureBody, selectAssignVariableMatch);
                        continue;
                    }

                    var identityFieldMatch = mIdentityFieldMatcher.Match(dataLine);
                    if (identityFieldMatch.Success)
                    {
                        dataLine = mIdentityFieldMatcher.Replace(dataLine, "PRIMARY KEY GENERATED ALWAYS AS IDENTITY");
                    }

                    var likeCharacterClassMatch = mLikeCharacterClassMatcher.Match(dataLine);
                    if (likeCharacterClassMatch.Success)
                    {
                        dataLine = mLikeCharacterClassMatcher.Replace(dataLine, "SIMILAR TO${ComparisonSpec}");
                    }

                    var endMatch = endStatementMatcher.Match(dataLine);
                    if (endMatch.Success && controlBlockStack.Count > 0)
                    {
                        var leadingWhitespace = endMatch.Groups["LeadingWhitespace"].Value;

                        var extraInfo = endMatch.Groups["ExtraInfo"].Value;

                        switch (controlBlockStack.Pop())
                        {
                            case ControlBlockTypes.If:
                                // If the next line is ELSE, skip this END statement
                                ReadAndCacheLines(reader, cachedLines, 1);
                                if (cachedLines.Count > 0 && cachedLines.Peek().Trim().StartsWith("Else", StringComparison.OrdinalIgnoreCase))
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

                        var updatedLine = UpdateFunctionNames(UpdateVariableNames(dataLine));
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

                    // ReSharper disable once ConvertIfStatementToSwitchStatement
                    if (trimmedLine.Equals("break") || trimmedLine.Equals("break;"))
                    {
                        // Use 'exit;' to break out of a loop
                        dataLine = dataLine.Replace("break", "exit");
                        if (!dataLine.EndsWith(";"))
                            dataLine += ";";

                        AppendLine(storedProcedureInfo.ProcedureBody, dataLine);
                        continue;
                    }

                    if (trimmedLine.Equals("continue"))
                    {
                        // Add a semicolon
                        AppendLine(storedProcedureInfo.ProcedureBody, dataLine + ";");
                        continue;
                    }

                    if (trimmedLine.Equals("continue;"))
                    {
                        // Store as-is
                        AppendLine(storedProcedureInfo.ProcedureBody, dataLine);
                        continue;
                    }

                    if (trimmedLine.StartsWith("While ", StringComparison.OrdinalIgnoreCase))
                    {
                        // While statement
                        // Change to "While ... Loop"

                        var updatedLine = UpdateFunctionNames(UpdateVariableNames(dataLine));
                        AppendLine(storedProcedureInfo.ProcedureBody, updatedLine + " Loop");

                        controlBlockStack.Push(ControlBlockTypes.While);

                        // If the next line starts with BEGIN, skip it
                        // (since PostgreSQL syntax does not use Begin at the start of While Loops, only at the start of multi-line If blocks)
                        ReadAndCacheLines(reader, cachedLines, 1);
                        if (cachedLines.Count > 0 && cachedLines.Peek().Trim().StartsWith("Begin", StringComparison.OrdinalIgnoreCase))
                        {
                            cachedLines.Dequeue();
                        }

                        // If the next non-blank / non-comment line starts with "SELECT TOP 1", add a comment about converting to a For loop
                        ReadAndCacheLines(reader, cachedLines, 1, false);

                        while (cachedLines.Count > 0 && IsBlankOrComment(cachedLines.Peek()))
                        {
                            AppendLine(storedProcedureInfo.ProcedureBody, cachedLines.Dequeue());

                            ReadAndCacheLines(reader, cachedLines, 1, false);
                        }

                        if (cachedLines.Count > 0)
                        {
                            var selectTopOneMatch = selectTopMatcher.Match(cachedLines.Peek());
                            if (selectTopOneMatch.Success && selectTopOneMatch.Groups["RowCount"].Value.Equals("1"))
                            {
                                var whiteSpace = selectTopOneMatch.Groups["LeadingWhitespace"].Value;

                                var loopComment = new StringBuilder();
                                loopComment.AppendFormat("{0}-- This While loop can probably be converted to a For loop; for example:", whiteSpace).AppendLine();
                                loopComment.AppendFormat("{0}--    For _itemName In", whiteSpace).AppendLine();
                                loopComment.AppendFormat("{0}--        SELECT item_name", whiteSpace).AppendLine();
                                loopComment.AppendFormat("{0}--        FROM TmpSourceTable", whiteSpace).AppendLine();
                                loopComment.AppendFormat("{0}--        ORDER BY entry_id", whiteSpace).AppendLine();
                                loopComment.AppendFormat("{0}--    Loop", whiteSpace).AppendLine();
                                loopComment.AppendFormat("{0}--        ...", whiteSpace).AppendLine();
                                loopComment.AppendFormat("{0}--    End Loop", whiteSpace).AppendLine().AppendLine();

                                AppendLine(storedProcedureInfo.ProcedureBody, loopComment.ToString());
                                storedProcedureInfo.ProcedureBody.Add(string.Empty);
                            }
                        }

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
                        var targetProcedureAndParams = trimmedLine.Substring("exec @myError = ".Length);
                        if (mOptions.ConvertNamesToSnakeCase)
                        {
                            targetProcedureAndParams = SnakeCaseFirstWord(targetProcedureAndParams);
                        }

                        var updatedLine = "Call " + UpdateVariableNames(targetProcedureAndParams);
                        if (updatedLine.Contains("="))
                            updatedLine = ReplaceText(updatedLine, @"\s*=\s*", " => ");

                        AppendLine(storedProcedureInfo.ProcedureBody, leadingWhitespace + updatedLine);
                        continue;
                    }

                    if (trimmedLine.StartsWith("exec ", StringComparison.OrdinalIgnoreCase))
                    {
                        UpdateAndAppendExecLine(storedProcedureInfo.ProcedureBody, dataLine, trimmedLine);
                        continue;
                    }

                    var updateOrDeleteQueryMatch = mUpdateOrDeleteQueryMatcher.Match(trimmedLine);
                    if (updateOrDeleteQueryMatch.Success)
                    {
                        mostRecentUpdateOrDeleteType = updateOrDeleteQueryMatch.Groups["QueryType"].Value;
                        mostRecentUpdateOrDeleteTable = updateOrDeleteQueryMatch.Groups["TargetTable"].Value;
                    }
                    else
                    {
                        if (mostRecentUpdateOrDeleteTable.Length > 0 &&
                            trimmedLine.StartsWith("FROM " + mostRecentUpdateOrDeleteTable, StringComparison.OrdinalIgnoreCase))
                        {
                            UpdateAndAppendLine(storedProcedureInfo.ProcedureBody, dataLine);

                            var leadingWhitespace = GetLeadingWhitespace(dataLine);

                            var updateQueryWarnings = new List<string>
                            {
                                // This UPDATE query includes the target table name in the FROM clause
                                string.Empty,
                                "/********************************************************************************",
                                "** This " + mostRecentUpdateOrDeleteType + " query includes the target table name in the FROM clause",
                                "** The WHERE clause needs to have a self join to the target table, for example:",
                                string.Format("**   UPDATE {0}", mostRecentUpdateOrDeleteTable),
                                "**   SET ...",
                                "**   FROM source",
                                string.Format("**   WHERE source.id = {0}.id;", mostRecentUpdateOrDeleteTable)
                            };

                            if (mostRecentUpdateOrDeleteType.StartsWith("delete", StringComparison.OrdinalIgnoreCase))
                            {
                                updateQueryWarnings.Add("**");
                                updateQueryWarnings.Add("** Delete queries must also include the USING keyword");
                                updateQueryWarnings.Add("** Alternatively, the more standard approach is to rearrange the query to be similar to");
                                updateQueryWarnings.Add(string.Format(
                                    "**   DELETE FROM {0} WHERE id in (SELECT id from ...)", mostRecentUpdateOrDeleteTable));
                            }

                            updateQueryWarnings.Add("********************************************************************************/");
                            updateQueryWarnings.Add(string.Empty);
                            updateQueryWarnings.Add("                       ToDo: Fix this query");
                            updateQueryWarnings.Add(string.Empty);

                            foreach (var item in updateQueryWarnings)
                            {
                                if (item.Length > 0)
                                    UpdateAndAppendLine(storedProcedureInfo.ProcedureBody, leadingWhitespace + item);
                                else
                                    UpdateAndAppendLine(storedProcedureInfo.ProcedureBody, item);
                            }

                            continue;
                        }
                    }

                    // Normal line of code (or whitespace); append it to the body
                    UpdateAndAppendLine(storedProcedureInfo.ProcedureBody, dataLine);
                }

                AppendProcedureToWriter(writer, storedProcedureInfo, tableNameMap, columnNameMap, updateSchemaOnTables);

                return true;
            }
            catch (Exception ex)
            {
                OnErrorEvent("Error in ProcessFile", ex);
                return false;
            }
        }

        private bool FindTablesByName(
            Dictionary<string, WordReplacer> tableNameMap,
            string dataLine,
            ICollection<string> tablesInLine
        )
        {
            tablesInLine.Clear();

            foreach (var item in tableNameMap.Keys)
            {
                if (dataLine.IndexOf(item, StringComparison.OrdinalIgnoreCase) > 0)
                    tablesInLine.Add(item);
            }

            return tablesInLine.Count > 0;
        }

        private bool NextCachedLineIsBegin(Queue<string> cachedLines, ICollection<string> procedureBody, Stack<ControlBlockTypes> controlBlockStack)
        {
            if (cachedLines.Count == 0)
                return false;

            if (!cachedLines.Peek().Trim().StartsWith("Begin", StringComparison.OrdinalIgnoreCase))
                return false;

            // The next line starts with Begin
            // Push "if" onto controlBlockStack
            // Skip the line if no comment; otherwise, write the comment

            controlBlockStack.Push(ControlBlockTypes.If);
            var beginStatement = ReplaceText(cachedLines.Dequeue(), "begin", "Begin");

            AppendLineComment(procedureBody, beginStatement);
            return true;
        }

        /// <summary>
        /// Read and cache the specified number of lines
        /// </summary>
        /// <param name="reader"></param>
        /// <param name="cachedLines"></param>
        /// <param name="countToRead"></param>
        /// <param name="countBlankLinesAndComments">
        /// If false, when counting lines read, do not increment the count for blank lines or comment lines
        /// </param>
        private void ReadAndCacheLines(
            StreamReader reader,
            Queue<string> cachedLines,
            int countToRead,
            bool countBlankLinesAndComments = true)
        {
            var linesRead = 0;

            while (true)
            {
                if (countBlankLinesAndComments && linesRead >= countToRead)
                {
                    return;
                }

                if (!countBlankLinesAndComments && cachedLines.Count >= countToRead)
                {
                    return;
                }

                if (reader.EndOfStream)
                    return;

                var dataLine = reader.ReadLine();
                cachedLines.Enqueue(dataLine);

                if (countBlankLinesAndComments)
                {
                    linesRead++;
                }
                else if (!string.IsNullOrWhiteSpace(dataLine) && !dataLine.Trim().StartsWith("--"))
                {
                    linesRead++;
                }
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

            var updatedVariableName = UpdateVariablePrefix("@" + assignVariableMatch.Groups["VariableName"].Value);

            return string.Format("{0}{1} := {2};{3}",
                assignVariableMatch.Groups["LeadingWhitespace"].Value,
                updatedVariableName,
                assignedValueToUse,
                commentText);
        }

        private List<string> ReplaceNamesInBlock(
            IReadOnlyDictionary<string, WordReplacer> tableNameMap,
            Dictionary<string, Dictionary<string, WordReplacer>> columnNameMap,
            IReadOnlyList<string> currentBlock,
            bool updateSchemaOnTables)
        {
            const int MINIMUM_COLUMN_NAME_LENGTH_TO_RENAME = 3;

            // Step through the columns loaded from the merged ColumnNameMap.txt file

            var updatedLines = new List<string>();

            // Keys in this dictionary are table names; values are the order that the table names appear in the current block of lines
            var referencedTables = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);

            // ReSharper disable once LoopCanBeConvertedToQuery
            foreach (var dataLine in currentBlock)
            {
                var updatedLine = NameUpdater.FindAndUpdateTableNames(tableNameMap, referencedTables, dataLine, updateSchemaOnTables);

                updatedLines.Add(updatedLine);
            }

            var blockUpdated = false;

            // Look for column names in updatedLines, updating as appropriate
            for (var i = 0; i < updatedLines.Count; i++)
            {
                var dataLine = updatedLines[i];

                var workingCopy = NameUpdater.UpdateColumnNames(columnNameMap, referencedTables, dataLine, false, false, MINIMUM_COLUMN_NAME_LENGTH_TO_RENAME);

                if (currentBlock[i].Equals(workingCopy))
                {
                    continue;
                }

                blockUpdated = true;
                updatedLines[i] = workingCopy;
            }

            if (blockUpdated && mOptions.VerboseOutput)
            {
                OnDebugEvent("Updated block:\n    " + string.Join("\n    ", updatedLines));
            }

            return updatedLines;
        }

        /// <summary>
        /// Case insensitive search/replace
        /// </summary>
        /// <remarks>The text to find is treated as RegEx, so be careful with symbols</remarks>
        /// <param name="dataLine"></param>
        /// <param name="textToFind"></param>
        /// <param name="replacementText"></param>
        /// <returns>Updated text</returns>
        private string ReplaceText(string dataLine, string textToFind, string replacementText)
        {
            return Regex.Replace(dataLine, textToFind, replacementText, RegexOptions.IgnoreCase);
        }

        private string ReplaceLeadingTabs(string dataLine)
        {
            var match = mLeadingTabMatcher.Match(dataLine);
            if (!match.Success)
                return dataLine;

            var spaceCount = match.Length * 4;

            return mLeadingTabMatcher.Replace(dataLine, new string(' ', spaceCount));
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

            // Skip SQL Server specific lines, including:
            //  SET ANSI_NULLS ON
            //  SET ANSI_NULLS OFF
            //  SET QUOTED_IDENTIFIER ON
            //  SET QUOTED_IDENTIFIER OFF

            // ReSharper disable once StringLiteralTypo

            if (dataLine.StartsWith("SET ANSI_NULLS O", StringComparison.OrdinalIgnoreCase) ||
                dataLine.StartsWith("SET QUOTED_IDENTIFIER O", StringComparison.OrdinalIgnoreCase) ||
                dataLine.StartsWith("GRANT EXECUTE", StringComparison.OrdinalIgnoreCase) ||
                dataLine.StartsWith("WITH SCHEMABINDING", StringComparison.OrdinalIgnoreCase))
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
            if (cachedLines.Count > 0 && string.IsNullOrWhiteSpace(cachedLines.Peek()))
            {
                cachedLines.Dequeue();
            }
        }

        private string SnakeCaseFirstWord(string targetProcedureAndParams)
        {
            var updatedLine = targetProcedureAndParams.Trim();

            var lineParts = updatedLine.Split(new[] { ' ' }, 2);

            if (lineParts.Length == 1)
            {
                return ConvertNameToSnakeCase(lineParts[0]);
            }

            var targetProcedure = lineParts[0].Equals("VerifySPAuthorized", StringComparison.OrdinalIgnoreCase)
                ? "VerifySpAuthorized"
                : lineParts[0];

            if (targetProcedure.StartsWith("dbo.", StringComparison.OrdinalIgnoreCase) && targetProcedure.Length > 4)
                targetProcedure = "public." + targetProcedure.Substring(4);

            return ConvertNameToSnakeCase(targetProcedure) + " " + lineParts[1];
        }

        private void StorePrintVariable(ICollection<string> procedureBody, Match printMatch)
        {
            var updatedVariableName = UpdateVariablePrefix("@" + printMatch.Groups["VariableName"].Value);

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
            string updatedArgumentLine;

            if (argumentNameMatch.Success)
            {
                argumentName = UpdateVariablePrefix(argumentNameMatch.Value);
                updatedArgumentLine = mVariableNameMatcher.Replace(dataLine, UpdateVariableNameEvaluator).Trim();
            }
            else
            {
                // This shouldn't normally happen, but could indicate a comment line in the argument list
                // Can also happen if a stored procedure does not have any arguments, or did not surround the argument list with parentheses
                argumentName = string.Empty;
                updatedArgumentLine = dataLine.Trim();
            }

            if (updatedArgumentLine.IndexOf("varchar", StringComparison.OrdinalIgnoreCase) > 0)
            {
                // Change any instance of varchar(10) or larger to text
                updatedArgumentLine = VarcharToText(updatedArgumentLine);
            }

            // Note that PostgreSQL 11 and 12 support IN or INOUT arguments; not OUT arguments
            if (updatedArgumentLine.IndexOf(" output,", StringComparison.OrdinalIgnoreCase) > 0)
            {
                updatedArgumentLine = "INOUT " + updatedArgumentLine.Replace(" output,", ",").Trim();
            }
            else if (updatedArgumentLine.EndsWith("output"))
            {
                updatedArgumentLine = "INOUT " + updatedArgumentLine.Substring(0, updatedArgumentLine.Length - "output".Length).Trim();
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

            // Stored procedures with smallint parameters are harder to call, since you have to explicitly cast numbers to ::smallint
            // Thus, replace both tinyint and smallint with int (aka integer or int4)
            updatedArgumentLine = ReplaceText(updatedArgumentLine, @"\b(tinyint|smallint)\b", "int");

            storedProcedureInfo.ProcedureArguments.Add(ReplaceTabs(updatedArgumentLine));
        }

        private void StoreProcedureCommentLine(StoredProcedureDDL storedProcedureInfo, string dataLine, out bool startOfDateBlock)
        {
            // Replace tabs with spaces
            // However, handle spaces in the stored procedure comment block specially

            dataLine = ReplaceText(dataLine, "#Tmp", "Tmp");
            dataLine = UpdateVariableNames(dataLine);

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

                startOfDateBlock = false;
                return;
            }

            // The line is of the form
            // **  Desc:
            // **  Auth:
            // **  Date:

            string updatedDataLine;
            if (labelMatch.Groups["Value"].Value.Length > 0)
            {
                // There is text after the label
                updatedDataLine = string.Format(
                    "**  {0}:   {1}",
                    labelMatch.Groups["Label"].Value,
                    labelMatch.Groups["Value"].Value);
            }
            else
            {
                // No text after the label
                updatedDataLine = string.Format(
                    "**  {0}:",
                    labelMatch.Groups["Label"].Value);
            }

            storedProcedureInfo.ProcedureCommentBlock.Add(ReplaceTabs(updatedDataLine));

            startOfDateBlock = labelMatch.Groups["Label"].Value.Equals("Date", StringComparison.OrdinalIgnoreCase);
        }

        private void StoreSelectAssignVariable(ICollection<string> procedureBody, Match selectAssignVariableMatch)
        {
            var updatedVariableName = UpdateVariablePrefix("@" + selectAssignVariableMatch.Groups["VariableName"].Value);

            var updatedLine = string.Format("{0}SELECT {1} INTO {2}",
                selectAssignVariableMatch.Groups["LeadingWhitespace"].Value,
                selectAssignVariableMatch.Groups["SourceColumn"].Value,
                updatedVariableName
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
            var variableName = declareMatch.Groups["VariableName"].Value;

            if (variableName.Equals("myError", StringComparison.OrdinalIgnoreCase))
            {
                // Skip this variable
                return;
            }

            // Use an @ sign here
            // UpdateVariablePrefix will change it to an underscore
            var variableDeclaration = string.Format("@{0}{1}",
            variableName,
            VarcharToText(declareMatch.Groups["DataType"].Value));

            var variableDeclarationToLower = UpdateVariablePrefix(variableDeclaration);

            string updatedDeclaration;
            var assignedValue = declareMatch.Groups["AssignedValue"].Value;

            if (string.IsNullOrWhiteSpace(assignedValue))
            {
                updatedDeclaration = ReplaceTabs(variableDeclarationToLower);
            }
            else
            {
                updatedDeclaration = ReplaceTabs(variableDeclarationToLower + ":= " + assignedValue);
            }

            storedProcedureInfo.LocalVariablesToDeclare.Add(updatedDeclaration);
        }

        private void UpdateAndAppendExecLine(ICollection<string> procedureBody, string dataLine, string trimmedLine)
        {
            var leadingWhitespace = GetLeadingWhitespace(dataLine);
            var targetProcedureAndParams = trimmedLine.Substring("exec ".Length);

            if (mOptions.ConvertNamesToSnakeCase)
            {
                var execMatch = mExecStoreReturnMatcher.Match(trimmedLine);

                if (execMatch.Success)
                {
                    targetProcedureAndParams = string.Format("{0} = {1}",
                        execMatch.Groups["TargetVariable"],
                        SnakeCaseFirstWord(execMatch.Groups["TargetProcedureAndParams"].Value));
                }
                else
                {
                    targetProcedureAndParams = SnakeCaseFirstWord(targetProcedureAndParams);
                }
            }

            var updatedLine = "Call " + UpdateVariableNames(targetProcedureAndParams);
            if (updatedLine.Contains("="))
                updatedLine = ReplaceText(updatedLine, @"\s*=\s*", " => ");

            AppendLine(procedureBody, leadingWhitespace + updatedLine);
        }

        private void UpdateAndAppendLine(ICollection<string> procedureBody, string dataLine)
        {
            dataLine = UpdateVariableNames(dataLine);
            dataLine = UpdateSetStatement(dataLine);
            dataLine = UpdatePrintStatement(dataLine);
            dataLine = UpdateFunctionNames(dataLine);

            if (dataLine.Contains("+") && dataLine.Contains("'"))
            {
                dataLine = UpdateConcatenationOperator(dataLine);
            }

            if (dataLine.IndexOf("varchar", StringComparison.OrdinalIgnoreCase) > 0)
            {
                // Change any instance of varchar(10) or larger to text
                dataLine = VarcharToText(dataLine);
            }

            if (dataLine.Trim().StartsWith("exec ", StringComparison.OrdinalIgnoreCase))
            {
                UpdateAndAppendExecLine(procedureBody, dataLine, dataLine.Trim());
                return;
            }

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

            while (true)
            {
                // The data line might have multiple instances of text like "Convert(varchar(12), @sourceVariable)"
                // Replace them one at a time
                var convertMatch = mConvertDataTypeUpdater.Match(dataLine);
                if (convertMatch.Success)
                {
                    var dataType = VarcharToText(convertMatch.Groups["DataType"].Value);
                    var variableName = convertMatch.Groups["VariableName"].Value;

                    var castCommand = string.Format("_{0}::{1}", variableName, dataType);

                    dataLine = mConvertDataTypeUpdater.Replace(dataLine, castCommand, 1);
                }
                else
                {
                    break;
                }
            }

            return dataLine;
        }

        private string UpdateConcatenationOperator(string dataLine)
        {
            var updatedLineA = mConcatenationReplacerA.Replace(dataLine, "||");
            return mConcatenationReplacerB.Replace(updatedLineA, "||");
        }

        private string UpdatePrintStatement(string dataLine)
        {
            if (!dataLine.Trim().StartsWith("Print", StringComparison.OrdinalIgnoreCase))
                return dataLine;

            var printIndex = dataLine.IndexOf("Print", StringComparison.OrdinalIgnoreCase);
            var leadingWhitespace = GetLeadingWhitespace(dataLine);

            var updatedLine = leadingWhitespace + "RAISE INFO '%'," + dataLine.Substring(printIndex + "Print".Length) + ";";

            return UpdateConcatenationOperator(updatedLine);
        }

        private string UpdateSetStatement(string dataLine)
        {
            var assignVariableMatch = mSetStatementMatcher.Match(dataLine);
            if (!assignVariableMatch.Success)
                return dataLine;

            return ReformatSetStatement(assignVariableMatch);
        }

        /// <summary>
        /// Looks for table names in cachedLines, then uses that information to update column names
        /// </summary>
        /// <param name="cachedLines"></param>
        /// <param name="tableNameMap">
        /// Dictionary where keys are the original (source) table names
        /// and values are WordReplacer classes that track the new table names and new column names in PostgreSQL
        /// </param>
        /// <param name="columnNameMap">
        /// Dictionary where keys are new table names
        /// and values are a Dictionary of mappings of original column names to new column names in PostgreSQL;
        /// names should not have double quotes around them
        /// </param>
        /// <param name="updateSchemaOnTables"></param>
        private void UpdateTableAndColumnNames(
            List<string> cachedLines,
            Dictionary<string, WordReplacer> tableNameMap,
            Dictionary<string, Dictionary<string, WordReplacer>> columnNameMap,
            bool updateSchemaOnTables)
        {
            var tablesInLine = new List<string>();
            var updatedLineIndices = new SortedSet<int>();

            var index = 0;
            while (index < cachedLines.Count)
            {
                var matchFound = FindTablesByName(tableNameMap, cachedLines[index], tablesInLine);

                if (!matchFound)
                {
                    index++;
                    continue;
                }

                var currentBlock = FindCurrentBlock(cachedLines, index, updatedLineIndices, out var blockStartIndex);

                var updatedBlock = ReplaceNamesInBlock(tableNameMap, columnNameMap, currentBlock, updateSchemaOnTables);

                for (var i = 0; i < updatedBlock.Count; i++)
                {
                    var targetIndex = blockStartIndex + i;
                    updatedLineIndices.Add(targetIndex);

                    cachedLines[targetIndex] = updatedBlock[i];
                }

                index = blockStartIndex + updatedBlock.Count;
            }
        }

        /// <summary>
        /// Change all @ symbols to _
        /// Also change to camelCase
        /// </summary>
        /// <param name="dataLine"></param>
        private string UpdateVariableNames(string dataLine)
        {
            if (!dataLine.Contains("@"))
                return dataLine;

            return mVariableNameMatcher.Replace(dataLine, UpdateVariableNameEvaluator);
        }

        /// <summary>
        /// If dataLine starts with @, change it to _
        /// Also change to camelCase
        /// </summary>
        /// <param name="dataLine"></param>
        private string UpdateVariablePrefix(string dataLine)
        {
            string textToCheck;
            if (dataLine.StartsWith("_"))
            {
                if (dataLine.Length < 2)
                    return dataLine;

                // Already converted to PostgreSQL; change back to @
                textToCheck = "@" + dataLine.Substring(1);
            }
            else
            {
                textToCheck = dataLine;
            }

            if (!mVariableStartMatcher.IsMatch(textToCheck))
                return dataLine;

            return mVariableStartMatcher.Replace(textToCheck, UpdateVariablePrefixEvaluator);
        }

        /// <summary>
        /// This method replaces the @ with _, then changes to lowercase
        /// </summary>
        /// <param name="match"></param>
        private static string UpdateVariablePrefixEvaluator(Match match)
        {
            return match.Result("_$1").ToLower();
        }

        /// <summary>
        /// This method replaces the @ with _, then changes to lowercase
        /// </summary>
        /// <param name="match"></param>
        private static string UpdateVariableNameEvaluator(Match match)
        {
            return match.Result("_$1").ToLower() + match.Result("$2");
        }

        internal string VarcharToText(string dataLine)
        {
            return mVarcharMatcher.Replace(dataLine, "text");
        }
    }
}
