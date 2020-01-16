using System;
using System.Collections.Generic;
using System.IO;
using System.Text.RegularExpressions;
using PRISM;

namespace SQLServer_Stored_Procedure_Converter
{
    class StoredProcedureConverter : EventNotifier
    {
        private readonly Regex mCommentBlockLabelMatcher = new Regex(
            @"^\*\*\s+(?<Label>Desc|Auth|Date):\s*(?<Value>.*)",
            RegexOptions.Compiled | RegexOptions.IgnoreCase);

        private readonly Regex mVarcharMatcher = new Regex(
            @"n*varchar\(\d{2,}\)",
            RegexOptions.Compiled | RegexOptions.IgnoreCase);

        private readonly StoredProcedureConverterOptions mOptions;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="options"></param>
        public StoredProcedureConverter(StoredProcedureConverterOptions options)
        {
            mOptions = options;
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
                var procedureNameMatcher = new Regex(
                    @"\[[^\]]+\]\.\[(?<ProcedureName>[^\]]+)\]",
                    RegexOptions.Compiled | RegexOptions.IgnoreCase);

                var declareAndAssignMatcher = new Regex(
                    @"^(?<LeadingWhitespace>\s*)Declare\s+@(?<VariableName>[^\s]+)(?<DataType>[^=]+)\s*=\s*(?<AssignedValue>.+)",
                    RegexOptions.Compiled | RegexOptions.IgnoreCase);

                var declareMatcher = new Regex(
                    @"^(?<LeadingWhitespace>\s*)Declare\s+@(?<VariableName>[^\s]+)(?<DataType>[^=]+)",
                    RegexOptions.Compiled | RegexOptions.IgnoreCase);

                var setVariableMatcher = new Regex(
                    @"^(?<LeadingWhitespace>\s*)Set\s+@(?<VariableName>[^=]+)\s*=\s*(?<AssignedValue>.*)",
                    RegexOptions.Compiled | RegexOptions.IgnoreCase);

                var endStatementMatcher = new Regex(
                    @"^(?<LeadingWhitespace>\s*)End\b(?<ExtraInfo>.*)",
                    RegexOptions.Compiled | RegexOptions.IgnoreCase);

                var schemaName = string.IsNullOrWhiteSpace(mOptions.SchemaName) ? "public" : mOptions.SchemaName;

                var foundStartOfProcedureCommentBlock = false;
                var foundEndOfProcedureCommentBlock = false;

                var foundArgumentListStart = false;
                var foundArgumentListEnd = false;

                var cachedLineAvailable = false;
                var cachedNextLine = string.Empty;

                var skipNextLineIfGo = false;

                var storedProcedureInfo = new StoredProcedureDDL(string.Empty);

                // Stacks track objects using last in, first out (LIFO)
                var controlBlockStack = new Stack<string>();

                using (var reader = new StreamReader(new FileStream(inputFile.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite)))
                using (var writer = new StreamWriter(new FileStream(outputFile.FullName, FileMode.Create, FileAccess.Write, FileShare.Read)))
                {
                    while (!reader.EndOfStream)
                    {
                        string dataLine;
                        if (cachedLineAvailable)
                        {
                            dataLine = string.Copy(cachedNextLine);
                            cachedLineAvailable = false;
                        }
                        else
                        {
                            dataLine = reader.ReadLine();
                        }


                        // Skip lines that are null, but don't skip blank lines
                        if (dataLine == null)
                            continue;

                        if (skipNextLineIfGo && dataLine.StartsWith("GO"))
                        {
                            skipNextLineIfGo = false;
                            continue;
                        }

                        if (SkipLine(dataLine, out skipNextLineIfGo))
                            continue;

                        if (dataLine.StartsWith("CREATE PROCEDURE", StringComparison.OrdinalIgnoreCase))
                        {
                            storedProcedureInfo.ToWriterForPostgres(writer);

                            foundStartOfProcedureCommentBlock = false;
                            foundEndOfProcedureCommentBlock = false;
                            foundArgumentListStart = false;
                            foundArgumentListEnd = false;

                            skipNextLineIfGo = false;

                            var matchedName = procedureNameMatcher.Match(dataLine);
                            string procedureNameWithSchema;
                            if (matchedName.Success)
                            {
                                procedureNameWithSchema = schemaName + "." + matchedName.Groups["ProcedureName"].Value;
                            }
                            else
                            {
                                procedureNameWithSchema = schemaName + "." + dataLine.Substring("CREATE PROCEDURE".Length + 1);
                            }

                            storedProcedureInfo.Reset(procedureNameWithSchema);

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

                        // Replace tabs with spaces
                        var declareAndAssignMatch = declareAndAssignMatcher.Match(dataLine);
                        if (declareAndAssignMatch.Success)
                        {
                            StoreVariableToDeclare(storedProcedureInfo, declareAndAssignMatch);
                            StoreVariableAssignment(storedProcedureInfo, declareAndAssignMatch);
                            continue;
                        }

                        var declareMatch = declareMatcher.Match(dataLine);
                        if (declareMatch.Success)
                        {
                            StoreVariableToDeclare(storedProcedureInfo, declareAndAssignMatch);
                            continue;
                        }

                        var assignVariableMatch = setVariableMatcher.Match(dataLine);
                        if (assignVariableMatch.Success)
                        {
                            StoreVariableAssignment(storedProcedureInfo, assignVariableMatch);
                            continue;
                        }

                        dataLine = ReplaceText(dataLine, "IsNull", "Coalesce");

                        var endMatch = endStatementMatcher.Match(dataLine);
                        if (endMatch.Success && controlBlockStack.Count > 0)
                        {
                            var leadingWhitespace = endMatch.Groups["LeadingWhitespace"].Value.Replace("\t", "    ");

                            var extraInfo = endMatch.Groups["ExtraInfo"].Success ? endMatch.Groups["ExtraInfo"].Value : string.Empty;

                            var controlBlock = controlBlockStack.Pop();
                            switch (controlBlock)
                            {
                                case "if":
                                    storedProcedureInfo.ProcedureBody.Add(leadingWhitespace + "End If" + extraInfo);
                                    continue;

                                case "while":
                                    storedProcedureInfo.ProcedureBody.Add(leadingWhitespace + "End Loop" + extraInfo);
                                    continue;
                            }
                        }

                        if (dataLine.Trim().StartsWith("If "))
                        {
                            // If statement
                            // Change to "If ... Then"
                            storedProcedureInfo.ProcedureBody.Add(ReplaceTabs(dataLine) + " Then");

                            // Peak at the next line
                            // If it starts with Begin, push "if" onto controlBlockStack but skip the line
                            // If starts with Else, write out the line, then peak at the next line
                            //    If the line after Else starts with Begin, push "if" onto controlBlockStack and skip the Begin
                            // If it does not start with Begin, write the line, then auto-add End If

                            var nextLine = reader.ReadLine();

                            throw new NotImplementedException("ToDo");

                            cachedLineAvailable = true;
                            cachedNextLine = nextLine;

                            controlBlockStack.Push("if");

                            continue;

                        }

                        if (dataLine.Trim().StartsWith("While "))
                        {
                            // While statement
                            // Change to "While ... Loop"
                            storedProcedureInfo.ProcedureBody.Add(ReplaceTabs(dataLine) + " Loop");

                            // Assume the next line starts with Begin
                            controlBlockStack.Push("while");
                        }

                        storedProcedureInfo.ProcedureBody.Add(ReplaceTabs(dataLine));
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

        private bool SkipLine(string dataLine, out bool skipNextLineIfGo)
        {

            if (dataLine.StartsWith("/****** Object: "))
            {
                skipNextLineIfGo = false;
                return true;
            }

            if (dataLine.StartsWith("SET ANSI_NULLS ON") ||
                dataLine.StartsWith("SET QUOTED_IDENTIFIER ON"))
            {
                skipNextLineIfGo = true;
                return true;
            }

            skipNextLineIfGo = false;
            return false;
        }

        private string ReplaceText(string dataLine, string textToFind, string replacementText)
        {
            return Regex.Replace(dataLine, textToFind, replacementText, RegexOptions.IgnoreCase);
        }

        private string ReplaceTabs(string dataLine)
        {
            return dataLine.Replace("\t", "    ").TrimEnd();
        }

        private void StoreProcedureArgument(StoredProcedureDDL storedProcedureInfo, string dataLine)
        {

            if (string.IsNullOrWhiteSpace(dataLine))
                return;

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
                updatedArgumentLine = "OUT " + updatedArgumentLine.Substring(updatedArgumentLine.Length - "output".Length).Trim();
            }

            storedProcedureInfo.ProcedureArguments.Add(ReplaceTabs(updatedArgumentLine));
        }

        private void StoreProcedureCommentLine(StoredProcedureDDL storedProcedureInfo, string dataLine)
        {
            // Replace tabs with spaces
            // However, handle spaces in the stored procedure comment block specially

            var labelMatch = mCommentBlockLabelMatcher.Match(dataLine);
            if (labelMatch.Success)
            {
                string updatedDataLine;
                if (labelMatch.Groups["Value"].Success)
                {
                    updatedDataLine = string.Format(
                        "**  {0}   {1}",
                        labelMatch.Groups["Label"].Value,
                        labelMatch.Groups["Value"].Value);
                }
                else
                {
                    updatedDataLine = string.Format(
                        "**  {0}",
                        labelMatch.Groups["Label"].Value);
                }

                storedProcedureInfo.ProcedureCommentBlock.Add(ReplaceTabs(updatedDataLine));
            }
            else
            {
                storedProcedureInfo.ProcedureCommentBlock.Add(ReplaceTabs(dataLine));
            }
        }

        private void StoreVariableAssignment(StoredProcedureDDL storedProcedureInfo, Match reMatch)
        {
            var assignedValue = reMatch.Groups["AssignedValue"].Success ? reMatch.Groups["AssignedValue"].Value : string.Empty;

            var variableAssignment = string.Format("{0}_{1} := {2}",
                reMatch.Groups["LeadingWhitespace"].Value,
                reMatch.Groups["VariableName"].Value,
                assignedValue);

            storedProcedureInfo.ProcedureBody.Add(ReplaceTabs(variableAssignment));

        }

        private void StoreVariableToDeclare(StoredProcedureDDL storedProcedureInfo, Match reMatch)
        {

            var variableDeclaration = string.Format("_{0} {1}",
                reMatch.Groups["VariableName"].Value,
                VarcharToText(reMatch.Groups["DataType"].Value));

            storedProcedureInfo.LocalVariablesToDeclare.Add(ReplaceTabs(variableDeclaration));
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
