using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using PRISM;

namespace SQLServer_Stored_Procedure_Converter
{
    public class StoredProcedureConverterOptions
    {
        /// <summary>
        /// Program date
        /// </summary>
        public const string PROGRAM_DATE = "April 14, 2022";

        [Option("StoredProcFile", "Input", "I", ArgPosition = 1, Required = true,
            HelpShowsDefault = false, IsInputFilePath = true,
            HelpText = "File with SQL Server stored procedures to convert")]
        public string SQLServerStoredProcedureFile { get; set; }

        [Option("OutputFile", "O", ArgPosition = 2, HelpShowsDefault = false,
            HelpText = "Output file path")]
        public string OutputFilePath { get; set; }

        [Option("Schema", HelpShowsDefault = false,
            HelpText = "Schema to use for stored procedures")]
        public string SchemaName { get; set; } = "public";

        [Option("Map", "M", HelpShowsDefault = false, IsInputFilePath = true,
            HelpText = "Column name map file (typically created by sqlserver2pgsql.pl); tab-delimited file with five columns:\n" +
                       "SourceTable  SourceName  Schema  NewTable  NewName")]
        public string ColumnNameMapFile { get; set; }

        [Option("SnakeCase", "Snake", HelpShowsDefault = true,
            HelpText = "When true, convert procedure and function names to snake_case")]
        public bool ConvertNamesToSnakeCase { get; set; } = true;

        /// <summary>
        /// List of stored procedures to skip when converting the source file
        /// </summary>
        public SortedSet<string> StoredProcedureNamesToSkip { get; } = new(StringComparer.OrdinalIgnoreCase);

        [Option("StoredProceduresToSkip", "SkipList", HelpShowsDefault = false,
            HelpText = "Comma separated list of stored procedure names to skip while converting the source file")]
        // ReSharper disable once UnusedMember.Global
        public string StoredProceduresToSkip
        {
            get => string.Join(", ", StoredProcedureNamesToSkip);
            set
            {
                StoredProcedureNamesToSkip.Clear();
                if (string.IsNullOrWhiteSpace(value))
                    return;

                foreach (var item in value.Split(','))
                {
                    StoredProcedureNamesToSkip.Add(item.Trim());
                }
            }
        }

        [Option("Verbose", "V", HelpShowsDefault = true,
            HelpText = "When true, display each updated code block")]
        public bool VerboseOutput { get; set; }

        /// <summary>
        /// Constructor
        /// </summary>
        public StoredProcedureConverterOptions()
        {
            SQLServerStoredProcedureFile = string.Empty;
            OutputFilePath = string.Empty;
            ColumnNameMapFile = string.Empty;
        }

        /// <summary>
        /// Get the program version
        /// </summary>
        public static string GetAppVersion()
        {
            return Assembly.GetExecutingAssembly().GetName().Version + " (" + PROGRAM_DATE + ")";
        }

        /// <summary>
        /// Show the options at the console
        /// </summary>
        public void OutputSetOptions()
        {
            Console.WriteLine("Options:");
            Console.WriteLine();

            Console.WriteLine(" {0,-35} {1}", "Input file with stored procedures:", PathUtils.CompactPathString(SQLServerStoredProcedureFile, 80));

            Console.WriteLine(" {0,-35} {1}", "Output file path:", PathUtils.CompactPathString(OutputFilePath, 80));

            if (!string.IsNullOrWhiteSpace(SchemaName))
            {
                Console.WriteLine(" {0,-35} {1}", "Schema name:", SchemaName);
            }

            if (!string.IsNullOrWhiteSpace(ColumnNameMapFile))
            {
                Console.WriteLine(" {0,-35} {1}", "Column name map file:", PathUtils.CompactPathString(ColumnNameMapFile, 80));
            }

            Console.WriteLine(" {0,-35} {1}", "Convert names to snake case:", ConvertNamesToSnakeCase);

            Console.WriteLine();
        }

        /// <summary>
        /// Validate the options
        /// </summary>
        public bool ValidateArgs(out string errorMessage)
        {
            if (string.IsNullOrWhiteSpace(SQLServerStoredProcedureFile))
            {
                errorMessage = "Use /I to specify the input file";
                return false;
            }

            if (string.IsNullOrWhiteSpace(OutputFilePath))
            {
                OutputFilePath = GetDefaultOutputFilePath();
            }

            errorMessage = string.Empty;

            return true;
        }

        public string GetDefaultOutputFilePath()
        {
            return GetDefaultOutputFilePath(SQLServerStoredProcedureFile);
        }

        public string GetDefaultOutputFilePath(string inputFilePath)
        {
            var inputFile = new FileInfo(inputFilePath);

            var newFileName = Path.GetFileNameWithoutExtension(inputFile.Name) + "_postgres.sql";

            return string.IsNullOrEmpty(inputFile.DirectoryName) ? newFileName : Path.Combine(inputFile.DirectoryName, newFileName);
        }
    }
}
