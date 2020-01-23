using System;
using System.IO;
using System.Reflection;
using PRISM;

namespace SQLServer_Stored_Procedure_Converter
{
    public class StoredProcedureConverterOptions
    {
        #region "Constants and Enums"

        /// <summary>
        /// Program date
        /// </summary>
        public const string PROGRAM_DATE = "January 17, 2020";

        #endregion

        #region "Properties"

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

        #endregion

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
        /// <returns></returns>
        public static string GetAppVersion()
        {
            var version = Assembly.GetExecutingAssembly().GetName().Version + " (" + PROGRAM_DATE + ")";

            return version;
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

            Console.WriteLine();
        }

        /// <summary>
        /// Validate the options
        /// </summary>
        /// <returns></returns>
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
