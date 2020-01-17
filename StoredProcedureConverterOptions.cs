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
        public const string PROGRAM_DATE = "January 15, 2020";

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

        #endregion

        /// <summary>
        /// Constructor
        /// </summary>
        public StoredProcedureConverterOptions()
        {
            OutputFilePath = string.Empty;

            SQLServerStoredProcedureFile = string.Empty;
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

            Console.WriteLine(" {0,-48} {1}", "Input file with stored procedures:", SQLServerStoredProcedureFile);

            Console.WriteLine(" {0,-48} {1}", "Output file path:", OutputFilePath);

            if (!string.IsNullOrWhiteSpace(SchemaName))
            {
                Console.WriteLine(" {0,-48} {1}", "Schema name:", SchemaName);
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
