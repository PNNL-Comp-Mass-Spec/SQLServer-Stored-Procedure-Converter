using System;
using System.IO;
using System.Reflection;
using System.Threading;
using PRISM;

namespace SQLServer_Stored_Procedure_Converter
{
    internal class Program
    {
        // Ignore Spelling: Conf

        private static DateTime mLastProgressTime;

        /// <summary>
        /// The main entry point for the application.
        /// </summary>
        private static int Main(string[] args)
        {
            mLastProgressTime = DateTime.UtcNow;

            var asmName = typeof(Program).GetTypeInfo().Assembly.GetName();
            var exeName = Path.GetFileName(Assembly.GetExecutingAssembly().Location);       // Alternatively: System.AppDomain.CurrentDomain.FriendlyName
            var version = StoredProcedureConverterOptions.GetAppVersion();

            var parser = new CommandLineParser<StoredProcedureConverterOptions>(asmName.Name, version)
            {
                ProgramInfo = "This program converts SQL Server stored procedures to PostgreSQL compatible stored procedures. " +
                              "The converted procedures will typically need additional manual adjustments to become usable.",

                ContactInfo = "Program written by Matthew Monroe for the Department of Energy" + Environment.NewLine +
                              "(PNNL, Richland, WA) in 2020" +
                              Environment.NewLine + Environment.NewLine +
                              "E-mail: matthew.monroe@pnnl.gov or proteomics@pnnl.gov" + Environment.NewLine +
                              "Website: https://github.com/PNNL-Comp-Mass-Spec/ or https://panomics.pnnl.gov/ or https://www.pnnl.gov/integrative-omics",

                UsageExamples = {
                        exeName +
                        " StoredProcedures_SQLServer.sql",
                        exeName +
                        " StoredProcedures_SQLServer.sql /O:StoredProcedures_postgres.sql"
                    }
            };

            parser.AddParamFileKey("Conf");

            var parseResults = parser.ParseArgs(args);
            var options = parseResults.ParsedResults;

            try
            {
                if (!parseResults.Success)
                {
                    Thread.Sleep(1500);
                    return -1;
                }

                if (!options.ValidateArgs(out var errorMessage))
                {
                    parser.PrintHelp();

                    Console.WriteLine();
                    ConsoleMsgUtils.ShowWarning("Validation error:");
                    ConsoleMsgUtils.ShowWarning(errorMessage);

                    Thread.Sleep(1500);
                    return -1;
                }

                options.OutputSetOptions();
            }
            catch (Exception e)
            {
                Console.WriteLine();
                Console.Write($"Error running {exeName}");
                Console.WriteLine(e.Message);
                Console.WriteLine($"See help with {exeName} --help");
                return -1;
            }

            try
            {
                var procedureConverter = new StoredProcedureConverter(options);

                procedureConverter.ProgressUpdate += Processor_ProgressUpdate;

                var success = procedureConverter.ProcessFile(options.SQLServerStoredProcedureFile);

                if (success)
                {
                    Console.WriteLine();
                    Console.WriteLine("Processing complete");
                    Thread.Sleep(250);
                    return 0;
                }

                ConsoleMsgUtils.ShowWarning("Processing error");
                Thread.Sleep(2000);
                return -1;
            }
            catch (Exception ex)
            {
                ConsoleMsgUtils.ShowError("Error occurred in Program->Main", ex);
                Thread.Sleep(2000);
                return -1;
            }
        }

        private static void Processor_DebugEvent(string message)
        {
            ConsoleMsgUtils.ShowDebug(message);
        }

        private static void Processor_ProgressUpdate(string progressMessage, float percentComplete)
        {
            if (DateTime.UtcNow.Subtract(mLastProgressTime).TotalSeconds < 3)
                return;

            Console.WriteLine();
            mLastProgressTime = DateTime.UtcNow;
            Processor_DebugEvent(percentComplete.ToString("0.0") + "%, " + progressMessage);
        }
    }
}
