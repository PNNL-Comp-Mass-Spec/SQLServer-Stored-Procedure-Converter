using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace SQLServer_Stored_Procedure_Converter
{
    internal class StoredProcedureDDL
    {
        // Ignore Spelling: auth, plpgsql

        /// <summary>
        /// True if a function, false if a stored procedure
        /// </summary>
        public bool IsFunction { get; private set; }

        /// <summary>
        /// Local variables defined in the procedure
        /// </summary>
        public List<string> LocalVariablesToDeclare { get; }

        /// <summary>
        /// Processing options
        /// </summary>
        public StoredProcedureConverterOptions Options { get; }

        /// <summary>
        /// List of arguments for the procedure
        /// </summary>
        public List<string> ProcedureArguments { get; }

        /// <summary>
        /// Comments associated with the procedure arguments
        /// Keys are the argument name, values are the comment (without --)
        /// </summary>
        /// <remarks>
        /// This comments will be added to the procedure comment block
        /// </remarks>
        public List<KeyValuePair<string, string>> ProcedureArgumentComments { get; }

        /// <summary>
        /// Main body of the procedure
        /// </summary>
        public List<string> ProcedureBody { get; }

        /// <summary>
        /// Comment block
        /// </summary>
        /// <remarks>
        /// Inserted between the AS and DECLARE keywords
        /// </remarks>
        public List<string> ProcedureCommentBlock { get; }

        /// <summary>
        /// Procedure name
        /// </summary>
        public string ProcedureName { get; private set; }

        /// <summary>
        /// Constructor
        /// </summary>
        public StoredProcedureDDL(StoredProcedureConverterOptions options, string procedureName)
        {
            Options = options;

            LocalVariablesToDeclare = new List<string>();
            ProcedureArguments = new List<string>();
            ProcedureArgumentComments = new List<KeyValuePair<string, string>>();
            ProcedureBody = new List<string>();
            ProcedureCommentBlock = new List<string>();

            Reset(procedureName);
        }

        /// <summary>
        /// Get the object name, without the schema
        /// </summary>
        /// <param name="objectName"></param>
        public static string GetNameWithoutSchema(string objectName)
        {
            if (string.IsNullOrWhiteSpace(objectName))
                return string.Empty;

            var periodIndex = objectName.IndexOf('.');
            if (periodIndex > 0 && periodIndex < objectName.Length - 1)
                return objectName.Substring(periodIndex + 1);

            return objectName;
        }

        /// <summary>
        /// Clear all cached data
        /// </summary>
        /// <param name="procedureName">Procedure or function name</param>
        /// <param name="isFunction">True if this DDL is for a function; false for a stored procedure</param>
        public void Reset(string procedureName, bool isFunction = false)
        {
            ProcedureName = procedureName;
            IsFunction = isFunction;

            LocalVariablesToDeclare.Clear();
            ProcedureArguments.Clear();
            ProcedureArgumentComments.Clear();
            ProcedureBody.Clear();
            ProcedureCommentBlock.Clear();
        }

        /// <summary>
        /// Write the DDL for creating this stored procedure in PostgreSQL
        /// </summary>
        /// <param name="writer"></param>
        public void ToWriterForPostgres(StreamWriter writer)
        {
            if (string.IsNullOrWhiteSpace(ProcedureName) || ProcedureBody.Count == 0)
                return;

            writer.WriteLine();

            var newProcedureName = Options.ConvertNamesToSnakeCase
                ? StoredProcedureConverter.ConvertNameToSnakeCase(ProcedureName)
                : ProcedureName;

            var createProcedure = "CREATE OR REPLACE PROCEDURE " + newProcedureName;

            if (ProcedureArguments.Count == 0)
            {
                writer.WriteLine(createProcedure + "()");
            }
            else
            {
                writer.WriteLine(createProcedure);
                writer.WriteLine("(");
                foreach (var item in ProcedureArguments)
                {
                    writer.WriteLine("    " + item);
                }
                writer.WriteLine(")");
            }

            writer.WriteLine("LANGUAGE plpgsql");
            writer.WriteLine("AS $$");

            // The procedure comment block must appear after $$, otherwise it will be discarded (and not associated with the procedure)
            var argumentCommentsAdded = false;

            foreach (var item in ProcedureCommentBlock)
            {
                if (ProcedureArgumentComments.Count > 0)
                {
                    if (item.StartsWith("**  Auth:"))
                    {
                        WriteArgumentComments(writer);
                        argumentCommentsAdded = true;
                    }
                    else if (!argumentCommentsAdded && item.EndsWith("********/"))
                    {
                        WriteArgumentComments(writer);
                        argumentCommentsAdded = true;
                    }
                }
                writer.WriteLine(item);
            }

            if (ProcedureArgumentComments.Count > 0 && !argumentCommentsAdded)
            {
                writer.WriteLine("/******************");
                WriteArgumentComments(writer);
                writer.WriteLine("******************/");
            }

            if (LocalVariablesToDeclare.Count > 0)
            {
                writer.WriteLine("DECLARE");
                foreach (var item in LocalVariablesToDeclare)
                {
                    if (item.StartsWith("_myRowCount", StringComparison.OrdinalIgnoreCase))
                    {
                        writer.WriteLine("    _myRowCount int := 0;");
                        continue;
                    }

                    writer.WriteLine("    " + item + ";");
                }
            }

            writer.WriteLine("BEGIN");

            foreach (var item in ProcedureBody)
            {
                writer.WriteLine(item);
            }

            writer.WriteLine("END");
            writer.WriteLine("$$;");
            writer.WriteLine();

            var procedureNameWithoutSchema = GetNameWithoutSchema(ProcedureName);
            writer.WriteLine("COMMENT ON PROCEDURE {0} IS '{1}';", newProcedureName, procedureNameWithoutSchema);
        }

        private void WriteArgumentComments(TextWriter writer)
        {
            if (ProcedureArgumentComments.Count == 0)
                return;

            // Find the longest argument name by examining the Keys in ProcedureArgumentComments
            var maxArgNameLength = ProcedureArgumentComments.Max(argumentComment => argumentComment.Key.Length);

            writer.WriteLine("**  Arguments:");

            // Format string will be of the form
            // "**    {0,-12} {1}"
            var formatString = string.Format("**    {{0,-{0}}} {{1}}", maxArgNameLength + 2);

            foreach (var argumentComment in ProcedureArgumentComments)
            {
                writer.WriteLine(formatString, argumentComment.Key, argumentComment.Value);
            }
            writer.WriteLine("**");
        }
    }
}
