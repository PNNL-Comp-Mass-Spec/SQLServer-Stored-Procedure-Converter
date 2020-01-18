using System.Collections.Generic;
using System.IO;

namespace SQLServer_Stored_Procedure_Converter
{
    class StoredProcedureDDL
    {
        /// <summary>
        /// True if a function, false if a stored procedure
        /// </summary>
        public bool IsFunction { get; private set; }

        /// <summary>
        /// Local variables defined in the procedure
        /// </summary>
        public List<string> LocalVariablesToDeclare { get; }

        /// <summary>
        /// List of arguments for the procedure
        /// </summary>
        public List<string> ProcedureArguments { get; }

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
        public StoredProcedureDDL(string procedureName)
        {
            LocalVariablesToDeclare = new List<string>();
            ProcedureArguments = new List<string>();
            ProcedureBody = new List<string>();
            ProcedureCommentBlock = new List<string>();

            Reset(procedureName);
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
            ProcedureBody.Clear();
            ProcedureCommentBlock.Clear();
        }

        /// <summary>
        /// Write the DDL for creating this stored procedure in PostgreSQl
        /// </summary>
        /// <param name="writer"></param>
        public void ToWriterForPostgres(StreamWriter writer)
        {
            if (string.IsNullOrWhiteSpace(ProcedureName) || ProcedureBody.Count == 0)
                return;

            writer.WriteLine();

            var createProcedure = "CREATE OR REPLACE PROCEDURE " + ProcedureName;

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

            // The comment block must appear after $$, otherwise it will be discarded (and not associated with the procedure)
            foreach (var item in ProcedureCommentBlock)
            {
                writer.WriteLine(item);
            }

            if (LocalVariablesToDeclare.Count > 0)
            {
                writer.WriteLine("DECLARE");
                foreach (var item in LocalVariablesToDeclare)
                {
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
        }
    }
}
