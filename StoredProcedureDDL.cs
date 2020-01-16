using System.Collections.Generic;
using System.IO;

namespace SQLServer_Stored_Procedure_Converter
{
    class StoredProcedureDDL
    {

        public List<string> LocalVariablesToDeclare { get; }

        public List<string> ProcedureArguments { get; }

        public List<string> ProcedureBody { get; }

        public List<string> ProcedureCommentBlock { get; }


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
        /// <param name="procedureName"></param>
        public void Reset(string procedureName)
        {
            ProcedureName = procedureName;

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
            if (ProcedureBody.Count == 0)
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

            foreach (var item in ProcedureArguments)
            {
                writer.WriteLine("    " + item);
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
