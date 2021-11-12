# SQL Server Stored Procedure Converter

This program converts SQL Server stored procedures to PostgreSQL compatible stored procedures.
The converted procedures will typically need additional manual adjustments to become usable.

## Console Switches

SQLServer_Stored_Procedure_Converter is a console application, and must be run from the Windows command prompt.

```
SQLServer_Stored_Procedure_Converter.exe
  /I:InputFilePath
  /O:OutputFilePath
  [/Schema:SchemaName]
  [/Map:NameMapFile.txt]
  [/SkipList:StoredProcedureSkipiList]
  [/ParamFile:ParamFileName.conf] [/CreateParamFile]
```

The input file should be a SQL text file with CREATE PROCEDURE statements

Optionally use `/O` to specify the output file path

Use `/Schema` to specify schema name prefix procedure names with

The `/Map` file is is a tab-delimited text file with five columns
* The Map file matches the format of the merged NameMap file created by the PgSQL View Creator Helper (https://github.com/PNNL-Comp-Mass-Spec/PgSQL-View-Creator-Helper)
* It also matches the file created by sqlserver2pgsql (https://github.com/PNNL-Comp-Mass-Spec/sqlserver2pgsql)
* Example data:

| SourceTable   | SourceName           | Schema | NewTable        | NewName               |
|---------------|----------------------|--------|-----------------|-----------------------|
| T_Log_Entries | Entry_ID             | mc     | t_log_entries   | entry_id              |
| T_Log_Entries | posted_by            | mc     | t_log_entries   | posted_by             |
| T_Log_Entries | posting_time         | mc     | t_log_entries   | posting_time          |
| T_Log_Entries | type                 | mc     | t_log_entries   | type                  |
| T_Log_Entries | message              | mc     | t_log_entries   | message               |
| T_Log_Entries | Entered_By           | mc     | t_log_entries   | entered_by            |
| T_Mgrs        | m_id                 | mc     | t_mgrs          | mgr_id                |
| T_Mgrs        | m_name               | mc     | t_mgrs          | mgr_name              |
| T_Mgrs        | mgr_type_id          | mc     | t_mgrs          | mgr_type_id           |
| T_Mgrs        | param_value_changed  | mc     | t_mgrs          | param_value_changed   |
| T_Mgrs        | control_from_website | mc     | t_mgrs          | control_from_website  |
| T_Mgrs        | comment              | mc     | t_mgrs          | comment               |
| T_Mgrs        | M_TypeID             | mc     | t_mgrs          | mgr_type_id           |
| T_Mgrs        | M_ParmValueChanged   | mc     | t_mgrs          | param_value_changed   |
| T_Mgrs        | M_ControlFromWebsite | mc     | t_mgrs          | control_from_website  |
| T_Mgrs        | M_Comment            | mc     | t_mgrs          | comment               |

Use `/SkipList` or `/StoredProceduresToSkip` to define a comma separated list of stored procedures to skip while parsing the input file

The processing options can be specified in a parameter file using `/ParamFile:Options.conf` or `/Conf:Options.conf`
* Define options using the format `ArgumentName=Value`
* Lines starting with `#` or `;` will be treated as comments
* Additional arguments on the command line can supplement or override the arguments in the parameter file

Use `/CreateParamFile` to create an example parameter file
* By default, the example parameter file content is shown at the console
* To create a file named Options.conf, use `/CreateParamFile:Options.conf`

## Contacts

Written by Matthew Monroe for the Department of Energy (PNNL, Richland, WA) \
E-mail: matthew.monroe@pnnl.gov or proteomics@pnnl.gov\
Website: https://github.com/PNNL-Comp-Mass-Spec/ or https://panomics.pnnl.gov/ or https://www.pnnl.gov/integrative-omics/
Source code: https://github.com/PNNL-Comp-Mass-Spec/SQLServer-Stored-Procedure-Converter

## License

Licensed under the 2-Clause BSD License; you may not use this program except
in compliance with the License.  You may obtain a copy of the License at
https://opensource.org/licenses/BSD-2-Clause

Copyright 2020 Battelle Memorial Institute
