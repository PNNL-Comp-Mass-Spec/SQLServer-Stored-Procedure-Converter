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
  [/ParamFile:ParamFileName.conf] [/CreateParamFile]
```

The input file should be a SQL text file with CREATE PROCEDURE statements

Optionally use `/O` to specify the output file path

Use `/Schema` to specify schema name prefix procedure names with

The processing options can be specified in a parameter file using `/ParamFile:Options.conf` or `/Conf:Options.conf`
* Define options using the format `ArgumentName=Value`
* Lines starting with `#` or `;` will be treated as comments
* Additional arguments on the command line can supplement or override the arguments in the parameter file

Use `/CreateParamFile` to create an example parameter file
* By default, the example parameter file content is shown at the console
* To create a file named Options.conf, use `/CreateParamFile:Options.conf`

## Contacts

Written by Matthew Monroe for the Department of Energy (PNNL, Richland, WA) \
E-mail: matthew.monroe@pnnl.gov or matt@alchemistmatt.com\
Website: https://omics.pnl.gov/ or https://panomics.pnnl.gov/

## License

Licensed under the 2-Clause BSD License; you may not use this file except
in compliance with the License.  You may obtain a copy of the License at
https://opensource.org/licenses/BSD-2-Clause

Copyright 2020 Battelle Memorial Institute
