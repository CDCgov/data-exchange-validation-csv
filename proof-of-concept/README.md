A place to store proof-of-concept research done in determining which tools to use.

NOTE: the large test file used for these is too big to be checked into git. It was created by taking the provided example file, and then copying its rows until it was large enough for the test


# Determining which third-party tool to use
## Assumptions​
* Validating the file should be fast and reliable​
* If a CDC Program wants DEX to perform specific validations, then that program will provide a machine-readable specification​
  * The specification file is required to be in a specific format, which the DEX team will determine based on the choice of validation tool​
  * Basic validation (filetype) will be the only offering without a previously provided schema​
* A CDC Program providing a new or updated specification will not generally require redeploying any part of the pipeline​
* We are only considering asynchronous validations​
* Parsing and Validation will be implemented using third-party, open-source libraries​

## Potential Solution – Convert to JSON​
* JSON is more standardized than CSV in general, and thus has a standardized [Schema](https://json-schema.org/learn/getting-started-step-by-step.html​) format​
* We can use any parser to read the CSV and validate against RFC 4180, then convert the data to JSON format and validate against JSON Schema​
* Many different open-source libraries exist for this validation, all using the same standardized Schema format​
* Could then also apply this approach to other file formats in the future​

## JVM Libraries
| Name​                         | RFC 4180 Parsing​ | Input Data Stream​ | Chunking[^1]​ | Codeless Schema[^2]​ | Comprehensive Validation[^3]​ |
| ---                          | ---              | ---               | ---          | ---                 | ---                          |
| Matt Krystoff Drools Library​ | YES​              | NO​                | YES​          | NO​                  | YES​                          |
| Digital Preservation​         | YES​              | YES​               | NO​           | YES (ish)​           | YES​                          |
| OpenCSV​                      | YES​              | YES​               | YES​          | NO​                  | YES​                          |
| Jackson CSV​                  | YES​              | YES​               | YES​          | NO​                  | YES​                          |
| Spata​                        | YES​              | YES​               | NO​           | NO​                  | YES​                          |
| Apache Commons CSV​           | YES​              | YES​               | YES​          | -​                   | -​                            |
| SuperCSV​                     | NO​               | YES​               | NO​           | NO​                  | YES​                          |
| Ostermiller Utils​            | NO​               | YES​               | NO​           | -​                   | -​                            |
| CSV-Schema​                   | NO​               | NO​                | NO​           | YES​                 | NO​                           |
| Valiktor​                     | -​                | -​                 | -​            | NO​                  | YES​                          |
| NetworkNT JSON​               | -​                | -​                 | -​            | YES​                 | YES​                          |

[^1]: Chunking – Tool has built-in ability to separate a large file into chunks for multithreading​
[^2]: Codeless Schema – Schema is specified outside of codebase and does not require compile and deploy to handle schema changes
[^3]: Comprehensive Validation – Tool can handle the row-level validations currently required (non-empty field, group of fields in which one is non-empty, and value from prescribed list)

## Python Libraries
| Name​                         | RFC 4180 Parsing​ | Input Data Stream​ | Chunking[^1]​ | Codeless Schema[^2]​ | Comprehensive Validation[^3]​ |
| ---                          | ---              | ---               | ---          | ---                 | ---                          |
| Pandas​                       | YES​              | YES​               | YES​          | -​                   | -​                            |
| CSV DictReader​               | YES​              | YES​               | YES​          | -​                   | -​                            |
| Frictionless​                 | YES​              | NO​                | NO​           | NO​                  | NO​                           |
| CSV-Schema​                   | NO​               | NO​                | NO​           | YES​                 | NO​                           |
| Cerberus​                     | -​                | -​                 | -​            | YES​                 | NO​                           |
| Pandas-validation​            | -​                | -​                 | -​            | NO​                  | YES​                          |
| Pandera​                      | -​                | -​                 | -​            | NO​                  | YES​                          |
| Pandas-schema​                | -​                | -​                 | -​            | NO​                  | YES​                          |
| Pydantic​                     | -​                | -​                 | -​            | NO​                  | YES​                          |

[^1]: Chunking – Tool has built-in ability to separate a large file into chunks for multithreading​
[^2]: Codeless Schema – Schema is specified outside of codebase and does not require compile and deploy to handle schema changes
[^3]: Comprehensive Validation – Tool can handle the row-level validations currently required (non-empty field, group of fields in which one is non-empty, and value from prescribed list)

## Validate Metrics (local machine, not Azure)
Machine Specs:​
* Processor - Intel(R) Core(TM) i7-10850H CPU @ 2.70GHz   2.71 GHz​
* RAM - 32 GB​
* OS - Windows 10, 64-bit​
| Tool​                                                         | POC Test​                                         | Run Time   |
| ---                                                          | ---                                              | ---        |
| JVM Digital Preservation​                                     | 1 GB file <br> Required and conditionally fields ​| 3 minutes​​​  |
| Python CSV-Schema​                                            | 1 GB file ​<br> Required fields ​                  | 2 minutes​​​  |
| Python Pandas+Cerberus​                                       | 5 GB file ​<br> Required and conditionally fields​ | > 10 hours​ |
| Python Frictionless​                                          | 5 GB file ​<br> Required fields ​                  | 20 minutes​ |
| JVM Jackson+NetworkNT (JSON)​                                 | 5 GB file ​<br> Required and conditionally fields ​| 6 minutes​  |
| JVM Jackson+NetworkNT (JSON) <br> + chunking and multithread​ | 5 GB file ​<br> Required and conditionally fields | 25 seconds​ |

## Chunking Metrics (local machine, not Azure)​
​Machine Specs:​
* Processor - Intel(R) Core(TM) i7-10850H CPU @ 2.70GHz   2.71 GHz​
* RAM - 32 GB​
* OS - Windows 10, 64-bit​
Test Description
* 5 GB file
* streamed entire file into tool, then iterated and chunked
* only validation performed is ability to parse the file as CSV
* no multithreading
| Tool​                   | Run Time    |
| ---                    | ---         |
| JVM Jackson CSV​        | 46 seconds​  |
| JVM Open CSV​           | 67 seconds​  |
| JVM Apache Commons CSV​ | 96 seconds​  |
| Python CSV​             | 107 seconds​ |
| Python Pandas​          | 85 minutes​  |

## Digital Preservation vs JSON conversion​ - Column Validations​
| Validation​                     | Digital Preservation​ | JSON​ |
| ---                            | ---                  | ---  |
| Enforce Required Columns​       | YES​                  | YES​  |
| Enforce No Extra Columns​       | YES​                  | YES​  |
| Enforce No Duplicate Columns​   | YES​                  | YES​  |
| Enforce Column Ordering​        | YES​                  | NO​   |
| Enforce Header Case​            | YES​                  | YES​  |
| Enforce Header Whitespace​      | YES​                  | YES​  |
| -                              | -                    | -    |
| Ignore Optional Columns​        | NO​                   | YES​  |
| Conditionally Required Columns​ | NO​                   | YES​  |
| Ignore Extra Columns​           | NO​                   | YES​  |
| Ignore Column Ordering​         | NO​                   | YES​  |
| Ignore Header Case​             | YES​                  | NO​   |
| Ignore Header Whitespace​       | NO​                   | NO​   | 

## Digital Preservation vs JSON conversion​ - Row Validations​
| Validation​                                   | Digital Preservation​ | JSON​ |
| ---                                          | ---                  | ---  |
| Enforce All Rows Same Number Fields​          | YES​                  | YES​  |
| Enforce Non-Empty Fields​                     | YES​                  | YES​  |
| Enforce 1 Field Non-Empty in Group of Fields | YES​                  | YES​  |
| Enforce Value From Prescribed List​           | YES​                  | YES​  |
| Enforce Value Matches Regex​                  | YES​                  | YES​  |

Many other validations are available in both tools

## Conclusions
* No single tool is a perfect option​
* Digital Preservation comes the closest as single tool, though its schema format is relatively strict and it does not offer out-of-the-box chunking​
* Separate parser with JSON validator seems viable​, though this requires more coding to link everything together
* Based on our research, Jackson CSV seems the best option for parsing against RFC 4180 standard and chunking
* Every tool requires some amount of writing code to chunk the data