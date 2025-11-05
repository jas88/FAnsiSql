

# Packages Used

### Risk Assessment common to all:
1. Packages on NuGet are virus scanned by the NuGet site.
2. This package is widely used and is actively maintained.
3. It is open source (Except Oracle ODP.NET).

| Package | Version | Source Code | License | Purpose | Additional Risk Assessment |
| ------- | ------- | ------------| ------- | ------- | -------------------------- |
| **Database Drivers** | | | | | |
| Microsoft.Data.SqlClient | 6.1.2 | [GitHub](https://github.com/dotnet/SqlClient) | [MIT](https://opensource.org/licenses/MIT) | Enables interaction with Microsoft Sql Server databases | Official MS project |
| MySqlConnector | 2.4.0 | [GitHub](https://github.com/mysql-net/MySqlConnector) | [MIT](https://github.com/mysql-net/MySqlConnector/blob/master/LICENSE) | Enables interaction with MySql databases | High performance async MySQL driver |
| Oracle.ManagedDataAccess.Core | 23.26.0 | Closed Source | [OTNLA](https://www.oracle.com/downloads/licenses/distribution-license.html) | Enables interaction with Oracle databases | Oracle official driver, closed source |
| Npgsql | 9.0.4 | [GitHub](https://github.com/npgsql/npgsql) | [PostgreSQL](https://github.com/npgsql/npgsql/blob/dev/LICENSE) | Enables interaction with Postgres databases | Official PostgreSQL .NET provider |
| Microsoft.Data.Sqlite | 9.0.10 | [GitHub](https://github.com/dotnet/efcore) | [MIT](https://opensource.org/licenses/MIT) | Enables interaction with SQLite databases | Official MS SQLite provider |
| **Core Libraries** | | | | | |
| HIC.TypeGuesser | 1.2.7 | [GitHub](https://github.com/HicServices/TypeGuesser) | [MIT](https://opensource.org/licenses/MIT) | Allows picking system Types for untyped strings e.g. `"12.3"` | Data type inference from strings |
| System.Linq.Async | 6.0.3 | [GitHub](https://github.com/dotnet/reactive) | [MIT](https://opensource.org/licenses/MIT) | Adds async support to LINQ | Enables async LINQ operations |
| **Build & Development Tools** | | | | | |
| **Code Analysis Tools** | | | | | |
| Microsoft.CodeAnalysis.Analyzers | 3.3.4 | [GitHub](https://github.com/dotnet/roslyn-analyzers) | [MIT](https://opensource.org/licenses/MIT) | Roslyn analyzers for code quality | Build-time analysis, private assets |
| Microsoft.CodeAnalysis.CSharp | 4.8.0 | [GitHub](https://github.com/dotnet/roslyn) | [MIT](https://opensource.org/licenses/MIT) | C# compiler platform | Source generation, private assets |
| Microsoft.CodeAnalysis.CSharp.Workspaces | 4.8.0 | [GitHub](https://github.com/dotnet/roslyn) | [MIT](https://opensource.org/licenses/MIT) | C# workspace support | Source generation, private assets |
| Microsoft.CodeAnalysis.Workspaces.Common | 4.8.0 | [GitHub](https://github.com/dotnet/roslyn) | [MIT](https://opensource.org/licenses/MIT) | Common workspace APIs | Source generation, private assets |
| System.Composition | 7.0.0 | [GitHub](https://github.com/dotnet/runtime) | [MIT](https://opensource.org/licenses/MIT) | Managed Extensibility Framework | Analyzer composition, private assets |
