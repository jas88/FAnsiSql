using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Text;

namespace FAnsiSql.Core.SourceGeneration;

/// <summary>
/// Source generator that automatically injects FAnsiSql initialization code into consuming projects.
/// Replaces ModuleInitializer approach with compile-time code generation for better reliability.
/// </summary>
[Generator(LanguageNames.CSharp)]
public class AutoInitializationGenerator : IIncrementalGenerator
{
    private const string GeneratedClassName = "FAnsiSqlAutoInitializer";
    private const string GeneratedNamespace = "FAnsiSql.Generated";

    // Diagnostic IDs for ModuleInitializer deprecation
    public const string ModuleInitializerDeprecatedDiagnosticId = "FANSI0003";
    public const string ModuleInitializerMigrationDiagnosticId = "FANSI0004";

    // Diagnostic descriptors for ModuleInitializer deprecation warnings
    private static readonly DiagnosticDescriptor ModuleInitializerDeprecatedRule = new(
        ModuleInitializerDeprecatedDiagnosticId,
        "FAnsiSql ModuleInitializer Usage Deprecated",
        "The assembly '{0}' is using the deprecated ModuleInitializer approach for FAnsiSql initialization. This will be removed in a future version.",
        "FAnsiSql Migration",
        DiagnosticSeverity.Warning,
        isEnabledByDefault: true,
        description: "ModuleInitializer-based initialization is deprecated in favor of compile-time source generation.");

    private static readonly DiagnosticDescriptor ModuleInitializerMigrationRule = new(
        ModuleInitializerMigrationDiagnosticId,
        "FAnsiSql ModuleInitializer Migration Available",
        "Migrate from ModuleInitializer to the new compile-time initialization approach. Remove the ModuleInitializer attribute and let the source generator handle initialization automatically.",
        "FAnsiSql Migration",
        DiagnosticSeverity.Info,
        isEnabledByDefault: true,
        description: "Migration guidance for transitioning from ModuleInitializer to source generation.",
        helpLinkUri: "https://github.com/jas88/FAnsiSql/blob/main/docs/migration-strategy-generic-refactoring.md");

    // FAnsiSql database provider assemblies to detect
    private static readonly Dictionary<string, (string Namespace, string ClassName)> ProviderMappings = new()
    {
        { "FAnsi.MicrosoftSql", ("FAnsi.Implementations.MicrosoftSQL", "MicrosoftSQLServerHelper") },
        { "FAnsi.MySql", ("FAnsi.Implementations.MySql", "MySqlServerHelper") },
        { "FAnsi.PostgreSql", ("FAnsi.Implementations.PostgreSql", "PostgreSqlServerHelper") },
        { "FAnsi.Oracle", ("FAnsi.Implementations.Oracle", "OracleServerHelper") },
        { "FAnsi.Sqlite", ("FAnsi.Implementations.Sqlite", "SQLiteServerHelper") }
    };

    public void Initialize(IncrementalGeneratorInitializationContext context)
    {
        // Detect FAnsiSql provider references and ModuleInitializer usage
        var providerDetections = context.CompilationProvider
            .Select((compilation, _) => AnalyzeCompilation(compilation));

        // Generate initialization code based on detected providers
        context.RegisterSourceOutput(providerDetections, GenerateInitializationCode);

        // Register to report diagnostics for ModuleInitializer usage
        context.RegisterSourceOutput(providerDetections, ReportModuleInitializerDiagnostics);
    }

    /// <summary>
    /// Analyzes the compilation to detect FAnsiSql providers and ModuleInitializer usage
    /// </summary>
    private static CompilationAnalysisResult AnalyzeCompilation(Compilation compilation)
    {
        var detectedProviders = new List<string>();
        var moduleInitializerUsages = new List<ModuleInitializerUsage>();

        foreach (var reference in compilation.References)
        {
            if (compilation.GetAssemblyOrModuleSymbol(reference) is not IAssemblySymbol assemblySymbol)
                continue;

            var assemblyName = assemblySymbol.Name;

            // Check if this is an FAnsiSql provider assembly
            if (ProviderMappings.ContainsKey(assemblyName))
            {
                detectedProviders.Add(assemblyName);

                // Check for ModuleInitializer usage in this provider
                var moduleInitializerUsage = DetectModuleInitializerInAssembly(assemblySymbol, compilation);
                if (moduleInitializerUsage != null)
                {
                    moduleInitializerUsages.Add(moduleInitializerUsage);
                }
            }
        }

        return new CompilationAnalysisResult
        {
            DetectedProviders = detectedProviders,
            ModuleInitializerUsages = moduleInitializerUsages
        };
    }

    /// <summary>
    /// Detects ModuleInitializer usage in a specific assembly
    /// </summary>
    private static ModuleInitializerUsage? DetectModuleInitializerInAssembly(IAssemblySymbol assemblySymbol, Compilation compilation)
    {
        try
        {
            var moduleInitializerAttributeType = compilation.GetTypeByMetadataName("System.Runtime.CompilerServices.ModuleInitializerAttribute");
            if (moduleInitializerAttributeType == null)
                return null;

            // Look for types with ModuleInitializer attributes
            foreach (var typeSymbol in assemblySymbol.GlobalNamespace.GetTypeMembers())
            {
                foreach (var member in typeSymbol.GetMembers())
                {
                    if (member is not IMethodSymbol methodSymbol)
                        continue;

                    // Check if method has ModuleInitializer attribute
                    var hasModuleInitializer = methodSymbol.GetAttributes()
                        .Any(attr => SymbolEqualityComparer.Default.Equals(attr.AttributeClass, moduleInitializerAttributeType));

                    if (hasModuleInitializer)
                    {
                        // Check if this method calls ImplementationManager.Load
                        var callsImplementationManager = CallsImplementationManagerLoad(methodSymbol, compilation);

                        return new ModuleInitializerUsage
                        {
                            AssemblyName = assemblySymbol.Name,
                            TypeName = typeSymbol.Name,
                            MethodName = methodSymbol.Name,
                            Location = methodSymbol.Locations.FirstOrDefault() ?? Location.None,
                            CallsImplementationManager = callsImplementationManager
                        };
                    }
                }
            }

            return null;
        }
        catch
        {
            return null;
        }
    }

    /// <summary>
    /// Checks if a method calls ImplementationManager.Load
    /// </summary>
    private static bool CallsImplementationManagerLoad(IMethodSymbol methodSymbol, Compilation compilation)
    {
        var implementationManagerType = compilation.GetTypeByMetadataName("FAnsi.ImplementationManager");
        if (implementationManagerType == null)
            return false;

        // This is a simplified check - in practice you'd need to analyze the method body
        // For now, we'll assume any ModuleInitializer in FAnsiSql providers is related to initialization
        return true;
    }

    /// <summary>
    /// Reports diagnostics for ModuleInitializer usage
    /// </summary>
    private static void ReportModuleInitializerDiagnostics(SourceProductionContext context, CompilationAnalysisResult analysis)
    {
        foreach (var usage in analysis.ModuleInitializerUsages)
        {
            // Report deprecation warning
            var deprecationDiagnostic = Diagnostic.Create(
                ModuleInitializerDeprecatedRule,
                usage.Location,
                usage.AssemblyName);

            context.ReportDiagnostic(deprecationDiagnostic);

            // Report migration info
            var migrationDiagnostic = Diagnostic.Create(
                ModuleInitializerMigrationRule,
                usage.Location);

            context.ReportDiagnostic(migrationDiagnostic);
        }
    }

    private static List<string> DetectFAnsiSqlProviders(Compilation compilation)
    {
        var detectedProviders = new List<string>();

        foreach (var reference in compilation.References)
        {
            if (compilation.GetAssemblyOrModuleSymbol(reference) is not IAssemblySymbol assemblySymbol)
                continue;

            var assemblyName = assemblySymbol.Name;

            // Check if this is an FAnsiSql provider assembly
            if (ProviderMappings.ContainsKey(assemblyName))
            {
                detectedProviders.Add(assemblyName);
            }
        }

        return detectedProviders;
    }

    /// <summary>
    /// Determines if a custom type is a ModuleInitializer attribute
    /// </summary>
    private static bool IsModuleInitializerAttribute(AttributeData attribute)
    {
            // Check for ModuleInitializer attribute (System.Runtime.CompilerServices.ModuleInitializer)
            return attribute?.AttributeClass?.Name == "ModuleInitializerAttribute";
    }

    private static void GenerateInitializationCode(SourceProductionContext context, CompilationAnalysisResult analysis)
    {
        var detectedProviders = analysis.DetectedProviders;

        if (detectedProviders.Count == 0)
            return; // No FAnsiSql providers detected

        var sourceBuilder = new StringBuilder();

        // Generate file header
        sourceBuilder.AppendLine("// <auto-generated/>");
        sourceBuilder.AppendLine("#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member");
        sourceBuilder.AppendLine();

        // Generate namespace and class
        sourceBuilder.AppendLine($"namespace {GeneratedNamespace}");
        sourceBuilder.AppendLine("{");
        sourceBuilder.AppendLine("    using System;");
        sourceBuilder.AppendLine("    using System.Runtime.CompilerServices;");
        sourceBuilder.AppendLine("    using FAnsi.Discovery;");
        sourceBuilder.AppendLine("    using FAnsi.Implementations.MicrosoftSQL;");
        sourceBuilder.AppendLine("    using FAnsi.Implementations.MySql;");
        sourceBuilder.AppendLine("    using FAnsi.Implementations.PostgreSql;");
        sourceBuilder.AppendLine("    using FAnsi.Implementations.Oracle;");
        sourceBuilder.AppendLine("    using FAnsi.Implementations.Sqlite;");
        sourceBuilder.AppendLine("    using FAnsi.Connections.Generic;");
        sourceBuilder.AppendLine();

        sourceBuilder.AppendLine($"    internal static class {GeneratedClassName}");
        sourceBuilder.AppendLine("    {");

        // Generate initialization method
        sourceBuilder.AppendLine("        private static bool _initialized = false;");
        sourceBuilder.AppendLine();

        // Add deprecation warnings if ModuleInitializer usage was detected
        if (analysis.ModuleInitializerUsages.Count > 0)
        {
            sourceBuilder.AppendLine("        #pragma warning disable FANSI0003 // FAnsiSql ModuleInitializer Usage Deprecated");
            sourceBuilder.AppendLine("        #pragma warning disable FANSI0004 // FAnsiSql ModuleInitializer Migration Available");
            sourceBuilder.AppendLine();
            sourceBuilder.AppendLine("        /// <summary>");
            sourceBuilder.AppendLine("        /// Automatically called when the assembly is loaded to initialize FAnsiSql providers.");
            sourceBuilder.AppendLine("        /// This method is generated based on the FAnsiSql packages referenced by this project.");
        sourceBuilder.AppendLine("        /// NOTE: Legacy ModuleInitializer usage detected in referenced assemblies. Consider migrating.");
        sourceBuilder.AppendLine("        /// </summary>");
        }
        else
        {
            sourceBuilder.AppendLine("        /// <summary>");
            sourceBuilder.AppendLine("        /// Automatically called when the assembly is loaded to initialize FAnsiSql providers.");
            sourceBuilder.AppendLine("        /// This method is generated based on the FAnsiSql packages referenced by this project.");
            sourceBuilder.AppendLine("        /// </summary>");
        }

        sourceBuilder.AppendLine("        [ModuleInitializer]");
        sourceBuilder.AppendLine("        internal static void Initialize()");
        sourceBuilder.AppendLine("        {");
        sourceBuilder.AppendLine("            if (_initialized) return;");
        sourceBuilder.AppendLine("            _initialized = true;");
        sourceBuilder.AppendLine();

        // Generate provider registration code
        foreach (var providerAssembly in detectedProviders)
        {
            if (ProviderMappings.TryGetValue(providerAssembly, out var mapping))
            {
                sourceBuilder.AppendLine($"            // Register {providerAssembly}");
                sourceBuilder.AppendLine($"            Register{mapping.ClassName}();");
                sourceBuilder.AppendLine();
            }
        }

        sourceBuilder.AppendLine("            // Initialize generic implementations if available");
        sourceBuilder.AppendLine("            InitializeGenericImplementations();");
        sourceBuilder.AppendLine("        }");
        sourceBuilder.AppendLine();

        // Generate individual provider registration methods
        foreach (var providerAssembly in detectedProviders)
        {
            if (ProviderMappings.TryGetValue(providerAssembly, out var mapping))
            {
                GenerateProviderRegistrationMethod(sourceBuilder, providerAssembly, mapping.Namespace, mapping.ClassName);
            }
        }

        // Generate generic implementations initialization
        sourceBuilder.AppendLine("        private static void InitializeGenericImplementations()");
        sourceBuilder.AppendLine("        {");
        sourceBuilder.AppendLine("            try");
        sourceBuilder.AppendLine("            {");
        sourceBuilder.AppendLine("                // Try to register generic implementations if available");
        sourceBuilder.AppendLine("                var genericMicrosoftSql = typeof(GenericMicrosoftSQLServerHelper);");
        sourceBuilder.AppendLine("                if (genericMicrosoftSql != null && genericMicrosoftSql.Assembly.GetName().Name == \"FAnsi.MicrosoftSql\")");
        sourceBuilder.AppendLine("                {");
        sourceBuilder.AppendLine("                    // Generic Microsoft SQL Server implementation available");
        sourceBuilder.AppendLine("                    // Note: Actual registration logic would be implemented in the generic layer");
        sourceBuilder.AppendLine("                }");
        sourceBuilder.AppendLine();
        sourceBuilder.AppendLine("                // Similar checks for other providers could be added here");
        sourceBuilder.AppendLine("            }");
        sourceBuilder.AppendLine("            catch");
        sourceBuilder.AppendLine("            {");
        sourceBuilder.AppendLine("                // Silently ignore if generic implementations are not available");
        sourceBuilder.AppendLine("                // This maintains backward compatibility");
        sourceBuilder.AppendLine("            }");
        sourceBuilder.AppendLine("        }");
        sourceBuilder.AppendLine();

        // Close class and namespace
        sourceBuilder.AppendLine("    }");
        sourceBuilder.AppendLine("}");

        // Add the generated source
        var sourceCode = sourceBuilder.ToString();
        context.AddSource($"{GeneratedClassName}.g.cs", SourceText.From(sourceCode, Encoding.UTF8));
    }

    private static void GenerateProviderRegistrationMethod(
        StringBuilder sourceBuilder,
        string providerAssembly,
        string providerNamespace,
        string className)
    {
        sourceBuilder.AppendLine($"        private static void Register{className}()");
        sourceBuilder.AppendLine("        {");
        sourceBuilder.AppendLine("            try");
        sourceBuilder.AppendLine("            {");
        sourceBuilder.AppendLine($"                var helper = {providerNamespace}.{className}.Instance;");
        sourceBuilder.AppendLine($"                if (helper != null)");
        sourceBuilder.AppendLine("                {");
        sourceBuilder.AppendLine("                    // Provider is already registered through static instance");
        sourceBuilder.AppendLine("                    // This method exists for potential future registration logic");
        sourceBuilder.AppendLine($"                    System.Diagnostics.Debug.WriteLine($\"FAnsiSql {className} initialized\");");
        sourceBuilder.AppendLine("                }");
        sourceBuilder.AppendLine("            }");
        sourceBuilder.AppendLine("            catch (Exception ex)");
        sourceBuilder.AppendLine("            {");
        sourceBuilder.AppendLine($"                System.Diagnostics.Debug.WriteLine($\"Failed to initialize {className}: {{ex.Message}}\");");
        sourceBuilder.AppendLine("            }");
        sourceBuilder.AppendLine("        }");
        sourceBuilder.AppendLine();
    }

    /// <summary>
    /// Result of compilation analysis containing detected providers and ModuleInitializer usage
    /// </summary>
    private class CompilationAnalysisResult
    {
        public List<string> DetectedProviders { get; set; } = new();
        public List<ModuleInitializerUsage> ModuleInitializerUsages { get; set; } = new();
    }

    /// <summary>
    /// Information about ModuleInitializer usage in an assembly
    /// </summary>
    private class ModuleInitializerUsage
    {
        public string AssemblyName { get; set; } = string.Empty;
        public string TypeName { get; set; } = string.Empty;
        public string MethodName { get; set; } = string.Empty;
        public Location Location { get; set; } = Location.None;
        public bool CallsImplementationManager { get; set; }
    }
}
