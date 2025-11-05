using System.Collections.Immutable;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Diagnostics;
using Microsoft.CodeAnalysis.Operations;

namespace FAnsiSql.Migration.Analyzer;

/// <summary>
/// Main entry point for FAnsiSql migration analyzer.
/// Detects casting patterns and suggests generic alternatives.
/// </summary>
[DiagnosticAnalyzer(LanguageNames.CSharp)]
public class FAnsiSqlMigrationAnalyzer : DiagnosticAnalyzer
{
    public const string ConnectionCastingDiagnosticId = "FANSI0001";
    public const string CommandCastingDiagnosticId = "FANSI0002";

    private static readonly DiagnosticDescriptor ConnectionCastingRule = new(
        ConnectionCastingDiagnosticId,
        "FAnsiSql Connection Casting",
        "Casting IManagedConnection.Connection to '{0}'. Consider using IManagedConnection<TConnection, TTransaction> generic interface.",
        "FAnsiSql Migration",
        DiagnosticSeverity.Info,
        isEnabledByDefault: true,
        description: "Casting IManagedConnection.Connection to concrete type.");

    private static readonly DiagnosticDescriptor CommandCastingRule = new(
        CommandCastingDiagnosticId,
        "FAnsiSql Command Casting",
        "Casting DbCommand to '{0}'. Consider using generic IDiscoveredServerHelper<T, T, T, T, T, T> interface.",
        "FAnsiSql Migration",
        DiagnosticSeverity.Info,
        isEnabledByDefault: true,
        description: "Casting DbCommand to concrete database command type.");

    public override ImmutableArray<DiagnosticDescriptor> SupportedDiagnostics =>
        ImmutableArray.Create(ConnectionCastingRule, CommandCastingRule);

    public override void Initialize(AnalysisContext context)
    {
        context.ConfigureGeneratedCodeAnalysis(GeneratedCodeAnalysisFlags.None);
        context.EnableConcurrentExecution();

        context.RegisterSyntaxNodeAction(AnalyzeCastExpression, SyntaxKind.CastExpression);
    }

    private static void AnalyzeCastExpression(SyntaxNodeAnalysisContext context)
    {
        var castExpression = (CastExpressionSyntax)context.Node;
        var targetType = context.SemanticModel.GetTypeInfo(castExpression.Type).Type;

        if (targetType == null)
            return;

        // Check for connection casting pattern
        if (IsConnectionCasting(castExpression, context.SemanticModel))
        {
            var diagnostic = Diagnostic.Create(
                ConnectionCastingRule,
                castExpression.GetLocation(),
                targetType.Name);

            context.ReportDiagnostic(diagnostic);
            return;
        }

        // Check for command casting pattern
        if (IsCommandCasting(castExpression, context.SemanticModel))
        {
            var diagnostic = Diagnostic.Create(
                CommandCastingRule,
                castExpression.GetLocation(),
                targetType.Name);

            context.ReportDiagnostic(diagnostic);
        }
    }

    private static bool IsConnectionCasting(CastExpressionSyntax castExpression, SemanticModel semanticModel)
    {
        // Pattern: (ConcreteConnection)connection.Connection
        if (castExpression.Expression is not MemberAccessExpressionSyntax memberAccess)
            return false;

        if (memberAccess.Name.Identifier.Text != "Connection")
            return false;

        if (memberAccess.Expression is not IdentifierNameSyntax identifier)
            return false;

        var symbol = semanticModel.GetSymbolInfo(identifier).Symbol;
        if (symbol == null)
            return false;

        var symbolType = (symbol as ILocalSymbol)?.Type ??
                         (symbol as IParameterSymbol)?.Type ??
                         (symbol as IFieldSymbol)?.Type ??
                         (symbol as IPropertySymbol)?.Type;
        if (symbolType == null)
            return false;

        var iManagedConnectionType = semanticModel.Compilation.GetTypeByMetadataName("FAnsi.Connections.IManagedConnection");
        return iManagedConnectionType != null && symbolType.AllInterfaces.Contains(iManagedConnectionType);
    }

    private static bool IsCommandCasting(CastExpressionSyntax castExpression, SemanticModel semanticModel)
    {
        // Pattern: (ConcreteCommand)dbCommand
        var targetType = semanticModel.GetTypeInfo(castExpression.Type).Type;
        if (targetType == null)
            return false;

        // Check if casting to a command type
        if (!targetType.Name.Contains("Command"))
            return false;

        var expressionType = semanticModel.GetTypeInfo(castExpression.Expression).Type;
        if (expressionType == null)
            return false;

        var dbCommandType = semanticModel.Compilation.GetTypeByMetadataName("System.Data.Common.DbCommand");
        return dbCommandType != null &&
               (expressionType.Equals(dbCommandType) || expressionType.AllInterfaces.Contains(dbCommandType));
    }
}
