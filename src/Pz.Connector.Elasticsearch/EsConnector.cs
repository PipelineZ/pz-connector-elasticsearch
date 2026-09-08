using System.Reflection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Elasticsearch;

/// <summary>Elasticsearch for pz: an index is a table-shaped dataset typed from its mapping and paged
/// through a point in time; a sink output is a bulk append, a bulk upsert by <c>_id</c>, or a fresh
/// index swapped in behind an alias.</summary>
public sealed class EsConnector : IConnector
{
    private readonly ILoggerFactory _loggerFactory;

    public EsConnector(ILoggerFactory? loggerFactory = null)
    {
        _loggerFactory = loggerFactory ?? NullLoggerFactory.Instance;
    }

    public ConnectorInfo Info { get; } = new(
        "elasticsearch",
        typeof(EsConnector).Assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion
            ?? "0.0.0",
        ProtocolVersion.Major);

    public ConnectorCapabilities Capabilities =>
        ConnectorCapabilities.ColumnPruning | ConnectorCapabilities.BoundedWindow | ConnectorCapabilities.InclusiveWatermarkBound
        | ConnectorCapabilities.Merge | ConnectorCapabilities.ReplaceWrites;

    public string ConnectionConfigSchema => """{ "type": "object" }""";

    public string DatasetConfigSchema => """{ "type": "object" }""";

    public ValueTask<ValidationResult> ValidateAsync(ConnectorConfig config, CancellationToken ct) =>
        ValueTask.FromResult(ValidationResult.Success);

    public ValueTask<ConnectionCheck> CheckConnectionAsync(ConnectorConfig config, CancellationToken ct) =>
        throw new NotImplementedException();
}
