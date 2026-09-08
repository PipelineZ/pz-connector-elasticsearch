using System.Reflection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Elasticsearch;

/// <summary>Elasticsearch for pz: an index is a table-shaped dataset typed from its mapping and paged
/// through a point in time; a sink output is a bulk append, a bulk upsert by <c>_id</c>, or a fresh
/// index swapped in behind an alias.</summary>
public sealed class EsConnector : IConnector, ISourceConnector
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

    public string ConnectionConfigSchema => """
        { "type": "object", "required": ["url"], "properties": {
            "url": { "type": "string" },
            "api_key": { "type": "string" },
            "username": { "type": "string" },
            "password": { "type": "string" },
            "ca_cert": { "type": "string" },
            "ca_fingerprint": { "type": "string" },
            "insecure": { "type": "boolean" },
            "timeout": { "type": "integer", "minimum": 1 },
            "base_dir": { "type": "string" } },
          "additionalProperties": false }
        """;

    public string DatasetConfigSchema => """
        { "type": "object", "properties": {
            "index": { "type": "string" },
            "query": { "type": ["object", "string"] },
            "page_size": { "type": "integer", "minimum": 1, "maximum": 10000 },
            "json_fields": { "type": "array", "items": { "type": "string" } },
            "pit_keep_alive": { "type": "string" },
            "bulk_size": { "type": "integer", "minimum": 1, "maximum": 10000 },
            "bulk_bytes": { "type": "integer", "minimum": 1024 } },
          "additionalProperties": false }
        """;

    public ValueTask<ValidationResult> ValidateAsync(ConnectorConfig config, CancellationToken ct)
    {
        var errors = new List<string>();
        EsConnectionConfig.Parse(config, errors);
        return ValueTask.FromResult(errors.Count == 0 ? ValidationResult.Success : new ValidationResult(errors));
    }

    ValueTask<ISource> ISourceConnector.OpenAsync(ConnectorConfig config, CancellationToken ct)
    {
        var connection = ParseOrThrow(config);
        return ValueTask.FromResult<ISource>(new EsSource(connection, EsClientFactory.Create(connection), _loggerFactory.CreateLogger<EsSource>()));
    }

    public async ValueTask<ConnectionCheck> CheckConnectionAsync(ConnectorConfig config, CancellationToken ct)
    {
        var errors = new List<string>();
        var connection = EsConnectionConfig.Parse(config, errors);
        if (connection is null)
        {
            return new ConnectionCheck(false, string.Join("; ", errors));
        }

        try
        {
            var client = EsClientFactory.Create(connection);
            var info = await client.InfoAsync(ct).ConfigureAwait(false);
            if (!info.IsValidResponse)
            {
                return new ConnectionCheck(false, EsErrors.FromResponse(info, connection.Redactor, "checking the connection").Message);
            }

            return new ConnectionCheck(true, $"{info.ClusterName} ({info.Version?.Number})");
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            // Every failure is a failed probe, never a crash -- an unreadable ca_cert file throws out
            // of the factory, and reporting it is the whole point of the check. Cancellation is not a
            // probe result and still propagates.
            return new ConnectionCheck(false, connection.Redactor.Redact($"elasticsearch: {ex.Message}"));
        }
    }

    private static EsConnectionConfig ParseOrThrow(ConnectorConfig config)
    {
        var errors = new List<string>();
        return EsConnectionConfig.Parse(config, errors)
            ?? throw new PzConnectorException("elasticsearch: invalid connection config: " + string.Join("; ", errors), isTransient: false);
    }
}
