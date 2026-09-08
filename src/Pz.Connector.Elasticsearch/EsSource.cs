using System.Diagnostics.CodeAnalysis;
using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.IndexManagement;
using Microsoft.Extensions.Logging;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Elasticsearch;

/// <summary>An index (alias, or pattern) as one table-shaped dataset: the mapping is the schema,
/// the engine's watermark bounds become a range filter, and pruning narrows <c>_source</c>. One
/// partition per dataset -- a point in time is a per-read snapshot, and sliced reads are not in
/// scope. No native scan: DuckDB cannot speak the Elasticsearch protocol.</summary>
internal sealed class EsSource(EsConnectionConfig connection, ElasticsearchClient client, ILogger logger) : ISource
{
    public async ValueTask<DatasetSchema> GetSchemaAsync(DatasetSpec spec, CancellationToken ct)
    {
        var dataset = ParseDataset(spec);
        var plan = await PlanAsync(dataset, spec, ct).ConfigureAwait(false);
        return new DatasetSchema(plan.Schema);
    }

    public bool TryGetNativeScan(DatasetSpec spec, [NotNullWhen(true)] out NativeScan? scan)
    {
        scan = null;
        return false;
    }

    public async ValueTask<IReadOnlyList<IDatasetPartition>> PlanReadAsync(DatasetSpec spec, ReadHints hints, CancellationToken ct)
    {
        var dataset = ParseDataset(spec);
        var plan = await PlanAsync(dataset, spec, ct).ConfigureAwait(false);
        EsSearchBuilder.ValidateCursor(plan, spec, connection.Redactor);
        var projected = plan.Project(hints.Columns);
        logger.LogDebug("elasticsearch: dataset {Dataset}: {Columns} of {Total} columns, page size {PageSize}",
            spec.Dataset, projected.Columns.Count, plan.Columns.Count, dataset.PageSize);
        return [new EsPartition(connection, client, dataset, projected, spec, logger)];
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    private async Task<ColumnPlan> PlanAsync(EsDatasetConfig dataset, DatasetSpec spec, CancellationToken ct)
    {
        var response = await client.Indices.GetMappingAsync(new GetMappingRequest(Indices.Parse(dataset.Index)), ct).ConfigureAwait(false);
        ct.ThrowIfCancellationRequested();
        if (!response.IsValidResponse)
        {
            throw EsErrors.FromResponse(response, connection.Redactor, $"dataset '{spec.Dataset}': fetching the mapping of '{dataset.Index}'");
        }

        if (response.Mappings.Count == 0)
        {
            // A pattern that matches nothing answers with an empty object rather than a 404; a
            // misspelled 'index:' must not read as an empty dataset.
            throw EsErrors.Fatal($"dataset '{spec.Dataset}': 'index' '{dataset.Index}' matches no index; create it or fix 'index:'", connection.Redactor);
        }

        return EsSchemaMapper.Plan(response.Mappings, dataset, spec.Dataset, connection.Redactor);
    }

    private EsDatasetConfig ParseDataset(DatasetSpec spec)
    {
        var errors = new List<string>();
        return EsDatasetConfig.Parse(spec, errors) ?? throw EsErrors.Fatal(string.Join("; ", errors), connection.Redactor);
    }
}
