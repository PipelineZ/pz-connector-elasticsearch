using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using Apache.Arrow;
using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.IndexManagement;
using Microsoft.Extensions.Logging;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Elasticsearch;

/// <summary>Bulk writes. <c>append</c> lets Elasticsearch assign ids, <c>merge</c> indexes each row
/// under an id built from its keys (a full-document index is an upsert), <c>replace</c> writes a
/// fresh index and swaps it in behind the output's alias in one atomic aliases request. No native
/// copy (DuckDB cannot speak the Elasticsearch protocol), and <see cref="AbortSemantics.BestEffort"/>:
/// an aborted replace deletes its staging index, but bulks an aborted append or merge already sent
/// are visible and cannot be unsent.</summary>
internal sealed class EsSink(
    EsConnectionConfig connection, ElasticsearchClient client, ILogger logger, TimeProvider time, Random random) : ISink
{
    public AbortSemantics AbortSemantics => AbortSemantics.BestEffort;

    public bool TryGetNativeCopy(OutputSpec spec, [NotNullWhen(true)] out NativeCopy? copy)
    {
        copy = null;
        return false;
    }

    public async ValueTask<ISinkWriteSession> BeginWriteAsync(OutputSpec spec, Schema schema, CancellationToken ct)
    {
        var errors = new List<string>();
        var output = EsOutputConfig.Parse(spec, errors);
        EsOutputConfig.ValidateSchema(spec, schema, errors);
        if (output is null || errors.Count > 0)
        {
            // Parse and ValidateSchema already name the output in every message they add.
            throw EsErrors.Fatal(string.Join("; ", errors), connection.Redactor);
        }

        return spec.Mode switch
        {
            "append" => new EsWriteSession(connection, client, output, schema, [], output.Index, null, logger),
            "merge" => new EsWriteSession(connection, client, output, schema, spec.Keys, output.Index, null, logger),
            _ => await BeginReplaceAsync(spec, schema, output, ct).ConfigureAwait(false),
        };
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    /// <summary>The output name must be an alias or absent; a concrete index of that name is refused
    /// rather than deleted, because only an alias swap makes the old data disappear atomically. The
    /// staging index copies the current write index's mapping so types stay as they were.</summary>
    private async Task<EsWriteSession> BeginReplaceAsync(OutputSpec spec, Schema schema, EsOutputConfig output, CancellationToken ct)
    {
        var redactor = connection.Redactor;
        var context = $"output '{spec.Output}'";
        var alias = await client.Indices.GetAliasAsync(new GetAliasRequest(Names.Parse(output.Index)), ct).ConfigureAwait(false);
        ct.ThrowIfCancellationRequested();
        string[] oldIndices;
        if (alias.IsValidResponse)
        {
            oldIndices = (alias.Aliases?.Keys ?? []).Order(StringComparer.Ordinal).ToArray();
        }
        else if (alias.ApiCallDetails.HttpStatusCode == 404)
        {
            var exists = await client.Indices.ExistsAsync(new Elastic.Clients.Elasticsearch.IndexManagement.ExistsRequest(Indices.Parse(output.Index)), ct).ConfigureAwait(false);
            ct.ThrowIfCancellationRequested();
            if (exists.Exists)
            {
                throw EsErrors.Fatal(
                    $"{context}: '{output.Index}' is an index, not an alias; replace swaps an alias so the old data disappears " +
                    "atomically -- reindex it behind an alias first", redactor);
            }

            oldIndices = [];
        }
        else
        {
            throw EsErrors.FromResponse(alias, redactor, $"{context}: resolving '{output.Index}'");
        }

        var staging = StagingIndexName(output.Index);
        var create = new CreateIndexRequest(staging);
        if (oldIndices.Length > 0)
        {
            var current = await client.Indices.GetAsync(new GetIndexRequest(Indices.Parse(oldIndices[0])), ct).ConfigureAwait(false);
            ct.ThrowIfCancellationRequested();
            if (!current.IsValidResponse)
            {
                throw EsErrors.FromResponse(current, redactor, $"{context}: reading the mapping of '{oldIndices[0]}'");
            }

            create.Mappings = current.Indices.Values.FirstOrDefault()?.Mappings;
        }

        var created = await client.Indices.CreateAsync(create, ct).ConfigureAwait(false);
        ct.ThrowIfCancellationRequested();
        if (!created.IsValidResponse)
        {
            throw EsErrors.FromResponse(created, redactor, $"{context}: creating the staging index '{staging}'");
        }

        logger.LogDebug("elasticsearch: output {Output}: replace stages into {Staging} behind alias {Alias} ({Old} current index(es))",
            spec.Output, staging, output.Index, oldIndices.Length);
        return new EsWriteSession(connection, client, output, schema, [], staging, new ReplacePlan(output.Index, oldIndices, staging), logger);
    }

    /// <summary>Index names are lowercase by rule; the alias's own case is kept in the alias.</summary>
    private string StagingIndexName(string alias)
    {
        var stamp = time.GetUtcNow().ToString("yyyyMMddHHmmssfff", CultureInfo.InvariantCulture);
        var suffix = random.Next(0, 0x1000000).ToString("x6", CultureInfo.InvariantCulture);
        return $"{alias.ToLowerInvariant()}-pz-{stamp}-{suffix}";
    }
}

/// <summary>What a replace commit swaps: the alias, the indices currently behind it, and the
/// staging index that takes their place.</summary>
internal sealed record ReplacePlan(string Alias, IReadOnlyList<string> OldIndices, string StagingIndex);
