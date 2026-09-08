using System.Runtime.CompilerServices;
using System.Text.Json;
using Apache.Arrow;
using Elastic.Clients.Elasticsearch;
using Elastic.Transport;
using Microsoft.Extensions.Logging;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Elasticsearch;

/// <summary>The whole dataset as one pz partition: open a point in time, page with
/// <c>search_after</c> until a page comes back short, close the point in time. The search itself
/// goes through the client's transport as raw bytes: the client's typed
/// <c>SearchResponse&lt;T&gt;</c> is built by a reflective converter factory that has no metadata
/// under Native AOT, while the typed request serializes fine. Cancellation is observed per page
/// and inside every call; a cancelled call is rethrown as cancellation, never classified as a
/// transient failure.</summary>
internal sealed class EsPartition(
    EsConnectionConfig connection, ElasticsearchClient client, EsDatasetConfig dataset, ColumnPlan plan,
    DatasetSpec spec, ILogger logger) : IDatasetPartition
{
    private static readonly EndpointPath SearchPath = new(Elastic.Transport.HttpMethod.POST, "/_search");
    private static readonly TimeSpan CloseBudget = TimeSpan.FromSeconds(10);
    private static readonly JsonElement EmptySource = JsonDocument.Parse("{}").RootElement.Clone();

    public async IAsyncEnumerable<RecordBatch> ReadAsync(BatchOptions options, [EnumeratorCancellation] CancellationToken ct)
    {
        var redactor = connection.Redactor;
        var context = $"dataset '{spec.Dataset}'";
        var opened = await client.OpenPointInTimeAsync(
            new OpenPointInTimeRequest(Indices.Parse(dataset.Index)) { KeepAlive = new Duration(dataset.PitKeepAlive) }, ct).ConfigureAwait(false);
        ct.ThrowIfCancellationRequested();
        if (!opened.IsValidResponse || string.IsNullOrEmpty(opened.Id))
        {
            throw EsErrors.FromResponse(opened, redactor, $"{context}: opening a point in time on '{dataset.Index}'");
        }

        var pitId = opened.Id;
        var builder = new DocumentBatchBuilder(plan, options, spec.Dataset, redactor);
        var pages = 0L;
        var rows = 0L;
        try
        {
            IReadOnlyList<JsonElement>? searchAfter = null;
            while (true)
            {
                ct.ThrowIfCancellationRequested();
                var request = EsSearchBuilder.Build(dataset, plan, spec, pitId, searchAfter);
                var response = await client.Transport.RequestAsync<BytesResponse>(SearchPath, PostData.Serializable(request), null, null, ct)
                    .ConfigureAwait(false);
                ct.ThrowIfCancellationRequested();
                if (!response.ApiCallDetails.HasSuccessfulStatusCode)
                {
                    throw EsErrors.FromResponse(response, redactor, $"{context}: searching '{dataset.Index}'");
                }

                pages++;
                using var doc = JsonDocument.Parse(response.Body);
                var root = doc.RootElement;
                if (root.TryGetProperty("pit_id", out var newPit) && newPit.ValueKind == JsonValueKind.String)
                {
                    pitId = newPit.GetString()!;
                }

                var hits = root.GetProperty("hits").GetProperty("hits");
                var count = 0;
                JsonElement last = default;
                foreach (var hit in hits.EnumerateArray())
                {
                    count++;
                    last = hit;
                    var id = hit.GetProperty("_id").GetString() ?? "";
                    var source = hit.TryGetProperty("_source", out var s) ? s : EmptySource;
                    builder.Append(id, source);
                    rows++;
                    if (builder.TryTakeBatch(out var batch))
                    {
                        yield return batch!;
                    }
                }

                if (count < dataset.PageSize)
                {
                    break;
                }

                searchAfter = last.GetProperty("sort").EnumerateArray().Select(e => e.Clone()).ToList();
            }

            if (builder.Flush() is { } tail)
            {
                yield return tail;
            }

            logger.LogDebug("elasticsearch: dataset {Dataset}: {Rows} documents in {Pages} pages", spec.Dataset, rows, pages);
        }
        finally
        {
            await ClosePointInTimeAsync(pitId).ConfigureAwait(false);
        }
    }

    /// <summary>Best effort, on its own short budget rather than the read's token: a cancelled or
    /// failed read still frees its point in time, and one that cannot be freed expires by itself.</summary>
    private async Task ClosePointInTimeAsync(string pitId)
    {
        using var cts = new CancellationTokenSource(CloseBudget);
        try
        {
            var closed = await client.ClosePointInTimeAsync(new ClosePointInTimeRequest { Id = pitId }, cts.Token).ConfigureAwait(false);
            if (!closed.IsValidResponse)
            {
                logger.LogDebug("elasticsearch: dataset {Dataset}: closing the point in time failed (HTTP {Status}); it expires on its own",
                    spec.Dataset, closed.ApiCallDetails.HttpStatusCode);
            }
        }
        catch (Exception ex) when (ex is OperationCanceledException or TransportException)
        {
            logger.LogDebug("elasticsearch: dataset {Dataset}: closing the point in time did not complete; it expires on its own", spec.Dataset);
        }
    }
}
