using Apache.Arrow;
using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.IndexManagement;
using Elastic.Transport;
using Microsoft.Extensions.Logging;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Elasticsearch;

/// <summary>One output's write: rows accumulate into a <c>_bulk</c> body that is sent whenever it
/// reaches the output's document or byte bound, commit flushes the rest, refreshes the target so
/// the write is searchable at once, and for a replace swaps the alias. The bulk goes through the
/// client's transport as bytes the connector wrote itself and comes back as the typed
/// <see cref="BulkResponse"/>: the client's generic bulk operation types have no metadata under
/// Native AOT, the response type does. A bulk answer that rejects any item fails the write -- a
/// rejected item is a row that did not land, and the engine's retry re-delivers the slice.</summary>
internal sealed class EsWriteSession : ISinkWriteSession
{
    private readonly EsRedactor _redactor;
    private readonly ElasticsearchClient _client;
    private readonly EsOutputConfig _output;
    private readonly string _target;
    private readonly ReplacePlan? _replace;
    private readonly ILogger _logger;
    private readonly BulkBodyWriter _body;
    private readonly EndpointPath _bulkPath;
    private long _rows;
    private long _batches;
    private long _requests;
    private bool _committed;
    private bool _aborted;

    public EsWriteSession(EsConnectionConfig connection, ElasticsearchClient client, EsOutputConfig output, Schema schema,
        IReadOnlyList<string> keys, string target, ReplacePlan? replace, ILogger logger)
    {
        _redactor = connection.Redactor;
        _client = client;
        _output = output;
        _target = target;
        _replace = replace;
        _logger = logger;
        _body = new BulkBodyWriter(schema, keys, output.Index, _redactor);
        _bulkPath = new EndpointPath(Elastic.Transport.HttpMethod.POST, $"/{Uri.EscapeDataString(target)}/_bulk");
    }

    /// <summary>The index rows actually go to: the output's index, or a replace's staging index.</summary>
    internal string TargetIndex => _target;

    public async ValueTask WriteBatchAsync(RecordBatch batch, CancellationToken ct)
    {
        ThrowIfFinished();
        for (var row = 0; row < batch.Length; row++)
        {
            _body.Append(batch, row);
            _rows++;
            if (_body.Count >= _output.BulkSize || _body.Bytes >= _output.BulkBytes)
            {
                await FlushAsync(ct).ConfigureAwait(false);
            }
        }

        _batches++;
    }

    public async ValueTask<WriteResult> CommitAsync(CancellationToken ct)
    {
        ThrowIfFinished();
        _committed = true;
        await FlushAsync(ct).ConfigureAwait(false);

        var refreshed = await _client.Indices.RefreshAsync(new RefreshRequest(Indices.Parse(_target)), ct).ConfigureAwait(false);
        ct.ThrowIfCancellationRequested();
        if (!refreshed.IsValidResponse)
        {
            throw EsErrors.FromResponse(refreshed, _redactor, $"output '{_output.Index}': refreshing '{_target}'");
        }

        if (_replace is { } replace)
        {
            await SwapAsync(replace, ct).ConfigureAwait(false);
        }

        _logger.LogDebug("elasticsearch: output {Output}: committed {Rows} rows in {Batches} batches over {Requests} bulk requests",
            _output.Index, _rows, _batches, _requests);
        return new WriteResult(_rows, _batches);
    }

    public async ValueTask AbortAsync(CancellationToken ct)
    {
        if (_committed)
        {
            throw new InvalidOperationException("AbortAsync after CommitAsync is not allowed");
        }

        _aborted = true;
        _body.Reset();
        if (_replace is { } replace)
        {
            // The staging index was never visible behind the alias; dropping it is the whole
            // cleanup. A failure here leaves an orphan the operator can delete, not a wrong result.
            var deleted = await _client.Indices.DeleteAsync(new DeleteIndexRequest(Indices.Parse(replace.StagingIndex)), ct).ConfigureAwait(false);
            if (!deleted.IsValidResponse)
            {
                _logger.LogWarning("elasticsearch: output {Output}: the staging index {Staging} could not be deleted after abort (HTTP {Status}); delete it by hand",
                    _output.Index, replace.StagingIndex, deleted.ApiCallDetails.HttpStatusCode);
            }
        }
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    private async Task FlushAsync(CancellationToken ct)
    {
        if (_body.Count == 0)
        {
            return;
        }

        var count = _body.Count;
        var payload = _body.TakeBody();
        var response = await _client.Transport.RequestAsync<BulkResponse>(_bulkPath, PostData.ReadOnlyMemory(payload), null, null, ct)
            .ConfigureAwait(false);
        ct.ThrowIfCancellationRequested();
        _requests++;
        var context = $"output '{_output.Index}': bulk of {count} document(s) into '{_target}'";
        if (!response.IsValidResponse)
        {
            throw EsErrors.FromResponse(response, _redactor, context);
        }

        if (response.Errors)
        {
            var failed = response.ItemsWithErrors.ToList();
            var first = failed[0];
            var id = first.Id is { Length: > 0 } ? $" (document '{first.Id}')" : "";
            var message = $"{context}: {failed.Count} of {count} item(s) rejected; first{id}: {first.Error?.Type}: {first.Error?.Reason}";
            throw EsErrors.IsTransient(first.Status, first.Error?.Type, null)
                ? EsErrors.Transient(message, _redactor)
                : EsErrors.Fatal(message, _redactor);
        }
    }

    /// <summary>One aliases request: the staging index takes the alias, every previous backing
    /// index is removed. Elasticsearch applies the actions atomically, so readers see either the
    /// old set or the new one. A failure here leaves the staging index in place, named, so the
    /// operator can finish or discard the swap.</summary>
    private async Task SwapAsync(ReplacePlan replace, CancellationToken ct)
    {
        var actions = new List<IndexUpdateAliasesAction>
        {
            new() { Add = new AddAction { Index = replace.StagingIndex, Alias = replace.Alias, IsWriteIndex = true } },
        };
        actions.AddRange(replace.OldIndices.Select(old => new IndexUpdateAliasesAction { RemoveIndex = new RemoveIndexAction { Index = old } }));
        var swapped = await _client.Indices.UpdateAliasesAsync(new UpdateAliasesRequest { Actions = actions }, ct).ConfigureAwait(false);
        ct.ThrowIfCancellationRequested();
        if (!swapped.IsValidResponse)
        {
            throw EsErrors.FromResponse(swapped, _redactor,
                $"output '{_output.Index}': swapping alias '{replace.Alias}' to '{replace.StagingIndex}' (the staging index holds the full write)");
        }
    }

    private void ThrowIfFinished()
    {
        if (_committed)
        {
            throw new InvalidOperationException("the session is already committed");
        }

        if (_aborted)
        {
            throw new InvalidOperationException("the session is aborted");
        }
    }
}
