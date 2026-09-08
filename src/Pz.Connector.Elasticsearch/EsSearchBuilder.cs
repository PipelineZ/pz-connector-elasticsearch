using System.Text;
using System.Text.Json;
using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.Core.Search;
using Elastic.Clients.Elasticsearch.QueryDsl;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Elasticsearch;

/// <summary>One page of the read as a typed <see cref="SearchRequest"/>: the point in time, a
/// <c>_shard_doc</c> sort (no sortable field needed, stable within the PIT), <c>search_after</c>
/// from the previous page, exactly the <c>_source</c> paths the plan reads, and the query. The
/// query is the user's Query DSL and the engine's watermark bounds, each carried verbatim inside a
/// <c>wrapper</c> clause under <c>bool.filter</c>, so the connector never re-models the DSL.</summary>
internal static class EsSearchBuilder
{
    private static readonly ColumnKind[] CursorKinds = [ColumnKind.Int32, ColumnKind.Int64, ColumnKind.Double, ColumnKind.Timestamp];

    public static SearchRequest Build(EsDatasetConfig dataset, ColumnPlan plan, DatasetSpec spec, string pitId,
        IReadOnlyList<JsonElement>? searchAfter)
    {
        var request = new SearchRequest
        {
            Pit = new PointInTimeReference(pitId) { KeepAlive = new Duration(dataset.PitKeepAlive) },
            Size = dataset.PageSize,
            Sort = [new SortOptions { Field = new FieldSort("_shard_doc") }],
            TrackTotalHits = new TrackHits(false),
            Source = plan.SourcePaths.Count == 0
                ? new SourceConfig(false)
                : new SourceConfig(new SourceFilter { Includes = Fields.FromStrings(plan.SourcePaths.ToArray()) }),
        };

        var clauses = new List<Query>(2);
        if (dataset.QueryJson is { } user)
        {
            clauses.Add(Wrap(user));
        }

        if (CursorRangeJson(spec) is { } range)
        {
            clauses.Add(Wrap(range));
        }

        if (clauses.Count > 0)
        {
            request.Query = new Query { Bool = new BoolQuery { Filter = clauses } };
        }

        if (searchAfter is { Count: > 0 })
        {
            request.SearchAfter = searchAfter.Select(ToFieldValue).ToList();
        }

        return request;
    }

    /// <summary>The engine's bounds as one <c>range</c> clause, or null when there is no watermark
    /// yet. Gated on <see cref="DatasetSpec.WatermarkValue"/>, not the cursor name alone: the name is
    /// stamped on every incremental spec, including a first run with nothing stored.</summary>
    public static string? CursorRangeJson(DatasetSpec spec)
    {
        if (spec.WatermarkCursor is not { } cursor)
        {
            return null;
        }

        var lower = spec.WatermarkValue;
        var upper = spec.WatermarkUpperBound;
        if (lower is null && upper is null)
        {
            return null;
        }

        using var buffer = new MemoryStream();
        using (var writer = new Utf8JsonWriter(buffer))
        {
            writer.WriteStartObject();
            writer.WriteStartObject("range");
            writer.WriteStartObject(cursor);
            if (lower is not null)
            {
                writer.WriteString(spec.WatermarkLowerInclusive ? "gte" : "gt", lower);
            }

            if (upper is not null)
            {
                writer.WriteString("lte", upper);
            }

            writer.WriteEndObject();
            writer.WriteEndObject();
            writer.WriteEndObject();
        }

        return Encoding.UTF8.GetString(buffer.ToArray());
    }

    /// <summary>A watermark cursor must be a column a <c>range</c> query can bound: a numeric or a
    /// standard-format date field. Checked against the unprojected plan, at plan time, whenever a
    /// cursor is named -- a first run has no value yet but the same misconfiguration.</summary>
    public static void ValidateCursor(ColumnPlan plan, DatasetSpec spec, EsRedactor redactor)
    {
        if (spec.WatermarkCursor is not { } cursor)
        {
            return;
        }

        var column = plan.Columns.FirstOrDefault(c => string.Equals(c.Name, cursor, StringComparison.Ordinal));
        if (column is null)
        {
            throw EsErrors.Fatal($"dataset '{spec.Dataset}': watermark cursor '{cursor}' is not a field of the index mapping", redactor);
        }

        if (!CursorKinds.Contains(column.Kind))
        {
            throw EsErrors.Fatal(
                $"dataset '{spec.Dataset}': watermark cursor '{cursor}' is mapped as '{column.MappingType}', which a range query " +
                "cannot bound; use a numeric or standard-format date field", redactor);
        }
    }

    private static Query Wrap(string json) =>
        new() { Wrapper = new WrapperQuery(Convert.ToBase64String(Encoding.UTF8.GetBytes(json))) };

    private static FieldValue ToFieldValue(JsonElement e) => e.ValueKind switch
    {
        JsonValueKind.Number when e.TryGetInt64(out var l) => FieldValue.Long(l),
        JsonValueKind.Number => FieldValue.Double(e.GetDouble()),
        JsonValueKind.String => FieldValue.String(e.GetString()!),
        JsonValueKind.True => FieldValue.True,
        JsonValueKind.False => FieldValue.False,
        _ => FieldValue.Null,
    };
}
