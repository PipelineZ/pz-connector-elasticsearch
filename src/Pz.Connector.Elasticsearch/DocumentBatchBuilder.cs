using System.Globalization;
using System.Text.Json;
using Apache.Arrow;
using Pz.Connectors.Abstractions;
using Pz.Connectors.Abstractions.Batches;

namespace Pz.Connector.Elasticsearch;

/// <summary>Documents in, Arrow batches out, one column per <see cref="ColumnSpec"/>. Values are
/// read straight off the hit's <c>_source</c> element by path and converted per the column's kind;
/// anything the mapping's type cannot hold -- an array where a scalar is expected, text where a
/// number is -- fails the read naming the field and the document, because a silently stringified or
/// truncated column is worse than a stopped run. Batches come from the ABI's pooled builder, so
/// every yielded batch is a fresh instance the engine owns outright.</summary>
internal sealed class DocumentBatchBuilder
{
    private readonly ColumnPlan _plan;
    private readonly ArrowBatchBuilder _inner;
    private readonly string _dataset;
    private readonly EsRedactor _redactor;
    private readonly object?[] _row;

    public DocumentBatchBuilder(ColumnPlan plan, BatchOptions options, string dataset, EsRedactor redactor)
    {
        _plan = plan;
        _inner = new ArrowBatchBuilder(plan.Schema, options.TargetBatchBytes, maxRowsPerBatch: options.MaxRowsPerBatch);
        _dataset = dataset;
        _redactor = redactor;
        _row = new object?[plan.Columns.Count];
    }

    public int PendingRows => _inner.PendingRows;

    public void Append(string id, JsonElement source)
    {
        var columns = _plan.Columns;
        for (var c = 0; c < columns.Count; c++)
        {
            var column = columns[c];
            _row[c] = column.Kind == ColumnKind.Id ? id : Convert(column, source, id);
        }

        _inner.AppendRow(_row);
    }

    public bool TryTakeBatch(out RecordBatch? batch) => _inner.TryTakeBatch(out batch);

    public RecordBatch? Flush() => _inner.Flush();

    private object? Convert(ColumnSpec column, JsonElement source, string id)
    {
        if (!TryGetPath(source, column.Path, out var value) || value.ValueKind is JsonValueKind.Null or JsonValueKind.Undefined)
        {
            return null;
        }

        if (column.Kind == ColumnKind.Json)
        {
            return value.GetRawText();
        }

        if (value.ValueKind == JsonValueKind.Array)
        {
            throw Refuse(column, id, "holds an array; list it under json_fields: to land it as JSON text");
        }

        if (value.ValueKind == JsonValueKind.Object)
        {
            throw Refuse(column, id, "holds an object; list it under json_fields: to land it as JSON text");
        }

        return column.Kind switch
        {
            ColumnKind.Int32 => ToInt32(column, value, id),
            ColumnKind.Int64 => ToInt64(column, value, id),
            ColumnKind.Double => ToDouble(column, value, id),
            ColumnKind.Boolean => ToBoolean(column, value, id),
            ColumnKind.Timestamp => ToTimestamp(column, value, id),
            ColumnKind.Text => value.ValueKind == JsonValueKind.String ? value.GetString() : value.GetRawText(),
            _ => throw new InvalidOperationException($"unexpected column kind {column.Kind}"),
        };
    }

    /// <summary>Walks <paramref name="path"/> into the source. Elasticsearch accepts both a nested
    /// object and a literal dotted key for the same field, so each level tries the nested step
    /// first and then the remaining path joined with dots as one key.</summary>
    internal static bool TryGetPath(JsonElement source, string[] path, out JsonElement value)
    {
        value = source;
        for (var i = 0; i < path.Length; i++)
        {
            if (value.ValueKind != JsonValueKind.Object)
            {
                return false;
            }

            if (value.TryGetProperty(path[i], out var next))
            {
                value = next;
                continue;
            }

            for (var j = i + 1; j < path.Length; j++)
            {
                if (value.TryGetProperty(string.Join('.', path, i, j - i + 1), out var dotted))
                {
                    value = dotted;
                    i = j;
                    goto found;
                }
            }

            return false;
            found:;
        }

        return true;
    }

    private object ToInt32(ColumnSpec column, JsonElement value, string id)
    {
        var l = ToInt64(column, value, id);
        if (l is < int.MinValue or > int.MaxValue)
        {
            throw Refuse(column, id, $"holds {l}, outside the mapped 32-bit range");
        }

        return (int)l;
    }

    private long ToInt64(ColumnSpec column, JsonElement value, string id)
    {
        if (value.ValueKind == JsonValueKind.Number && value.TryGetInt64(out var n))
        {
            return n;
        }

        var text = value.ValueKind == JsonValueKind.String ? value.GetString() : value.GetRawText();
        if (long.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed))
        {
            return parsed;
        }

        // An integral double ("1.0") is what a coercing mapping accepted; a fraction is not.
        if (double.TryParse(text, NumberStyles.Float, CultureInfo.InvariantCulture, out var d) && double.IsInteger(d)
            && d is >= long.MinValue and <= long.MaxValue)
        {
            return (long)d;
        }

        throw Refuse(column, id, $"holds {Describe(value)} where the mapping says {column.MappingType}");
    }

    private double ToDouble(ColumnSpec column, JsonElement value, string id)
    {
        if (value.ValueKind == JsonValueKind.Number && value.TryGetDouble(out var d))
        {
            return d;
        }

        var text = value.ValueKind == JsonValueKind.String ? value.GetString() : value.GetRawText();
        if (double.TryParse(text, NumberStyles.Float, CultureInfo.InvariantCulture, out var parsed))
        {
            return parsed;
        }

        throw Refuse(column, id, $"holds {Describe(value)} where the mapping says {column.MappingType}");
    }

    private bool ToBoolean(ColumnSpec column, JsonElement value, string id)
    {
        switch (value.ValueKind)
        {
            case JsonValueKind.True: return true;
            case JsonValueKind.False: return false;
            case JsonValueKind.String:
                var s = value.GetString();
                if (s == "true") return true;
                if (s == "false" || s == "") return false;
                break;
        }

        throw Refuse(column, id, $"holds {Describe(value)} where the mapping says boolean");
    }

    private DateTimeOffset ToTimestamp(ColumnSpec column, JsonElement value, string id)
    {
        if (value.ValueKind == JsonValueKind.Number && value.TryGetInt64(out var epoch))
        {
            return column.Epoch == EpochUnit.Seconds
                ? DateTimeOffset.FromUnixTimeSeconds(epoch)
                : DateTimeOffset.FromUnixTimeMilliseconds(epoch);
        }

        if (value.ValueKind == JsonValueKind.String)
        {
            var text = value.GetString()!;
            if (DateTimeOffset.TryParse(text, CultureInfo.InvariantCulture,
                    DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal, out var parsed))
            {
                // Microsecond precision is the column's; a nanosecond string keeps its first six.
                return new DateTimeOffset(parsed.Ticks - parsed.Ticks % 10, TimeSpan.Zero);
            }

            // A digit string is an epoch number a coercing mapping accepted as text.
            if (long.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out var textEpoch))
            {
                return column.Epoch == EpochUnit.Seconds
                    ? DateTimeOffset.FromUnixTimeSeconds(textEpoch)
                    : DateTimeOffset.FromUnixTimeMilliseconds(textEpoch);
            }
        }

        throw Refuse(column, id, $"holds {Describe(value)}, which is not an ISO-8601 timestamp or an epoch number");
    }

    private static string Describe(JsonElement value) =>
        value.ValueKind == JsonValueKind.String ? $"\"{value.GetString()}\"" : value.GetRawText();

    private PzConnectorException Refuse(ColumnSpec column, string id, string what) =>
        EsErrors.Fatal($"dataset '{_dataset}': field '{column.Name}' of document '{id}' {what}", _redactor);
}
