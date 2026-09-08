using Apache.Arrow;
using Apache.Arrow.Types;
using Elastic.Clients.Elasticsearch.IndexManagement;
using Elastic.Clients.Elasticsearch.Mapping;

namespace Pz.Connector.Elasticsearch;

/// <summary>How a column's value is read out of a document. <see cref="Id"/> is the document id,
/// which lives outside <c>_source</c>.</summary>
internal enum ColumnKind { Int32, Int64, Double, Boolean, Timestamp, Text, Json, Id }

internal enum EpochUnit { Milliseconds, Seconds }

/// <summary>One column: its name, the path of the field inside <c>_source</c>, how to convert it,
/// and (for timestamps) what a bare number means.</summary>
internal sealed record ColumnSpec(string Name, string[] Path, ColumnKind Kind, EpochUnit Epoch = EpochUnit.Milliseconds)
{
    public string MappingType { get; init; } = "";
}

/// <summary>The dataset's Arrow schema and, per column, how to fill it.</summary>
internal sealed class ColumnPlan
{
    public ColumnPlan(IReadOnlyList<ColumnSpec> columns)
    {
        Columns = columns;
        Schema = new Schema(columns.Select(c => new Field(c.Name, ArrowType(c.Kind), c.Kind != ColumnKind.Id)).ToList(), null);
    }

    public IReadOnlyList<ColumnSpec> Columns { get; }

    public Schema Schema { get; }

    /// <summary>The <c>_source</c> paths this plan reads; empty when only <c>_id</c> is wanted.</summary>
    public IReadOnlyList<string> SourcePaths =>
        Columns.Where(c => c.Kind != ColumnKind.Id).Select(c => string.Join('.', c.Path)).ToList();

    /// <summary>Narrows to the named columns, in this plan's own order. A name this plan does not
    /// have means the hint is unusable, and the full plan is returned -- the engine then drops the
    /// hint the same way and the pipeline's SQL reports the unknown column.</summary>
    public ColumnPlan Project(IReadOnlyList<string>? columns)
    {
        if (columns is not { Count: > 0 })
        {
            return this;
        }

        var wanted = new HashSet<string>(columns, StringComparer.OrdinalIgnoreCase);
        var kept = Columns.Where(c => wanted.Contains(c.Name)).ToList();
        return kept.Count == wanted.Count ? new ColumnPlan(kept) : this;
    }

    private static IArrowType ArrowType(ColumnKind kind) => kind switch
    {
        ColumnKind.Int32 => Int32Type.Default,
        ColumnKind.Int64 => Int64Type.Default,
        ColumnKind.Double => DoubleType.Default,
        ColumnKind.Boolean => BooleanType.Default,
        ColumnKind.Timestamp => new TimestampType(TimeUnit.Microsecond, "UTC"),
        _ => StringType.Default,
    };
}

/// <summary>Turns an index mapping into a <see cref="ColumnPlan"/>: leaf fields depth-first in the
/// order Elasticsearch returns them (sorted by name), objects flattened into dotted columns,
/// everything without a scalar spelling landed as JSON text, and one trailing <c>_id</c>.
/// Multi-fields and field aliases are indexes over other fields' values, not source data, and are
/// skipped.</summary>
internal static class EsSchemaMapper
{
    /// <summary>The date formats whose values this connector can parse: ISO-8601 with an optional
    /// time and zone, and epoch numbers. Anything else in a mapping's <c>format</c> lands as text.</summary>
    private static readonly string[] StandardDateFormats =
        ["strict_date_optional_time", "strict_date_optional_time_nanos", "date_optional_time", "epoch_millis", "epoch_second"];

    public static ColumnPlan Plan(IReadOnlyDictionary<string, IndexMappingRecord> mappings, EsDatasetConfig dataset, string datasetName, EsRedactor redactor)
    {
        var columns = new List<ColumnSpec>();
        var byName = new Dictionary<string, ColumnSpec>(StringComparer.Ordinal);
        foreach (var (index, record) in mappings)
        {
            if (record.Mappings?.Properties is not { } properties)
            {
                continue;
            }

            foreach (var column in Walk(properties, [], dataset.JsonFields))
            {
                if (byName.TryGetValue(column.Name, out var existing))
                {
                    if (existing.Kind != column.Kind || existing.Epoch != column.Epoch)
                    {
                        throw EsErrors.Fatal(
                            $"dataset '{datasetName}': field '{column.Name}' is mapped as '{existing.MappingType}' in one index and " +
                            $"'{column.MappingType}' in '{index}'; 'index:' spans indices whose mappings disagree, so narrow it or list " +
                            "the field under json_fields:", redactor);
                    }

                    continue;
                }

                byName[column.Name] = column;
                columns.Add(column);
            }
        }

        columns.Add(new ColumnSpec("_id", [], ColumnKind.Id));
        return new ColumnPlan(columns);
    }

    private static IEnumerable<ColumnSpec> Walk(Properties properties, string[] parent, IReadOnlySet<string> jsonFields)
    {
        foreach (var (propertyName, property) in properties)
        {
            var path = parent.Append(propertyName.Name ?? "").ToArray();
            var name = string.Join('.', path);
            var type = property.Type ?? "";
            if (jsonFields.Contains(name))
            {
                yield return new ColumnSpec(name, path, ColumnKind.Json) { MappingType = type };
                continue;
            }

            switch (type)
            {
                case "alias":
                    continue;
                case "object":
                    if (property is ObjectProperty { Properties: { } children } && children.Any())
                    {
                        foreach (var child in Walk(children, path, jsonFields))
                        {
                            yield return child;
                        }
                    }
                    else
                    {
                        yield return new ColumnSpec(name, path, ColumnKind.Json) { MappingType = type };
                    }

                    continue;
                case "date":
                case "date_nanos":
                    var format = property switch
                    {
                        DateProperty d => d.Format,
                        DateNanosProperty dn => dn.Format,
                        _ => null,
                    };
                    yield return DateColumn(name, path, type, format);
                    continue;
            }

            var kind = type switch
            {
                "integer" or "short" or "byte" => ColumnKind.Int32,
                "long" or "unsigned_long" => ColumnKind.Int64,
                "float" or "half_float" or "double" or "scaled_float" => ColumnKind.Double,
                "boolean" => ColumnKind.Boolean,
                "keyword" or "constant_keyword" or "wildcard" or "text" or "match_only_text" or "search_as_you_type"
                    or "ip" or "version" or "binary" => ColumnKind.Text,
                _ => ColumnKind.Json,
            };
            yield return new ColumnSpec(name, path, kind) { MappingType = type };
        }
    }

    private static ColumnSpec DateColumn(string name, string[] path, string type, string? format)
    {
        if (string.IsNullOrEmpty(format))
        {
            return new ColumnSpec(name, path, ColumnKind.Timestamp) { MappingType = type };
        }

        var alternatives = format.Split("||", StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        if (alternatives.Any(a => !StandardDateFormats.Contains(a, StringComparer.Ordinal)))
        {
            return new ColumnSpec(name, path, ColumnKind.Text) { MappingType = $"{type} ({format})" };
        }

        var epoch = alternatives.Contains("epoch_second") && !alternatives.Contains("epoch_millis")
            ? EpochUnit.Seconds
            : EpochUnit.Milliseconds;
        return new ColumnSpec(name, path, ColumnKind.Timestamp, epoch) { MappingType = type };
    }
}
