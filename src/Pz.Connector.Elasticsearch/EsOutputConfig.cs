using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Elasticsearch;

/// <summary>Per-output write options: <c>index</c> (defaults to the entity name), <c>bulk_size</c>
/// documents and <c>bulk_bytes</c> per <c>_bulk</c> request. Column checks against the real
/// schema happen at BeginWriteAsync (<see cref="ValidateSchema"/>); Parse only knows the option
/// shapes.</summary>
internal sealed record EsOutputConfig(string Index, int BulkSize, int BulkBytes)
{
    public const int DefaultBulkSize = 1000;
    public const int MaxBulkSize = 10_000;
    public const int DefaultBulkBytes = 5 * 1024 * 1024;
    public const int MinBulkBytes = 1024;

    private static readonly string[] KnownKeys = ["index", "bulk_size", "bulk_bytes"];
    private static readonly string[] Modes = ["append", "merge", "replace"];

    /// <summary>Exactly what <see cref="RowJsonWriter"/> can spell -- pz's v0 type matrix. A column
    /// outside it is refused here, while the errors still aggregate and before a request exists.</summary>
    private static readonly ArrowTypeId[] JsonTypes =
    [
        ArrowTypeId.String, ArrowTypeId.Int32, ArrowTypeId.Int64, ArrowTypeId.Double,
        ArrowTypeId.Decimal128, ArrowTypeId.Boolean, ArrowTypeId.Date32, ArrowTypeId.Timestamp,
    ];

    public static EsOutputConfig? Parse(OutputSpec spec, List<string> errors)
    {
        var start = errors.Count;
        var prefix = $"output '{spec.Output}'";
        foreach (var key in spec.Options.Keys.Where(k => !KnownKeys.Contains(k, StringComparer.Ordinal)))
        {
            errors.Add($"{prefix}: unknown write option '{key}'; known: {string.Join(", ", KnownKeys)}");
        }

        if (!Modes.Contains(spec.Mode, StringComparer.Ordinal))
        {
            errors.Add($"{prefix}: mode '{spec.Mode}' is not supported; elasticsearch supports append, merge, and replace");
        }

        var index = spec.Output;
        if (spec.Options.TryGetValue("index", out var indexRaw))
        {
            index = indexRaw?.ToString() ?? "";
            if (index.Length == 0)
            {
                errors.Add($"{prefix}: 'index' must be a non-empty string");
            }
        }

        var bulkSize = Int(spec, "bulk_size", DefaultBulkSize, 1, MaxBulkSize, prefix, errors);
        var bulkBytes = Int(spec, "bulk_bytes", DefaultBulkBytes, MinBulkBytes, int.MaxValue, prefix, errors);

        return errors.Count == start ? new EsOutputConfig(index, bulkSize, bulkBytes) : null;
    }

    /// <summary>Every column must be spellable as JSON, and a merge output's keys must be present
    /// columns: the id is built from them, and a missing key is the one thing the engine cannot
    /// fix downstream.</summary>
    public static void ValidateSchema(OutputSpec spec, Schema schema, List<string> errors)
    {
        var prefix = $"output '{spec.Output}'";
        foreach (var field in schema.FieldsList)
        {
            if (!JsonTypes.Contains(field.DataType.TypeId))
            {
                errors.Add($"{prefix}: column '{field.Name}' is {field.DataType.TypeId}, which the document's JSON cannot carry; "
                    + $"allowed: {string.Join(", ", JsonTypes)}. Drop it from the pipeline's projection or cast it");
            }
        }

        if (spec.Mode == "merge")
        {
            if (spec.Keys.Count == 0)
            {
                errors.Add($"{prefix}: mode merge needs 'keys:' -- the document _id is built from them");
            }

            foreach (var key in spec.Keys.Where(k => schema.FieldsList.All(f => f.Name != k)))
            {
                errors.Add($"{prefix}: key column '{key}' is not in the pipeline's output");
            }
        }
    }

    private static int Int(OutputSpec spec, string option, int fallback, int min, int max, string prefix, List<string> errors)
    {
        if (!spec.Options.TryGetValue(option, out var raw) || raw is null)
        {
            return fallback;
        }

        if (raw is IFormattable f && long.TryParse(f.ToString(null, System.Globalization.CultureInfo.InvariantCulture), out var n)
            && n >= min && n <= max)
        {
            return (int)n;
        }

        errors.Add(max == int.MaxValue
            ? $"{prefix}: '{option}' must be an integer of at least {min}"
            : $"{prefix}: '{option}' must be an integer between {min} and {max}");
        return fallback;
    }
}
