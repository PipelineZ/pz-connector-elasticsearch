using System.Globalization;
using System.Text.Json;
using System.Text.RegularExpressions;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Elasticsearch;

/// <summary>Per-dataset read options: <c>index</c> (defaults to the entity name), <c>query</c>
/// (Query DSL as a YAML mapping or a JSON string), <c>page_size</c>, <c>json_fields</c>,
/// <c>pit_keep_alive</c>.</summary>
internal sealed partial record EsDatasetConfig(
    string Index, string? QueryJson, int PageSize, IReadOnlySet<string> JsonFields, string PitKeepAlive)
{
    public const int DefaultPageSize = 1000;
    public const int MaxPageSize = 10_000;
    public const string DefaultPitKeepAlive = "5m";

    private static readonly string[] KnownKeys = ["index", "query", "page_size", "json_fields", "pit_keep_alive"];

    public static EsDatasetConfig? Parse(DatasetSpec spec, List<string> errors)
    {
        var start = errors.Count;
        var prefix = $"dataset '{spec.Dataset}'";
        foreach (var key in spec.Options.Keys.Where(k => !KnownKeys.Contains(k, StringComparer.Ordinal)))
        {
            errors.Add($"{prefix}: unknown read option '{key}'; known: {string.Join(", ", KnownKeys)}");
        }

        var index = spec.Dataset;
        if (spec.Options.TryGetValue("index", out var indexRaw))
        {
            index = indexRaw?.ToString() ?? "";
            if (index.Length == 0)
            {
                errors.Add($"{prefix}: 'index' must be a non-empty string");
            }
        }

        string? query = null;
        if (spec.Options.TryGetValue("query", out var queryRaw) && queryRaw is not null)
        {
            query = QueryToJson(queryRaw, prefix, errors);
        }

        var pageSize = DefaultPageSize;
        if (spec.Options.TryGetValue("page_size", out var pageRaw) && pageRaw is not null)
        {
            if (pageRaw is IFormattable f && long.TryParse(f.ToString(null, CultureInfo.InvariantCulture), out var n) && n is >= 1 and <= MaxPageSize)
            {
                pageSize = (int)n;
            }
            else
            {
                errors.Add($"{prefix}: 'page_size' must be an integer between 1 and {MaxPageSize}");
            }
        }

        var jsonFields = new HashSet<string>(StringComparer.Ordinal);
        if (spec.Options.TryGetValue("json_fields", out var fieldsRaw) && fieldsRaw is not null)
        {
            if (fieldsRaw is IEnumerable<object?> list && fieldsRaw is not string)
            {
                foreach (var item in list)
                {
                    if (item?.ToString() is { Length: > 0 } name)
                    {
                        jsonFields.Add(name);
                    }
                    else
                    {
                        errors.Add($"{prefix}: 'json_fields' entries must be non-empty field paths");
                    }
                }
            }
            else
            {
                errors.Add($"{prefix}: 'json_fields' must be a list of field paths");
            }
        }

        var keepAlive = DefaultPitKeepAlive;
        if (spec.Options.TryGetValue("pit_keep_alive", out var keepRaw) && keepRaw is not null)
        {
            keepAlive = keepRaw.ToString() ?? "";
            if (!Duration().IsMatch(keepAlive))
            {
                errors.Add($"{prefix}: 'pit_keep_alive' must be an Elasticsearch duration such as 5m, 30s, or 1h; got '{keepAlive}'");
            }
        }

        return errors.Count == start ? new EsDatasetConfig(index, query, pageSize, jsonFields, keepAlive) : null;
    }

    /// <summary>A YAML mapping becomes the equivalent JSON object; a string must already be one.
    /// The DSL itself is never interpreted here -- Elasticsearch reports a malformed query with
    /// its own reason at read time.</summary>
    private static string? QueryToJson(object raw, string prefix, List<string> errors)
    {
        if (raw is string text)
        {
            try
            {
                using var doc = JsonDocument.Parse(text);
                if (doc.RootElement.ValueKind != JsonValueKind.Object)
                {
                    errors.Add($"{prefix}: 'query' must be a JSON object (Elasticsearch Query DSL)");
                    return null;
                }

                return doc.RootElement.GetRawText();
            }
            catch (JsonException ex)
            {
                errors.Add($"{prefix}: 'query' is not valid JSON: {ex.Message}");
                return null;
            }
        }

        if (raw is IEnumerable<KeyValuePair<string, object?>>)
        {
            return YamlJson.Write(raw);
        }

        errors.Add($"{prefix}: 'query' must be a mapping (Query DSL) or a JSON string");
        return null;
    }

    [GeneratedRegex("^[1-9][0-9]*(ms|s|m|h|d)$")]
    private static partial Regex Duration();
}

/// <summary>Writes the object graph a YAML loader hands over (mappings, lists, scalars) as JSON.
/// Scalars keep their YAML type: an unquoted 42 is a number, "42" a string.</summary>
internal static class YamlJson
{
    public static string Write(object? value)
    {
        using var buffer = new MemoryStream();
        using (var writer = new Utf8JsonWriter(buffer))
        {
            WriteValue(writer, value);
        }

        return System.Text.Encoding.UTF8.GetString(buffer.ToArray());
    }

    private static void WriteValue(Utf8JsonWriter writer, object? value)
    {
        switch (value)
        {
            case null: writer.WriteNullValue(); break;
            case string s: writer.WriteStringValue(s); break;
            case bool b: writer.WriteBooleanValue(b); break;
            case int i: writer.WriteNumberValue(i); break;
            case long l: writer.WriteNumberValue(l); break;
            case double d: writer.WriteNumberValue(d); break;
            case decimal m: writer.WriteNumberValue(m); break;
            case IEnumerable<KeyValuePair<string, object?>> map:
                writer.WriteStartObject();
                foreach (var (k, v) in map)
                {
                    writer.WritePropertyName(k);
                    WriteValue(writer, v);
                }

                writer.WriteEndObject();
                break;
            case IEnumerable<object?> list:
                writer.WriteStartArray();
                foreach (var item in list)
                {
                    WriteValue(writer, item);
                }

                writer.WriteEndArray();
                break;
            case IFormattable f: writer.WriteStringValue(f.ToString(null, CultureInfo.InvariantCulture)); break;
            default: writer.WriteStringValue(value.ToString()); break;
        }
    }
}
