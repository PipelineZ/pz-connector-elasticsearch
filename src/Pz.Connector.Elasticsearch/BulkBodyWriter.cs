using System.Text.Encodings.Web;
using System.Text.Json;
using Apache.Arrow;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Elasticsearch;

/// <summary>Accumulates one <c>_bulk</c> body: an <c>index</c> action line, then the row as a JSON
/// document, per row. With key columns the action names the <c>_id</c> (a merge is a full-document
/// index by id); without them Elasticsearch assigns one. Everything a row contributes is copied
/// into this writer's own buffer before <see cref="Append"/> returns, so nothing from the
/// engine-owned batch outlives the call.</summary>
internal sealed class BulkBodyWriter
{
    private static readonly byte[] AnonymousAction = "{\"index\":{}}\n"u8.ToArray();
    private static readonly byte[] IdActionHead = "{\"index\":{\"_id\":"u8.ToArray();
    private static readonly byte[] IdActionTail = "}}\n"u8.ToArray();
    private static readonly byte[] NewLine = "\n"u8.ToArray();
    // The same relaxed escaping as the document itself: only '"', '\\' and control characters.
    private static readonly JsonWriterOptions IdOptions = new() { Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping, SkipValidation = true };

    private readonly RowJsonWriter _json;
    private readonly int[] _keyIndexes;
    private readonly string[] _keyNames;
    private readonly string _output;
    private readonly EsRedactor _redactor;
    private readonly MemoryStream _buffer = new();

    public BulkBodyWriter(Schema schema, IReadOnlyList<string> keys, string output, EsRedactor redactor)
    {
        _json = new RowJsonWriter(schema, Enumerable.Range(0, schema.FieldsList.Count).ToArray());
        _keyNames = keys.ToArray();
        _keyIndexes = keys.Select(k => schema.FieldsList.ToList().FindIndex(f => f.Name == k)).ToArray();
        if (_keyIndexes.Any(i => i < 0))
        {
            throw new ArgumentException("every key must be a schema column", nameof(keys));
        }

        _output = output;
        _redactor = redactor;
    }

    public int Count { get; private set; }

    public long Bytes => _buffer.Length;

    public void Append(RecordBatch batch, int row)
    {
        if (_keyIndexes.Length == 0)
        {
            _buffer.Write(AnonymousAction);
        }
        else
        {
            _buffer.Write(IdActionHead);
            using (var writer = new Utf8JsonWriter(_buffer, IdOptions))
            {
                writer.WriteStringValue(DocumentId(batch, row));
            }

            _buffer.Write(IdActionTail);
        }

        _buffer.Write(_json.Write(batch, row));
        _buffer.Write(NewLine);
        Count++;
    }

    /// <summary>The key values formatted canonically, joined with <c>|</c> for a composite key. A
    /// null key value has no id spelling; refusing here keeps the row out of the request.</summary>
    internal string DocumentId(RecordBatch batch, int row)
    {
        if (_keyIndexes.Length == 1)
        {
            return ArrowScalars.Format(batch.Column(_keyIndexes[0]), row) ?? throw NullKey(_keyNames[0]);
        }

        var parts = new string[_keyIndexes.Length];
        for (var k = 0; k < _keyIndexes.Length; k++)
        {
            parts[k] = ArrowScalars.Format(batch.Column(_keyIndexes[k]), row) ?? throw NullKey(_keyNames[k]);
        }

        return string.Join('|', parts);
    }

    /// <summary>The body accumulated so far, and a reset. The memory is this writer's and is
    /// overwritten by the next Append, so the caller sends it before appending again.</summary>
    public ReadOnlyMemory<byte> TakeBody()
    {
        var body = _buffer.ToArray();
        Reset();
        return body;
    }

    public void Reset()
    {
        _buffer.SetLength(0);
        Count = 0;
    }

    private PzConnectorException NullKey(string column) =>
        EsErrors.Fatal($"output '{_output}': key column '{column}' is null in a row; a document _id cannot be built from a null key", _redactor);
}
