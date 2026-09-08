using System.Text;
using Apache.Arrow;
using Apache.Arrow.Types;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Elasticsearch.Tests;

public sealed class BulkBodyWriterTests
{
    private static readonly Schema Schema = new(
    [
        new Field("id", Int64Type.Default, true),
        new Field("tenant", StringType.Default, true),
        new Field("name", StringType.Default, true),
    ], null);

    private static RecordBatch Batch() => new(Schema,
    [
        new Int64Array.Builder().Append(1).Append(2).AppendNull().Build(),
        new StringArray.Builder().Append("acme").Append("a\"b").Append("x").Build(),
        new StringArray.Builder().Append("one").AppendNull().Append("three").Build(),
    ], 3);

    [Fact]
    public void Append_without_keys_lets_elasticsearch_assign_ids()
    {
        using var batch = Batch();
        var writer = new BulkBodyWriter(Schema, [], "out", EsRedactor.None);
        writer.Append(batch, 0);
        writer.Append(batch, 1);

        Assert.Equal(2, writer.Count);
        Assert.Equal(
            "{\"index\":{}}\n{\"id\":1,\"tenant\":\"acme\",\"name\":\"one\"}\n" +
            "{\"index\":{}}\n{\"id\":2,\"tenant\":\"a\\\"b\",\"name\":null}\n",
            Encoding.UTF8.GetString(writer.TakeBody().Span));
        Assert.Equal(0, writer.Count);
        Assert.Equal(0, writer.Bytes);
    }

    [Fact]
    public void Keys_become_the_document_id_single_or_composite_and_escaped()
    {
        using var batch = Batch();
        var single = new BulkBodyWriter(Schema, ["id"], "out", EsRedactor.None);
        single.Append(batch, 0);
        Assert.StartsWith("{\"index\":{\"_id\":\"1\"}}\n{\"id\":1,", Encoding.UTF8.GetString(single.TakeBody().Span));

        var composite = new BulkBodyWriter(Schema, ["tenant", "id"], "out", EsRedactor.None);
        Assert.Equal("acme|1", composite.DocumentId(batch, 0));
        composite.Append(batch, 1);
        Assert.StartsWith("{\"index\":{\"_id\":\"a\\\"b|2\"}}\n", Encoding.UTF8.GetString(composite.TakeBody().Span));
    }

    [Fact]
    public void A_null_key_is_refused_naming_the_column_and_leaves_the_body_untouched()
    {
        using var batch = Batch();
        var writer = new BulkBodyWriter(Schema, ["tenant", "id"], "orders_out", EsRedactor.None);
        writer.Append(batch, 0);

        var ex = Assert.Throws<PzConnectorException>(() => writer.Append(batch, 2));

        Assert.False(ex.IsTransient);
        Assert.Equal("elasticsearch: output 'orders_out': key column 'id' is null in a row; a document _id cannot be built from a null key", ex.Message);
        Assert.Equal(1, writer.Count);
    }

    [Fact]
    public void Bytes_track_the_body_and_reset_clears_it()
    {
        using var batch = Batch();
        var writer = new BulkBodyWriter(Schema, [], "out", EsRedactor.None);
        writer.Append(batch, 0);
        var after = writer.Bytes;
        Assert.True(after > 0);

        writer.Reset();
        Assert.Equal(0, writer.Bytes);
        Assert.Equal(0, writer.Count);
    }

    [Fact]
    public void A_key_outside_the_schema_is_a_programming_error()
    {
        Assert.Throws<ArgumentException>(() => new BulkBodyWriter(Schema, ["nope"], "out", EsRedactor.None));
    }
}
