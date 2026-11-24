using System.Data.SqlTypes;
using System.Text;
using Apache.Arrow;
using Apache.Arrow.Types;

namespace Pz.Connector.Elasticsearch.Tests;

public sealed class RowJsonWriterTests
{
    private static readonly Schema Schema = new(
    [
        new Field("i32", Int32Type.Default, true),
        new Field("i64", Int64Type.Default, true),
        new Field("f64", DoubleType.Default, true),
        new Field("dec", new Decimal128Type(38, 9), true),
        new Field("s", StringType.Default, true),
        new Field("b", BooleanType.Default, true),
        new Field("d", Date32Type.Default, true),
        new Field("ts", new TimestampType(TimeUnit.Microsecond, "UTC"), true),
    ], null);

    private static RecordBatch Batch()
    {
        var ts = new DateTimeOffset(2026, 9, 7, 10, 11, 12, TimeSpan.Zero).AddTicks(1234560);
        return new RecordBatch(Schema,
        [
            new Int32Array.Builder().Append(7).AppendNull().Build(),
            new Int64Array.Builder().Append(9_000_000_000L).AppendNull().Build(),
            new DoubleArray.Builder().Append(1.5).Append(double.NaN).Build(),
            new Decimal128Array.Builder(new Decimal128Type(38, 9)).Append(12.345m).AppendNull().Build(),
            new StringArray.Builder().Append("he said \"hi\"").AppendNull().Build(),
            new BooleanArray.Builder().Append(true).AppendNull().Build(),
            new Date32Array.Builder().Append(new DateOnly(2026, 9, 7)).AppendNull().Build(),
            new TimestampArray.Builder(new TimestampType(TimeUnit.Microsecond, "UTC")).Append(ts).AppendNull().Build(),
        ], 2);
    }

    [Fact]
    public void Writes_every_type_per_the_documented_format()
    {
        using var batch = Batch();
        var writer = new RowJsonWriter(Schema, [0, 1, 2, 3, 4, 5, 6, 7]);

        var json = Encoding.UTF8.GetString(writer.Write(batch, 0));

        Assert.Equal(
            """{"i32":7,"i64":9000000000,"f64":1.5,"dec":"12.345000000","s":"he said \"hi\"","b":true,"d":"2026-09-07","ts":"2026-09-07T10:11:12.123456Z"}""",
            json);
    }

    [Fact]
    public void A_decimal_wider_than_system_decimal_keeps_every_digit()
    {
        var schema = new Schema([new Field("dec", new Decimal128Type(38, 9), true)], null);
        var wide = SqlDecimal.Parse("12345678901234567890123456789.123456789");
        using var batch = new RecordBatch(schema, [new Decimal128Array.Builder(new Decimal128Type(38, 9)).Append(wide).Build()], 1);
        var writer = new RowJsonWriter(schema, [0]);

        Assert.Equal("""{"dec":"12345678901234567890123456789.123456789"}""", Encoding.UTF8.GetString(writer.Write(batch, 0)));
        Assert.Equal("12345678901234567890123456789.123456789", ArrowScalars.Format(batch.Column(0), 0));
    }

    [Fact]
    public void Nulls_and_nan_write_null_and_projection_is_honored()
    {
        using var batch = Batch();
        var writer = new RowJsonWriter(Schema, [2, 4, 0]);

        Assert.Equal("""{"f64":null,"s":null,"i32":null}""", Encoding.UTF8.GetString(writer.Write(batch, 1)));
    }

    [Fact]
    public void Scalars_format_for_document_ids()
    {
        using var batch = Batch();

        Assert.Equal("7", ArrowScalars.Format(batch.Column(0), 0));
        Assert.Equal("9000000000", ArrowScalars.Format(batch.Column(1), 0));
        Assert.Equal("he said \"hi\"", ArrowScalars.Format(batch.Column(4), 0));
        Assert.Equal("true", ArrowScalars.Format(batch.Column(5), 0));
        Assert.Equal("2026-09-07", ArrowScalars.Format(batch.Column(6), 0));
        Assert.Equal("2026-09-07T10:11:12.123456Z", ArrowScalars.Format(batch.Column(7), 0));
        Assert.Null(ArrowScalars.Format(batch.Column(0), 1));
        Assert.Null(ArrowScalars.Bytes(batch.Column(4), 1));
        Assert.Equal(Encoding.UTF8.GetBytes("7"), ArrowScalars.Bytes(batch.Column(0), 0));
    }

    [Fact]
    public void Nested_cells_are_json_objects_and_arrays()
    {
        var item = new StructType([new Field("sku", StringType.Default, true), new Field("qty", Int32Type.Default, true)]);
        var schema = new Schema([new Field("items", new ListType(new Field("item", item, true)), true),
            new Field("tags", new ListType(new Field("item", StringType.Default, true)), true)], null);
        var skus = new StringArray.Builder().Append("x").Append("y").Build();
        var qtys = new Int32Array.Builder().Append(1).AppendNull().Build();
        var structs = new StructArray(item, 2, [skus, qtys], ArrowBuffer.Empty, 0);
        var items = new ListArray(schema.FieldsList[0].DataType, 1,
            new ArrowBuffer.Builder<int>().Append(0).Append(2).Build(), structs, ArrowBuffer.Empty, 0);
        var tagValues = new StringArray.Builder().Append("p").Append("q").Build();
        var tags = new ListArray(schema.FieldsList[1].DataType, 1,
            new ArrowBuffer.Builder<int>().Append(0).Append(2).Build(), tagValues, ArrowBuffer.Empty, 0);
        using var batch = new RecordBatch(schema, [items, tags], 1);

        var json = Encoding.UTF8.GetString(new RowJsonWriter(schema, [0, 1]).Write(batch, 0));

        Assert.Equal("""{"items":[{"sku":"x","qty":1},{"sku":"y","qty":null}],"tags":["p","q"]}""", json);
    }
}
