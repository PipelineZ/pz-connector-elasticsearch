namespace Pz.Connector.Elasticsearch.Tests;

public sealed class EsRedactorTests
{
    [Fact]
    public void Replaces_every_occurrence_of_every_secret()
    {
        var redactor = new EsRedactor(["s3cret", "other"]);

        Assert.Equal("a *** b *** c ***", redactor.Redact("a s3cret b other c s3cret"));
    }

    [Fact]
    public void Rewrites_authorization_headers_and_credential_pairs_even_when_the_value_is_unknown()
    {
        var redactor = new EsRedactor([]);

        Assert.Equal("Authorization: *** then Authorization: *** ; api_key=*** password=*** url=http://h:9200",
            redactor.Redact("Authorization: ApiKey abc.def== then Authorization: Basic dXNlcjpwdw== ; api_key=\"k 1\" password=pw url=http://h:9200"));
    }

    [Fact]
    public void Empty_and_short_secrets_are_ignored()
    {
        var redactor = new EsRedactor(["", "ab"]);

        Assert.Equal("abc", redactor.Redact("abc"));
    }

    [Fact]
    public void None_is_identity()
    {
        Assert.Equal("x", EsRedactor.None.Redact("x"));
    }
}
