using System.Net.Sockets;
using System.Text;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Elasticsearch.Tests;

public sealed class EsErrorsTests
{
    [Theory]
    [InlineData(null, null, "HttpRequestException", true)]
    [InlineData(null, null, "IOException", true)]
    [InlineData(null, null, "SocketException", true)]
    [InlineData(null, null, "TaskCanceledException", true)]
    [InlineData(408, null, null, true)]
    [InlineData(429, "es_rejected_execution_exception", null, true)]
    [InlineData(429, "circuit_breaking_exception", null, true)]
    [InlineData(502, null, null, true)]
    [InlineData(503, null, null, true)]
    [InlineData(504, null, null, true)]
    [InlineData(404, "search_context_missing_exception", null, true)]
    [InlineData(400, "parsing_exception", null, false)]
    [InlineData(401, "security_exception", null, false)]
    [InlineData(403, "security_exception", null, false)]
    [InlineData(404, "index_not_found_exception", null, false)]
    [InlineData(500, null, null, false)]
    [InlineData(200, null, null, false)]
    public void Classifies_transient_by_status_error_type_and_exception(int? status, string? type, string? exception, bool transient)
    {
        Exception? original = exception switch
        {
            "HttpRequestException" => new HttpRequestException("refused"),
            "IOException" => new IOException("reset"),
            "SocketException" => new SocketException(),
            "TaskCanceledException" => new TaskCanceledException("timeout"),
            _ => null,
        };

        Assert.Equal(transient, EsErrors.IsTransient(status, type, original));
    }

    [Fact]
    public void Build_names_context_type_reason_and_the_auth_hint()
    {
        var ex = EsErrors.Build(401, "security_exception", "missing authentication credentials for REST request [/]",
            null, EsRedactor.None, "dataset 'orders': fetching the mapping");

        Assert.False(ex.IsTransient);
        Assert.Equal("elasticsearch: dataset 'orders': fetching the mapping: security_exception: missing authentication credentials "
            + "for REST request [/] (HTTP 401; check api_key, or username and password)", ex.Message);
    }

    [Fact]
    public void Build_hints_at_the_index_option_for_a_missing_index()
    {
        var ex = EsErrors.Build(404, "index_not_found_exception", "no such index [nope]", null, EsRedactor.None, "dataset 'x'");

        Assert.False(ex.IsTransient);
        Assert.Contains("(HTTP 404; create the index or fix 'index:')", ex.Message);
    }

    [Fact]
    public void Build_without_a_status_carries_the_exception_message_and_is_transient()
    {
        var ex = EsErrors.Build(null, null, null, new HttpRequestException("Connection refused (h:9200)"), new EsRedactor(["h:9200"]), "checking the connection");

        Assert.True(ex.IsTransient);
        Assert.Equal("elasticsearch: checking the connection: Connection refused (***)", ex.Message);
        Assert.IsType<HttpRequestException>(ex.InnerException);
    }

    [Fact]
    public void Build_with_a_status_but_no_error_body_says_so()
    {
        var ex = EsErrors.Build(502, null, null, null, EsRedactor.None, "bulk");

        Assert.True(ex.IsTransient);
        Assert.Equal("elasticsearch: bulk: HTTP 502 with no error body", ex.Message);
    }

    [Fact]
    public void TryReadError_parses_the_server_error_envelope_and_tolerates_other_bodies()
    {
        var body = Encoding.UTF8.GetBytes("""{"error":{"root_cause":[],"type":"parsing_exception","reason":"bad query"},"status":400}""");
        Assert.True(EsErrors.TryReadError(body, out var type, out var reason));
        Assert.Equal("parsing_exception", type);
        Assert.Equal("bad query", reason);

        Assert.True(EsErrors.TryReadError(Encoding.UTF8.GetBytes("""{"error":"alias [x] missing","status":404}"""), out type, out reason));
        Assert.Null(type);
        Assert.Equal("alias [x] missing", reason);

        Assert.False(EsErrors.TryReadError(Encoding.UTF8.GetBytes("<html>gateway</html>"), out _, out _));
        Assert.False(EsErrors.TryReadError([], out _, out _));
    }

    [Fact]
    public void Fatal_and_transient_prefix_and_redact()
    {
        var redactor = new EsRedactor(["hunter2"]);

        var fatal = EsErrors.Fatal("password hunter2 rejected", redactor);
        Assert.False(fatal.IsTransient);
        Assert.Equal("elasticsearch: password *** rejected", fatal.Message);

        var transient = EsErrors.Transient("node hunter2 away", redactor);
        Assert.True(transient.IsTransient);
        Assert.Equal("elasticsearch: node *** away", transient.Message);
    }

    [Fact]
    public void A_secret_that_is_a_substring_of_the_prefix_leaves_the_prefix_intact()
    {
        var redactor = new EsRedactor(["elastic"]);

        Assert.Equal("elasticsearch: user elastic: *** rejected", EsErrors.Fatal("user elastic: elastic rejected", redactor).Message
            .Replace("user ***:", "user elastic:"));
        Assert.StartsWith("elasticsearch: ctx: ", EsErrors.Build(401, "security_exception", "bad", null, redactor, "ctx").Message);
    }

    [Fact]
    public void Wrap_passes_connector_exceptions_through_and_classifies_others()
    {
        var already = new PzConnectorException("elasticsearch: x", isTransient: true);
        Assert.Same(already, EsErrors.Wrap(already, EsRedactor.None, "ctx"));

        var wrapped = EsErrors.Wrap(new HttpRequestException("refused"), EsRedactor.None, "ctx");
        Assert.True(wrapped.IsTransient);
        Assert.Equal("elasticsearch: ctx: refused", wrapped.Message);

        var other = EsErrors.Wrap(new InvalidOperationException("boom"), EsRedactor.None, "ctx");
        Assert.False(other.IsTransient);
    }
}
