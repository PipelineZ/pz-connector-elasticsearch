using Pz.Connectors.Abstractions;

namespace Pz.Connector.Elasticsearch.Tests;

public sealed class EsConnectionConfigTests
{
    private static ConnectorConfig Config(Dictionary<string, object?> values) => new(values);

    [Fact]
    public void Minimal_config_is_an_unauthenticated_http_node_with_defaults()
    {
        var errors = new List<string>();
        var config = EsConnectionConfig.Parse(Config(new() { ["url"] = "http://localhost:9200" }), errors);

        Assert.Empty(errors);
        Assert.NotNull(config);
        Assert.Equal(new Uri("http://localhost:9200"), config.Url);
        Assert.Equal(EsAuthKind.None, config.Auth.Kind);
        Assert.Null(config.CaCertPath);
        Assert.Null(config.CaFingerprint);
        Assert.False(config.Insecure);
        Assert.Equal(60, config.TimeoutSeconds);
    }

    [Fact]
    public void Api_key_is_registered_with_the_redactor()
    {
        var errors = new List<string>();
        var config = EsConnectionConfig.Parse(Config(new() { ["url"] = "https://h:9200", ["api_key"] = "abc123==", ["timeout"] = 5L }), errors);

        Assert.Empty(errors);
        Assert.Equal(EsAuthKind.ApiKey, config!.Auth.Kind);
        Assert.Equal("abc123==", config.Auth.ApiKey);
        Assert.Equal(5, config.TimeoutSeconds);
        Assert.Equal("key *** here", config.Redactor.Redact("key abc123== here"));
    }

    [Fact]
    public void Basic_auth_registers_the_password_only()
    {
        var errors = new List<string>();
        var config = EsConnectionConfig.Parse(Config(new() { ["url"] = "https://h:9200", ["username"] = "elastic", ["password"] = "hunter22" }), errors);

        Assert.Empty(errors);
        Assert.Equal(EsAuthKind.Basic, config!.Auth.Kind);
        Assert.Equal("elastic", config.Auth.Username);
        Assert.Equal("hunter22", config.Auth.Password);
        Assert.Equal("elastic:***", config.Redactor.Redact("elastic:hunter22"));
    }

    [Fact]
    public void Api_key_and_basic_auth_are_exclusive_and_basic_needs_both_halves()
    {
        var errors = new List<string>();
        Assert.Null(EsConnectionConfig.Parse(Config(new() { ["url"] = "https://h:9200", ["api_key"] = "k", ["username"] = "u", ["password"] = "p" }), errors));
        Assert.Single(errors);
        Assert.Contains("exclusive", errors[0]);

        errors.Clear();
        Assert.Null(EsConnectionConfig.Parse(Config(new() { ["url"] = "https://h:9200", ["username"] = "u" }), errors));
        Assert.Single(errors);
        Assert.Contains("come together", errors[0]);
    }

    [Fact]
    public void Relative_ca_cert_resolves_against_base_dir_and_rooted_paths_stay()
    {
        var errors = new List<string>();
        var relative = EsConnectionConfig.Parse(Config(new() { ["url"] = "https://h:9200", ["ca_cert"] = "certs/ca.crt", ["base_dir"] = "/proj" }), errors);
        Assert.Empty(errors);
        Assert.Equal(Path.Combine("/proj", "certs/ca.crt"), relative!.CaCertPath);

        var rooted = EsConnectionConfig.Parse(Config(new() { ["url"] = "https://h:9200", ["ca_cert"] = "/abs/ca.crt", ["base_dir"] = "/proj" }), errors);
        Assert.Equal("/abs/ca.crt", rooted!.CaCertPath);
    }

    [Fact]
    public void Trust_settings_are_exclusive_and_https_only()
    {
        var errors = new List<string>();
        Assert.Null(EsConnectionConfig.Parse(Config(new() { ["url"] = "https://h:9200", ["ca_cert"] = "ca.crt", ["insecure"] = true }), errors));
        Assert.Single(errors);
        Assert.Contains("exclusive", errors[0]);

        errors.Clear();
        Assert.Null(EsConnectionConfig.Parse(Config(new() { ["url"] = "http://h:9200", ["ca_fingerprint"] = "ab12" }), errors));
        Assert.Single(errors);
        Assert.Contains("only apply to an https url", errors[0]);

        errors.Clear();
        var ok = EsConnectionConfig.Parse(Config(new() { ["url"] = "https://h:9200", ["insecure"] = true }), errors);
        Assert.Empty(errors);
        Assert.True(ok!.Insecure);
    }

    [Fact]
    public void Url_must_be_present_absolute_and_http_or_https()
    {
        var errors = new List<string>();
        Assert.Null(EsConnectionConfig.Parse(Config(new()), errors));
        Assert.Contains(errors, e => e.Contains("'url' is required"));

        errors.Clear();
        Assert.Null(EsConnectionConfig.Parse(Config(new() { ["url"] = "localhost:9200" }), errors));
        Assert.Contains(errors, e => e.Contains("absolute http or https URL"));

        errors.Clear();
        Assert.Null(EsConnectionConfig.Parse(Config(new() { ["url"] = "ftp://h" }), errors));
        Assert.Contains(errors, e => e.Contains("absolute http or https URL"));
    }

    [Fact]
    public void Errors_aggregate_and_name_unknown_keys()
    {
        var errors = new List<string>();
        Assert.Null(EsConnectionConfig.Parse(Config(new() { ["timeout"] = 0L, ["insecure"] = "yes", ["cloud_id"] = "x" }), errors));

        Assert.Equal(4, errors.Count); // url, timeout, insecure, cloud_id
        Assert.Contains(errors, e => e.StartsWith("unknown connection key 'cloud_id'", StringComparison.Ordinal));
        Assert.Contains(errors, e => e.Contains("'timeout' must be a positive integer"));
        Assert.Contains(errors, e => e.Contains("'insecure' must be true or false"));
    }
}
