using Pz.Connectors.Abstractions;

namespace Pz.Connector.Elasticsearch;

internal enum EsAuthKind { None, ApiKey, Basic }

internal sealed record EsAuth(EsAuthKind Kind, string? ApiKey, string? Username, string? Password);

/// <summary>The typed connection surface. One node URL; api-key or basic credentials, never both;
/// one way to trust the server certificate (a CA file, its fingerprint, or nothing at all), each
/// only meaningful over https. Every credential value is registered with the redactor by
/// content.</summary>
internal sealed record EsConnectionConfig(
    Uri Url,
    EsAuth Auth,
    string? CaCertPath,
    string? CaFingerprint,
    bool Insecure,
    int TimeoutSeconds,
    EsRedactor Redactor)
{
    public const int DefaultTimeoutSeconds = 60;

    private static readonly string[] KnownKeys =
        ["url", "api_key", "username", "password", "ca_cert", "ca_fingerprint", "insecure", "timeout", "base_dir"];

    public static EsConnectionConfig? Parse(ConnectorConfig config, List<string> errors)
    {
        var start = errors.Count;
        var secrets = new List<string>();

        foreach (var key in config.Values.Keys.Where(k => !KnownKeys.Contains(k, StringComparer.Ordinal)))
        {
            errors.Add($"unknown connection key '{key}'; known keys: {string.Join(", ", KnownKeys.Where(k => k != "base_dir"))}");
        }

        Uri? url = null;
        var urlText = config.GetString("url");
        if (string.IsNullOrWhiteSpace(urlText))
        {
            errors.Add("'url' is required (http://host:9200 or https://host:9200)");
        }
        else if (!Uri.TryCreate(urlText, UriKind.Absolute, out url) || url.Scheme is not ("http" or "https"))
        {
            errors.Add($"'url' must be an absolute http or https URL; got '{urlText}'");
            url = null;
        }

        var apiKey = config.GetString("api_key");
        var username = config.GetString("username");
        var password = config.GetString("password");
        var hasApiKey = !string.IsNullOrEmpty(apiKey);
        var hasBasic = !string.IsNullOrEmpty(username) || !string.IsNullOrEmpty(password);
        var auth = new EsAuth(EsAuthKind.None, null, null, null);
        if (hasApiKey && hasBasic)
        {
            errors.Add("'api_key' and 'username'/'password' are exclusive; set one of them");
        }
        else if (hasApiKey)
        {
            auth = new EsAuth(EsAuthKind.ApiKey, apiKey, null, null);
            secrets.Add(apiKey!);
        }
        else if (hasBasic)
        {
            if (string.IsNullOrEmpty(username) || string.IsNullOrEmpty(password))
            {
                errors.Add("'username' and 'password' come together");
            }
            else
            {
                auth = new EsAuth(EsAuthKind.Basic, null, username, password);
                secrets.Add(password);
            }
        }

        var baseDir = config.GetString("base_dir");
        var caCert = config.GetString("ca_cert");
        if (string.IsNullOrEmpty(caCert))
        {
            caCert = null;
        }
        else if (baseDir is not null && !Path.IsPathRooted(caCert))
        {
            caCert = Path.Combine(baseDir, caCert);
        }

        var fingerprint = config.GetString("ca_fingerprint");
        fingerprint = string.IsNullOrEmpty(fingerprint) ? null : fingerprint;
        var insecure = false;
        if (config.Values.TryGetValue("insecure", out var insecureRaw) && insecureRaw is not null)
        {
            if (insecureRaw is bool b)
            {
                insecure = b;
            }
            else
            {
                errors.Add("'insecure' must be true or false");
            }
        }

        var trustSettings = (caCert is not null ? 1 : 0) + (fingerprint is not null ? 1 : 0) + (insecure ? 1 : 0);
        if (trustSettings > 1)
        {
            errors.Add("'ca_cert', 'ca_fingerprint' and 'insecure' are exclusive; set one of them");
        }
        else if (trustSettings == 1 && url is { Scheme: "http" })
        {
            errors.Add("'ca_cert', 'ca_fingerprint' and 'insecure' only apply to an https url");
        }

        var timeout = DefaultTimeoutSeconds;
        if (config.Values.ContainsKey("timeout"))
        {
            var value = config.GetInt("timeout");
            if (value is null or < 1)
            {
                errors.Add("'timeout' must be a positive integer number of seconds");
            }
            else
            {
                timeout = (int)Math.Min(value.Value, int.MaxValue);
            }
        }

        return errors.Count == start && url is not null
            ? new EsConnectionConfig(url, auth, caCert, fingerprint, insecure, timeout, new EsRedactor(secrets))
            : null;
    }
}
