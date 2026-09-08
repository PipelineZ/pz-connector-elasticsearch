using System.Net.Sockets;
using System.Text.Json;
using Elastic.Transport;
using Pz.Connectors.Abstractions;

namespace Pz.Connector.Elasticsearch;

/// <summary>Turns Elastic transport outcomes into the engine's exception, classified for retry.
/// Transient = the node or the network may recover on its own: no HTTP status at all (the request
/// never got an answer), a timeout, 408/429/502/503/504, and an expired point in time. Everything
/// about credentials, authorization, index existence, mappings, and malformed requests is not.
/// Unmapped statuses are non-transient: an unknown failure retried is a failure hidden. Messages
/// always pass the redactor -- the transport echoes configuration into its diagnostics.</summary>
internal static class EsErrors
{
    public static bool IsTransient(int? status, string? errorType, Exception? original)
    {
        if (status is null)
        {
            // No answer at all: refused, reset, or timed out inside the client. The client's own
            // timeout surfaces as a TaskCanceledException; the engine's cancellation never reaches
            // here (Wrap rethrows it).
            return original is HttpRequestException or IOException or SocketException or OperationCanceledException
                || original is null;
        }

        return status is 408 or 429 or 502 or 503 or 504
            || string.Equals(errorType, "search_context_missing_exception", StringComparison.Ordinal);
    }

    /// <summary>The one message shape: <c>elasticsearch: &lt;context&gt;: &lt;type&gt;: &lt;reason&gt; (HTTP n[; hint])</c>,
    /// or the exception's own message when there was no HTTP answer.</summary>
    public static PzConnectorException Build(int? status, string? errorType, string? reason, Exception? original,
        EsRedactor redactor, string context)
    {
        var transient = IsTransient(status, errorType, original);
        string detail;
        if (status is null)
        {
            detail = original?.Message ?? "no response";
        }
        else if (errorType is null && reason is null)
        {
            detail = $"HTTP {status} with no error body";
        }
        else
        {
            var hint = status switch
            {
                401 or 403 => "; check api_key, or username and password",
                404 when errorType == "index_not_found_exception" => "; create the index or fix 'index:'",
                _ => "",
            };
            var head = errorType is null ? reason : reason is null ? errorType : $"{errorType}: {reason}";
            detail = $"{head} (HTTP {status}{hint})";
        }

        return new PzConnectorException(redactor.Redact($"elasticsearch: {context}: {detail}"), transient, innerException: original);
    }

    /// <summary>A failed typed call: the client has already parsed the server error envelope.</summary>
    public static PzConnectorException FromResponse(Elastic.Transport.Products.Elasticsearch.ElasticsearchResponse response,
        EsRedactor redactor, string context)
    {
        var details = response.ApiCallDetails;
        var error = response.ElasticsearchServerError?.Error;
        return Build(details.HttpStatusCode, error?.Type, error?.Reason, details.OriginalException, redactor, context);
    }

    /// <summary>A failed raw transport call: the envelope, if any, is still in the body.</summary>
    public static PzConnectorException FromResponse(BytesResponse response, EsRedactor redactor, string context)
    {
        var details = response.ApiCallDetails;
        TryReadError(response.Body ?? [], out var type, out var reason);
        return Build(details.HttpStatusCode, type, reason, details.OriginalException, redactor, context);
    }

    /// <summary>Reads <c>{"error":{"type":..,"reason":..}}</c> or the string-valued
    /// <c>{"error":"..."}</c> some index-management endpoints answer with. False for anything that is
    /// not that envelope (a proxy's HTML page, an empty body).</summary>
    public static bool TryReadError(ReadOnlySpan<byte> body, out string? type, out string? reason)
    {
        type = null;
        reason = null;
        if (body.IsEmpty)
        {
            return false;
        }

        try
        {
            using var doc = JsonDocument.Parse(body.ToArray());
            if (doc.RootElement.ValueKind != JsonValueKind.Object || !doc.RootElement.TryGetProperty("error", out var error))
            {
                return false;
            }

            switch (error.ValueKind)
            {
                case JsonValueKind.String:
                    reason = error.GetString();
                    return true;
                case JsonValueKind.Object:
                    type = error.TryGetProperty("type", out var t) && t.ValueKind == JsonValueKind.String ? t.GetString() : null;
                    reason = error.TryGetProperty("reason", out var r) && r.ValueKind == JsonValueKind.String ? r.GetString() : null;
                    return true;
                default:
                    return false;
            }
        }
        catch (JsonException)
        {
            return false;
        }
    }

    public static PzConnectorException Wrap(Exception ex, EsRedactor redactor, string context)
    {
        if (ex is PzConnectorException already)
        {
            return already;
        }

        return new PzConnectorException(redactor.Redact($"elasticsearch: {context}: {ex.Message}"),
            IsTransient(null, null, ex), innerException: ex);
    }

    public static PzConnectorException Fatal(string message, EsRedactor redactor) =>
        new(redactor.Redact($"elasticsearch: {message}"), isTransient: false);

    public static PzConnectorException Transient(string message, EsRedactor redactor) =>
        new(redactor.Redact($"elasticsearch: {message}"), isTransient: true);
}
