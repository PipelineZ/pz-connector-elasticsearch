using System.Text.RegularExpressions;

namespace Pz.Connector.Elasticsearch;

/// <summary>Strips credentials from any text that may reach a PzConnectorException message, a log
/// line, or a ConnectionCheck: every configured secret value is replaced wherever it occurs (a
/// server or transport error can echo it in any position), and the credential-bearing shapes the
/// transport itself prints -- an <c>Authorization</c> header in an audit trail, an
/// <c>api_key=</c>/<c>password=</c> pair in a settings dump -- are rewritten even when the value is
/// not one of ours. Secrets shorter than 3 characters are not matched; replacing them would shred
/// unrelated text.</summary>
internal sealed partial class EsRedactor
{
    public const string Mask = "***";

    public static readonly EsRedactor None = new([]);

    private readonly string[] _secrets;

    public EsRedactor(IReadOnlyList<string> secrets)
    {
        _secrets = secrets.Where(s => s.Length >= 3).Distinct(StringComparer.Ordinal)
            .OrderByDescending(s => s.Length).ToArray();
    }

    public string Redact(string text)
    {
        if (string.IsNullOrEmpty(text))
        {
            return text;
        }

        foreach (var secret in _secrets)
        {
            text = text.Replace(secret, Mask, StringComparison.Ordinal);
        }

        text = AuthorizationHeader().Replace(text, m => $"{m.Groups["key"].Value} {Mask}");
        return CredentialPair().Replace(text, m => $"{m.Groups["key"].Value}={Mask}");
    }

    // "Authorization: ApiKey <token>" / "Authorization: Basic <token>" as the transport's audit
    // trail and HttpClient diagnostics print them; the token runs to the next whitespace.
    [GeneratedRegex("""(?<key>\bAuthorization:)\s+(?:ApiKey|Basic|Bearer)\s+\S+""", RegexOptions.IgnoreCase)]
    private static partial Regex AuthorizationHeader();

    // api_key=value / password="quoted value". The unquoted branch excludes ';' and ',' so a
    // "key=value; key2=value2" dump does not get its separator swallowed into the match.
    [GeneratedRegex("""(?<key>\b(?:api_key|apikey|password|passwd|access_key|secret_key)\b)=(?:"[^"]*"|[^\s;,]+)""", RegexOptions.IgnoreCase)]
    private static partial Regex CredentialPair();
}
