// Unset or blank means allow every origin, which is what the server did
// before `graphql.cors.allowed_origins` existed. A JSON array (including [])
// is an explicit allow-list.
function corsOptionsFromEnv(raw) {
    if (raw == null || String(raw).trim() === "") {
        return null;
    }

    let parsed;
    try {
        parsed = JSON.parse(raw);
    } catch {
        throw new Error("RINDEXER_GRAPHQL_ALLOWED_ORIGINS must be a JSON array of origins");
    }

    if (!Array.isArray(parsed) || parsed.some((origin) => typeof origin !== "string" || origin.trim() === "")) {
        throw new Error("RINDEXER_GRAPHQL_ALLOWED_ORIGINS must be a JSON array of origins");
    }

    return { origin: parsed };
}

module.exports = { corsOptionsFromEnv };
