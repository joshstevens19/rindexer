const assert = require("node:assert/strict");
const test = require("node:test");
const cors = require("cors");
const express = require("express");
const { corsOptionsFromEnv } = require("./cors");

function headerFor(options, origin, method = "GET") {
    const app = express();
    app.use(options ? cors(options) : cors());
    app.get("/graphql", (_req, res) => res.json({ ok: true }));
    return new Promise((resolve, reject) => {
        const server = app.listen(0, "127.0.0.1", async () => {
            try {
                const { port } = server.address();
                const res = await fetch(`http://127.0.0.1:${port}/graphql`, {
                    method,
                    headers: {
                        Origin: origin,
                        ...(method === "OPTIONS" ? { "Access-Control-Request-Method": "POST" } : {}),
                    },
                });
                resolve(res.headers.get("access-control-allow-origin"));
            } catch (err) {
                reject(err);
            } finally {
                server.close();
            }
        });
        server.on("error", reject);
    });
}

test("unset allow-list still allows any origin", async () => {
    assert.equal(corsOptionsFromEnv(undefined), null);
    assert.equal(corsOptionsFromEnv(""), null);
    assert.equal(corsOptionsFromEnv("   "), null);
    const echoed = await headerFor(corsOptionsFromEnv(undefined), "https://not-our-app.example");
    assert.equal(echoed, "*");
});

test("allow-list echoes a listed origin and drops the rest", async () => {
    const options = corsOptionsFromEnv(JSON.stringify(["https://app.example", "http://localhost:3000"]));
    assert.deepEqual(options.origin, ["https://app.example", "http://localhost:3000"]);
    assert.equal(await headerFor(options, "https://app.example"), "https://app.example");
    assert.equal(await headerFor(options, "https://not-our-app.example"), null);
    assert.equal(
        await headerFor(options, "https://app.example", "OPTIONS"),
        "https://app.example",
    );
    assert.equal(await headerFor(options, "https://not-our-app.example", "OPTIONS"), null);
});

test("empty allow-list allows no origin", async () => {
    const options = corsOptionsFromEnv("[]");
    assert.deepEqual(options.origin, []);
    assert.equal(await headerFor(options, "https://app.example"), null);
});

test("garbage allow-list is rejected", () => {
    assert.throws(() => corsOptionsFromEnv("https://app.example"), /JSON array/);
    assert.throws(() => corsOptionsFromEnv("[1]"), /JSON array/);
    assert.throws(() => corsOptionsFromEnv('[""]'), /JSON array/);
});
