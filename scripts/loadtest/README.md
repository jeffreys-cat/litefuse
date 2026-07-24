# Litefuse web load test (k6)

Load-tests the **web page (tRPC) read endpoints** — the queries the UI fires when
you open a page. Authenticates with a real NextAuth session cookie (not an API
key). Scope is **Doris-backed read queries**: PostgreSQL/S3 queries the pages also
fire (TableViewPresets, comments, media, annotation queues, datasets) are
intentionally excluded so the numbers isolate analytics-DB read latency.

## Structure

```
scripts/loadtest/
├── run.js              # entrypoint — PAGE=<name> selects which page to run
├── lib/
│   ├── config.js       # env vars (BASE_URL, PROJECT_ID, creds, VUS, …) + time windows
│   ├── auth.js         # NextAuth credentials login → session cookie
│   └── trpc.js         # superjson encoding, URL building, metrics, batched runs
├── pages/
│   ├── index.js        # registry — add a page module here to make it runnable
│   └── tracing.js      # Tracing page: list + observations tab + detail (Doris queries)
└── tools/
    ├── har-replay.js   # replay any page's tRPC GETs straight from a browser HAR
    └── trpc-read-simple.js  # minimal hardcoded example (superseded by run.js)
```

## Run

```sh
brew install k6   # once

# default page = tracing, 10 VUs, 1m
PROJECT_ID=<projectId> EMAIL=<email> PASSWORD=<pw> \
  k6 run scripts/loadtest/run.js

# pick a page / tune load
PAGE=tracing VUS=30 DURATION=3m PROJECT_ID=... EMAIL=... PASSWORD=... \
  k6 run scripts/loadtest/run.js

# run every registered page
PAGE=all VUS=20 DURATION=2m ... k6 run scripts/loadtest/run.js

# isolate per-query cost without concurrency contention
PAGE=tracing VUS=1 DURATION=30s ... k6 run scripts/loadtest/run.js

# see error bodies on non-200
DEBUG=1 ... k6 run scripts/loadtest/run.js
```

Env vars (all optional, see `lib/config.js`): `BASE_URL`, `PROJECT_ID`, `EMAIL`,
`PASSWORD`, `PAGE`, `VUS`, `DURATION`, `SLEEP`, `WINDOW_DAYS`.

## Results

Each query reports latency as `lat_<page>_<proc>` (p90/p95/p99). `http_req_failed`
should stay 0. The default thresholds (p95 < 2s, errors < 1%) are starting points —
tune in `run.js`.

> Numbers from a local dev server are for **relative comparison only**: Next.js dev
> is a single unoptimized process, and seed data is tiny. For real read latency,
> run `VUS=1` (strip contention) and/or test against a deployment with realistic
> data volume.

## Adding a page

A page module exports four things (see `pages/tracing.js` as the template):

- `name` — string, used in metric names and `PAGE=<name>`.
- `procs` — array of every tRPC procedure the page fires (for metric declaration).
- `discover(cookie)` *(optional)* — fetch real ids the page's detail (byId)
  queries need; returns an object passed back into `queries`.
- `queries({ ids })` — returns `[{ proc, input }]`; inputs are plain objects, the
  framework superjson-encodes them (Date fields handled automatically).

Then register it in `pages/index.js`.

**Fastest way to get a new page's queries right:** record a HAR of that page in the
browser (Network tab → Save all as HAR), inspect the real `input` payloads with
`tools/har-replay.js` (or just replay it directly), then transcribe the
Doris-backed ones into a page module. Verify against the running app — input
shapes have gotchas (0-based pagination, Date vs string fields, optional-vs-null).
