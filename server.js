import express from "express";
import pg from "pg";

const app = express();
app.use(express.json({ limit: "10mb" }));

const PORT = process.env.PORT || 3000;
const DATABASE_URL = process.env.DATABASE_URL;
const TOKEN = process.env.INGEST_TOKEN;

if (!DATABASE_URL) throw new Error("Missing DATABASE_URL");
if (!TOKEN) throw new Error("Missing INGEST_TOKEN");

const pool = new pg.Pool({ connectionString: DATABASE_URL });

const auth = (req, res, next) => {
  const h = req.headers.authorization || "";
  if (h !== `Bearer ${TOKEN}`) return res.sendStatus(401);
  next();
};

// init tables (separadas, seguro)
await pool.query(`
  create table if not exists content(
    platform text not null,
    account text not null,
    pk text not null,
    shortcode text,
    url text not null,
    posted_at date,
    unique(platform, account, pk)
  );
`);

await pool.query(`
  create table if not exists snapshot(
    platform text not null,
    account text not null,
    pk text not null,
    snapshot_date date not null,
    views bigint not null,
    comments bigint not null,
    unique(platform, account, pk, snapshot_date)
  );
`);

app.get("/health", (_req, res) => res.json({ ok: true }));

app.post("/ingest", auth, async (req, res) => {
  const { platform, account, snapshot_date, rows = [] } = req.body || {};
  if (!platform || !account || !snapshot_date) return res.status(400).json({ error: "Missing platform/account/snapshot_date" });

  const c = await pool.connect();
  try {
    await c.query("begin");

    for (const r of rows) {
      if (!r?.pk || !r?.url) continue;

      await c.query(
        `insert into content(platform,account,pk,shortcode,url,posted_at)
         values($1,$2,$3,$4,$5,$6)
         on conflict(platform,account,pk) do update
         set url=excluded.url,
             shortcode=coalesce(excluded.shortcode, content.shortcode),
             posted_at=coalesce(excluded.posted_at, content.posted_at)`,
        [platform, account, String(r.pk), r.shortcode ?? null, String(r.url), r.posted_at ?? null]
      );

      await c.query(
        `insert into snapshot(platform,account,pk,snapshot_date,views,comments)
         values($1,$2,$3,$4,$5,$6)
         on conflict(platform,account,pk,snapshot_date) do update
         set views=excluded.views, comments=excluded.comments`,
        [platform, account, String(r.pk), snapshot_date, Number(r.views) || 0, Number(r.comments) || 0]
      );
    }

    await c.query("commit");
    res.json({ ok: true, received: rows.length });
  } catch (e) {
    await c.query("rollback");
    res.status(500).json({ ok: false, error: String(e?.message || e) });
  } finally {
    c.release();
  }
});

app.listen(PORT, () => console.log("up on", PORT));
