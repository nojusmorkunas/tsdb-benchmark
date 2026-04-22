"""Generate an interactive HTML dashboard from the Parquet data."""
import duckdb
import json
import os

PARQUET = "transformedData.parquet"
OUTPUT = "dashboard.html"

con = duckdb.connect()

print("Querying aggregate hourly data ...")
overall = con.sql(f"""
    SELECT
        date_trunc('hour', Timestamp::TIMESTAMP) AS hour,
        EnergyFlowDirection AS dir,
        SUM(Value) AS total,
        AVG(Value) AS avg,
        COUNT(DISTINCT Ean) AS meter_count
    FROM '{PARQUET}'
    GROUP BY 1, 2
    ORDER BY 1, 2
""").df()

print("Querying per-EAN daily totals ...")
daily = con.sql(f"""
    SELECT
        Ean,
        Timestamp::TIMESTAMP::DATE AS day,
        EnergyFlowDirection AS dir,
        SUM(Value) AS total
    FROM '{PARQUET}'
    GROUP BY 1, 2, 3
    ORDER BY 1, 2, 3
""").df()

print("Finding top 50 meters by total E17 value ...")
top_eans = con.sql(f"""
    SELECT Ean, SUM(Value) AS total
    FROM '{PARQUET}'
    WHERE EnergyFlowDirection = 'E17'
    GROUP BY 1
    ORDER BY 2 DESC
    LIMIT 50
""").df()["Ean"].tolist()

print("Querying 15-min detail for top 50 meters ...")
ean_list = ", ".join(f"'{e}'" for e in top_eans)
detail = con.sql(f"""
    SELECT Ean, Timestamp, EnergyFlowDirection AS dir, Value
    FROM '{PARQUET}'
    WHERE Ean IN ({ean_list})
    ORDER BY Ean, Timestamp
""").df()

# Get full EAN list for dropdown
all_eans = con.sql(f"""
    SELECT DISTINCT Ean FROM '{PARQUET}' ORDER BY 1
""").df()["Ean"].tolist()

# Prepare JSON data
print("Preparing JSON data ...")

overall_e17 = overall[overall["dir"] == "E17"]
overall_e18 = overall[overall["dir"] == "E18"]

overview_data = {
    "e17": {"x": overall_e17["hour"].astype(str).tolist(), "y": overall_e17["total"].tolist()},
    "e18": {"x": overall_e18["hour"].astype(str).tolist(), "y": overall_e18["total"].tolist()},
}

# Per-EAN daily data (for ranking table)
daily_ranking = {}
for _, row in daily.iterrows():
    ean = row["Ean"]
    if ean not in daily_ranking:
        daily_ranking[ean] = {"e17": 0, "e18": 0}
    daily_ranking[ean][row["dir"].lower()] += row["total"]

top_list = sorted(daily_ranking.items(), key=lambda x: x[1]["e17"], reverse=True)[:200]
ranking_data = [{"ean": ean, "e17": round(v["e17"], 3), "e18": round(v["e18"], 3)} for ean, v in top_list]

# Detail data keyed by EAN
detail_data = {}
for ean in top_eans:
    ean_df = detail[detail["Ean"] == ean]
    e17 = ean_df[ean_df["dir"] == "E17"]
    e18 = ean_df[ean_df["dir"] == "E18"]
    detail_data[ean] = {
        "e17": {"x": e17["Timestamp"].tolist(), "y": e17["Value"].tolist()},
        "e18": {"x": e18["Timestamp"].tolist(), "y": e18["Value"].tolist()},
    }

data_json = json.dumps({
    "overview": overview_data,
    "ranking": ranking_data,
    "detail": detail_data,
    "allEans": all_eans,
    "topEans": top_eans,
}, default=str)

print(f"JSON data size: {len(data_json) / 1024 / 1024:.1f} MB")

# Build HTML
html = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Energy Data Dashboard</title>
<script src="https://cdn.plot.ly/plotly-2.35.0.min.js"></script>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; background: #0f1117; color: #e0e0e0; }
  .header { padding: 20px 30px; background: #161b22; border-bottom: 1px solid #30363d; }
  .header h1 { font-size: 22px; font-weight: 600; }
  .header .subtitle { color: #8b949e; font-size: 14px; margin-top: 4px; }
  .container { max-width: 1400px; margin: 0 auto; padding: 20px; }
  .card { background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 20px; margin-bottom: 20px; }
  .card h2 { font-size: 16px; margin-bottom: 4px; color: #c9d1d9; }
  .card .card-desc { font-size: 13px; color: #8b949e; margin-bottom: 14px; line-height: 1.5; }
  .controls { display: flex; gap: 12px; align-items: center; flex-wrap: wrap; margin-bottom: 12px; }
  .controls label { font-size: 13px; color: #8b949e; }
  .controls select, .controls input {
    background: #0d1117; border: 1px solid #30363d; color: #c9d1d9;
    padding: 6px 10px; border-radius: 6px; font-size: 13px;
  }
  .controls input { width: 280px; }
  .controls select { min-width: 220px; }
  table { width: 100%; border-collapse: collapse; font-size: 13px; }
  th, td { padding: 8px 12px; text-align: left; border-bottom: 1px solid #21262d; }
  th { color: #8b949e; font-weight: 500; position: sticky; top: 0; background: #161b22; }
  tr:hover { background: #1c2129; }
  tr.selected { background: #1f6feb33; }
  .table-wrap { max-height: 400px; overflow-y: auto; }
  .clickable { cursor: pointer; }
  .tag { display: inline-block; padding: 2px 8px; border-radius: 12px; font-size: 11px; font-weight: 600; }
  .tag-e17 { background: #388bfd33; color: #58a6ff; }
  .tag-e18 { background: #3fb95033; color: #56d364; }
  .stats { display: flex; gap: 16px; margin-bottom: 16px; flex-wrap: wrap; }
  .stat { background: #0d1117; border: 1px solid #30363d; border-radius: 8px; padding: 12px 18px; }
  .stat .val { font-size: 22px; font-weight: 700; }
  .stat .lbl { font-size: 12px; color: #8b949e; margin-top: 2px; }
  .info-banner { background: #1c2129; border: 1px solid #30363d; border-radius: 6px; padding: 12px 16px; font-size: 13px; color: #8b949e; margin-bottom: 12px; }
  .legend-box { background: #0d1117; border: 1px solid #30363d; border-radius: 8px; padding: 16px 20px; margin-bottom: 20px; display: flex; gap: 32px; flex-wrap: wrap; }
  .legend-item { display: flex; align-items: flex-start; gap: 10px; }
  .legend-item .dot { width: 10px; height: 10px; border-radius: 50%; margin-top: 4px; flex-shrink: 0; }
  .legend-item .legend-title { font-size: 13px; font-weight: 600; color: #c9d1d9; }
  .legend-item .legend-desc { font-size: 12px; color: #8b949e; margin-top: 2px; }
</style>
</head>
<body>
<div class="header">
  <h1>Energy Meter Dashboard</h1>
  <p class="subtitle">Visualizing energy consumption and production data from smart meters, recorded at 15-minute intervals.</p>
</div>
<div class="container">

  <!-- Glossary / Legend -->
  <div class="legend-box">
    <div class="legend-item">
      <div>
        <div class="legend-title">EAN (Meter ID)</div>
        <div class="legend-desc">Unique 18-digit identification number assigned to each energy connection point (metering location).</div>
      </div>
    </div>
    <div class="legend-item">
      <div class="dot" style="background:#58a6ff;"></div>
      <div>
        <div class="legend-title">E17 &mdash; Consumption (Offtake)</div>
        <div class="legend-desc">Energy drawn <strong>from the grid</strong> by the consumer. Higher values = more electricity used.</div>
      </div>
    </div>
    <div class="legend-item">
      <div class="dot" style="background:#56d364;"></div>
      <div>
        <div class="legend-title">E18 &mdash; Injection (Production)</div>
        <div class="legend-desc">Energy fed <strong>back into the grid</strong> by the consumer (e.g. from solar panels). Higher values = more electricity produced.</div>
      </div>
    </div>
    <div class="legend-item">
      <div>
        <div class="legend-title">Value</div>
        <div class="legend-desc">Energy reading per 15-minute interval (in kWh). Each data point represents one quarter-hour measurement.</div>
      </div>
    </div>
  </div>

  <div class="stats" id="stats"></div>

  <div class="card">
    <h2>Grid Overview &mdash; Combined Hourly Energy (All Meters)</h2>
    <p class="card-desc">
      Total energy summed across all meters for each hour.
      The <span class="tag tag-e17">blue line</span> shows total electricity consumed from the grid,
      the <span class="tag tag-e18">green line</span> shows total electricity injected back.
      Use the range buttons (1d / 3d / 1w) or drag the slider below the chart to zoom into a time window.
    </p>
    <div id="overviewChart" style="height:350px;"></div>
  </div>

  <div class="card">
    <h2>Individual Meter Detail &mdash; 15-Minute Readings</h2>
    <p class="card-desc">
      Select a specific meter (EAN) to see its consumption and injection at full 15-minute resolution.
      Meters marked with a star (&starf;) have pre-loaded detailed data (top 50 by consumption).
      You can search by typing part of the EAN number, or click a row in the ranking table below.
    </p>
    <div class="controls">
      <label>Search meter by EAN number:</label>
      <input type="text" id="eanSearch" placeholder="Type part of an EAN to filter the list ..." oninput="filterEans()">
      <select id="eanSelect" onchange="loadDetail()"></select>
    </div>
    <div class="info-banner" id="detailInfo">Select a meter from the dropdown above to view its detailed 15-minute energy readings over time.</div>
    <div id="detailChart" style="height:350px;"></div>
  </div>

  <div class="card">
    <h2>Meter Ranking &mdash; Highest Total Consumption</h2>
    <p class="card-desc">
      Top 200 meters sorted by their total E17 (consumption) value over the entire period.
      The "Injection/Consumption Ratio" column shows what percentage of consumed energy was produced back &mdash;
      a high ratio suggests the meter has solar panels or other generation.
      Click any row to view that meter's detailed chart above.
    </p>
    <div class="table-wrap">
      <table>
        <thead><tr>
          <th>#</th>
          <th>Meter EAN</th>
          <th><span class="tag tag-e17">Consumption</span> Total (kWh)</th>
          <th><span class="tag tag-e18">Injection</span> Total (kWh)</th>
          <th>Injection / Consumption Ratio</th>
        </tr></thead>
        <tbody id="rankingBody"></tbody>
      </table>
    </div>
  </div>

</div>

<script>
const DATA = """ + data_json + """;

const plotLayout = {
  paper_bgcolor: '#161b22', plot_bgcolor: '#0d1117',
  font: { color: '#c9d1d9', size: 12 },
  margin: { t: 30, r: 30, b: 50, l: 60 },
  xaxis: { gridcolor: '#21262d', linecolor: '#30363d',
    rangeselector: {
      buttons: [
        { count: 1, label: '1d', step: 'day', stepmode: 'backward' },
        { count: 3, label: '3d', step: 'day', stepmode: 'backward' },
        { count: 7, label: '1w', step: 'day', stepmode: 'backward' },
        { step: 'all', label: 'All' }
      ],
      bgcolor: '#0d1117', activecolor: '#1f6feb', font: { color: '#c9d1d9' }
    },
    rangeslider: { bgcolor: '#0d1117', thickness: 0.08 },
  },
  yaxis: { gridcolor: '#21262d', linecolor: '#30363d', title: 'Energy (kWh)' },
  legend: { orientation: 'h', y: 1.12 },
  hovermode: 'x unified',
};

// Stats
const allEans = DATA.allEans;
const ranking = DATA.ranking;
const totalE17 = ranking.reduce((s, r) => s + r.e17, 0);
const totalE18 = ranking.reduce((s, r) => s + r.e18, 0);
document.getElementById('stats').innerHTML = `
  <div class="stat"><div class="val">${allEans.length.toLocaleString()}</div><div class="lbl">Unique Meters (EANs)</div></div>
  <div class="stat"><div class="val">${DATA.overview.e17.x.length}</div><div class="lbl">Hours of Data Recorded</div></div>
  <div class="stat"><div class="val">${totalE17.toLocaleString(undefined,{maximumFractionDigits:0})} kWh</div><div class="lbl">Total Consumption (top 200 meters)</div></div>
  <div class="stat"><div class="val">${totalE18.toLocaleString(undefined,{maximumFractionDigits:0})} kWh</div><div class="lbl">Total Injection (top 200 meters)</div></div>
`;

// Overview chart
Plotly.newPlot('overviewChart', [
  { x: DATA.overview.e17.x, y: DATA.overview.e17.y, name: 'Consumption (energy taken from grid)', type: 'scatter', mode: 'lines', line: { color: '#58a6ff', width: 1.5 } },
  { x: DATA.overview.e18.x, y: DATA.overview.e18.y, name: 'Injection (energy fed back to grid)', type: 'scatter', mode: 'lines', line: { color: '#56d364', width: 1.5 } },
], { ...plotLayout }, { responsive: true });

// Populate dropdown
const select = document.getElementById('eanSelect');
function populateSelect(eans) {
  select.innerHTML = '<option value="">-- Select EAN --</option>';
  eans.forEach(ean => {
    const opt = document.createElement('option');
    opt.value = ean;
    const hasDetail = DATA.detail[ean] ? ' ★' : '';
    opt.textContent = ean + hasDetail;
    select.appendChild(opt);
  });
}
populateSelect(DATA.topEans.concat(allEans.filter(e => !DATA.topEans.includes(e))));

function filterEans() {
  const q = document.getElementById('eanSearch').value.trim();
  if (!q) { populateSelect(DATA.topEans.concat(allEans.filter(e => !DATA.topEans.includes(e)))); return; }
  const filtered = allEans.filter(e => e.includes(q));
  populateSelect(filtered);
}

function loadDetail() {
  const ean = select.value;
  if (!ean) return;
  const info = document.getElementById('detailInfo');
  if (DATA.detail[ean]) {
    const d = DATA.detail[ean];
    info.textContent = `Meter ${ean}: showing ${d.e17.x.length} consumption readings + ${d.e18.x.length} injection readings at 15-minute intervals.`;
    Plotly.newPlot('detailChart', [
      { x: d.e17.x, y: d.e17.y, name: 'Consumption (from grid)', type: 'scatter', mode: 'lines', line: { color: '#58a6ff', width: 1.2 } },
      { x: d.e18.x, y: d.e18.y, name: 'Injection (to grid)', type: 'scatter', mode: 'lines', line: { color: '#56d364', width: 1.2 } },
    ], { ...plotLayout, xaxis: { ...plotLayout.xaxis, rangeslider: { bgcolor: '#0d1117', thickness: 0.08 } } }, { responsive: true });
  } else {
    info.textContent = `Meter ${ean} does not have pre-loaded detail data. Only the top 50 meters by consumption have 15-minute data embedded in this dashboard.`;
    Plotly.purge('detailChart');
  }
}

// Ranking table
const tbody = document.getElementById('rankingBody');
ranking.forEach((r, i) => {
  const ratio = r.e17 > 0 ? (r.e18 / r.e17 * 100).toFixed(1) + '%' : '—';
  const tr = document.createElement('tr');
  tr.className = 'clickable';
  tr.innerHTML = `<td>${i+1}</td><td>${r.ean}</td><td>${r.e17.toLocaleString(undefined,{maximumFractionDigits:2})}</td><td>${r.e18.toLocaleString(undefined,{maximumFractionDigits:2})}</td><td>${ratio}</td>`;
  tr.onclick = () => { select.value = r.ean; loadDetail(); };
  tbody.appendChild(tr);
});
</script>
</body>
</html>"""

with open(OUTPUT, "w") as f:
    f.write(html)

print(f"\nDashboard written to {OUTPUT} ({os.path.getsize(OUTPUT)/1024/1024:.1f} MB)")
print(f"Open it with: xdg-open {OUTPUT}")
