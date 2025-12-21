import pandas as pd
import requests
import os
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import datetime
from dotenv import load_dotenv

# --- SETTINGS ---
load_dotenv()
API_KEY = os.getenv("LANGAME_API_KEY") or "ВСТАВЬТЕ_ВАШ_КЛЮЧ"
FILE_NAME = 'Покупка пакетов.xlsx'
BASE_URL = 'https://cyberx165.langame-pr.ru/public_api'

def safe_request(endpoint):
    headers = {'X-API-KEY': API_KEY, 'accept': 'application/json'}
    try:
        r = requests.get(f"{BASE_URL}{endpoint}", headers=headers)
        if r.status_code == 200:
            raw = r.json()
            return raw if isinstance(raw, list) else raw.get('data', raw.get('items', []))
    except:
        pass
    return []

def fetch_config():
    print("🌐 Скачивание конфигурации тарифов...")

    # 1. Tariff Names
    tariffs_raw = safe_request("/tariffs/types_groups/list")
    # map id -> name
    tariff_map = {t['id']: t['name'] for t in tariffs_raw if 'id' in t}

    # 2. Zones
    zones_raw = safe_request("/global/types_of_pc_in_clubs/list")
    zone_map = {z['id']: z['name'] for z in zones_raw if 'id' in z}

    # 3. Time Periods (Restrictions)
    periods_raw = safe_request("/tariffs/time_period/list")

    # Structure: restrictions[zone_id][tariff_id] = { 'start': H, 'end': H }
    # Note: A tariff might have multiple periods (days vs weekends), we'll try to capture the "main" one or list all.
    # We aggregate by taking the widest or most common range for visualization?
    # Or better: separating by Day Type ID?
    # Let's map periods by d_id (Day Type) too.

    # restrictions[zone_id][d_id][tariff_id] = {'start': float, 'end': float}
    restrictions = {}

    for p in periods_raw:
        tid = p.get('tariff_packet_id')
        zid = p.get('packets_type_PC')
        did = p.get('tariff_groups')

        t_from = p.get('time_from') # "08:00:00"
        t_to = p.get('time_to')     # "17:00:00"

        if tid and zid and did and t_from and t_to:
            if zid not in restrictions: restrictions[zid] = {}
            if did not in restrictions[zid]: restrictions[zid][did] = {}

            # Convert to float hour (08:30 -> 8.5)
            def to_h(t_str):
                parts = t_str.split(':')
                return int(parts[0]) + int(parts[1])/60.0

            restrictions[zid][did][tid] = {
                'start': to_h(t_from),
                'end': to_h(t_to)
            }

    # Also fetch day type names
    day_types = {d['id']: d['name'] for d in safe_request("/tariffs/groups/list") if 'id' in d}

    # Calendar for historical mapping
    calendar = {d['date']: d['tariff_groups'] for d in safe_request("/tariffs/by_days/list") if 'date' in d}

    return tariff_map, zone_map, restrictions, day_types, calendar

def analyze_time_distribution(file_path, tariff_map, zone_map, calendar):
    print("📂 Analyzing Purchase Times...")
    try:
        df = pd.read_excel(file_path)
    except Exception as e:
        print(f"Error: {e}")
        return None

    # Parse Purchase Date
    df['dt_buy'] = pd.to_datetime(df['Дата покупки тарифа'], dayfirst=True, errors='coerce')
    df = df.dropna(subset=['dt_buy'])
    df['date_str'] = df['dt_buy'].dt.strftime('%Y-%m-%d')
    df['hour'] = df['dt_buy'].dt.hour + df['dt_buy'].dt.minute/60.0 # Float hour

    # Structure:
    # usage_stats[zone_id][d_id][tariff_id][hour_bin] = count
    usage_stats = {}

    # Reverse maps
    name_to_tid = {v.lower().strip(): k for k, v in tariff_map.items()}
    name_to_zid = {v.lower().strip(): k for k, v in zone_map.items()}

    for _, row in df.iterrows():
        t_name = str(row.get('Название тарифа')).lower().strip()
        z_name = str(row.get('ПК')).lower().strip() # Note: This is PC name, need to map to Zone via linking
        # Wait, previous script mapped PC -> Zone. I need that map here too.
        # But 'fetch_data' in 'anal.py' built it. I need to replicate that logic in fetch_config.
        # For now, I'll update fetch_config to return pc_map.
        pass

    return None

def fetch_pc_map():
    # Helper to get PC -> Zone ID map
    links = safe_request("/global/linking_pc_by_type/list")
    zones_raw = safe_request("/global/types_of_pc_in_clubs/list")
    zone_ids = {z['id'] for z in zones_raw if 'id' in z}

    pc_map = {}
    for l in links:
        num = str(l.get('pc_number') or l.get('name')).strip().lower()
        z_id = l.get('packets_type_PC')
        if num and z_id in zone_ids:
            pc_map[num] = z_id
    return pc_map

if __name__ == "__main__":
    t_map, z_map, limits, d_types, cal = fetch_config()
    pc_map = fetch_pc_map()

    # Re-implement analyze loop properly now
    print("📂 Analyzing Purchase Times...")
    try:
        df = pd.read_excel(FILE_NAME)
        df['dt_buy'] = pd.to_datetime(df['Дата покупки тарифа'], dayfirst=True, errors='coerce')
        df = df.dropna(subset=['dt_buy'])
        df['date_str'] = df['dt_buy'].dt.strftime('%Y-%m-%d')
        df['hour'] = df['dt_buy'].dt.hour # Integer hour bin for histogram

        # usage[zone_id][d_id][tariff_id] = {0: count, 1: count... 23: count}
        usage = {}

        name_to_tid = {v.lower().strip(): k for k, v in t_map.items()}

        for _, row in df.iterrows():
            pc = str(row.get('ПК')).lower().strip()
            z_id = pc_map.get(pc)

            t_name = str(row.get('Название тарифа')).lower().strip()
            t_id = name_to_tid.get(t_name)

            d_id = cal.get(row['date_str'])

            if z_id and t_id and d_id:
                if z_id not in usage: usage[z_id] = {}
                if d_id not in usage[z_id]: usage[z_id][d_id] = {}
                if t_id not in usage[z_id][d_id]: usage[z_id][d_id][t_id] = {h: 0 for h in range(24)}

                usage[z_id][d_id][t_id][row['hour']] += 1

        print(f"✅ Analyzed usage stats.")

    except Exception as e:
        print(f"❌ Error: {e}")
        usage = {}

    # --- GENERATE REPORT ---
    print("🎨 Generating Time Analysis Report...")

    # Iterate Zones -> Day Types -> Tariffs
    # Create a subplot for each Zone/DayType combo? Or just one big report with many charts.

    html_content = """
    <html>
    <head>
        <title>CyberX Time Analysis</title>
        <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
        <style>
            body { background-color: #121212; color: #e0e0e0; font-family: sans-serif; padding: 20px; }
            .card { background: #1e1e1e; border: 1px solid #333; margin-bottom: 20px; padding: 15px; border-radius: 8px; }
            h2 { color: #ff4d4d; }
            h3 { color: #aaa; margin-top: 0; }
            .chart-box { height: 400px; }
        </style>
    </head>
    <body>
        <h1>Анализ Временных Границ Тарифов</h1>
    """

    for zid, zname in z_map.items():
        if zid not in usage: continue

        html_content += f"<div class='card'><h2>Зона: {zname}</h2>"

        for did, dname in d_types.items():
            if did not in usage[zid]: continue

            html_content += f"<h3>📅 {dname}</h3>"

            for tid, hours_data in usage[zid][did].items():
                tname = t_map.get(tid, f"ID {tid}")

                # Check for restrictions
                res = limits.get(zid, {}).get(did, {}).get(tid)

                # Create Plotly Figure
                fig = go.Figure()

                # 1. Bar Chart of Demand
                x_axis = list(range(24))
                y_axis = [hours_data.get(h, 0) for h in x_axis]

                fig.add_trace(go.Bar(
                    x=x_axis, y=y_axis, name='Покупки', marker_color='#36a2eb'
                ))

                # 2. Highlight Active Window (if exists)
                if res:
                    start, end = res['start'], res['end']
                    # Handle crossing midnight? e.g. 22 to 08
                    # If end < start, draw two rects: start-24 and 0-end

                    shapes = []
                    if end < start:
                        shapes.append(dict(type="rect", x0=start, x1=24, y0=0, y1=max(y_axis)*1.1, fillcolor="green", opacity=0.2, line_width=0))
                        shapes.append(dict(type="rect", x0=0, x1=end, y0=0, y1=max(y_axis)*1.1, fillcolor="green", opacity=0.2, line_width=0))
                    else:
                        shapes.append(dict(type="rect", x0=start, x1=end, y0=0, y1=max(y_axis)*1.1, fillcolor="green", opacity=0.2, line_width=0))

                    fig.update_layout(shapes=shapes)

                    # Logic for Recommendation
                    # Check "Cliff Effect": Sales at (end + 1) hour?
                    cliff_sales = hours_data.get(int(end), 0) + hours_data.get(int(end)+1, 0)
                    if cliff_sales > sum(y_axis)*0.1: # if >10% of sales happen right after close
                        html_content += f"<p style='color:orange'>⚠️ <b>Продлить тариф?</b> {int(cliff_sales)} продаж сразу после {end}:00.</p>"

                fig.update_layout(
                    title=f"{tname} (Спрос по часам)",
                    plot_bgcolor='#1e1e1e',
                    paper_bgcolor='#1e1e1e',
                    font_color='#e0e0e0',
                    margin=dict(t=30, b=0, l=0, r=0),
                    height=300
                )

                # Convert to HTML
                chart_html = fig.to_html(full_html=False, include_plotlyjs=False)
                html_content += f"<div class='chart-box'>{chart_html}</div>"

        html_content += "</div>"

    html_content += "</body></html>"

    with open("TIME_REPORT.html", "w", encoding="utf-8") as f:
        f.write(html_content)
    print("✅ Report generated: TIME_REPORT.html")
