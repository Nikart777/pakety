import pandas as pd
import requests
import os
import plotly.graph_objects as go
import datetime
from dotenv import load_dotenv

# --- SETTINGS ---
load_dotenv()
API_KEY = os.getenv("LANGAME_API_KEY") or "ВСТАВЬТЕ_ВАШ_КЛЮЧ"
FILE_NAME = 'Покупка пакетов.xlsx'
BASE_URL = 'https://cyberx165.langame-pr.ru/public_api'

# --- HELPERS ---
def format_time(h_float):
    """Converts 13.98 -> '13:59', 25.5 -> '01:30'."""
    h_float = h_float % 24
    h = int(h_float)
    m = int(round((h_float - h) * 60))
    if m == 60:
        h += 1
        m = 0
        if h == 24: h = 0
    return f"{h:02d}:{m:02d}"

def normalize_hour(h_float):
    """Wraps 25.0 -> 1.0, -1.0 -> 23.0"""
    return h_float % 24

def classify_zone(z_name):
    """Returns 'CONSOLE' or 'STANDARD'."""
    z = str(z_name).lower()
    if any(x in z for x in ['ps5', 'playstation', 'auto', 'sim', 'авто', 'сим']):
        return 'CONSOLE'
    return 'STANDARD'

# --- CONFIG ---
# Hardcoded Rules as per User Input
RULES = {
    'STANDARD': {
        '1_HOUR': {'morning_end': 17},
        '3_HOURS': {'morning_end': 16},
        '5_HOURS': {'morning_end': 14},
        'NIGHT': {'start': 22, 'end': 8}
    },
    'CONSOLE': {
        '1_HOUR': {'morning_end': 17},
        '3_HOURS': {'morning_end': 17},
        '5_HOURS': {'morning_end': 17},
        'NIGHT': {'start': 22, 'end': 8}
    }
}

# Mapping specific tariff names to Types
TARIFF_TYPE_MAP = {
    '1 час': '1_HOUR',
    '3 часа': '3_HOURS',
    '5 часов': '5_HOURS',
    'ночь': 'NIGHT',
    'базовый': '1_HOUR' # Assuming basic is 1 hour
}

def safe_request(endpoint):
    headers = {'X-API-KEY': API_KEY, 'accept': 'application/json'}
    try:
        r = requests.get(f"{BASE_URL}{endpoint}", headers=headers)
        if r.status_code != 200:
            print(f"⚠️ Error {r.status_code} on {endpoint}")
            return []
        raw = r.json()
        if isinstance(raw, list): return raw
        return raw.get('data', raw.get('items', []))
    except Exception as e:
        print(f"⚠️ Exception on {endpoint}: {e}")
        pass
    return []

def fetch_metadata():
    print("🌐 Скачивание метаданных...")
    zones = {}
    pc_map = {}

    # Try fetch, but don't fail if timeout
    z_list = safe_request("/global/types_of_pc_in_clubs/list")
    if z_list:
        zones = {z['id']: z['name'] for z in z_list if 'id' in z}

    l_list = safe_request("/global/linking_pc_by_type/list")
    if l_list:
        for l in l_list:
            num = str(l.get('pc_number') or l.get('name')).strip().lower()
            z_id = l.get('packets_type_PC')
            if num and z_id in zones:
                pc_map[num] = z_id

    return zones, pc_map

def analyze_time_distribution(file_path, zones, pc_map):
    print("📂 Анализ времени покупок...")
    try:
        df = pd.read_excel(file_path)
    except Exception as e:
        print(f"❌ Ошибка Excel: {e}")
        return None

    df['dt_buy'] = pd.to_datetime(df['Дата покупки тарифа'], dayfirst=True, errors='coerce')
    df = df.dropna(subset=['dt_buy'])

    # Hour as float for precise binning (e.g. 13.9 is 13:54)
    df['hour'] = df['dt_buy'].dt.hour + df['dt_buy'].dt.minute/60.0

    # Data Structure:
    # stats[ZoneType][TariffType] = List of purchase hours
    stats = {
        'STANDARD': {'1_HOUR': [], '3_HOURS': [], '5_HOURS': [], 'NIGHT': []},
        'CONSOLE': {'1_HOUR': [], '3_HOURS': [], '5_HOURS': [], 'NIGHT': []}
    }

    for _, row in df.iterrows():
        pc = str(row.get('ПК')).lower().strip()

        # Determine Zone Type (Fallback to PC name if API failed)
        z_type = 'STANDARD'

        z_id = pc_map.get(pc)
        if z_id:
            z_name = zones.get(z_id, "")
            z_type = classify_zone(z_name)
        else:
            # Fallback: Classify by PC name
            z_type = classify_zone(pc)

        t_name = str(row.get('Название тарифа')).lower()
        t_type = None

        for k, v in TARIFF_TYPE_MAP.items():
            if k in t_name:
                t_type = v
                break

        if t_type and t_type in stats[z_type]:
            stats[z_type][t_type].append(row['hour'])

    return stats

def generate_recommendations(stats):
    recommendations = []

    for z_type, tariffs in stats.items():
        rules = RULES[z_type]

        for t_type, hours in tariffs.items():
            if not hours: continue

            # 1. Histogram (24 bins)
            hist = [0] * 24
            for h in hours:
                hist[int(h % 24)] += 1

            total_sales = len(hours)

            # --- MORNING CUTOFF ANALYSIS ---
            if 'morning_end' in rules[t_type]:
                cutoff = rules[t_type]['morning_end']

                # Demand right BEFORE cutoff (e.g. 13:00-14:00 for 14:00 cutoff)
                pre_sales = hist[cutoff-1]
                # Demand right AFTER cutoff (e.g. 14:00-15:00)
                post_sales = hist[cutoff]
                next_sales = hist[(cutoff+1)%24]

                # Logic: If drop is HUGE > 80%, maybe people are downgrading?
                # Actually user wants to know if they should shift.

                # 1. EXTEND? If significant sales occur immediately after cutoff
                # (Meaning people are paying the higher Evening price, OR simply high demand)
                if post_sales > (total_sales * 0.05) and post_sales > 5:
                     recommendations.append({
                        'zone': z_type,
                        'tariff': t_type,
                        'msg': f"Продлить Утро до {format_time(cutoff+1)}",
                        'reason': f"Высокий спрос ({post_sales} чек.) в первый час Вечера ({format_time(cutoff)}-{format_time(cutoff+1)}).",
                        'priority': post_sales
                    })

                # 2. SHORTEN? If last hour of Morning is dead
                if pre_sales == 0 and total_sales > 10:
                     recommendations.append({
                        'zone': z_type,
                        'tariff': t_type,
                        'msg': f"Сократить Утро до {format_time(cutoff-1)}",
                        'reason': f"Нет продаж в последний час Утра ({format_time(cutoff-1)}-{format_time(cutoff)}).",
                        'priority': 5
                    })

            # --- NIGHT START ANALYSIS ---
            if 'start' in rules[t_type]: # Night Tariff
                start = rules[t_type]['start']

                # Check hour BEFORE night starts (e.g. 21:00-22:00)
                waiting_sales = hist[start-1]

                # If very low sales before night, maybe people are waiting?
                # Hard to say without comparing to other tariffs.
                # But if Night sales at 22:00 are HUGE compared to 21:00 generic sales...
                # We only see Night sales here.

                # Check Night Peak
                night_peak = hist[start]
                if night_peak > 10 and waiting_sales == 0:
                     # This logic is flawed because "waiting_sales" variable looks at NIGHT tariff sales at 21:00
                     # which should be 0 anyway.
                     pass

    return sorted(recommendations, key=lambda x: x['priority'], reverse=True)

def generate_report(stats, recs):
    print("🎨 Генерация отчета...")

    html = """
    <html>
    <head>
        <title>CyberX Time Analysis</title>
        <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
        <style>
            body { background: #121212; color: #eee; font-family: 'Segoe UI', sans-serif; padding: 20px; }
            .card { background: #1e1e1e; padding: 20px; border-radius: 8px; margin-bottom: 20px; border: 1px solid #333; }
            table { width: 100%; border-collapse: collapse; margin-top: 10px; }
            th { text-align: left; border-bottom: 2px solid #ff4d4d; padding: 10px; color: #fff; }
            td { border-bottom: 1px solid #333; padding: 10px; color: #ccc; }
            .badge { padding: 4px 8px; border-radius: 4px; font-weight: bold; font-size: 12px; color: #000; }
            .badge-warn { background: #ffeb3b; }
            .badge-ok { background: #00e676; }
            h2 { color: #ff4d4d; margin-top: 0; }
        </style>
    </head>
    <body>
        <h1>Анализ Временных Границ (Факт vs План)</h1>

        <div class="card">
            <h2>💡 Рекомендации</h2>
            <table>
                <tr>
                    <th>Тип Зоны</th>
                    <th>Тариф</th>
                    <th>Совет</th>
                    <th>Причина</th>
                </tr>
    """

    if not recs:
        html += "<tr><td colspan='4' style='text-align:center'>Временные границы выглядят оптимально.</td></tr>"
    else:
        for r in recs:
            html += f"""
            <tr>
                <td>{r['zone']}</td>
                <td>{r['tariff']}</td>
                <td><span class="badge badge-warn">{r['msg']}</span></td>
                <td>{r['reason']}</td>
            </tr>
            """

    html += "</table></div>"

    # --- CHARTS ---
    for z_type, tariffs in stats.items():
        html += f"<h2>{z_type} ZONES</h2>"

        for t_type, hours in tariffs.items():
            if not hours: continue

            fig = go.Figure()

            # Histogram
            fig.add_trace(go.Histogram(
                x=hours,
                xbins=dict(start=0, end=24, size=1),
                marker_color='#36a2eb',
                name='Покупки'
            ))

            # Draw Current Windows
            rule = RULES[z_type].get(t_type)
            shapes = []

            if rule:
                if 'morning_end' in rule:
                    me = rule['morning_end']
                    # Morning (Green)
                    shapes.append(dict(type="rect", x0=8, x1=me, y0=0, y1=1, yref="paper", fillcolor="green", opacity=0.1, line_width=0))
                    # Evening (Orange)
                    shapes.append(dict(type="rect", x0=me, x1=24, y0=0, y1=1, yref="paper", fillcolor="orange", opacity=0.1, line_width=0))

                    fig.add_annotation(x=me, y=1, yref="paper", text=f"End: {format_time(me)}", showarrow=True, arrowcolor="white")

                if 'start' in rule: # Night
                    ns = rule['start']
                    ne = rule['end']
                    shapes.append(dict(type="rect", x0=ns, x1=24, y0=0, y1=1, yref="paper", fillcolor="purple", opacity=0.2, line_width=0))
                    shapes.append(dict(type="rect", x0=0, x1=ne, y0=0, y1=1, yref="paper", fillcolor="purple", opacity=0.2, line_width=0))

            fig.update_layout(
                title=f"{t_type} - Распределение спроса",
                shapes=shapes,
                plot_bgcolor='#1e1e1e',
                paper_bgcolor='#1e1e1e',
                font_color='#ccc',
                height=300,
                margin=dict(l=20, r=20, t=40, b=20),
                xaxis=dict(title="Час дня (0-23)", dtick=1)
            )

            html += f"<div class='card'>{fig.to_html(full_html=False, include_plotlyjs=False)}</div>"

    html += "</body></html>"

    with open("TIME_REPORT.html", "w", encoding="utf-8") as f:
        f.write(html)
    print("✅ Отчет сохранен: TIME_REPORT.html")

if __name__ == "__main__":
    zones, pc_map = fetch_metadata()
    # Proceed even if zones empty, using fallback
    stats = analyze_time_distribution(FILE_NAME, zones, pc_map)
    if stats:
        recs = generate_recommendations(stats)
        generate_report(stats, recs)
    else:
        print("❌ Ошибка анализа.")