import pandas as pd
import requests
import os
import datetime
import numpy as np
from dotenv import load_dotenv

# --- НАСТРОЙКИ ---
load_dotenv()
API_KEY = os.getenv("LANGAME_API_KEY") or "ВСТАВЬТЕ_ВАШ_КЛЮЧ"
FILE_NAME = 'Покупка пакетов.xlsx'
BASE_URL = 'https://cyberx165.langame-pr.ru/public_api'

# Настройки чувствительности робота
HIGH_LOAD_THRESHOLD = 0.80  # Если продаж > 80% от рекорда этой зоны -> ПОДНЯТЬ
LOW_LOAD_THRESHOLD = 0.20   # Если продаж < 20% от рекорда -> АКЦИЯ

def normalize_name(val):
    return str(val).strip().lower()

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

# --- 1. СБОР ДАННЫХ ---
def fetch_data():
    print("🌐 Скачивание данных (Тарифы, Цены, Зоны)...")
    zones = {z['id']: z['name'] for z in safe_request("/global/types_of_pc_in_clubs/list") if 'id' in z}

    all_tariffs = safe_request("/tariffs/types_groups/list")
    target_tariffs = {}

    # Фильтр тарифов (Берем только основные)
    keywords = {
        '1 час': '1_HOUR',
        '3 часа': '3_HOURS',
        '5 часов': '5_HOURS',
        'ночь': 'NIGHT'
    }

    for t in all_tariffs:
        name_lower = t['name'].lower()
        if any(x in name_lower for x in ['абонемент', '24', '50', 'доплата']): continue

        for kw, code in keywords.items():
            if kw in name_lower:
                target_tariffs[t['id']] = {'name': t['name'], 'code': code}
                break

    # Цены
    prices_raw = safe_request("/tariffs/time_period/list")
    price_grid = {}

    for p in prices_raw:
        tid = p.get('tariff_packet_id')
        zid = p.get('packets_type_PC')
        did = p.get('tariff_groups')
        price = p.get('price', 0)

        if tid not in target_tariffs: continue
        t_code = target_tariffs[tid]['code']

        # Определяем слот: День (04:00-17:00) или Вечер (17:00-04:00)
        t_start = int(p.get('time_from', '00').split(':')[0])
        time_slot = 'day' if 4 <= t_start < 17 else 'evening'
        if t_code == 'NIGHT': time_slot = 'night'

        if zid not in price_grid: price_grid[zid] = {}
        if t_code not in price_grid[zid]: price_grid[zid][t_code] = {}
        if did not in price_grid[zid][t_code]: price_grid[zid][t_code][did] = {}

        curr = price_grid[zid][t_code][did].get(time_slot, 0)
        if price > curr:
            price_grid[zid][t_code][did][time_slot] = price

    day_types = {d['id']: d['name'] for d in safe_request("/tariffs/groups/list") if 'id' in d}
    calendar = {d['date']: d['tariff_groups'] for d in safe_request("/tariffs/by_days/list") if 'date' in d}

    links = safe_request("/global/linking_pc_by_type/list")
    pc_map = {}
    zone_capacity = {}

    for l in links:
        num = l.get('pc_number') or l.get('name')
        z_id = l.get('packets_type_PC')
        if num and z_id in zones:
            pc_map[normalize_name(num)] = z_id
            zone_capacity[z_id] = zone_capacity.get(z_id, 0) + 1

    t_name_map = {normalize_name(t['name']): t['id'] for t in all_tariffs}

    return zones, target_tariffs, price_grid, day_types, calendar, pc_map, t_name_map, zone_capacity

# --- 2. АНАЛИЗ EXCEL ---
def analyze_excel(file_path, zones, target_tariffs, pc_map, t_name_map, calendar):
    print("📂 Анализ продаж и подсчет чеков (Почасовой анализ)...")
    try:
        df = pd.read_excel(file_path)
    except Exception as e:
        print(f"Ошибка чтения Excel: {e}")
        return None, None, None, None

    # 1. Parsing dates
    df['dt_start'] = pd.to_datetime(df['Дата активации сессии'], dayfirst=True, errors='coerce')
    df['dt_end'] = pd.to_datetime(df['Дата завершения сессии'], dayfirst=True, errors='coerce')

    # Fallback for old purchases logic if session data missing (though user confirmed it exists)
    if 'Дата покупки тарифа' in df.columns:
        df['dt_buy'] = pd.to_datetime(df['Дата покупки тарифа'], dayfirst=True, errors='coerce')
        # If start is missing, use buy date
        df['dt_start'] = df['dt_start'].fillna(df['dt_buy'])

    df = df.dropna(subset=['dt_start'])
    df['date_str'] = df['dt_start'].dt.strftime('%Y-%m-%d')

    # Structures
    sales_stats = {}
    dates_per_group = {} # {d_id: set(dates)}

    # Hourly Occupancy: hourly_stats[zone][hour] = total_minutes_occupied
    hourly_stats = {}

    # Retention
    phone_counts = {}

    duration_map = { '1_HOUR': 1, '3_HOURS': 3, '5_HOURS': 5, 'NIGHT': 10 }

    for _, row in df.iterrows():
        pc = normalize_name(row.get('ПК'))
        z_id = pc_map.get(pc)
        t_name = normalize_name(row.get('Название тарифа'))
        t_id = t_name_map.get(t_name)
        d_id = calendar.get(row['date_str'])

        # Phone stats
        phone = str(row.get('Номер телефона гостя', ''))
        if len(phone) > 5:
            phone_counts[phone] = phone_counts.get(phone, 0) + 1

        if d_id and row['date_str']:
            if d_id not in dates_per_group: dates_per_group[d_id] = set()
            dates_per_group[d_id].add(row['date_str'])

        # Financials
        cash = float(row.get('Списано рублей', 0) or 0)
        bonus = float(row.get('Списано бонусов', 0) or 0)

        # --- TARIFF STATS ---
        if z_id and t_id in target_tariffs and d_id:
            t_code = target_tariffs[t_id]['code']
            start_h = row['dt_start'].hour

            time_slot = 'day' if 4 <= start_h < 17 else 'evening'
            if t_code == 'NIGHT': time_slot = 'night'

            if z_id not in sales_stats: sales_stats[z_id] = {}
            if t_code not in sales_stats[z_id]: sales_stats[z_id][t_code] = {}
            if d_id not in sales_stats[z_id][t_code]:
                sales_stats[z_id][t_code][d_id] = {
                    'day': {'count':0, 'hours':0, 'cash':0, 'bonus':0},
                    'evening': {'count':0, 'hours':0, 'cash':0, 'bonus':0},
                    'night': {'count':0, 'hours':0, 'cash':0, 'bonus':0}
                }

            slot_data = sales_stats[z_id][t_code][d_id][time_slot]
            slot_data['count'] += 1
            est_duration = duration_map.get(t_code, 0)
            slot_data['hours'] += est_duration
            slot_data['cash'] += cash
            slot_data['bonus'] += bonus

            # --- HOURLY OCCUPANCY CALCULATION ---
            # If we have end date, iterate hours. If not, estimate.
            s = row['dt_start']
            e = row['dt_end']
            if pd.isnull(e):
                e = s + pd.Timedelta(hours=est_duration)

            # Simple hourly bucket fill
            # We iterate from start hour to end hour
            # Note: sessions can cross midnight.
            curr = s
            while curr < e:
                h = curr.hour
                if z_id not in hourly_stats: hourly_stats[z_id] = {i: 0 for i in range(24)}
                hourly_stats[z_id][h] += 1
                curr += pd.Timedelta(hours=1)

    # Convert sets to counts
    day_counts = {k: len(v) for k, v in dates_per_group.items()}

    # Normalize hourly stats to "Avg Concurrent Users"
    # Total Hits per Hour / Total Days in Dataset (Approx)
    total_unique_days = len(set().union(*dates_per_group.values())) if dates_per_group else 1

    normalized_hourly = {}
    for z, hours in hourly_stats.items():
        normalized_hourly[z] = {h: cnt / total_unique_days for h, cnt in hours.items()}

    # Retention Rate
    repeats = sum(1 for c in phone_counts.values() if c > 1)
    retention_rate = (repeats / len(phone_counts) * 100) if phone_counts else 0

    return sales_stats, day_counts, normalized_hourly, retention_rate

# --- 3. РЕКОМЕНДАЦИИ И ОТЧЕТ ---
def get_recommendation(peak_load_pct, avg_load_pct, bonus_share_pct, price, current_bonus_limit=0.15):
    """
    Returns (action_code, new_price, reason)
    Russian responses.
    """
    # 1. PEAK LOAD > 90% -> CRITICAL RAISE
    if peak_load_pct >= 90:
        new_price = int(price * 1.20 / 10) * 10
        return 'UP', new_price, f"ПИКОВАЯ ЗАГРУЗКА ({int(peak_load_pct)}%) - СРОЧНО ПОДНЯТЬ"

    # 2. HIGH DEMAND (Avg > 70%) -> RAISE
    if avg_load_pct >= 70:
        new_price = int(price * 1.10 / 10) * 10
        return 'UP', new_price, f"Высокий спрос ({int(avg_load_pct)}%)"

    # 3. LOW LOAD + HIGH BONUS DEMAND -> ALLOW MORE BONUSES
    limit_pct = current_bonus_limit * 100
    if avg_load_pct <= 30 and bonus_share_pct >= (limit_pct * 0.9):
        return 'BONUS_UP', price, f"Низкая загр. ({int(avg_load_pct)}%), но бонусы популярны. Увеличьте лимит."

    # 4. CRITICAL LOW LOAD -> PROMO
    if avg_load_pct <= 20 and peak_load_pct < 50:
        return 'PROMO', price, f"Простой ПК ({int(avg_load_pct)}%). Нужна акция."

    return 'OK', price, ""

def generate_flyer_with_stats(zones, price_grid, sales_stats, day_types, zone_capacities, day_counts, hourly_stats, retention_rate):
    print("🎨 Рисуем отчет с доказательствами...")

    # --- CALCULATE AGGREGATES FOR DASHBOARD ---
    total_sales_count = 0
    total_revenue_cash = 0
    total_revenue_bonus = 0
    total_hours_sold = 0
    total_capacity_hours = 0

    # Pre-calc aggregates
    for z in sales_stats:
        z_cap = zone_capacities.get(z, 0)
        for t in sales_stats[z]:
            for d in sales_stats[z][t]:
                for s in sales_stats[z][t][d]:
                    data = sales_stats[z][t][d][s]
                    total_sales_count += data['count']
                    total_revenue_cash += data['cash']
                    total_revenue_bonus += data['bonus']
                    total_hours_sold += data['hours']

                    # Capacity Estimate: 1 date * slot hours
                    slot_h = 13 if s == 'day' else (11 if s == 'evening' else 10) # rough estimate
                    # Actually we need to know HOW MANY DAYS are in this dataset for this D_ID
                    # But simplified: we compare sold hours vs potential hours per session
                    # Better: Load % is calculated per cell.

    total_revenue = total_revenue_cash + total_revenue_bonus
    bonus_share_global = (total_revenue_bonus / total_revenue * 100) if total_revenue else 0

    # --- BUILD HEATMAP DATA ---
    # Rows: Zones, Cols: Hours 0-23
    heatmap_html = "<div style='overflow-x:auto;'><h3>Карта Почасовой Загрузки (Среднее кол-во занятых ПК)</h3><table style='font-size:10px; width:100%; border-spacing: 2px; border-collapse: separate;'>"
    heatmap_html += "<tr><td style='width:100px;'></td>" + "".join([f"<td style='text-align:center; color:#888;'>{h:02d}</td>" for h in range(24)]) + "</tr>"

    for zid, zname in sorted(zones.items()):
        if zid not in hourly_stats: continue
        z_cap = zone_capacities.get(zid, 1)
        heatmap_html += f"<tr><td style='color:#ddd; font-weight:bold; text-align:right; padding-right:10px;'>{zname}</td>"
        for h in range(24):
            val = hourly_stats[zid].get(h, 0)
            # intensity 0-1
            intensity = min(val / z_cap, 1.0) if z_cap > 0 else 0
            # Color: From Dark (#222) to Red (#ff4d4d)
            # Simple approach: R=255, G=255*(1-int), B=255*(1-int) for red scale?
            # Or Blue (empty) to Red (Full).
            # Let's do Green (Low) -> Yellow -> Red (High)

            bg = "#222"
            val_fmt = f"{val:.1f}" if val > 0 else ""

            if intensity > 0.8: bg = f"rgba(255, 77, 77, {intensity})" # Red
            elif intensity > 0.4: bg = f"rgba(255, 234, 0, {intensity})" # Yellow
            elif intensity > 0: bg = f"rgba(0, 230, 118, {intensity})" # Green

            heatmap_html += f"<td style='background:{bg}; color:white; text-align:center; padding:4px; border-radius:2px;'>{val_fmt}</td>"
        heatmap_html += "</tr>"
    heatmap_html += "</table></div>"

    html = f"""
    <html>
    <head>
        <title>CyberX Аналитика v2.0</title>
        <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
        <style>
            body {{
                font-family: 'Segoe UI', Arial, sans-serif;
                background-color: #121212;
                color: #e0e0e0;
                padding: 20px;
            }}
            .container {{ max-width: 1280px; margin: 0 auto; }}
            h1 {{ text-align: center; color: #ff4d4d; margin-bottom: 20px; letter-spacing: 1px; }}

            /* Dashboard */
            .dashboard {{ display: flex; gap: 20px; margin-bottom: 40px; justify-content: center; flex-wrap: wrap; }}
            .kpi-card {{ background: #252525; padding: 20px; border-radius: 8px; border-left: 4px solid #ff4d4d; min-width: 200px; }}
            .kpi-val {{ font-size: 24px; font-weight: bold; color: white; }}
            .kpi-label {{ font-size: 12px; text-transform: uppercase; color: #888; }}

            /* Charts */
            .charts-row {{ display: flex; gap: 20px; margin-bottom: 40px; }}
            .chart-container {{ flex: 1; background: #1e1e1e; padding: 15px; border-radius: 8px; min-height:300px; }}

            .legend {{ text-align: center; color: #aaa; margin-bottom: 20px; font-size: 14px; border-top: 1px solid #333; padding-top: 20px; }}

            .zone-card {{
                background: #1e1e1e;
                border: 1px solid #333;
                border-top: 3px solid #ff4d4d;
                border-radius: 8px;
                margin-bottom: 40px;
                box-shadow: 0 4px 15px rgba(0,0,0,0.3);
            }}

            .zone-header {{
                background: #252525;
                padding: 15px 20px;
                font-size: 20px;
                font-weight: bold;
                color: #fff;
                border-bottom: 1px solid #333;
                display: flex; justify-content: space-between; align-items: center;
            }}

            table {{ width: 100%; border-collapse: collapse; }}

            th {{
                background: #2a2a2a;
                color: #888;
                padding: 12px;
                font-size: 11px;
                text-transform: uppercase;
                border-bottom: 1px solid #444;
            }}

            td {{
                padding: 10px;
                text-align: center;
                border-bottom: 1px solid #333;
                vertical-align: top;
            }}

            .row-title {{ text-align: left; color: #ddd; font-weight: bold; width: 140px; padding-left: 20px;}}

            /* Блок цены и статистики */
            .cell-container {{ display: flex; flex-direction: column; align-items: center; gap: 4px; }}

            .price-tag {{
                font-size: 18px; font-weight: bold; color: #fff;
            }}

            .stats-info {{
                font-size: 10px; color: #666; font-family: monospace;
            }}

            .rec-badge {{
                font-size: 10px; padding: 3px 6px; border-radius: 4px; color: #121212; font-weight: bold; margin-bottom: 2px; display: inline-block;
            }}
            .rec-up {{ background: #00e676; box-shadow: 0 0 8px rgba(0,230,118,0.2); }}
            .rec-promo {{ background: #29b6f6; color: white; }}
            .rec-bonus-up {{ background: #ffea00; color: black; }}

            .split-row {{ display: flex; width: 100%; }}
            .split-col {{ flex: 1; border-right: 1px solid #333; padding: 0 5px; }}
            .split-col:last-child {{ border: none; }}
            .time-label {{ font-size: 9px; color: #555; display: block; margin-bottom: 4px; }}

            .empty {{ color: #444; font-size: 20px; }}
        </style>
    </head>
    <body>
        <div class="container">
            <h1>Умный Прайс-Лист (Аналитика)</h1>

            <div class="dashboard">
                <div class="kpi-card">
                    <div class="kpi-val">{int(total_sales_count)}</div>
                    <div class="kpi-label">Чеков проанализировано</div>
                </div>
                <div class="kpi-card">
                    <div class="kpi-val">{int(total_revenue):,} ₽</div>
                    <div class="kpi-label">Общая выручка</div>
                </div>
                <div class="kpi-card">
                    <div class="kpi-val">{int(bonus_share_global)}%</div>
                    <div class="kpi-label">Доля бонусов (Avg)</div>
                </div>
                <div class="kpi-card">
                    <div class="kpi-val">{int(retention_rate)}%</div>
                    <div class="kpi-label">Возврат клиентов (Retention)</div>
                </div>
            </div>

            <div class="charts-row">
                <div class="chart-container" style="flex:0 0 300px;">
                    <canvas id="revChart"></canvas>
                </div>
                <div class="chart-container">
                    {heatmap_html}
                </div>
            </div>

            <div class="legend">
                <span style="color:#00e676">▲ ПОВЫСИТЬ</span> = Пик >90% или Ср. >70% &nbsp;|&nbsp;
                <span style="color:#29b6f6">▼ АКЦИЯ</span> = Загрузка <20% &nbsp;|&nbsp;
                <span style="color:#ffea00">★ БОНУСЫ</span> = Популярны бонусы
            </div>

            <script>
                const ctx = document.getElementById('revChart').getContext('2d');
                new Chart(ctx, {{
                    type: 'doughnut',
                    data: {{
                        labels: ['Рубли', 'Бонусы'],
                        datasets: [{{
                            data: [{int(total_revenue_cash)}, {int(total_revenue_bonus)}],
                            backgroundColor: ['#36a2eb', '#ff6384'],
                            borderWidth: 0
                        }}]
                    }},
                    options: {{
                        responsive: true,
                        maintainAspectRatio: false,
                        plugins: {{
                            legend: {{ position: 'bottom', labels: {{ color: 'white' }} }},
                            title: {{ display: true, text: 'Выручка', color: 'white' }}
                        }}
                    }}
                }});
            </script>
    """

    col_order = ['1_HOUR', '3_HOURS', '5_HOURS', 'NIGHT']

    sorted_zones = sorted(zones.items(), key=lambda x: x[1])

    for zid, zname in sorted_zones:
        if zid not in price_grid: continue

        html += f"""
        <div class="zone-card">
            <div class="zone-header">{zname}</div>
            <table>
                <thead>
                    <tr>
                        <th style="text-align:left; padding-left:20px;">День недели</th>
                        <th>1 ЧАС</th>
                        <th>3 ЧАСА</th>
                        <th>5 ЧАСОВ</th>
                        <th>НОЧЬ</th>
                    </tr>
                </thead>
                <tbody>
        """

        day_ids = sorted(price_grid[zid][list(price_grid[zid].keys())[0]].keys())

        for did in day_ids:
            dname = day_types.get(did, 'Day')
            html += f"<tr><td class='row-title'>{dname}</td>"

            for t_code in col_order:
                data = price_grid[zid].get(t_code, {}).get(did, {})
                # Use default structure for missing data
                default_slot = {'count':0, 'hours':0, 'cash':0, 'bonus':0}
                default_stats = {'day': default_slot.copy(), 'evening': default_slot.copy(), 'night': default_slot.copy()}

                stats = sales_stats.get(zid, {}).get(t_code, {}).get(did, default_stats)

                # Capacity for this zone
                z_cap = zone_capacities.get(zid, 1)

                def render_cell(slot):
                    price = int(data.get(slot, 0))
                    if price == 0: return "<span class='empty'>-</span>"

                    # СТАТИСТИКА
                    slot_data = stats.get(slot, default_slot)
                    count = slot_data['count']
                    hours_sold = slot_data['hours']
                    cash = slot_data['cash']
                    bonus = slot_data['bonus']

                    # Calculate Load %
                    slot_hours_duration = 13 if slot == 'day' else (11 if slot == 'evening' else 10)
                    # Use specific day count for this tariff group (did)
                    days_in_group = day_counts.get(did, 1)
                    total_capacity_hours = z_cap * slot_hours_duration * max(1, days_in_group)

                    avg_load_pct = 0
                    if total_capacity_hours > 0:
                        avg_load_pct = int((hours_sold / total_capacity_hours) * 100)

                    # Calculate PEAK Load for this slot
                    # Day: 04-16, Evening: 17-03, Night: 22-08 (approx)
                    hours_to_check = []
                    if slot == 'day': hours_to_check = list(range(4, 17))
                    elif slot == 'evening': hours_to_check = list(range(17, 24)) + list(range(0, 4))
                    elif slot == 'night': hours_to_check = list(range(22, 24)) + list(range(0, 8))

                    max_occupancy = 0
                    if zid in hourly_stats:
                        for h in hours_to_check:
                            max_occupancy = max(max_occupancy, hourly_stats[zid].get(h, 0))

                    # hourly_stats values are already normalized to "Avg Concurrent Users"
                    peak_load_pct = int((max_occupancy / z_cap) * 100) if z_cap > 0 else 0

                    # Bonus Share
                    total_paid = cash + bonus
                    bonus_share = (bonus / total_paid * 100) if total_paid > 0 else 0

                    # Recommendation
                    rec_action, rec_price, rec_reason = get_recommendation(peak_load_pct, avg_load_pct, bonus_share, price)

                    stats_html = f"<span class='stats-info'>Av:{avg_load_pct}% / Pk:{peak_load_pct}%</span>"
                    # Add bonus info if significant
                    if bonus_share > 5:
                         stats_html += f"<br><span class='stats-info' style='color:#ff6384'>B: {int(bonus_share)}%</span>"

                    rec_html = ""
                    if rec_action == 'UP':
                        rec_html = f"<div class='rec-badge rec-up'>▲ {rec_price}</div>"
                    elif rec_action == 'PROMO':
                        rec_html = f"<div class='rec-badge rec-promo'>PROMO</div>"
                    elif rec_action == 'BONUS_UP':
                        rec_html = f"<div class='rec-badge rec-bonus-up'>★ BONUS</div>"

                    return f"""
                    <div class="cell-container">
                        {rec_html}
                        <span class="price-tag">{price}</span>
                        {stats_html}
                    </div>
                    """

                if t_code == 'NIGHT':
                    html += f"<td>{render_cell('night')}</td>"
                else:
                    html += f"""
                    <td>
                        <div class="split-row">
                            <div class="split-col">
                                <span class="time-label">День</span>
                                {render_cell('day')}
                            </div>
                            <div class="split-col">
                                <span class="time-label">Вечер</span>
                                {render_cell('evening')}
                            </div>
                        </div>
                    </td>
                    """
            html += "</tr>"
        html += "</tbody></table></div>"

    html += "</div></body></html>"

    with open("FLYER_WITH_STATS.html", "w", encoding="utf-8") as f:
        f.write(html)
    print("\n✅ ОТЧЕТ ГОТОВ: FLYER_WITH_STATS.html")

if __name__ == "__main__":
    if not API_KEY or "ВСТАВЬТЕ" in API_KEY:
        print("❌ Ключ API не найден.")
    else:
        zones, targets, prices, dtypes, cal, pc_map, t_map, zone_caps = fetch_data()
        stats, day_counts, hourly_stats, retention = analyze_excel(FILE_NAME, zones, targets, pc_map, t_map, cal)
        if stats:
            generate_flyer_with_stats(zones, prices, stats, dtypes, zone_caps, day_counts, hourly_stats, retention)
        else:
            print("❌ Ошибка с Excel файлом")