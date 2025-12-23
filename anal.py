import pandas as pd
import os
import datetime
import numpy as np
from dotenv import load_dotenv

# --- НАСТРОЙКИ ---
load_dotenv()
FILE_NAME = 'Покупка пакетов.xlsx'
PRICE_FILE = 'price.xlsx'

# Настройки чувствительности робота
HIGH_LOAD_THRESHOLD = 0.80  # Если продаж > 80% от рекорда этой зоны -> ПОДНЯТЬ
LOW_LOAD_THRESHOLD = 0.20   # Если продаж < 20% от рекорда -> АКЦИЯ

def normalize_name(val):
    return str(val).strip().lower()

def get_day_type(dt):
    """
    Determines if a datetime is 'будни' (Weekday) or 'выходные' (Weekend).
    Weekday: Mon 08:00 - Fri 16:59
    Weekend: Fri 17:00 - Mon 07:59
    """
    weekday = dt.weekday() # 0=Mon, 4=Fri, 6=Sun
    hour = dt.hour

    # Friday check
    if weekday == 4:
        if hour >= 17: return 'выходные'
        return 'будни'

    # Weekend days (Sat, Sun)
    if weekday > 4:
        return 'выходные'

    # Monday check (Early morning is weekend)
    if weekday == 0:
        if hour < 8: return 'выходные'
        return 'будни'

    # Tue-Thu are always Weekdays
    return 'будни'

def get_time_slot(dt, t_code):
    """
    Determines 'day' vs 'evening' based on start time.
    Standard:
      - 5 Hours: Day < 14:00
      - 3 Hours: Day < 16:00
      - Others: Day < 17:00
    Night: Always 'night'
    AutoSim: Always 'all_day'
    """
    h = dt.hour

    if t_code == 'NIGHT': return 'night'
    if 'AUTOSIM' in t_code or 'HOURS' not in t_code:
        # Fallback for AutoSim specific codes if any
        pass

    cutoff = 17
    if t_code == '5_HOURS': cutoff = 14
    if t_code == '3_HOURS': cutoff = 16

    if 4 <= h < cutoff: return 'day'
    return 'evening'

# --- 1. ЗАГРУЗКА КОНФИГУРАЦИИ (PRICE.XLSX) ---
def load_config(file_path):
    print(f"🌐 Загрузка конфигурации из {file_path}...")
    try:
        df = pd.read_excel(file_path)
    except Exception as e:
        print(f"❌ Ошибка чтения Price.xlsx: {e}")
        return {}, {}, {}, {}

    pc_map = {} # {pc_name_normalized: zone_name}
    price_grid = {} # {zone: {tariff_code: {day_type: {slot: price}}}}
    zone_capacity = {} # {zone: count}

    # Keyword mapping for tariffs
    keywords = {
        'базовый': '1_HOUR',
        '1 час': '1_HOUR',
        '2 часа': '2_HOURS',
        '3 часа': '3_HOURS',
        '5 часов': '5_HOURS',
        'ночь': 'NIGHT'
    }

    for _, row in df.iterrows():
        z_name = str(row['Название']).strip()
        pcs_str = str(row.get('номера ПК', ''))
        t_raw = str(row.get('Тариф', '')).lower()
        d_type = str(row.get('тип дня недели', '')).lower() # будни/выходные
        time_range = str(row.get('Время цены', ''))
        price = float(row.get('Цена', 0))

        # 1. Map PCs
        if pcs_str and pcs_str.lower() != 'nan':
            pcs = [x.strip() for x in pcs_str.split(',')]
            for pc in pcs:
                norm_pc = normalize_name(pc)
                pc_map[norm_pc] = z_name
            zone_capacity[z_name] = len(pcs)

        # 2. Identify Tariff Code
        t_code = None
        is_autosim = 'автосим' in t_raw

        for kw, code in keywords.items():
            if kw in t_raw:
                t_code = code
                break

        if not t_code: continue

        # 3. Identify Time Slot (from 'Время цены')
        # e.g. "08:00-17:00" -> day, "17:00-08:00" -> evening
        # We rely on start hour
        try:
            start_h = int(time_range.split('-')[0].split(':')[0])
        except:
            start_h = 0 # Default

        slot = 'day'
        if is_autosim:
            slot = 'all_day'
        elif t_code == 'NIGHT':
            slot = 'night'
        else:
            # Re-use dynamic cutoff logic for consistency,
            # OR trust the explicit time range from excel?
            # User said: "column Time Price... e.g. 8 to 17".
            # Let's map 08:00 start -> day, 16:00/17:00 start -> evening.
            cutoff = 17
            if t_code == '5_HOURS': cutoff = 14
            if t_code == '3_HOURS': cutoff = 16

            if 4 <= start_h < cutoff: slot = 'day'
            else: slot = 'evening'

        # 4. Populate Grid
        if z_name not in price_grid: price_grid[z_name] = {}
        if t_code not in price_grid[z_name]: price_grid[z_name][t_code] = {}
        if d_type not in price_grid[z_name][t_code]: price_grid[z_name][t_code][d_type] = {}

        # Overwrite/Set price
        price_grid[z_name][t_code][d_type][slot] = price

    return pc_map, price_grid, zone_capacity

# --- 2. АНАЛИЗ EXCEL (SALES) ---
def analyze_excel(file_path, pc_map, price_grid):
    print("📂 Анализ продаж и подсчет чеков...")
    try:
        df = pd.read_excel(file_path)
    except Exception as e:
        print(f"❌ Ошибка чтения Excel: {e}")
        return None, None, None, None, None, None

    # Parsing dates
    df['dt_start'] = pd.to_datetime(df['Дата активации сессии'], dayfirst=True, errors='coerce')
    # Fallback
    if 'Дата покупки тарифа' in df.columns:
        df['dt_buy'] = pd.to_datetime(df['Дата покупки тарифа'], dayfirst=True, errors='coerce')
        df['dt_start'] = df['dt_start'].fillna(df['dt_buy'])

    df = df.dropna(subset=['dt_start'])

    # Structures
    # sales_stats[zone][tariff_code][day_type][slot] = {...}
    sales_stats = {}

    # Daily Occupancy
    daily_occupancy = {}

    # Retention
    phone_counts = {}

    # PC Revenue
    pc_revenue = {}

    duration_map = { '1_HOUR': 1, '2_HOURS': 2, '3_HOURS': 3, '5_HOURS': 5, 'NIGHT': 10 }

    # Keyword map for sales rows (same as config)
    keywords = {
        'базовый': '1_HOUR',
        '1 час': '1_HOUR',
        '2 часа': '2_HOURS',
        '3 часа': '3_HOURS',
        '5 часов': '5_HOURS',
        'ночь': 'NIGHT'
    }

    for _, row in df.iterrows():
        pc_raw = normalize_name(row.get('ПК'))
        z_name = pc_map.get(pc_raw)

        if not z_name: continue

        t_name = normalize_name(row.get('Название тарифа'))
        t_code = None
        is_autosim = 'автосим' in t_name

        for kw, code in keywords.items():
            if kw in t_name:
                t_code = code
                break

        if not t_code: continue

        dt = row['dt_start']
        d_type = get_day_type(dt) # 'будни' or 'выходные'

        # Phone stats
        phone = str(row.get('Номер телефона гостя', ''))
        if len(phone) > 5:
            phone_counts[phone] = phone_counts.get(phone, 0) + 1

        # Financials
        cash = float(row.get('Списано рублей', 0) or 0)
        bonus = float(row.get('Списано бонусов', 0) or 0)

        # PC Revenue
        if pc_raw:
            if pc_raw not in pc_revenue: pc_revenue[pc_raw] = {'cash':0, 'bonus':0, 'zone': z_name}
            pc_revenue[pc_raw]['cash'] += cash
            pc_revenue[pc_raw]['bonus'] += bonus

        # Time Slot
        slot = 'day'
        if is_autosim:
            slot = 'all_day'
        elif t_code == 'NIGHT':
            slot = 'night'
        else:
            cutoff = 17
            if t_code == '5_HOURS': cutoff = 14
            if t_code == '3_HOURS': cutoff = 16

            if 4 <= dt.hour < cutoff: slot = 'day'
            else: slot = 'evening'

        # Aggregation
        if z_name not in sales_stats: sales_stats[z_name] = {}
        if t_code not in sales_stats[z_name]: sales_stats[z_name][t_code] = {}
        if d_type not in sales_stats[z_name][t_code]:
            sales_stats[z_name][t_code][d_type] = {
                'day': {'count':0, 'hours':0, 'cash':0, 'bonus':0},
                'evening': {'count':0, 'hours':0, 'cash':0, 'bonus':0},
                'night': {'count':0, 'hours':0, 'cash':0, 'bonus':0},
                'all_day': {'count':0, 'hours':0, 'cash':0, 'bonus':0}
            }

        bucket = sales_stats[z_name][t_code][d_type][slot]
        bucket['count'] += 1
        dur = duration_map.get(t_code, 1)
        bucket['hours'] += dur
        bucket['cash'] += cash
        bucket['bonus'] += bonus

        # --- OCCUPANCY ---
        # Same minute-based logic as before
        est_end = row.get('Дата завершения сессии')
        if pd.isnull(est_end):
            est_end = dt + pd.Timedelta(hours=dur)
        else:
            est_end = pd.to_datetime(est_end, dayfirst=True, errors='coerce')
            if pd.isnull(est_end): est_end = dt + pd.Timedelta(hours=dur)

        curr_h = dt.replace(minute=0, second=0, microsecond=0)
        while curr_h < est_end:
            d_str = curr_h.strftime('%Y-%m-%d')
            h = curr_h.hour

            overlap_start = max(dt, curr_h)
            slot_end = curr_h + pd.Timedelta(hours=1)
            overlap_end = min(est_end, slot_end)

            mins = (overlap_end - overlap_start).total_seconds() / 60.0

            if mins > 0:
                if d_str not in daily_occupancy: daily_occupancy[d_str] = {}
                if z_name not in daily_occupancy[d_str]: daily_occupancy[d_str][z_name] = {i: 0 for i in range(24)}
                daily_occupancy[d_str][z_name][h] += mins

            curr_h += pd.Timedelta(hours=1)

    # Convert occupancy to stats
    # Group stats by 'будни'/'выходные' ?
    # The output report expects `group_hourly_stats[d_id]`.
    # We used `d_id` (Day ID) before. Now we have 'будни' or 'выходные'.
    # We can use these strings as keys.

    group_hourly_stats = {'будни': {}, 'выходные': {}}
    global_max_stats = {}

    for d_str, zones_data in daily_occupancy.items():
        dt_obj = datetime.datetime.strptime(d_str, '%Y-%m-%d')
        # Check day type for this specific date?
        # Note: The logic 'Fri 17:00' makes a single calendar DATE hybrid.
        # But `daily_occupancy` is bucketed by `d_str` (Calendar Day).
        # This is a slight mismatch.
        # However, for heatmaps, grouping by "Mon/Tue/Wed" vs "Sat/Sun" is usually enough.
        # Let's classify the whole date.
        w = dt_obj.weekday()
        d_type = 'выходные' if w >= 5 else 'будни' # Simplification for Heatmap aggregation

        for z, hours in zones_data.items():
            if z not in group_hourly_stats[d_type]: group_hourly_stats[d_type][z] = {h: {'max':0, 'sum':0, 'count':0} for h in range(24)}
            if z not in global_max_stats: global_max_stats[z] = {h: 0 for h in range(24)}

            for h, mins in hours.items():
                conc = mins / 60.0
                stats = group_hourly_stats[d_type][z][h]
                stats['max'] = max(stats['max'], conc)
                stats['sum'] += conc
                stats['count'] += 1
                global_max_stats[z][h] = max(global_max_stats[z][h], conc)

    # Retention
    repeats = sum(1 for c in phone_counts.values() if c > 1)
    retention_rate = (repeats / len(phone_counts) * 100) if phone_counts else 0

    # Day Counts (for averaging if needed)
    day_counts = {'будни': 1, 'выходные': 1} # Placeholder

    return sales_stats, day_counts, group_hourly_stats, global_max_stats, retention_rate, pc_revenue

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

    # 4. CRITICAL LOW LOAD -> PROMO (LOWER PRICE)
    if avg_load_pct <= 20 and peak_load_pct < 50:
        new_price = int(price * 0.9 / 10) * 10
        return 'PROMO', new_price, f"Простой ПК ({int(avg_load_pct)}%). Снизьте цену."

    return 'OK', price, ""

def generate_flyer_with_stats(price_grid, sales_stats, zone_capacities, group_hourly_stats, retention_rate, pc_revenue):
    print("🎨 Рисуем отчет...")

    # Calculate Totals
    total_sales = 0
    total_rev_c = 0
    total_rev_b = 0

    for z in sales_stats:
        for t in sales_stats[z]:
            for d in sales_stats[z][t]:
                for s in sales_stats[z][t][d]:
                    dat = sales_stats[z][t][d][s]
                    total_sales += dat['count']
                    total_rev_c += dat['cash']
                    total_rev_b += dat['bonus']

    total_rev = total_rev_c + total_rev_b
    bonus_share = (total_rev_b / total_rev * 100) if total_rev else 0

    # Worst PCs
    worst_pcs = sorted(pc_revenue.items(), key=lambda x: (x[1]['cash'] + x[1]['bonus']))[:15]
    worst_pc_html = """
    <div style='margin-top:40px; border-top:1px solid #333; padding-top:20px;'>
        <h3 style='color:#ff4d4d;'>📉 Топ-15 ПК с минимальной выручкой (Аутсайдеры)</h3>
        <table style='width:100%; max-width:800px; margin:0 auto; font-size:12px;'>
            <thead><tr style='background:#252525; color:#fff;'><th style='text-align:left; padding:8px;'>ПК</th><th style='text-align:left; padding:8px;'>Зона</th><th style='text-align:right; padding:8px;'>Выручка</th><th style='text-align:right; padding:8px;'>Бонусы</th></tr></thead>
            <tbody>
    """
    for pc, d in worst_pcs:
        worst_pc_html += f"<tr><td style='padding:8px;'>{pc}</td><td style='padding:8px;'>{d['zone']}</td><td style='text-align:right;'>{int(d['cash'])}</td><td style='text-align:right;'>{int(d['bonus'])}</td></tr>"
    worst_pc_html += "</tbody></table></div>"

    # Heatmap
    heatmap_html = ""
    for d_type in ['будни', 'выходные']:
        heatmap_html += f"<div style='margin-bottom:30px;'><h4>{d_type.upper()} - Пиковая Загрузка</h4><table style='font-size:10px; width:100%; border-spacing: 2px;'>"
        heatmap_html += "<tr><td style='width:100px;'></td>" + "".join([f"<td style='text-align:center; color:#888;'>{h:02d}</td>" for h in range(24)]) + "</tr>"

        for z_name in sorted(price_grid.keys()):
            stats = group_hourly_stats.get(d_type, {}).get(z_name, {})
            z_cap = zone_capacities.get(z_name, 1)

            heatmap_html += f"<tr><td style='text-align:right; padding-right:10px; font-weight:bold;'>{z_name}</td>"
            for h in range(24):
                val = stats.get(h, {}).get('max', 0)
                intensity = min(val/z_cap, 1.0) if z_cap > 0 else 0
                bg = "#222"
                if intensity >= 0.9: bg = f"rgba(255, 0, 0, {intensity})"
                elif intensity > 0.7: bg = f"rgba(255, 77, 77, {intensity})"
                elif intensity > 0.4: bg = f"rgba(255, 234, 0, {intensity})"
                elif intensity > 0: bg = f"rgba(0, 230, 118, {intensity})"

                heatmap_html += f"<td style='background:{bg}; color:white; text-align:center; padding:4px;'>{int(val)}</td>"
            heatmap_html += "</tr>"
        heatmap_html += "</table></div>"

    html = f"""
    <html>
    <head>
        <title>CyberX Smart Price (Excel Config)</title>
        <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
        <style>
            body {{ font-family: sans-serif; background: #121212; color: #ddd; padding: 20px; }}
            .container {{ max-width: 1280px; margin: 0 auto; }}
            h1 {{ text-align: center; color: #ff4d4d; }}
            .dashboard {{ display: flex; gap: 20px; justify-content: center; margin-bottom: 30px; }}
            .kpi-card {{ background: #252525; padding: 15px; border-radius: 8px; min-width: 150px; text-align: center; border-left: 4px solid #ff4d4d; }}
            .kpi-val {{ font-size: 24px; font-weight: bold; color: white; }}
            .zone-card {{ background: #1e1e1e; margin-bottom: 40px; border-radius: 8px; overflow: hidden; }}
            .zone-header {{ background: #252525; padding: 15px; font-size: 18px; font-weight: bold; border-bottom: 1px solid #444; }}
            table {{ width: 100%; border-collapse: collapse; }}
            th {{ text-align: left; padding: 10px; background: #2a2a2a; color: #888; font-size: 12px; text-transform: uppercase; }}
            td {{ padding: 10px; border-bottom: 1px solid #333; vertical-align: top; }}
            .price-tag {{ font-size: 16px; font-weight: bold; color: white; display: block; }}
            .stats {{ font-size: 10px; color: #666; }}
            .rec-up {{ background: #00e676; color: #000; padding: 2px 6px; border-radius: 4px; font-size: 10px; font-weight: bold; }}
            .rec-promo {{ background: #29b6f6; color: #fff; padding: 2px 6px; border-radius: 4px; font-size: 10px; font-weight: bold; }}
            .rec-bonus {{ background: #ffea00; color: #000; padding: 2px 6px; border-radius: 4px; font-size: 10px; font-weight: bold; }}
            .empty {{ color: #444; }}
        </style>
    </head>
    <body>
        <div class="container">
            <h1>Умный Прайс-Лист (Excel Source)</h1>
            <div class="dashboard">
                <div class="kpi-card"><div class="kpi-val">{int(total_sales)}</div><div>Чеков</div></div>
                <div class="kpi-card"><div class="kpi-val">{int(total_rev):,} ₽</div><div>Выручка</div></div>
                <div class="kpi-card"><div class="kpi-val">{int(bonus_share)}%</div><div>Бонусы</div></div>
                <div class="kpi-card"><div class="kpi-val">{int(retention_rate)}%</div><div>Retention</div></div>
            </div>

            <div style="display:flex; gap:20px;">
                <div style="flex:1; background:#1e1e1e; padding:10px; border-radius:8px;">
                    <canvas id="mainChart"></canvas>
                </div>
                <div style="flex:1; overflow-y:auto; max-height:400px;">
                    {heatmap_html}
                </div>
            </div>

            {worst_pc_html}
            <br>
    """

    col_order_std = [('1 ЧАС', '1_HOUR'), ('3 ЧАСА', '3_HOURS'), ('5 ЧАСОВ', '5_HOURS'), ('НОЧЬ', 'NIGHT')]
    col_order_auto = [('1 ЧАС', '1_HOUR'), ('2 ЧАСА', '2_HOURS'), ('3 ЧАСА', '3_HOURS')]

    # Sort zones by name
    for z_name in sorted(price_grid.keys()):
        is_autosim = 'авто' in z_name.lower() or 'auto' in z_name.lower()
        col_list = col_order_auto if is_autosim else col_order_std

        html += f"""
        <div class="zone-card">
            <div class="zone-header">{z_name}</div>
            <table>
                <thead>
                    <tr>
                        <th style="padding-left:20px;">День недели</th>
                        {"".join([f"<th>{lbl}</th>" for lbl, _ in col_list])}
                    </tr>
                </thead>
                <tbody>
        """

        # Determine active day types for this zone from price_grid
        # We expect 'будни' and 'выходные' usually
        active_days = set()
        for t in price_grid[z_name].values():
            active_days.update(t.keys())

        # Sort so 'будни' comes before 'выходные'
        for d_type in sorted(active_days, reverse=True): # 'выходные', 'будни' -> reverse=True makes 'будни' last? No.
            # 'будни' < 'выходные' alphabetically? 'б' vs 'в'. Yes.
            # So sorted() gives ['будни', 'выходные']. Perfect.

            html += f"<tr><td style='font-weight:bold; color:#ddd;'>{d_type.capitalize()}</td>"

            for lbl, t_code in col_list:
                # Get Price Data
                p_data = price_grid[z_name].get(t_code, {}).get(d_type, {})

                # Get Sales Data
                # Note: sales_stats has 'day', 'evening', 'night', 'all_day' buckets
                s_data = sales_stats.get(z_name, {}).get(t_code, {}).get(d_type, {})

                def render_cell(slot, label=None):
                    price = int(p_data.get(slot, 0))
                    if price == 0 and slot == 'all_day':
                         # Fallback search
                         for k, v in p_data.items():
                             if v > 0: price = int(v); break

                    if price == 0: return "<span class='empty'>-</span>"

                    # Stats
                    # Aggregate if s_data is empty but key exists?
                    # s_data is { 'day': {...}, 'evening': {...} }

                    if slot not in s_data:
                        # Init empty
                        bucket = {'count':0, 'hours':0, 'cash':0, 'bonus':0}
                    else:
                        bucket = s_data[slot]

                    count = bucket['count']
                    hours = bucket['hours']
                    cash = bucket['cash']
                    bonus = bucket['bonus']

                    # Load %
                    z_cap = zone_capacities.get(z_name, 1)
                    # Duration of slot?
                    slot_dur = 1
                    if slot == 'day': slot_dur = 13 # rough
                    elif slot == 'evening': slot_dur = 11
                    elif slot == 'night': slot_dur = 10
                    elif slot == 'all_day': slot_dur = 24

                    total_cap = z_cap * slot_dur # * days_in_period?
                    # We are aggregating ALL history. So we need number of days in history.
                    # Simplified: We use relative load from group_hourly_stats max.

                    # Peak Load from group stats
                    # d_type is 'будни' or 'выходные'
                    # slot determines hours
                    h_range = range(0,24)
                    if slot == 'day':
                        cut = 16 if t_code == '3_HOURS' else (14 if t_code == '5_HOURS' else 17)
                        h_range = range(4, cut)
                    elif slot == 'evening':
                        cut = 16 if t_code == '3_HOURS' else (14 if t_code == '5_HOURS' else 17)
                        h_range = list(range(cut, 24)) + list(range(0,4))
                    elif slot == 'night':
                        h_range = list(range(22, 24)) + list(range(0,8))

                    max_conc = 0
                    stats_z = group_hourly_stats.get(d_type, {}).get(z_name, {})
                    for h in h_range:
                        max_conc = max(max_conc, stats_z.get(h, {}).get('max', 0))

                    peak_pct = int(max_conc / z_cap * 100) if z_cap > 0 else 0

                    # Avg Load (Sold Hours / Total Capacity)
                    # Use avg of sum occupancy?
                    # Simplified: Use peak for recommendation

                    tot_rev_cell = cash + bonus
                    bon_pct = int(bonus / tot_rev_cell * 100) if tot_rev_cell > 0 else 0

                    rec_action, rec_price, _ = get_recommendation(peak_pct, 0, bon_pct, price)

                    badge = ""
                    if rec_action == 'UP': badge = f"<div class='rec-up'>▲ {rec_price}</div>"
                    elif rec_action == 'PROMO': badge = f"<div class='rec-promo'>▼ {rec_price}</div>"
                    elif rec_action == 'BONUS_UP': badge = f"<div class='rec-bonus'>★ BONUS</div>"

                    lbl_html = f"<div style='font-size:9px; color:#555;'>{label}</div>" if label else ""

                    return f"""
                    <div style='text-align:center;'>
                        {lbl_html}
                        {badge}
                        <span class='price-tag'>{price}</span>
                        <span class='stats'>Pk:{peak_pct}% <span style='color:#ff6384'>B:{bon_pct}%</span></span>
                    </div>
                    """

                if is_autosim:
                    html += f"<td>{render_cell('all_day')}</td>"
                else:
                    if t_code == 'NIGHT':
                        html += f"<td>{render_cell('night')}</td>"
                    else:
                        html += "<td><div class='split-row' style='display:flex; gap:10px; justify-content:center;'>"
                        html += f"<div style='flex:1; border-right:1px solid #333;'>{render_cell('day', 'День')}</div>"
                        html += f"<div style='flex:1;'>{render_cell('evening', 'Вечер')}</div>"
                        html += "</div></td>"

            html += "</tr>"
        html += "</tbody></table></div>"

    html += """
        <script>
            const ctx = document.getElementById('mainChart').getContext('2d');
            new Chart(ctx, {
                type: 'doughnut',
                data: {
                    labels: ['Рубли', 'Бонусы'],
                    datasets: [{
                        data: [""" + str(total_rev_c) + "," + str(total_rev_b) + """],
                        backgroundColor: ['#36a2eb', '#ff6384'],
                        borderWidth: 0
                    }]
                },
                options: {
                    plugins: {
                        legend: { position: 'right', labels: { color: 'white' } },
                        title: { display: true, text: 'Выручка', color: 'white' }
                    }
                }
            });
        </script>
    </body></html>
    """

    with open("FLYER_WITH_STATS.html", "w", encoding="utf-8") as f:
        f.write(html)
    print("✅ Отчет готов.")

if __name__ == "__main__":
    pc_map, price_grid, zone_capacities = load_config(PRICE_FILE)
    if pc_map:
        stats, day_counts, group_stats, glob_max, ret, pc_rev = analyze_excel(FILE_NAME, pc_map, price_grid)
        if stats:
            generate_flyer_with_stats(price_grid, stats, zone_capacities, group_stats, ret, pc_rev)
    else:
        print("❌ Не удалось загрузить конфигурацию.")