import pandas as pd
import os
import datetime
import numpy as np
from dotenv import load_dotenv

# --- НАСТРОЙКИ ---
load_dotenv()
FILE_NAME = 'Покупка пакетов.xlsx'
PRICE_FILE = 'price.xlsx'
COMPETITORS_FILE = 'competitors.xlsx'

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

def get_cutoff_hour(t_code):
    """Returns the hour where Day ends and Evening starts."""
    if t_code == '5_HOURS': return 14
    if t_code == '3_HOURS': return 16
    return 17

def get_tariff_code(name_raw):
    """Normalize tariff name to code."""
    name_lower = normalize_name(name_raw)
    keywords = {
        'базовый': '1_HOUR',
        '1 час': '1_HOUR',
        '2 часа': '2_HOURS',
        '3 часа': '3_HOURS',
        '5 часов': '5_HOURS',
        'ночь': 'NIGHT'
    }

    # AutoSim check
    is_autosim = 'автосим' in name_lower

    for kw, code in keywords.items():
        if kw in name_lower:
            return code, is_autosim

    return None, False

# --- 1. ЗАГРУЗКА КОНФИГУРАЦИИ (PRICE.XLSX) ---
def load_config(file_path):
    print(f"🌐 Загрузка конфигурации из {file_path}...")
    try:
        df = pd.read_excel(file_path)
        df.columns = df.columns.str.strip()
    except Exception as e:
        print(f"❌ Ошибка чтения Price.xlsx: {e}")
        return {}, {}, {}, {}

    pc_map = {}
    price_grid = {}
    zone_capacity = {}

    required_cols = ['Название', 'номера ПК', 'Тариф', 'тип дня недели', 'Время цены', 'Цена']
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        print(f"❌ Ошибка: В файле {file_path} не найдены столбцы: {missing}")
        return {}, {}, {}, {}

    for _, row in df.iterrows():
        z_name = str(row['Название']).strip()
        pcs_str = str(row.get('номера ПК', ''))
        t_raw = str(row.get('Тариф', ''))
        d_type = str(row.get('тип дня недели', '')).lower()
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
        t_code, is_autosim = get_tariff_code(t_raw)
        if not t_code: continue

        # 3. Identify Time Slot
        try:
            start_h = int(time_range.split('-')[0].split(':')[0])
        except:
            start_h = 0

        slot = 'day'
        if is_autosim:
            slot = 'all_day'
        elif t_code == 'NIGHT':
            slot = 'night'
        else:
            cutoff = get_cutoff_hour(t_code)
            if 4 <= start_h < cutoff: slot = 'day'
            else: slot = 'evening'

        # 4. Populate Grid
        if z_name not in price_grid: price_grid[z_name] = {}
        if t_code not in price_grid[z_name]: price_grid[z_name][t_code] = {}
        if d_type not in price_grid[z_name][t_code]: price_grid[z_name][t_code][d_type] = {}

        price_grid[z_name][t_code][d_type][slot] = price

    return pc_map, price_grid, zone_capacity

# --- 2. ЗАГРУЗКА КОНКУРЕНТОВ ---
def load_competitors(file_path):
    print(f"⚔️ Загрузка конкурентов из {file_path}...")
    market_data = {} # {Zone: {TariffCode: {'fair': X, 'avg': Y, 'k': Z}}}

    if not os.path.exists(file_path):
        print(f"⚠️ Файл конкурентов {file_path} не найден. Работаем без рыночного фильтра.")
        return market_data

    try:
        df = pd.read_excel(file_path)
        df.columns = df.columns.str.strip()

        cols = df.columns.tolist()
        price_cols = [c for c in cols if 'цена' in c.lower() and 'конкурент' in c.lower()]

        for _, row in df.iterrows():
            z_name = str(row.get('Ваша Зона', '')).strip()
            t_raw = str(row.get('Тариф', '')).strip()
            k = float(row.get('Ваш Коэффициент', 1.0))

            t_code, _ = get_tariff_code(t_raw)
            if not t_code or not z_name: continue

            # Calculate Avg (ignoring NaNs and 0s)
            prices = []
            for c in price_cols:
                val = row.get(c)
                try:
                    val = float(val)
                    if val > 0 and not np.isnan(val):
                        prices.append(val)
                except:
                    pass

            if prices:
                avg_price = sum(prices) / len(prices)
                fair_price = avg_price * k

                if z_name not in market_data: market_data[z_name] = {}
                market_data[z_name][t_code] = {
                    'fair': int(fair_price),
                    'avg': int(avg_price),
                    'k': k
                }

    except Exception as e:
        print(f"❌ Ошибка чтения конкурентов: {e}")

    return market_data

# --- 3. АНАЛИЗ EXCEL (SALES) ---
def analyze_excel(file_path, pc_map, price_grid):
    print("📂 Анализ продаж и подсчет чеков...")
    try:
        df = pd.read_excel(file_path)
    except Exception as e:
        print(f"❌ Ошибка чтения Excel: {e}")
        return None, None, None, None, None, None

    df['dt_start'] = pd.to_datetime(df['Дата активации сессии'], dayfirst=True, errors='coerce')
    if 'Дата покупки тарифа' in df.columns:
        df['dt_buy'] = pd.to_datetime(df['Дата покупки тарифа'], dayfirst=True, errors='coerce')
        df['dt_start'] = df['dt_start'].fillna(df['dt_buy'])

    df = df.dropna(subset=['dt_start'])

    sales_stats = {}
    daily_occupancy = {}
    phone_counts = {}
    pc_revenue = {}

    duration_map = { '1_HOUR': 1, '2_HOURS': 2, '3_HOURS': 3, '5_HOURS': 5, 'NIGHT': 10 }

    for _, row in df.iterrows():
        pc_raw = normalize_name(row.get('ПК'))
        z_name = pc_map.get(pc_raw)

        if not z_name: continue

        t_name = normalize_name(row.get('Название тарифа'))
        t_code, is_autosim = get_tariff_code(t_name)

        if not t_code: continue

        dt = row['dt_start']
        d_type = get_day_type(dt)

        phone = str(row.get('Номер телефона гостя', ''))
        if len(phone) > 5:
            phone_counts[phone] = phone_counts.get(phone, 0) + 1

        cash = float(row.get('Списано рублей', 0) or 0)
        bonus = float(row.get('Списано бонусов', 0) or 0)

        if pc_raw:
            if pc_raw not in pc_revenue: pc_revenue[pc_raw] = {'cash':0, 'bonus':0, 'zone': z_name}
            pc_revenue[pc_raw]['cash'] += cash
            pc_revenue[pc_raw]['bonus'] += bonus

        slot = 'day'
        if is_autosim:
            slot = 'all_day'
        elif t_code == 'NIGHT':
            slot = 'night'
        else:
            cutoff = get_cutoff_hour(t_code)
            if 4 <= dt.hour < cutoff: slot = 'day'
            else: slot = 'evening'

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

    group_hourly_stats = {'будни': {}, 'выходные': {}}
    global_max_stats = {}

    for d_str, zones_data in daily_occupancy.items():
        base_dt = datetime.datetime.strptime(d_str, '%Y-%m-%d')

        for z, hours in zones_data.items():
            if z not in global_max_stats: global_max_stats[z] = {h: 0 for h in range(24)}

            for h, mins in hours.items():
                full_dt = base_dt.replace(hour=h)
                d_type = get_day_type(full_dt)

                if z not in group_hourly_stats[d_type]:
                    group_hourly_stats[d_type][z] = {h: {'max':0, 'sum':0, 'count':0} for h in range(24)}

                conc = mins / 60.0

                stats = group_hourly_stats[d_type][z][h]
                stats['max'] = max(stats['max'], conc)
                stats['sum'] += conc
                stats['count'] += 1

                global_max_stats[z][h] = max(global_max_stats[z][h], conc)

    repeats = sum(1 for c in phone_counts.values() if c > 1)
    retention_rate = (repeats / len(phone_counts) * 100) if phone_counts else 0

    day_counts = {'будни': 1, 'выходные': 1}

    return sales_stats, day_counts, group_hourly_stats, global_max_stats, retention_rate, pc_revenue

# --- 4. РЕКОМЕНДАЦИИ С УЧЕТОМ РЫНКА ---
def get_recommendation(peak_load_pct, price, bonus_share_pct, market_info=None):
    """
    Returns (action_code, new_price, reason)
    market_info: {'fair': X, 'avg': Y, 'k': Z} or None
    """
    # 1. Base Logic (Internal Load)
    proposed_price = price
    action = 'OK'
    reason = ""

    if peak_load_pct >= 90:
        proposed_price = int(price * 1.20 / 10) * 10
        action = 'UP'
        reason = f"Пик {peak_load_pct}%"
    elif peak_load_pct <= 20:
        proposed_price = int(price * 0.9 / 10) * 10
        action = 'PROMO'
        reason = f"Простой {peak_load_pct}%"

    # 2. Market Guardrail
    if market_info:
        fair_price = market_info['fair']

        # Case A: We want to raise price, but market is lower
        if action == 'UP' and proposed_price > fair_price:
            # Check if we are already above market
            if price >= fair_price:
                # Dangerous to raise further
                return 'WARN', price, f"Рынок ({fair_price}р) держит цену. Рост опасен."
            else:
                # Cap at fair price
                proposed_price = fair_price
                reason += f" (Лимит рынка {fair_price}р)"

        # Case B: We are PROMO, check if we are significantly above market
        if action == 'PROMO' and price > fair_price:
            reason += f". Выше рынка ({fair_price}р)!"

    # 3. Bonus Logic (Low load, high bonus usage)
    if peak_load_pct <= 30 and bonus_share_pct >= 13 and action != 'PROMO':
        return 'BONUS_UP', price, "Лимит бонусов"

    return action, proposed_price, reason

def generate_flyer_with_stats(price_grid, sales_stats, zone_capacities, group_hourly_stats, retention_rate, pc_revenue, market_data):
    print("🎨 Рисуем отчет...")

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
        <title>CyberX Smart Price (Market Aware)</title>
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
            .rec-warn {{ background: #ff9800; color: #000; padding: 2px 6px; border-radius: 4px; font-size: 10px; font-weight: bold; }}
            .rec-promo {{ background: #29b6f6; color: #fff; padding: 2px 6px; border-radius: 4px; font-size: 10px; font-weight: bold; }}
            .rec-bonus {{ background: #ffea00; color: #000; padding: 2px 6px; border-radius: 4px; font-size: 10px; font-weight: bold; }}
            .empty {{ color: #444; }}
            .mkt-info {{ font-size: 9px; color: #aaa; border-top: 1px dashed #444; margin-top: 4px; padding-top: 2px; }}
        </style>
    </head>
    <body>
        <div class="container">
            <h1>Умный Прайс-Лист (Анализ Рынка)</h1>
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

        active_days = set()
        for t in price_grid[z_name].values():
            active_days.update(t.keys())

        for d_type in sorted(active_days, reverse=True):
            html += f"<tr><td style='font-weight:bold; color:#ddd;'>{d_type.capitalize()}</td>"

            for lbl, t_code in col_list:
                p_data = price_grid[z_name].get(t_code, {}).get(d_type, {})
                s_data = sales_stats.get(z_name, {}).get(t_code, {}).get(d_type, {})

                # Market Data for this specific cell
                mkt_info = market_data.get(z_name, {}).get(t_code)

                def render_cell(slot, label=None):
                    price = int(p_data.get(slot, 0))
                    if price == 0 and slot == 'all_day':
                         for k, v in p_data.items():
                             if v > 0: price = int(v); break

                    if price == 0: return "<span class='empty'>-</span>"

                    if slot not in s_data:
                        bucket = {'count':0, 'hours':0, 'cash':0, 'bonus':0}
                    else:
                        bucket = s_data[slot]

                    cash = bucket['cash']
                    bonus = bucket['bonus']
                    z_cap = zone_capacities.get(z_name, 1)

                    h_range = range(0,24)
                    if slot == 'day':
                        cut = get_cutoff_hour(t_code)
                        h_range = range(4, cut)
                    elif slot == 'evening':
                        cut = get_cutoff_hour(t_code)
                        h_range = list(range(cut, 24)) + list(range(0,4))
                    elif slot == 'night':
                        h_range = list(range(22, 24)) + list(range(0,8))

                    max_conc = 0
                    stats_z = group_hourly_stats.get(d_type, {}).get(z_name, {})
                    for h in h_range:
                        max_conc = max(max_conc, stats_z.get(h, {}).get('max', 0))

                    peak_pct = int(max_conc / z_cap * 100) if z_cap > 0 else 0

                    tot_rev_cell = cash + bonus
                    bon_pct = int(bonus / tot_rev_cell * 100) if tot_rev_cell > 0 else 0

                    rec_action, rec_price, rec_reason = get_recommendation(peak_pct, price, bon_pct, mkt_info)

                    badge = ""
                    if rec_action == 'UP': badge = f"<div class='rec-up'>▲ {rec_price}</div>"
                    elif rec_action == 'PROMO': badge = f"<div class='rec-promo'>▼ {rec_price}</div>"
                    elif rec_action == 'BONUS_UP': badge = f"<div class='rec-bonus'>★ BONUS</div>"
                    elif rec_action == 'WARN': badge = f"<div class='rec-warn'>⚠ РЫНОК</div>"

                    lbl_html = f"<div style='font-size:9px; color:#555;'>{label}</div>" if label else ""

                    mkt_html = ""
                    if mkt_info:
                        mkt_html = f"<div class='mkt-info'>Fair: {mkt_info['fair']}</div>"

                    return f"""
                    <div style='text-align:center;'>
                        {lbl_html}
                        {badge}
                        <span class='price-tag'>{price}</span>
                        <span class='stats'>Pk:{peak_pct}% <span style='color:#ff6384'>B:{bon_pct}%</span></span>
                        {mkt_html}
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
    market_data = load_competitors(COMPETITORS_FILE)

    if pc_map:
        stats, day_counts, group_stats, glob_max, ret, pc_rev = analyze_excel(FILE_NAME, pc_map, price_grid)
        if stats:
            generate_flyer_with_stats(price_grid, stats, zone_capacities, group_stats, ret, pc_rev, market_data)
    else:
        print("❌ Не удалось загрузить конфигурацию.")