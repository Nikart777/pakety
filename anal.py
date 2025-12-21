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
    for l in links:
        num = l.get('pc_number') or l.get('name')
        z_id = l.get('packets_type_PC')
        if num and z_id in zones:
            pc_map[normalize_name(num)] = z_id
            
    t_name_map = {normalize_name(t['name']): t['id'] for t in all_tariffs}

    return zones, target_tariffs, price_grid, day_types, calendar, pc_map, t_name_map

# --- 2. АНАЛИЗ EXCEL ---
def analyze_excel(file_path, zones, target_tariffs, pc_map, t_name_map, calendar):
    print("📂 Анализ продаж и подсчет чеков...")
    try:
        df = pd.read_excel(file_path)
    except:
        return None, None
    
    if 'Дата покупки тарифа' in df.columns:
        df['dt'] = pd.to_datetime(df['Дата покупки тарифа'], dayfirst=True, errors='coerce')
    df = df.dropna(subset=['dt'])
    
    df['hour'] = df['dt'].dt.hour
    df['date_str'] = df['dt'].dt.strftime('%Y-%m-%d')
    
    sales_stats = {}
    zone_peaks = {} # Рекорд продаж в зоне (для понимания 100% загрузки)

    for _, row in df.iterrows():
        pc = normalize_name(row.get('ПК'))
        z_id = pc_map.get(pc)
        t_name = normalize_name(row.get('Название тарифа'))
        t_id = t_name_map.get(t_name)
        d_id = calendar.get(row['date_str'])
        
        if z_id and t_id in target_tariffs and d_id:
            t_code = target_tariffs[t_id]['code']
            hour = row['hour']
            
            time_slot = 'day' if 4 <= hour < 17 else 'evening'
            if t_code == 'NIGHT': time_slot = 'night'
            
            if z_id not in sales_stats: sales_stats[z_id] = {}
            if t_code not in sales_stats[z_id]: sales_stats[z_id][t_code] = {}
            if d_id not in sales_stats[z_id][t_code]: sales_stats[z_id][t_code][d_id] = {'day': 0, 'evening': 0, 'night': 0}
            
            sales_stats[z_id][t_code][d_id][time_slot] += 1
            
            # Обновляем пик
            current_val = sales_stats[z_id][t_code][d_id][time_slot]
            if z_id not in zone_peaks: zone_peaks[z_id] = 1
            if current_val > zone_peaks[z_id]:
                zone_peaks[z_id] = current_val

    return sales_stats, zone_peaks

# --- 3. ГЕНЕРАЦИЯ HTML ---
def generate_flyer_with_stats(zones, price_grid, sales_stats, day_types, zone_peaks):
    print("🎨 Рисуем отчет с доказательствами...")
    
    html = """
    <html>
    <head>
        <title>CyberX Smart Pricing</title>
        <style>
            body { 
                font-family: 'Segoe UI', Arial, sans-serif; 
                background-color: #121212; 
                color: #e0e0e0; 
                padding: 20px; 
            }
            .container { max-width: 1280px; margin: 0 auto; }
            h1 { text-align: center; color: #ff4d4d; margin-bottom: 10px; }
            .legend { text-align: center; color: #aaa; margin-bottom: 40px; font-size: 14px; }
            
            .zone-card {
                background: #1e1e1e;
                border: 1px solid #333;
                border-top: 3px solid #ff4d4d;
                border-radius: 8px;
                margin-bottom: 40px;
                box-shadow: 0 4px 15px rgba(0,0,0,0.3);
            }
            
            .zone-header {
                background: #252525;
                padding: 15px 20px;
                font-size: 20px;
                font-weight: bold;
                color: #fff;
                border-bottom: 1px solid #333;
                display: flex; justify-content: space-between; align-items: center;
            }
            
            table { width: 100%; border-collapse: collapse; }
            
            th { 
                background: #2a2a2a; 
                color: #888; 
                padding: 12px; 
                font-size: 11px; 
                text-transform: uppercase;
                border-bottom: 1px solid #444;
            }
            
            td { 
                padding: 10px; 
                text-align: center; 
                border-bottom: 1px solid #333; 
                vertical-align: top;
            }
            
            .row-title { text-align: left; color: #ddd; font-weight: bold; width: 140px; padding-left: 20px;}
            
            /* Блок цены и статистики */
            .cell-container { display: flex; flex-direction: column; align-items: center; gap: 4px; }
            
            .price-tag { 
                font-size: 18px; font-weight: bold; color: #fff; 
            }
            
            .stats-info {
                font-size: 10px; color: #666; font-family: monospace;
            }
            
            .rec-badge { 
                font-size: 11px; padding: 2px 8px; border-radius: 4px; color: #121212; font-weight: bold; margin-bottom: 2px;
            }
            .rec-up { background: #00e676; box-shadow: 0 0 8px rgba(0,230,118,0.2); }
            .rec-down { background: #29b6f6; color: white; }
            
            .split-row { display: flex; width: 100%; }
            .split-col { flex: 1; border-right: 1px solid #333; padding: 0 5px; }
            .split-col:last-child { border: none; }
            .time-label { font-size: 9px; color: #555; display: block; margin-bottom: 4px; }
            
            .empty { color: #444; font-size: 20px; }
        </style>
    </head>
    <body>
        <div class="container">
            <h1>Умный Прайс-Лист</h1>
            <div class="legend">
                <span style="color:#00e676">▲ ЗЕЛЕНЫЙ</span> = Аншлаг (Поднять) &nbsp;|&nbsp; 
                <span style="color:#29b6f6">▼ СИНИЙ</span> = Пусто (Снизить) &nbsp;|&nbsp; 
                <span style="color:#666">(xx чек.)</span> = Количество продаж за период
            </div>
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
                stats = sales_stats.get(zid, {}).get(t_code, {}).get(did, {'day':0, 'evening':0, 'night':0})
                
                # Пик продаж для этой зоны (для расчета % загрузки)
                peak = zone_peaks.get(zid, 1)
                
                def render_cell(slot):
                    price = int(data.get(slot, 0))
                    if price == 0: return "<span class='empty'>-</span>"
                    
                    # СТАТИСТИКА
                    count = stats.get(slot, 0)
                    load_pct = int((count / peak) * 100) if peak > 0 else 0
                    
                    stats_html = f"<span class='stats-info'>({count} чек.)</span>"
                    
                    # ЛОГИКА РЕКОМЕНДАЦИЙ
                    rec_html = ""
                    
                    # Если продаж много (близко к рекорду)
                    if load_pct > (HIGH_LOAD_THRESHOLD * 100):
                        new_p = int(price * 1.15 / 10) * 10
                        rec_html = f"<div class='rec-badge rec-up'>▲ {new_p}</div>"
                        # Подсвечиваем статистику, чтобы было видно ПОЧЕМУ
                        stats_html = f"<span class='stats-info' style='color:#00e676'>({count} чек. / {load_pct}%)</span>"
                        
                    # Если продаж мало и не 0
                    elif load_pct < (LOW_LOAD_THRESHOLD * 100) and peak > 5:
                        rec_html = f"<div class='rec-badge rec-down'>PROMO</div>"
                        stats_html = f"<span class='stats-info' style='color:#29b6f6'>({count} чек.)</span>"
                        
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
        zones, targets, prices, dtypes, cal, pc_map, t_map = fetch_data()
        stats, peaks = analyze_excel(FILE_NAME, zones, targets, pc_map, t_map, cal)
        if stats:
            generate_flyer_with_stats(zones, prices, stats, dtypes, peaks)
        else:
            print("❌ Ошибка с Excel файлом")