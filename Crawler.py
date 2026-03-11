import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import random
import re

def scrape_weather_and_aqi():
    cities = {
        'beijing': '北京',
        'haerbin': '哈尔滨',
        'xian': '西安',
        'jinan': '济南',
        'shanghai': '上海',
        'guangzhou': '广州',
        'wuhan': '武汉',
        'chengdu': '成都'
    }

    weather_start_year = 2020
    weather_end_year = 2025


    aqi_start_year = 2020
    aqi_end_year = 2025

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
        'Accept-Language': 'zh-CN,zh;q=0.9',
        'Referer': 'https://www.tianqihoubao.com/',
        'Connection': 'keep-alive',
    }

    all_weather_data = []
    all_aqi_data = []

    print(f"\n采集天气数据 ({weather_start_year}-{weather_end_year})")
    for city_py, city_name in cities.items():
        print(f"\n正在采集天气 - 城市：{city_name} ({city_py})")

        for year in range(weather_start_year, weather_end_year + 1):
            for month in range(1, 13):
                yyyymm = f"{year}{month:02d}"
                url = f"https://www.tianqihoubao.com/lishi/{city_py}/month/{yyyymm}.html"

                try:
                    response = requests.get(url, headers=headers, timeout=120)
                    response.encoding = 'utf-8'

                    if response.status_code == 200:
                        soup = BeautifulSoup(response.text, 'html.parser')
                        table = None
                        for t in soup.find_all('table'):
                            if t.find('th', string=re.compile('日期')):
                                table = t
                                break

                        if table:
                            rows = table.find_all('tr')[1:]
                            count = 0
                            for row in rows:
                                tds = row.find_all('td')
                                if len(tds) < 4:
                                    continue

                                date_text = tds[0].get_text(strip=True)
                                date_match = re.search(r'(\d{4})年(\d{2})月(\d{2})日', date_text)
                                if date_match:
                                    full_date = f"{date_match.group(1)}-{date_match.group(2)}-{date_match.group(3)}"
                                else:
                                    continue

                                weather = tds[1].get_text(strip=True).replace('\n', ' ')

                                temp_text = tds[2].get_text(strip=True)
                                temps = re.findall(r'([-]?\d+)℃', temp_text)
                                high_temp = temps[0] if len(temps) > 0 else ''
                                low_temp = temps[1] if len(temps) > 1 else ''

                                wind_text = tds[3].get_text(strip=True)
                                day_wind = wind_text.split('/')[0].strip() if '/' in wind_text else wind_text

                                wind_parts = day_wind.split()
                                if len(wind_parts) >= 2:
                                    wind_direction = wind_parts[0]
                                    wind_level = ' '.join(wind_parts[1:])
                                else:
                                    wind_direction = day_wind
                                    wind_level = ''

                                row_data = {
                                    '城市': city_name,
                                    '日期': full_date,
                                    '天气状况': weather,
                                    '最高气温(℃)': high_temp,
                                    '最低气温(℃)': low_temp,
                                    '风向': wind_direction,
                                    '风力': wind_level,
                                }
                                all_weather_data.append(row_data)
                                count += 1

                            print(f"  天气成功: {yyyymm} 获取 {count} 条")
                        else:
                            print(f"  天气跳过: {yyyymm} (无表格)")
                    else:
                        print(f"  天气失败: {yyyymm} HTTP {response.status_code}")

                except Exception as e:
                    print(f"  天气异常: {yyyymm} - {e}")

                time.sleep(random.uniform(1.5, 4))

    print(f"\n第二步：采集AQI数据 ({aqi_start_year}-{aqi_end_year})")
    for city_py, city_name in cities.items():
        print(f"\n正在采集AQI - 城市：{city_name} ({city_py})")

        for year in range(aqi_start_year, aqi_end_year + 1):
            for month in range(1, 13):
                yyyymm = f"{year}{month:02d}"
                url = f"https://www.tianqihoubao.com/aqi/{city_py}-{yyyymm}.html"

                try:
                    response = requests.get(url, headers=headers, timeout=120)
                    response.encoding = 'utf-8'

                    if response.status_code == 200:
                        soup = BeautifulSoup(response.text, 'html.parser')

                        table = soup.find('table', class_='b')
                        if not table:
                            table = soup.find('table', attrs={'cellpadding': '2'})

                        if table:
                            rows = table.find_all('tr')[1:]
                            count = 0
                            for row in rows:
                                tds = row.find_all('td')
                                if len(tds) < 8:
                                    continue

                                date_text = tds[0].get_text(strip=True)
                                date_match = re.search(r'(\d{4}-\d{2}-\d{2})', date_text)
                                if date_match:
                                    full_date = date_match.group(1)
                                else:
                                    continue

                                quality = tds[1].get_text(strip=True)
                                aqi = tds[2].get_text(strip=True)
                                rank = tds[3].get_text(strip=True)
                                pm25 = tds[4].get_text(strip=True)
                                pm10 = tds[5].get_text(strip=True)
                                so2 = tds[6].get_text(strip=True) if len(tds) > 6 else ''
                                no2 = tds[7].get_text(strip=True) if len(tds) > 7 else ''
                                co = tds[8].get_text(strip=True) if len(tds) > 8 else ''
                                o3 = tds[9].get_text(strip=True) if len(tds) > 9 else ''

                                row_data = {
                                    '城市': city_name,
                                    '日期': full_date,
                                    '质量等级': quality,
                                    'AQI': aqi,
                                    'AQI排名': rank,
                                    'PM2.5': pm25,
                                    'PM10': pm10,
                                    'SO2': so2,
                                    'NO2': no2,
                                    'CO': co,
                                    'O3': o3,
                                }
                                all_aqi_data.append(row_data)
                                count += 1

                            print(f"  AQI成功: {yyyymm} 获取 {count} 条")
                        else:
                            print(f"  AQI跳过: {yyyymm} (无表格)")
                    else:
                        print(f"  AQI失败: {yyyymm} HTTP {response.status_code}")

                except Exception as e:
                    print(f"  AQI异常: {yyyymm} - {e}")

                time.sleep(random.uniform(2, 5))


    if all_weather_data or all_aqi_data:
        # 转换为DataFrame
        df_weather = pd.DataFrame(all_weather_data)
        df_aqi = pd.DataFrame(all_aqi_data)

        if not df_weather.empty:
            df_weather['日期'] = pd.to_datetime(df_weather['日期'])
        if not df_aqi.empty:
            df_aqi['日期'] = pd.to_datetime(df_aqi['日期'])

        if not df_weather.empty and not df_aqi.empty:
            df_merged = pd.merge(df_weather, df_aqi, on=['城市', '日期'], how='outer')
        elif not df_weather.empty:
            df_merged = df_weather
        elif not df_aqi.empty:
            df_merged = df_aqi
        else:
            df_merged = pd.DataFrame()


        if not df_merged.empty:
            df_merged = df_merged.sort_values(['城市', '日期']).reset_index(drop=True)

        output_file = 'history_weather_aqi_8cites.csv'
        df_merged.to_csv(output_file, index=False, encoding='utf-8-sig')

        total_records = len(df_merged)
        print(f"\n全部完成！共 {total_records} 条记录（天气 + AQI 合并），保存为: {output_file}")
        print("列包括：城市、日期、天气状况、最高气温(℃)、最低气温(℃)、风向、风力、质量等级、AQI、AQI排名、PM2.5、PM10、SO2、NO2、CO、O3")
    else:
        print("\n未获取到任何数据。")

if __name__ == "__main__":
    scrape_weather_and_aqi()
