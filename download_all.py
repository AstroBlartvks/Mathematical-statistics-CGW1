#!/usr/bin/env python3

import requests
import json
import time

HEADERS = {'User-Agent': 'Mozilla/5.0 (ITMO student AstroBlartvks)'}


def fetch_all_pages(url_template, desc=""):
    """Скачать все страницы MOEX ISS API, склеить data"""
    all_data = []
    columns = None
    start = 0

    while True:
        url = url_template + f"&start={start}"
        print(f"  [{desc}] GET start={start}", end=" ")

        # До 3 попыток на каждый запрос
        for attempt in range(3):
            try:
                resp = requests.get(url, headers=HEADERS, timeout=30)
                resp.raise_for_status()
                break
            except (requests.exceptions.ReadTimeout,
                    requests.exceptions.ConnectionError) as e:
                print(f"retry {attempt+1}", end=" ")
                time.sleep(2)
                if attempt == 2:
                    print(f"  Итого: {len(all_data)} строк")
                    return {'columns': columns, 'data': all_data}

        j = resp.json()

        rows = j['history']['data']
        if columns is None:
            columns = j['history']['columns']

        print(f"got {len(rows)} rows")

        if not rows:
            break

        all_data.extend(rows)
        start += 100
        time.sleep(1)

    print(f"  Итого: {len(all_data)} строк")
    return {'columns': columns, 'data': all_data}


def main():
    print("Загрузка полных данных с MOEX ISS API")

    # 1. IMOEX лог-доходности
    print("\n[1] IMOEX 2023-2026...")
    imoex = fetch_all_pages(
        "https://iss.moex.com/iss/history/engines/stock/markets/index/"
        "boards/SNDX/securities/IMOEX.json?from=2023-01-01&till=2026-03-28",
        desc="IMOEX"
    )
    with open('data/IMOEX_full.json', 'w', encoding='utf-8') as f:
        json.dump(imoex, f, indent=3, ensure_ascii=False)

    # 2. SBER полный (копейки)
    print("\n[2] SBER 2023-2026...")
    sber = fetch_all_pages(
        "https://iss.moex.com/iss/history/engines/stock/markets/shares/"
        "boards/TQBR/securities/SBER.json?from=2023-01-01&till=2026-03-28",
        desc="SBER"
    )
    with open('data/SBER_full.json', 'w', encoding='utf-8') as f:
        json.dump(sber, f, indent=3, ensure_ascii=False)

    # 3. IMOEX до кризиса 
    print("\n[3] IMOEX до кризиса (2021-06 - 2022-02)...")
    pre = fetch_all_pages(
        "https://iss.moex.com/iss/history/engines/stock/markets/index/"
        "boards/SNDX/securities/IMOEX.json?from=2021-06-01&till=2022-02-20",
        desc="PRE"
    )
    with open('data/BIIMOEX_pre.json', 'w', encoding='utf-8') as f:
        json.dump(pre, f, indent=3, ensure_ascii=False)

    # 4. IMOEX после кризиса 
    print("\n[4] IMOEX после кризиса (2022-04 - 2023-01)...")
    post = fetch_all_pages(
        "https://iss.moex.com/iss/history/engines/stock/markets/index/"
        "boards/SNDX/securities/IMOEX.json?from=2022-04-01&till=2023-01-01",
        desc="POST"
    )
    with open('data/BIIMOEX_post.json', 'w', encoding='utf-8') as f:
        json.dump(post, f, indent=3, ensure_ascii=False)



if __name__ == '__main__':
    main()