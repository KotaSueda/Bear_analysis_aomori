# くまログあおもりダウンロード ===========================================================================================
import requests
import pandas as pd
import numpy as np
import urllib.parse
import os
from datetime import datetime

def fetch_kumalog_aomori_sightings_csv(
    output_csv_path: str,
    archive_csv_path: str = "kumalog_aomori_archive_2017_2025.csv", # アーカイブファイル名を指定
    center_lat: float = 40.824589,
    center_lng: float = 140.740548,
    radius: str = "",
    animal_species_ids = ["1"], 
    municipality_ids = [], 
    startdate: str = None,
    enddate:   str = None
):
    # --- 期間の自動設定（2026年の最初から今日まで） ---
    today = datetime.now()
    if startdate is None:
        startdate = "2026-01-01" # 2026年以降のみ取得
    if enddate is None:
        enddate = today.strftime("%Y-%m-%d") # 常に今日の日付になるように設定
        
    print(f"取得期間: {startdate} 〜 {enddate}")

    # セッションを作成
    session = requests.Session()

    # 1) トップページに GET → Cookie に埋め込まれた XSRF-TOKEN を取得
    home_url = "https://kumalog-aomori.info/"
    resp = session.get(home_url)
    resp.raise_for_status()

    # Cookie から XSRF-TOKEN を取り出し、URLデコード
    raw_token = session.cookies.get("XSRF-TOKEN")
    if not raw_token:
        raise RuntimeError("XSRF-TOKEN が Cookie に含まれていません")
    csrf_token = urllib.parse.unquote(raw_token)

    # 2) API に対する POST リクエスト設定
    api_url = "https://kumalog-aomori.info/api/ver1/sightings/post_list"
    
    payload = {
        "lat": center_lat,
        "lng": center_lng,
        "filter": {
            "radius": radius,
            "info_type_ids": [],
            "animal_species_ids": animal_species_ids,
            "municipality_ids": municipality_ids,
            "startdate": startdate,
            "enddate": enddate
        }
    }
    
    headers = {
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "X-Requested-With": "XMLHttpRequest",
        "X-XSRF-TOKEN": csrf_token,
        "Referer": home_url,
        "Content-Type": "application/json",
    }

    # 3) POST 実行
    post_resp = session.post(api_url, json=payload, headers=headers)
    if post_resp.status_code != 200:
        print(f"❌ API リクエスト失敗 → status_code: {post_resp.status_code}")
        print("Response Text:")
        print(post_resp.text)
        post_resp.raise_for_status()

    data = post_resp.json()

    # 4) JSON から一覧データを取り出す
    sightings_data = None
    if isinstance(data, list):
        sightings_data = data
    elif "list" in data and isinstance(data["list"], list):
        sightings_data = data["list"]
    elif "data" in data and isinstance(data["data"], list):
        sightings_data = data["data"]
    elif "sightings" in data and isinstance(data["sightings"], list):
        sightings_data = data["sightings"]
    else:
        for key, val in data.items():
            if isinstance(val, list) and len(val) > 0:
                print(f"⚠ キー '{key}' の中にリストを発見。ここを使います。")
                sightings_data = val
                break

    if sightings_data is None:
        raise RuntimeError("JSON の中にリスト形式データが見つかりませんでした。構造を確認してください。")

    # 5) pandas DataFrame 化（2026年分の差分データ）
    df = pd.DataFrame(sightings_data)

    # 【加工1】sighting_datetimeから年・月・日・時を抽出
    df['sighting_datetime'] = pd.to_datetime(df['sighting_datetime'], errors='coerce')
    df['year'] = df['sighting_datetime'].dt.year
    df['month'] = df['sighting_datetime'].dt.month
    df['day'] = df['sighting_datetime'].dt.day
    df['hour'] = df['sighting_datetime'].dt.hour

    # 【加工2】info_type_id に基づくラベルの割り当て
    type_mapping = {
        '1': '目撃',
        '2': '人身被害',
        '3': '痕跡(食害)',
        '4': '痕跡(その他)'
    }
    df['sighting_condition'] = df['info_type_id'].astype(str).map(type_mapping).fillna('目撃')

    # ＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝
    # 過去データ(アーカイブ)とのマージ処理
    # ＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝
    if os.path.exists(archive_csv_path):
        print(f"📂 アーカイブファイル '{archive_csv_path}' を読み込み、結合します。")
        df_archive = pd.read_csv(archive_csv_path)
        
        # 過去データと今年のデータを縦に結合
        df_final = pd.concat([df_archive, df], ignore_index=True)
    else:
        print(f"⚠ アーカイブファイル '{archive_csv_path}' が見つかりません。2026年のデータのみ出力します。")
        df_final = df

    # 6) 最終データを CSV として保存
    df_final.to_csv(output_csv_path, index=False, encoding="utf-8-sig")
    print(f"✅ データを CSV として保存しました: {output_csv_path} (合計 {len(df_final)} 件)")
    print("---- 先頭5行 ----")
    print(df_final.head())

if __name__ == "__main__":
    # 出力先 CSV
    output_path = "kumalog_aomori_2026.csv"
    fetch_kumalog_aomori_sightings_csv(output_csv_path=output_path)
