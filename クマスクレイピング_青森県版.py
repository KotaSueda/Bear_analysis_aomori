# くまログあおもりダウンロード ===========================================================================================
import requests
import pandas as pd
import urllib.parse

def fetch_kumalog_aomori_sightings_csv(
    output_csv_path: str,
    # 青森市の中心周辺の座標に変更
    center_lat: float = 40.824589,
    center_lng: float = 140.740548,
    radius: str = "",
    animal_species_ids = ["1"], # 開発者ツールに合わせて文字列にしています
    municipality_ids = [], 
    startdate: str = "2017-01-01",
    enddate:   str = "2026-12-31"
):
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
    # 秋田版と同じシステム構造と仮定してエンドポイントを設定
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
        # 何かリストが隠れていないか探す
        for key, val in data.items():
            if isinstance(val, list) and len(val) > 0:
                print(f"⚠ キー '{key}' の中にリストを発見。ここを使います。")
                sightings_data = val
                break

    if sightings_data is None:
        raise RuntimeError("JSON の中にリスト形式データが見つかりませんでした。構造を確認してください。")

    # 5) pandas DataFrame 化 → CSV 保存
    df = pd.DataFrame(sightings_data)
    df.to_csv(output_csv_path, index=False, encoding="utf-8-sig")
    print(f"✅ データを CSV として保存しました: {output_csv_path}")
    print("---- 先頭5行 ----")
    print(df.head())

if __name__ == "__main__":
    # 出力先 CSV (ファイル名を青森県用に変更しています)
    output_path = "kumalog_aomori_2026.csv"
    fetch_kumalog_aomori_sightings_csv(output_csv_path=output_path)
