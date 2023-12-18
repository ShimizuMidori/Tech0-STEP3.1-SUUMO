#ライブラリのインポート
from bs4 import BeautifulSoup
import re #不要文字削除
import requests
from time import sleep
from tqdm import tqdm   #for文の進捗確認
import pandas as pd
#スプシ出力
#google spread sheets 出力
import gspread
from oauth2client.service_account import ServiceAccountCredentials
#環境変数関連
from dotenv import load_dotenv
load_dotenv()
import os
#SQLite
import sqlite3


# suumo対象のURL
# 最後のページの数値を取得
url = 'https://suumo.jp/chintai/tokyo/sc_bunkyo/?page={}'
res = requests.get(url.format(1))
#1ページ目のページネーションに全ページの番号が表示される
#1ページ目のページネーション部分をスクレイピングし、その中の最後の要素（最後のページ番号）を取得
res.encoding = 'utf-8'
soup = BeautifulSoup(res.text, 'html.parser')
last_page = int(soup.find('ol', class_='pagination-parts').find_all('li')[-1].text)
#[-1]でページネーションのリストから最後の要素を取得
#textでページ番号を取得
#intで整数型に変換

#空のリストを作成
data_list = []

# 正常にHTML情報が取得できれば以下のコードを実行
if res.status_code == 200:

    #文京区のSUUMO掲載物件全ページ情報を取得
    for page in tqdm(range(1, last_page +1)): #for文の進捗を確認
    #range(start, stop) 関数は、startから始まってstop-1までの数値のシーケンスを生成
    #stopの値自体はシーケンスに含まれないため +1で最後のページもスクレイピング
        target_url = url.format(page)
        #ページ取得できているかの確認
        # print("data_listの大きさ:",len(data_list))
        # print(target_url)

        #requestを使ってURLにアクセス
        res = requests.get(target_url)
        #相手サイトの負荷軽減
        sleep(1)
        #文字化け防止
        res.encoding = 'utf-8'
        #取得したHTMLをBeautifulSoupで解析
        soup = BeautifulSoup(res.text, 'html.parser')

        #全ての物件情報取得
        contents = soup.find_all('div', class_= 'cassetteitem')

        #for文で物件・部屋情報取得
        for content in contents:
            #物件・部屋情報を解析
            detail = content.find('div', class_='cassetteitem-detail')
            table = content.find('table', class_='cassetteitem_other')

            #物件情報から必要情報を取得
            name = detail.find('div', class_='cassetteitem_content-title').text
            address = detail.find('li', class_='cassetteitem_detail-col1').text
            access = detail.find('li', class_='cassetteitem_detail-col2').text
            age, story = detail.find('li', class_='cassetteitem_detail-col3').text.split()

            #部屋情報を取得
            tr_tags = table.find_all('tr', class_='js-cassette_link')

            #for文で部屋情報取得
            for tr_tag in tr_tags:
                #部屋情報から必要情報を取得
                floor, price, first_fee, capacity = tr_tag.find_all('td')[2:6]
                #さらに細かい情報取得
                rent, administration = price.find_all('li')
                deposit, gratuity = first_fee.find_all('li')
                madori, menseki = capacity.find_all('li')
                #取得した全ての情報を辞書に格納
                data = {
                    'name' : name,
                    'address' : address,
                    'access' : access,
                    'age' : age,
                    'story' : story,
                    'floor': floor.text,
                    'rent' : rent.text,
                    'administration' : administration.text,
                    'deposit' : deposit.text,
                    'gratuity' : gratuity.text,
                    'madori' : madori.text,
                    'menseki' : menseki.text
                }
                #取得した辞書を格納
                data_list.append(data)

#最後のインデックスを確認
data_list[-1]

#data_list の、access を分割して新しいキーを追加
for item in data_list:
    #先頭と末尾の改行文字を削除
    #strip() ・・・文字列の先頭と末尾にある空白文字を（スペース、タブ、改行文字（\n））を取り除く
    cleaned_access = item['access'].strip()
    # 改行で分割
    access_list = cleaned_access.split('\n')
    # 分割したデータを新しいキーとして追加
    item['access1'] = access_list[0] if len(access_list) > 0 else ""
    item['access2'] = access_list[1] if len(access_list) > 1 else ""
    item['access3'] = access_list[2] if len(access_list) > 2 else ""


#データフレームを作成
df = pd.DataFrame(data_list)


#クレジング
#DB全体の不要な文字を取り除く
def remove_unwanted_chars(text):
    if isinstance(text, str):
        return re.sub('[\n\r\t]', '', text)
    return text

# データフレームのすべての要素に関数を適用
df = df.applymap(remove_unwanted_chars)
#df.applymap() : データフレーム全体に対して、カッコ内の関数を適応


#df から 'access' 列を削除
df = df.drop(columns=['access'])

#アクセスから路線・駅名・徒歩時間を抽出
df[['access1_line', 'access1_station', 'access1_walk']] = df['access1'].str.extract(r'(.+?)/(.+?) 歩(.+?)分')
df[['access2_line', 'access2_station', 'access2_walk']] = df['access2'].str.extract(r'(.+?)/(.+?) 歩(.+?)分')
df[['access3_line', 'access3_station', 'access3_walk']] = df['access3'].str.extract(r'(.+?)/(.+?) 歩(.+?)分')

# 徒歩時間の列を整数型に変換
df['access1_walk'] = pd.to_numeric(df['access1_walk'], errors='coerce').fillna(0).astype(int)
df['access2_walk'] = pd.to_numeric(df['access2_walk'], errors='coerce').fillna(0).astype(int)
df['access3_walk'] = pd.to_numeric(df['access3_walk'], errors='coerce').fillna(0).astype(int)

#df から 'access1,2,3' 列を削除
df = df.drop(columns=['access1','access2','access3'])


# age '新築' を '築0年' に、その後 '築' と '年' を削除
df['age'] = df['age'].str.replace('新築', '築0年').str.replace('築', '').str.replace('年', '')
# 文字列を数値に変換。変換できない値は NaN に置き換える
df['age'] = pd.to_numeric(df['age'], errors='coerce')
# NaN 値を 0 に置き換える
df['age'] = df['age'].fillna(-1).astype(int)


# story, floorのデータの表現を統一
df['story'] = df['story'].str.replace('地下', 'B').str.replace('地上', '-').str.replace('平屋', '1階建').str.replace('階建', 'F')
df['floor'] = df['floor'].str.replace('階', '')


#お金（rent, administration	, deposit, gratuity）を整数型に変換
def yen_to_int(text):
    try:
        # エラーが出たので一旦全て文字列に変換
        text = str(text)
        if '-' in text:
            amount = 0
        elif '万円' in text:
            amount = float(text.replace('万円', '')) * 10000
        else:
            amount = float(text.replace('円', ''))
    except ValueError:
        amount = 0 # 形式に一致しない場合は0を返す
    return int(amount)

df['rent'] = df['rent'].apply(yen_to_int)
df['administration'] = df['administration'].apply(yen_to_int)
df['deposit'] = df['deposit'].apply(yen_to_int)
df['gratuity'] = df['gratuity'].apply(yen_to_int)


#mensekiのデータ（比例尺度）に変換
df['menseki'] = df['menseki'].str.replace('m2','').astype(float)


#重複削除
# 特定の列に基づいて重複を確認
duplicate_rows = df[df.duplicated(subset=['address', 'age', 'floor', 'rent', 'menseki'])]
# 重複件数の表示
print(f"重複件数: {duplicate_rows.shape[0]}")
# 重複データの表示
print(duplicate_rows)

# 重複データの削除
df.drop_duplicates(subset=['address', 'age', 'floor', 'rent', 'menseki'], inplace=True)



#csv出力
df.to_csv('SUUMO_bunkyo2.csv', index=False, encoding='utf-8-sig')
print("csv出力が完了しました")



#スプシ出力
#スコープとjsonファイルを使って認証情報を取得
SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
SERVICE_ACCOUNT_FILE = os.getenv('SERVICE_ACCOUNT_FILE')
credentials = ServiceAccountCredentials.from_json_keyfile_name(SERVICE_ACCOUNT_FILE, SCOPES)

#認証情報をauthorize関数に渡してスプレッドシートの操作権を取得
gs = gspread.authorize(credentials)

#シート情報を取得して変数に代入
SPREADSHEET_KEY = os.getenv('SPREADSHEET_KEY')
workbook = gs.open_by_key(SPREADSHEET_KEY)
worksheet = workbook.worksheet("suumo_DB2")

# NaN 値や無限大の値を None に置き換える
df = df.where(pd.notnull(df), None)
# データフレーム内に NaN 値や無限大の値があった（？）ためJSONでは扱えないとエラーに。
# JSONでも扱える形式に変換

# dfから値を習得
values = [df.columns.values.tolist()] + df.values.tolist()

# ワークシートの指定したセル(B2)から値を追加
worksheet.update("B2", values)



# SQLite
# SQLiteデータベースに接続
#SQLのデータベースの枠を作成
db_name = "scraping_fudosan.db"
conn = sqlite3.connect(db_name)

#dfをデータベースに入れ込む
df.to_sql("suumo_data", conn, if_exists="replace", index=False)

# データベース接続を閉じる
conn.close()