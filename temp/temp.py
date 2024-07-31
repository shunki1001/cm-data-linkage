# %%
import pandas as pd

df1 = pd.read_csv("mapping_csv/登録商品一覧 - CM内のセット品.csv").rename(
    columns={"親商品の商品管理番号": "商品管理番号"}
)
df2 = pd.read_csv(
    "mapping_csv/登録商品一覧 - RMSからダウンロードしたデータ.csv"
).rename(columns={"商品管理番号（商品URL）": "商品管理番号"})
df3 = pd.read_csv("mapping_csv/登録商品一覧 - 対応表.csv")
df4 = pd.read_csv("mapping_csv/登録漏れ受注データ - シート1.csv")

df1.head()
df2.head()
df3.head()
# %%
df4 = df4.rename(columns={"システム連携用SKU番号": "RSL店舗内商品コード"})
merged_df = pd.merge(df4, df3, on="RSL店舗内商品コード")

merged_df.to_csv("output3.csv")

# %%
merged_df = pd.merge(df2, df3, on="商品管理番号")

# Value1とValue2列が一致するレコードを抽出
matching_records = merged_df[
    merged_df["属性1名"] == merged_df["バリエーション項目選択肢1"]
].reset_index(drop=True)

matching_records = matching_records[
    [
        "商品管理番号",
        "属性1名",
        "JANコード",
        "システム連携用SKU番号",
        "バリエーション項目選択肢1",
    ]
]
matching_records.to_csv("output.csv")

# %%
merged_df = pd.merge(df1, df2, on="商品管理番号")

# Value1とValue2列が一致するレコードを抽出
matching_records = merged_df[
    merged_df["親属性１名"] == merged_df["バリエーション項目選択肢1"]
].reset_index(drop=True)

matching_records = matching_records[
    [
        "商品管理番号",
        "構成品属性１名",
        "親属性１名",
        "JANコード",
        "システム連携用SKU番号",
        "バリエーション項目選択肢1",
    ]
]
matching_records.to_csv("output2.csv")
