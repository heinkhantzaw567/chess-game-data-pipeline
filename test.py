import json
import pandas as pd

with open("raw_data/danielnaroditsky_2020_01.json") as f:
    data = json.load(f)

games = data["games"]
df = pd.DataFrame(games)



white_df = pd.json_normalize(df["white"]).add_prefix("white_")
black_df = pd.json_normalize(df["black"]).add_prefix("black_")

flat = pd.concat([df.drop(columns=["white", "black"]), white_df, black_df], axis=1)



white_ratings = pd.json_normalize(df["white"])["rating"]
black_ratings = pd.json_normalize(df["black"])["rating"]

diffs_vectorized = (white_ratings - black_ratings).abs()
diffs_loop = pd.Series([abs(w - b) for w, b in zip(white_ratings, black_ratings)])



df["white_result"] = pd.json_normalize(df["white"])["result"]
df["black_result"] = pd.json_normalize(df["black"])["result"]
df["eco"] = df["eco"] if "eco" in df.columns else None

df["winner"] = "draw"
df.loc[df["white_result"] == "win", "winner"] = "white"
df.loc[df["black_result"] == "win", "winner"] = "black"



opening_stats = df.groupby("eco")["winner"].value_counts(normalize=True).unstack(fill_value=0)
opening_stats["total_games"] = df.groupby("eco").size()
opening_stats = opening_stats.sort_values("total_games", ascending=False)

# print(games[0]['eco']) 
 # check what opening info is actually in there

# print(df['eco'])


flat['opening_name'] = df['eco'].str.split('/').str[-1].str.replace('-', ' ')
print(flat["opening_name"])
flat['opening_name'] = flat['opening_name'].fillna('Unknown')
flat['winner'] = pd.Series(df['winner'])
print(flat["winner"])
print(flat["eco"])
crosstab = pd.crosstab(flat['opening_name'],flat["winner"])


pivot = flat.pivot_table(
    values='eco',      # different col than index/columns
    index='opening_name',
    columns='winner',
    aggfunc='count',
    fill_value=0
)
print(pivot)
print(crosstab)

crosstab1 = pd.crosstab(flat["rated"],flat["time_class"])
print(crosstab1)

crosstab2 = pd.crosstab(flat["winner"],flat["rated"])
print(crosstab2)

pivot1 = flat.pivot_table(
    values="white_rating",
    index="time_class",
    aggfunc='mean',
    fill_value=0
)
print(pivot1)


pivot2 = flat.pivot_table(
    values="winner",
    index ="opening_name",
    aggfunc="count",
    fill_value=0
)
print(pivot2)
flat["rating_diff"] = (flat["white_rating"] - flat["black_rating"]).abs()
pivot3= flat.pivot_table(
    index="opening_name",
    columns="winner",
    values="rating_diff",
    aggfunc="mean",
    fill_value=0,
    margins=True
)
print(pivot3)

pivot4= flat.pivot_table(
    values="url",
    index="white_username",
    columns="winner",
    aggfunc="count",
    fill_value=0
)
print(pivot4)