# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'
# %% [markdown]
# # Exploratory Data Analysis
#
# ## FARS Data comparing Fatalities in Denver and Seattle

# %%
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import glob
from matplotlib import cm


# %%
colors = ["darkorange", "darkred"]
sns.set(rc={"figure.figsize": (4, 6)})
sns.set_palette(sns.color_palette(colors))

# %%
def concat_csv(glob_path):
    files = glob.glob(glob_path)
    dfs = [pd.read_csv(file, engine="python", header=0) for file in files]
    return (
        pd.concat(dfs, ignore_index=True)
        .sort_values(by=["Year"])
        .set_index(["Year", "City_Name"])
    )


# %% [markdown]
# ## Total Fatalities

# %%
path = "../data/better_streets/processed/FARS/den_sea_ped_bike_fatalities.csv/*.csv"


# %%
yr_df = concat_csv(path)


# %%
yr_df.head()


# %%
uyr_df = pd.pivot_table(
    yr_df, index="Year", values="Ped_Bike_Fatalities", columns=["City_Name"]
)
uyr_df.head()


# %%
bar_plot = uyr_df.plot.bar(
    figsize=(12, 8),
    title="Pedestrian and Cyclist Fatalities",
    color=["darkred", "darkorange"],
)
bar_plot.plot()
plt.savefig("./plots/den_sea_ped_bar.png")


# %%
trend_plot = sns.lmplot(
    x="Year",
    y="Ped_Bike_Fatalities",
    hue="City_Name",
    data=yr_df.reset_index(),
    legend=False,
)
plt.subplots_adjust(top=0.9)
plt.legend(loc="upper left")
plt.title("Pedestrian and Cyclist Fatalities")
plt.show()
trend_plot.savefig("./plots/den_sea_ped_trend.png", dpi=150)

# %% [markdown]
# ## Looking at Pedestrians and Cyclist Fatalities Separately

# %%
ped_df = concat_csv(
    "../data/better_streets/processed/FARS/den_sea_ped_fatalities.csv/*.csv"
)
ped_df.shape, ped_df.columns


# %%
bike_df = concat_csv(
    "../data/better_streets/processed/FARS/den_sea_bike_fatalities.csv/*.csv"
)
bike_df.shape, bike_df.columns


# %%
pb_df = ped_df.join(other=bike_df).fillna(value=0).reset_index()
pb_df.columns


# %%
pb_df["Total"] = pb_df["Ped_Fatalities"] + pb_df["Bike_Fatalities"]
pb_df = pb_df.rename(
    columns={"Ped_Fatalities": "Pedestrians", "Bike_Fatalities": "Cyclists"}
)
pb_df.columns
