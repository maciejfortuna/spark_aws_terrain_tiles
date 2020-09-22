import geopandas as gpd
import matplotlib.pyplot as plt
import pandas as pd
from shapely import wkt
import ast
import seaborn as sns
import boto3
from botocore import UNSIGNED
from botocore.client import Config
import glob
import os
import numpy as np
DOWNLOAD = False

def bbox_to_polygon(arr):
    arr = ast.literal_eval(arr)
    return f'POLYGON(({arr[0]} {arr[1]}, {arr[0]} {arr[3]}, {arr[2]} {arr[3]}, {arr[2]} {arr[1]}, {arr[0]} {arr[1]}))'
    # return f'POINT({arr[0]} {arr[1]})'

if DOWNLOAD:
    aws_access_key_id="YOUR_CREDENTIALS"
    aws_secret_access_key="YOUR_CREDENTIALS"
    aws_session_token="YOUR_CREDENTIALS"
    session = boto3.Session(aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key, aws_session_token=aws_session_token)
    s3 = session.resource('s3')
    bucket = s3.Bucket("YOUR_BUCKET_NAME")
    for obj in bucket.objects.all():
        if not os.path.exists(os.path.dirname(obj.key)):
            try:
                os.makedirs(os.path.dirname(obj.key))
            except:
                continue
        bucket.download_file(obj.key, obj.key)

world = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres'))
# change mapping system
world = world.to_crs(epsg=3857)
europe = world

# merge parts
all_parts = glob.glob("*/part*")

dfs = [pd.read_csv(part, header=None, names=["geometry", "MEAN_ELEVATION"],delimiter=";") for part in all_parts]
df = pd.concat(dfs,ignore_index = True)

df = df.sort_values(by='MEAN_ELEVATION', ascending=False)
el_min = df["MEAN_ELEVATION"].min()
el_max = df["MEAN_ELEVATION"].max()

df = df[df.MEAN_ELEVATION > 0]
all_mean = df["MEAN_ELEVATION"].mean()
df = df[df.MEAN_ELEVATION > all_mean]

# create squares to display
df["geometry"] = df["geometry"].apply(bbox_to_polygon)
df['geometry'] = df['geometry'].apply(wkt.loads)
gdf_mean = gpd.GeoDataFrame(df, geometry = 'geometry')

# plot
fig, ax1 = plt.subplots(1,1)
gdf_mean.plot(ax = ax1, cmap="hot_r", alpha = 1, legend=True)
europe.boundary.plot(edgecolor='gray', markersize = 1, linewidth= 1,alpha=1, ax=ax1)
ax1.set_facecolor((0.1, 0.1, 0.1))
ax1.set_xlim(-1500000,5500000)
ax1.set_ylim(4300000,11500000)
# plt.savefig('above.png', bbox_inches='tight',pad_inches=0)

plt.show()