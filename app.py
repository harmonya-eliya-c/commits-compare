import json
import pandas as pd
from collections import Counter
from flask import Flask, request, jsonify

app = Flask(__name__)

FILTERS_TYPE_TO_APP_NAME = {'tags': 'tag', "dataElements": 'data_element', "tagType": 'tag_type', "products": 'upc'}

# {key: ('col_name', top_k)}
GROUPS_MAP_TO_COLUMN = {"tags": ('tag', 10), "tagType": ("tag_type", None), "dataElements": ('data_element', None)}
SUBSET_INSTANCE = ['upc', 'tag', 'data_element']
SAMPLE_SIZE = 100


@app.route('/')
def status():
    return 'UP'


@app.route('/get_diff_by_commits', methods=["GET"])
def get_diff_by_commits():
    commitIds = request.form['commitIds']
    tagTypes = request.form['tagTypes']
    tags = request.form['tags']
    products = request.form['products']
    dataElements = request.form['dataElements']
    df_diff = pd.read_csv("data/metrics_df.csv", dtype={"upc": object})
    all_agg_data = agg_data(df_diff)
    df_diff_filter = filter_df(df_diff, commitIds, tagTypes, tags, products, dataElements)
    result_data = {
        "metrics": build_metrics(df_diff_filter, all_agg_data),
        "filters": get_filters(df_diff=df_diff_filter),
        "chart": metric_for_group(df_diff=df_diff_filter),
        "products": add_full_details(df_diff)
    }
    return jsonify(result_data)


def add_full_details(df_diff):
    sample_df = df_diff.sample(min(SAMPLE_SIZE, len(df_diff)))
    upcs = set(sample_df.upc.tolist())
    full_details_df = pd.read_csv('data/full_diff_metadata.csv', dtype={"upc": 'object'}, index_col=[0])
    df = full_details_df[full_details_df.upc.isin(upcs)]
    df = df.sample(min(SAMPLE_SIZE, len(df)))
    cols = df.columns.tolist()
    return {
        "headers": cols,
        "values": [list(row) for row in df.values]
    }


def metric_for_group(df_diff: pd.DataFrame):
    df_added = df_diff[df_diff.added_or_removed == 'added']
    df_removed = df_diff[df_diff.added_or_removed == 'removed']
    chart = {}
    for app_col_name, (col_name, top_k) in GROUPS_MAP_TO_COLUMN.items():
        chart.setdefault(app_col_name, {})
        if top_k is not None:
            counter = Counter(df_diff[col_name])
            keys = [k for (k, v) in counter.most_common(top_k)]
        else:
            keys = pd.unique(df_diff[col_name]).tolist()
        added = [len(df_added[df_added[col_name] == key]) for key in keys]
        removed = [len(df_removed[df_removed[col_name] == key]) for key in keys]
        chart[app_col_name] = {'keys': keys, 'added': added, 'deleted': removed}
    return chart


def build_metrics(df_diff, all_agg_data):
    return {"uniqueTagsAdded": metrics_unique_tags_added_or_removed(df_diff, 'added', all_agg_data),
            "uniqueTagsDeleted": metrics_unique_tags_added_or_removed(df_diff, 'removed', all_agg_data),
            "tagsInstancesAdded": metrics_tags_instances_added_or_removed(df_diff, "added", all_agg_data),
            "tagsInstancesRemoved": metrics_tags_instances_added_or_removed(df_diff, "removed", all_agg_data),
            "upcChanged": metrics_upc_changed(df_diff, all_agg_data)
            }


def agg_data(df: pd.DataFrame):
    added_df = df[df.added_or_removed == 'added'].drop_duplicates(subset=SUBSET_INSTANCE)
    removed_df = df[df.added_or_removed == 'removed'].drop_duplicates(subset=SUBSET_INSTANCE)
    return {"all_tags_instances_count_added": len(added_df),
            "all_tags_instances_count_removed": len(removed_df),
            "all_unique_tags_count_added": len(pd.unique(added_df.tag)),
            "all_unique_tags_count_removed": len(pd.unique(removed_df.tag)),
            "all_upc": len(pd.unique(df.upc).tolist())
            }


def filter_df(df: pd.DataFrame, commitIds, tagTypes, tags, products, dataElements):
    #TODO add filter for commit ids
    if tagTypes:
        df = df[df.tag_type.isin(tagTypes)]
    if tags:
        df = df[df.tag.isin(tags)]
    if products:
        df = df[df.upc.isin(products)]
    if dataElements:
        df = df[df.data_element.isin(dataElements)]
    return df


def metrics_unique_tags_added_or_removed(df_diff: pd.DataFrame, tag_status: str, agg_data):
    all_unique_tags = pd.unique(df_diff[df_diff.added_or_removed == tag_status].tag).tolist()
    return {"value": len(all_unique_tags),
            "percent": round((len(all_unique_tags) / agg_data[f'all_unique_tags_count_{tag_status}']) * 100, 2)}


def metrics_tags_instances_added_or_removed(df_diff: pd.DataFrame, tag_status: str, agg_data):
    all_tag_instances = df_diff[df_diff.added_or_removed == tag_status].drop_duplicates(
        subset=SUBSET_INSTANCE).tag.tolist()
    return {'value': len(all_tag_instances),
            "percent": round((len(all_tag_instances) / agg_data[f"all_tags_instances_count_{tag_status}"]) * 100, 2)}


def metrics_upc_changed(df_diff, agg_data):
    unique_tags = pd.unique(df_diff.upc).tolist()
    return {"value": len(unique_tags), "percent": round((len(unique_tags) / agg_data["all_upc"]) * 100, 2)}


def get_filters(df_diff: pd.DataFrame):
    return {key: pd.unique(df_diff[val].sample(min(SAMPLE_SIZE, len(df_diff)))).tolist() for key, val in
            FILTERS_TYPE_TO_APP_NAME.items()}


# main driver function
if __name__ == '__main__':
    # run() method of Flask class runs the application
    # on the local development server.
    app.run('0.0.0.0', port=5555)
