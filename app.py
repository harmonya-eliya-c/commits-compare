import json

from flask import Flask, request, jsonify

app = Flask(__name__)


@app.route('/')
def status():
    return 'UP'


@app.route('/get_diff_by_commits', method=["POST"])
def get_diff_by_commits():
    base_commit = request.form['base_commit']
    commit_to_compare = request.form['commit_to_compare']
    print(base_commit, commit_to_compare)
    with open("data/mock.json", 'r') as f:
        data = json.load(f)
    return jsonify(data)


# main driver function
if __name__ == '__main__':
    # run() method of Flask class runs the application
    # on the local development server.
    app.run('0.0.0.0', port=5555)