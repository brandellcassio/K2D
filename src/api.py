from mapper import Mapper
import flask
from flask import request, jsonify

main_mapper = Mapper()
app = flask.Flask(__name__)
app.config['DEBUG'] = True
@app.route("/", methods=['POST'])
def map_words():
    matches = request.json['matches'].split(" ")
    #return jsonify(matches)
    response = main_mapper.get_matches(matches)
    return jsonify(response)

app.run()