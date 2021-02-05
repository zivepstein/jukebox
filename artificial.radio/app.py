from flask import Flask, render_template
import os
import random
import json
import sys  
import math
app = Flask(__name__)


@app.route("/")
def index():
	return render_template('circular-wave.html')

if __name__ == "__main__":
    app.run(debug=True)