from flask import Flask, render_template
import os
import random
import json
import sys  
import math
app = Flask(__name__)



@app.route("/")
def index():
	return render_template('index.html', papers=map(lambda x: x[25:], papers), lpap = len(papers), data=data)

if __name__ == "__main__":
    app.run(debug=True)