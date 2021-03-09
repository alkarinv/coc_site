import datetime
import os
import re
import shutil
import tempfile
from collections import defaultdict
from enum import Enum

from flask import (
    Blueprint,
    current_app,
    flash,
    g,
    jsonify,
    redirect,
    render_template,
    request,
    send_file,
    send_from_directory,
    url_for,
)
from sqlalchemy.sql import func
from werkzeug import secure_filename
from werkzeug.exceptions import abort

import gsite.logger as logger
from gsite import db
from gsite.auth import login_not_required
from gsite.logger import ExceptionReturn, HandleException, HtmlException, printe, printl
from models.models import Role, User

# from gsite.utils.graph_helper import get_img_data


bp = Blueprint("main", __name__)


@login_not_required(bp)
@bp.route("/safe_error")
def safe_error():
    return render_template("safe_error.html")


@bp.route("/")
@HandleException(bp, "main.safe_error")
def index():
    from gsite.plots import get_img_data, graph_player_day, graph_player_history
    players = ["#JPCVVPYY", "#JLRGG0RL", "#YRCCG8U", "#JVRYY9UJ", "#28QQU9CU"]
    graph_player_day(players)
    plot1 = get_img_data()

    graph_player_history(players)
    plot2 = get_img_data()

    return render_template("main/index.html", plot1=plot1, plot2=plot2)


@bp.route("/table/<string:intags>")
@HandleException(bp, "main.index")
def table_tag(intags):
    from gsite.table import get_rates
    tags = ["#JPCVVPYY", "#JLRGG0RL", "#YRCCG8U", "#JVRYY9UJ", "#28QQU9CU"]

    intags = intags.split("-")
    print("#", intags)
    for t in intags:
        t = t.upper()
        if not t.startswith("#"):
            t = "#" + t
        tags.append(t)
    print("#", tags)
    print(tags)
    # tags = ["#JPCVVPYY"]
    p = get_rates(tags)
    # print("###############", p)
    s = sorted(p.values(), key=lambda x: x['name'].lower())
    return render_template("main/table.html", players=s)

@bp.route("/table")
@HandleException(bp, "main.index")
def table():
    from gsite.table import get_rates
    tags = ["#JPCVVPYY", "#JLRGG0RL", "#YRCCG8U", "#JVRYY9UJ", "#28QQU9CU"]
    # tags = ["#JPCVVPYY"]
    p = get_rates(tags)
    # print("###############", p)
    s = sorted(p.values(), key=lambda x: x['name'].lower())
    return render_template("main/table.html", players=s)

@bp.route("/bases")
@HandleException(bp, "main.index")
def bases():
    return render_template("main/table.html")
