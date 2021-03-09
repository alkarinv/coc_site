import base64
import io

import matplotlib

matplotlib.use("TkAgg")
import matplotlib.pyplot as plt  # ## fixes a bug that crashes mac mojave
import seaborn as sns


def get_img_data(clear=True):
    img = io.BytesIO()
    plt.savefig(img, format="png")
    img.seek(0)
    d = base64.b64encode(img.getvalue()).decode()
    if clear:
        ## Clear plot for new plots
        plt.clf()  #  clear data and axes
    return d


