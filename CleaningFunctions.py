#%%
import re


def convert_ar_characters(input_str):

    mapping = {
        "ك": "ک",
        "گ": "گ",
        "دِ": "د",
        "بِ": "ب",
        "زِ": "ز",
        "ذِ": "ذ",
        "شِ": "ش",
        "سِ": "س",
        "ى": "ی",
        "ي": "ی",
    }
    return _multiple_replace(mapping, input_str)


def _multiple_replace(mapping, text):
    pattern = "|".join(map(re.escape, mapping.keys()))
    return re.sub(pattern, lambda m: mapping[m.group()], str(text))


def vv(row):
    X = row.split("-")
    return int(X[0] + X[1] + X[2])


def vv2(row):
    X = row.split("/")
    return int(X[0] + X[1] + X[2])


def addDash(row):
    row = str(row)
    X = [1, 1, 1]
    X[0] = row[0:4]
    X[1] = row[4:6]
    X[2] = row[6:8]
    return X[0] + "-" + X[1] + "-" + X[2]


def removeSlash(row):
    X = row.split("/")
    if len(X[1]) < 2:
        X[1] = "0" + X[1]
    if len(X[2]) < 2:
        X[2] = "0" + X[2]
    return int(X[0] + X[1] + X[2])


def removeSlash2(row):
    X = row.split("/")
    if len(X[1]) < 2:
        X[1] = "0" + X[1]
    if len(X[0]) < 2:
        X[0] = "0" + X[0]

    return int(X[2] + X[0] + X[1])


def removeDash(row):
    X = row.split("-")
    if len(X[1]) < 2:
        X[1] = "0" + X[1]
    if len(X[2]) < 2:
        X[2] = "0" + X[2]
    return int(X[0] + X[1] + X[2])
