""" Helper file for io and file operations

"""
import glob
import os

INIT_VAL = object()


def find_file_with_alternate_extensions(filename, extensions=("mp3", "wav", "raw")):
    if os.path.exists(filename):
        return filename
    f, __ = os.path.splitext(filename)
    valid_files = set([f"{f}.{ext}" for ext in extensions])
    fn = "%s.*" % f
    files = glob.glob(fn)
    for f in files:
        if f in valid_files:
            return f
    return None


def find_file_prefix(filename, default_val=INIT_VAL):
    fn = "%s*" % filename
    files = glob.glob(fn)

    if len(files) != 1 and default_val != INIT_VAL:
        return default_val
    if not files:
        raise Exception("No recording matched '%s'" % filename)
    if len(files) > 1:
        raise Exception("Multiple recordings matched '%s'" % filename)
    return files[0]
