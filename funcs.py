"""Module containing generally useful functions"""
import os
import sys

def get_script_path():
    """Get the location of the script that's running"""
    return os.path.dirname(os.path.realpath(sys.argv[0]))

def new_folder(name):
    """Create new folder in the current working directory"""
    os.mkdir(os.getcwd() + "/" + name)