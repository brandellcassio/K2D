import sys
import os
sys.path.append('../')
from evaluation import EvaluationHandler

evaluation = EvaluationHandler()
evaluation.read_file(key_file="result_file", with_levels=True)