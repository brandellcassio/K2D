import sys
import os
sys.path.append('../')
from evaluation import EvaluationHandler

evaluation = EvaluationHandler()
evaluation.load_query_file()
evaluation.evaluate_nalir(with_levels=False)