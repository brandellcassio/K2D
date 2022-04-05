import sys
import os
sys.path.append('../')
from evaluation import EvaluationHandler

nalir_file = sys.argv[1]
output_file = sys.argv[2]

evaluation = EvaluationHandler()
evaluation.load_query_file()
evaluation.run_nalir_k2d_evaluation(nalir_file, output_file, with_levels=False)