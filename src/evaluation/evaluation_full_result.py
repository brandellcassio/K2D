import sys
import os
sys.path.append('../')
from evaluation import EvaluationHandler

evaluation = EvaluationHandler()
evaluation.run_evaluation_all_results()