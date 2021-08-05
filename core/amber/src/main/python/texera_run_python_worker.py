import sys

from core.python_worker import PythonWorker

if __name__ == '__main__':
    PythonWorker(host="localhost", input_port=int(sys.argv[1]), output_port=int(sys.argv[2])).run()
