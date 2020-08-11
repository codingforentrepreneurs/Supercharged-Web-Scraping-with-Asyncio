import fire
from .projects.spoonflower import run_spoonflower

class Pipeline():
    def __init__(self):
        self.spoonflower = run_spoonflower

if __name__ == "__main__":
    fire.Fire(Pipeline)
