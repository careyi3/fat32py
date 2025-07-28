import subprocess
import os


def test_examples_run():
    examples_dir = os.path.join(os.path.dirname(__file__), "../examples")
    for fname in os.listdir(examples_dir):
        if fname.endswith(".py"):
            path = os.path.join(examples_dir, fname)
            result = subprocess.run(["uv", "run", path])
            assert result.returncode == 0, f"{fname} failed to run"
