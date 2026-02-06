import subprocess
from pathlib import Path


class DeltaGenerator:
    def generate_patch(self, old_file: Path, new_file: Path, patch_file: Path) -> int:
        result = subprocess.run(
            ["xdelta3", "-e", "-s", str(old_file), str(new_file), str(patch_file)],
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode != 0:
            raise RuntimeError(f"Patch generation failed: {result.stderr}")

        return patch_file.stat().st_size
