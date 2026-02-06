import hashlib
from pathlib import Path
from typing import Dict


class ManifestBuilder:
    def __init__(self, chunk_size: int = 1024 * 1024) -> None:
        self.chunk_size = chunk_size

    def build_manifest(self, game_id: str, version: str, build_directory: Path) -> Dict:
        files = []
        total_size = 0

        for file_path in build_directory.rglob("*"):
            if not file_path.is_file():
                continue
            file_info = self._process_file(file_path, build_directory)
            files.append(file_info)
            total_size += file_info["size"]

        build_id = hashlib.sha256(f"{game_id}{version}".encode("utf-8")).hexdigest()[:16]
        return {
            "game_id": game_id,
            "version": version,
            "build_id": build_id,
            "total_size": total_size,
            "compressed_size": total_size,
            "chunk_size": self.chunk_size,
            "files": files,
        }

    def _process_file(self, file_path: Path, base_path: Path) -> Dict:
        relative_path = file_path.relative_to(base_path)
        size = file_path.stat().st_size

        chunks = []
        with file_path.open("rb") as handle:
            index = 0
            while True:
                chunk_data = handle.read(self.chunk_size)
                if not chunk_data:
                    break
                chunk_hash = hashlib.sha256(chunk_data).hexdigest()
                chunks.append(
                    {
                        "index": index,
                        "hash": chunk_hash,
                        "size": len(chunk_data),
                        "compression": "none",
                    }
                )
                index += 1

        file_hash = hashlib.sha256(file_path.read_bytes()).hexdigest()
        return {
            "path": str(relative_path),
            "size": size,
            "hash": file_hash,
            "chunks": chunks,
        }
