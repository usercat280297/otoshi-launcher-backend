import requests
import json
from typing import Optional, Dict, Any
from ..core.config import HUGGINGFACE_TOKEN, HF_REPO_ID

class HuggingFaceManifestService:
    """Service to fetch manifests from HuggingFace"""
    
    def __init__(self):
        self.token = HUGGINGFACE_TOKEN
        self.repo_id = HF_REPO_ID
        self.base_url = f"https://huggingface.co/datasets/{self.repo_id}/resolve/main"
        
    def get_manifest(self, game_slug: str, version: str = "latest") -> Optional[Dict[str, Any]]:
        """
        Fetch manifest for a game from HuggingFace
        
        Args:
            game_slug: Game identifier (e.g., "PEAK")
            version: Game version (e.g., "1.51a")
            
        Returns:
            Manifest dict or None if not found
        """
        headers = {}
        token = (self.token or "").strip()
        if token:
            headers["Authorization"] = f"Bearer {token}"
        
        # Try different manifest path patterns
        manifest_paths = [
            f"{game_slug}/{game_slug} {version}/manifest_{game_slug} {version}.json",
            f"{game_slug}/manifest_{version}.json",
            f"{game_slug}/manifest.json",
            f"{game_slug}/{version}/manifest.json"
        ]
        
        for path in manifest_paths:
            url = f"{self.base_url}/{path}"
            try:
                response = requests.get(url, headers=headers, allow_redirects=True, timeout=10)
                if response.status_code in (401, 403) and headers.get("Authorization"):
                    response = requests.get(url, headers={}, allow_redirects=True, timeout=10)
                if response.status_code == 200:
                    manifest = response.json()
                    # Add metadata
                    manifest["source"] = "huggingface"
                    manifest["repo_id"] = self.repo_id
                    manifest["path"] = path
                    return manifest
            except Exception as e:
                print(f"Failed to fetch {path}: {e}")
                continue
                
        return None
    
    def get_peak_manifest(self) -> Optional[Dict[str, Any]]:
        """Get PEAK game manifest specifically"""
        return self.get_manifest("PEAK", "1.51a")

# Global instance
hf_manifest_service = HuggingFaceManifestService()
