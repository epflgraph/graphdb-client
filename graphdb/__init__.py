from pathlib import Path
import shutil


def _ensure_default_config() -> None:
    """
    Ensure config.yaml exists at the location expected by graphdb.core.graphdb.
    """
    package_dir = Path(__file__).resolve().parent
    target_config = package_dir.parent / "config.yaml"
    candidate_sources = [
        Path.cwd() / "config.yaml",
        package_dir / "config.yaml",
    ]

    if target_config.exists():
        return

    for source in candidate_sources:
        if not source.exists():
            continue
        try:
            shutil.copyfile(source, target_config)
            break
        except OSError:
            # Keep imports resilient in read-only environments.
            break


_ensure_default_config()
