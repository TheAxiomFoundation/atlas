"""Modal deployment for Arch REST API.

Deploy with:
    modal deploy modal_app.py

Upload database (first time or after updates):
    modal volume put atlas-db atlas.db /data/atlas.db

Serve locally for testing:
    modal serve modal_app.py
"""

import modal

# Create the Modal app
app = modal.App("atlas")

# Volume for the SQLite database (persistent storage)
volume = modal.Volume.from_name("atlas-db", create_if_missing=True)
DB_PATH = "/data/atlas.db"

# Container image with dependencies
image = (
    modal.Image.debian_slim(python_version="3.14")
    .pip_install(
        "fastapi>=0.109",
        "uvicorn>=0.27",
        "pydantic>=2.0",
        "lxml>=5.0",
        "sqlite-utils>=3.35",
    )
    .add_local_dir("src", remote_path="/app/src")
    .env({"PYTHONPATH": "/app/src"})
)


@app.function(
    image=image,
    volumes={"/data": volume},
    scaledown_window=300,  # Keep warm for 5 minutes
)
@modal.concurrent(max_inputs=100)
@modal.asgi_app()
def fastapi_app():
    """Serve the Arch FastAPI application."""
    from atlas.api.main import create_app

    # Create app with volume-mounted database
    return create_app(db_path=DB_PATH)


@app.local_entrypoint()
def main():
    """CLI entrypoint for uploading database."""
    import subprocess
    import sys

    print("Arch Modal Deployment")
    print("=" * 40)
    print()
    print("Commands:")
    print("  modal deploy modal_app.py     # Deploy the API")
    print("  modal serve modal_app.py      # Test locally")
    print()
    print("Upload database (required before first deploy):")
    print("  modal volume put atlas-db atlas.db /data/atlas.db")
    print()
    print("Check volume contents:")
    print("  modal volume ls atlas-db /data/")
