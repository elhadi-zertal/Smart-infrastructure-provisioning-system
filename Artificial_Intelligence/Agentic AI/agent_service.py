from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from pydantic import BaseModel
from contextlib import asynccontextmanager
import asyncio
import logging
from agents import run_planner

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("[agent] Q&A service started.")
    yield
    logger.info("[agent] Q&A service stopped.")


app = FastAPI(
    title="Cluster Q&A Service",
    description="LLM-powered read-only assistant for admin questions about the cluster.",
    version="3.0",
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


class Query(BaseModel):
    question: str


@app.get("/")
def serve_gui():
    """Serve the Q&A GUI."""
    return FileResponse("gui.html")


@app.post("/ask")
async def ask(query: Query):
    """
    Ask the Planner agent a question about the cluster.
    The agent reads live cluster data and returns a plain-language answer.
    It never modifies anything — all actions go through main.py directly.
    """
    answer = await asyncio.to_thread(run_planner, query.question)
    return {"answer": answer}


@app.get("/health")
def health():
    return {"status": "ok"}



#run with
#pip install aiofiles   # needed for FileResponse
#uvicorn agent_service:app --host 0.0.0.0 --port 8002
