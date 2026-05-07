from dotenv import load_dotenv
load_dotenv()

from fastapi import FastAPI
from pydantic import BaseModel
from contextlib import asynccontextmanager
import asyncio
import logging
from agents import run_planner

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


class Query(BaseModel):
    question: str


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
