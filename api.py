"""
Lightweight API for PubMed study search + relevance scoring.

Accepts any question + data context, returns ranked relevant studies.
Designed for external apps that have their own database but want
CAN Explorer's smart PubMed search pipeline.

Run:  uvicorn api:app --host 0.0.0.0 --port 8000
Test: curl -X POST http://localhost:8000/find-studies \
        -H "Content-Type: application/json" \
        -d '{"question": "Cannabis use among Swedish youth", "context": "Usage rose from 7% to 10%"}'
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from report_generator import (
    search_pubmed_multi,
    filter_relevant_studies,
)

app = FastAPI(
    title="PubMed Study Finder",
    description="Smart PubMed search with GPT-powered query generation and embedding-based relevance scoring.",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["POST"],
    allow_headers=["*"],
)


class StudyRequest(BaseModel):
    question: str = Field(..., description="The research question (any language)")
    context: str = Field("", description="Optional data/answer context to improve search relevance")
    max_studies: int = Field(6, ge=1, le=15, description="Max studies to return")
    threshold: float = Field(0.30, ge=0.0, le=1.0, description="Minimum relevance score (0-1)")


class Study(BaseModel):
    title: str
    authors: list[str]
    year: str
    journal: str
    pmid: str
    abstract: str
    relevance_score: float


class StudyResponse(BaseModel):
    query_generated: list[str]
    studies: list[Study]
    total_raw: int


@app.post("/find-studies", response_model=StudyResponse)
def find_studies(req: StudyRequest):
    """
    1. GPT generates targeted English PubMed queries from the question
    2. Searches PubMed via E-utilities API
    3. Scores each study's abstract against the query via embedding cosine similarity
    4. Returns only studies above the relevance threshold, ranked by score
    """
    raw, queries = search_pubmed_multi(
        [], question=req.question, answer=req.context,
    )

    filtered = filter_relevant_studies(
        raw, req.question,
        anchor_texts=queries,
        threshold=req.threshold,
        max_studies=req.max_studies,
    )

    studies = [
        Study(
            title=s.get("title", ""),
            authors=s.get("authors", []),
            year=s.get("year", "n.d."),
            journal=s.get("journal", ""),
            pmid=s.get("pmid", ""),
            abstract=s.get("abstract", ""),
            relevance_score=round(s.get("relevance_score", 0), 3),
        )
        for s in filtered
    ]

    return StudyResponse(
        query_generated=queries,
        studies=studies,
        total_raw=len(raw),
    )
