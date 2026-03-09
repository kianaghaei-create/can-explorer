"""
Report Generator for CAN Explorer.

Generates CAN-branded PDF reports from AI chat results, enriched with
relevant PubMed studies. Pipeline:
  1. Decompose question into topics
  2. Search PubMed per topic (E-utilities API, no key needed)
  3. Filter by embedding cosine similarity to question
  4. GPT-4o writes structured report sections
  5. ReportLab renders a CAN-branded PDF
"""

import io
import json
import time
import requests
import xml.etree.ElementTree as ET
import numpy as np
from datetime import datetime
import os

from chat_engine import client  # reuse the working OpenAI client
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import mm
from reportlab.lib.colors import HexColor, white
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.enums import TA_JUSTIFY
from reportlab.platypus import (
    BaseDocTemplate, Frame, PageTemplate, NextPageTemplate,
    Paragraph, Spacer, Table, TableStyle,
    PageBreak, HRFlowable,
)

# client is imported from chat_engine (single source of truth for API key)

PUBMED_BASE = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"

# CAN brand palette
CAN_NAVY = HexColor("#1B3A6B")
CAN_LIGHT = HexColor("#B0C4DE")
CAN_BG = HexColor("#EEF3FA")
GREY_TEXT = HexColor("#333333")


# ── PubMed helpers ─────────────────────────────────────────────────────────────

def _parse_pubmed_xml(xml_bytes: bytes) -> list:
    """Parse PubMed efetch XML into list of study dicts."""
    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError:
        return []
    studies = []
    for article in root.findall(".//PubmedArticle"):
        try:
            title_el = article.find(".//ArticleTitle")
            title = (title_el.text or "").strip()
            if not title:
                continue
            abstract_parts = article.findall(".//AbstractText")
            abstract = " ".join(
                (el.text or "") for el in abstract_parts if el.text
            ).strip()
            if not abstract:
                continue
            year_el = article.find(".//PubDate/Year")
            year = year_el.text if year_el is not None else "n.d."
            authors = []
            for author in article.findall(".//Author")[:4]:
                ln = author.find("LastName")
                if ln is not None and ln.text:
                    authors.append(ln.text)
            journal_el = article.find(".//Journal/Title")
            journal = journal_el.text if journal_el is not None else ""
            pmid_el = article.find(".//PMID")
            pmid = pmid_el.text if pmid_el is not None else ""
            studies.append({
                "pmid": pmid, "title": title, "abstract": abstract,
                "year": year, "authors": authors, "journal": journal,
            })
        except Exception:
            continue
    return studies


def search_pubmed(query: str, max_results: int = 10) -> list:
    """Search PubMed and return articles with title + abstract."""
    try:
        r = requests.get(
            f"{PUBMED_BASE}/esearch.fcgi",
            params={
                "db": "pubmed", "term": query, "retmax": max_results,
                "retmode": "json", "sort": "relevance",
            },
            timeout=10,
        )
        pmids = r.json().get("esearchresult", {}).get("idlist", [])
    except Exception:
        return []
    if not pmids:
        return []
    time.sleep(0.35)
    try:
        r = requests.get(
            f"{PUBMED_BASE}/efetch.fcgi",
            params={
                "db": "pubmed", "id": ",".join(pmids),
                "retmode": "xml", "rettype": "abstract",
            },
            timeout=15,
        )
        return _parse_pubmed_xml(r.content)
    except Exception:
        return []


def build_pubmed_queries(question: str, answer: str = "") -> list:
    """
    Use GPT-4o-mini to generate 2-3 focused, substance-specific PubMed queries
    from the user's question and data answer.

    Returns a list of query strings ready for the PubMed esearch API.
    Each query is targeted (substance + population + geography where relevant)
    instead of using raw decomposed topics that match irrelevant papers.
    """
    context = f"Question: {question}"
    if answer:
        context += f"\n\nData finding: {answer[:500]}"
    try:
        resp = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": f"""You are a PubMed search expert.
Based on this CAN (Swedish alcohol & drug statistics) question and finding, generate
2-3 specific PubMed search queries to find genuinely relevant peer-reviewed studies.

{context}

Rules:
- Focus on the SUBSTANCE/TOPIC (e.g. snus, smokeless tobacco, nicotine, alcohol, cannabis)
- Include the POPULATION when relevant (adolescents, youth, students, adults)
- Include Sweden/Scandinavia/Nordic when the data is Sweden-specific
- Use MeSH terms and boolean operators
- Do NOT include irrelevant demographic filters (e.g. don't search for "girls" alone)
- Return JSON: {{"queries": ["query1", "query2", "query3"]}}

Examples:
- "snus use among Swedish adolescents" →
  ["(snus OR smokeless tobacco) AND (adolescents OR youth) AND (Sweden OR Scandinavia)",
   "nicotine pouches youth prevalence Sweden"]
- "alcohol consumption Sweden 2024" →
  ["alcohol consumption trends Sweden",
   "alcohol use disorder Sweden epidemiology"]"""}],
            temperature=0,
            max_tokens=300,
            response_format={"type": "json_object"},
        )
        result = json.loads(resp.choices[0].message.content)
        queries = result.get("queries", [])
        if queries:
            return queries[:3]
    except Exception:
        pass
    # Fallback: simple keyword extraction
    return [question[:200]]


def search_pubmed_multi(topics: list, max_per_topic: int = 8,
                        question: str = "", answer: str = "") -> tuple:
    """
    Run PubMed search using GPT-generated targeted queries (not raw decomposed topics).
    Returns (studies, queries) so the caller can use the English queries as
    the cosine-similarity anchor in filter_relevant_studies.
    """
    # Generate targeted queries from the full question + answer
    if question:
        queries = build_pubmed_queries(question, answer)
    else:
        # Legacy fallback: use topics with substance MeSH filter
        queries = [
            f"({t}) AND (substance use OR alcohol OR tobacco OR narcotics)[MeSH Terms]"
            for t in topics[:2]
        ]

    seen = set()
    all_studies = []
    for query in queries:
        for s in search_pubmed(query, max_results=max_per_topic):
            if s["pmid"] not in seen:
                seen.add(s["pmid"])
                s["search_topic"] = query
                all_studies.append(s)
        time.sleep(0.35)
    return all_studies, queries


# ── Relevance filter ───────────────────────────────────────────────────────────

def filter_relevant_studies(
    studies: list,
    question: str,
    threshold: float = 0.30,
    max_studies: int = 6,
    anchor_texts: list = None,
) -> list:
    """
    Score study abstracts via embedding cosine similarity.

    Uses anchor_texts (the English PubMed queries) as the comparison anchor
    rather than the raw question, which may be in Swedish. Comparing English
    queries to English abstracts gives much better cosine scores than
    comparing a Swedish question to English abstracts.

    Threshold 0.30 calibrated for English-query vs English-abstract cosine:
    - Truly irrelevant (HIV/cancer/concussions): typically 0.20–0.28
    - On-topic (snus/tobacco/youth Sweden):       typically 0.32–0.60
    """
    if not studies:
        return []

    # Build anchor: use English queries if available, otherwise fall back to question
    anchor = " ".join(anchor_texts) if anchor_texts else question

    texts = [anchor] + [f"{s['title']} {s['abstract']}" for s in studies]
    try:
        response = client.embeddings.create(
            input=texts, model="text-embedding-3-small"
        )
        vecs = np.array(
            [item.embedding for item in response.data], dtype=np.float32
        )
    except Exception as exc:
        import traceback, sys
        print(f"[report_generator] Embedding scoring failed: {exc}", file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
        return studies[:max_studies]

    # Normalize to unit vectors for proper cosine similarity
    norms = np.linalg.norm(vecs, axis=1, keepdims=True)
    norms = np.where(norms == 0, 1, norms)
    vecs = vecs / norms

    scores = vecs[1:] @ vecs[0]
    scored = [
        {**s, "relevance_score": float(scores[i])}
        for i, s in enumerate(studies)
        if scores[i] >= threshold
    ]
    scored.sort(key=lambda x: x["relevance_score"], reverse=True)
    return scored[:max_studies]


# ── Section generator ──────────────────────────────────────────────────────────

_PROMPT_SV = """\
Du skriver en professionell analytisk rapport för CAN (Centralförbundet för \
alkohol- och narkotikaupplysning).

FRÅGA: {question}

DATASAMMANFATTNING:
{data_preview}

AI-ANALYS:
{answer}

EXTERNA STUDIER (inkludera bara genuint relevanta):
{studies_text}

Skriv rapporten på svenska. Akademisk, neutral ton. Fyll INTE ut med generisk text.
Returnera JSON med dessa nycklar:
- "sammanfattning": 2-3 meningar, viktigaste fynd
- "inledning": 2-3 stycken, kontext och syfte
- "resultat": 3-5 stycken, detaljerade datafynd
- "diskussion": 2-4 stycken, tolkning och jämförelse med extern forskning
- "avslutande_kommentarer": 1-2 stycken med slutsatser
- "external_studies_commentary": 1 stycke om vilka externa studier som \
informerade diskussionen, eller tom sträng om inga var relevanta"""

_PROMPT_EN = """\
You are writing a professional analytical report for CAN \
(the Swedish National Centre for Alcohol and Drug Information).

QUESTION: {question}

DATA SUMMARY:
{data_preview}

AI ANALYSIS:
{answer}

EXTERNAL STUDIES (include only if genuinely relevant):
{studies_text}

Write in English. Academic, neutral, evidence-based tone. Do NOT pad with filler.
Return JSON with these keys:
- "sammanfattning": 2-3 sentences, key finding
- "inledning": 2-3 paragraphs, background and purpose
- "resultat": 3-5 paragraphs, detailed data findings
- "diskussion": 2-4 paragraphs, interpretation and comparison with research
- "avslutande_kommentarer": 1-2 paragraphs with conclusions
- "external_studies_commentary": 1 paragraph on which external studies informed \
the discussion, or empty string if none were relevant"""


def generate_report_sections(
    question: str,
    answer: str,
    data_preview: str,
    studies: list,
    language: str = "Swedish",
) -> dict:
    """Use GPT-4o to generate structured report sections."""
    if studies:
        parts = []
        for s in studies:
            au = ", ".join(s.get("authors", [])[:3])
            if len(s.get("authors", [])) > 3:
                au += " et al."
            parts.append(
                f'- {au} ({s.get("year","n.d.")}). "{s["title"]}". '
                f'{s.get("journal","")}.\n  Abstract: {s["abstract"][:400]}'
            )
        studies_text = "\n".join(parts)
    else:
        studies_text = "None."

    prompt = (_PROMPT_SV if language == "Swedish" else _PROMPT_EN).format(
        question=question,
        data_preview=(data_preview[:2000] if data_preview else "No data."),
        answer=answer[:3000],
        studies_text=studies_text[:3000],
    )
    resp = client.chat.completions.create(
        model="gpt-4o",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.3,
        max_tokens=3000,
        response_format={"type": "json_object"},
    )
    return json.loads(resp.choices[0].message.content)


# ── PDF builder ────────────────────────────────────────────────────────────────

def build_pdf(
    question: str,
    sections: dict,
    studies: list,
    data=None,
    language: str = "Swedish",
) -> bytes:
    """Render a CAN-branded PDF and return as bytes."""
    buf = io.BytesIO()
    PAGE_W, PAGE_H = A4
    LM = 22 * mm
    RM = 22 * mm
    BM = 18 * mm
    BAR_H = 9 * mm

    # Section label dictionaries
    if language == "Swedish":
        L = dict(
            cover_tag="ANALYTISK RAPPORT",
            summary="Sammanfattning",
            intro="Inledning",
            results="Resultat",
            discussion="Diskussion",
            conclusion="Avslutande kommentarer",
            ext_studies="Externa studier",
            refs="Referenser",
            data_table="Underliggande data (urval)",
            page="Sida",
            generated="Genererad",
            source="Källa: CAN Explorer — CAN:s officiella statistik",
            subtitle="Centralförbundet för alkohol- och narkotikaupplysning",
        )
    else:
        L = dict(
            cover_tag="ANALYTICAL REPORT",
            summary="Summary",
            intro="Introduction",
            results="Results",
            discussion="Discussion",
            conclusion="Concluding Remarks",
            ext_studies="External Studies",
            refs="References",
            data_table="Underlying Data (sample)",
            page="Page",
            generated="Generated",
            source="Source: CAN Explorer — official CAN statistics",
            subtitle="National Centre for Alcohol and Drug Information",
        )

    # ── Paragraph styles ──────────────────────────────────────
    section_style = ParagraphStyle(
        "SH", fontName="Helvetica-Bold", fontSize=13,
        textColor=CAN_NAVY, leading=17, spaceBefore=12, spaceAfter=5,
    )
    body_style = ParagraphStyle(
        "Body", fontName="Helvetica", fontSize=10,
        leading=15, textColor=GREY_TEXT, spaceAfter=8, alignment=TA_JUSTIFY,
    )
    summary_box_style = ParagraphStyle(
        "SumBox", fontName="Helvetica-Bold", fontSize=10,
        textColor=CAN_NAVY, leading=15, spaceAfter=6,
        backColor=CAN_BG, borderPad=8,
    )
    caption_style = ParagraphStyle(
        "Cap", fontName="Helvetica-Oblique", fontSize=8.5,
        textColor=HexColor("#666666"), leading=11, spaceAfter=3,
    )
    ref_style = ParagraphStyle(
        "Ref", fontName="Helvetica", fontSize=8.5,
        leading=12, textColor=GREY_TEXT, spaceAfter=5,
        leftIndent=14, firstLineIndent=-14,
    )
    study_block_style = ParagraphStyle(
        "StudyBlock", fontName="Helvetica", fontSize=9,
        leading=13, textColor=GREY_TEXT, spaceAfter=10,
        leftIndent=8, backColor=HexColor("#F8FAFF"), borderPad=6,
    )

    # ── Canvas callbacks (closures capturing local vars) ──────

    def _draw_cover(canvas, doc):
        canvas.saveState()
        # Navy header — top 42 %
        nh = PAGE_H * 0.42
        canvas.setFillColor(CAN_NAVY)
        canvas.rect(0, PAGE_H - nh, PAGE_W, nh, fill=1, stroke=0)

        # CAN wordmark
        canvas.setFillColor(white)
        canvas.setFont("Helvetica-Bold", 22)
        canvas.drawString(LM, PAGE_H - 20 * mm, "CAN")
        canvas.setFont("Helvetica", 9)
        canvas.setFillColor(CAN_LIGHT)
        canvas.drawString(LM, PAGE_H - 27 * mm, L["subtitle"])

        # Report tag near bottom of navy area
        canvas.setFont("Helvetica-Bold", 10)
        canvas.setFillColor(HexColor("#7EB3E8"))
        canvas.drawString(LM, PAGE_H - nh + 14 * mm, L["cover_tag"])

        # Question text — anchored 35 mm below the navy bottom edge
        navy_bottom = PAGE_H - nh          # y-coordinate of navy's lower edge
        y_title = navy_bottom - 35 * mm    # start title 35 mm below

        avail_w = PAGE_W - LM - RM
        fsize = 18
        fname = "Helvetica-Bold"
        words = question.split()
        lines = []
        line = []
        for w in words:
            test = " ".join(line + [w])
            if canvas.stringWidth(test, fname, fsize) <= avail_w:
                line.append(w)
            else:
                if line:
                    lines.append(" ".join(line))
                line = [w]
        if line:
            lines.append(" ".join(line))

        line_h = fsize * 1.5
        canvas.setFont(fname, fsize)
        canvas.setFillColor(CAN_NAVY)
        y = y_title
        for ln in lines:
            canvas.drawString(LM, y, ln)
            y -= line_h

        # Date 14 mm below last question line
        canvas.setFont("Helvetica", 11)
        canvas.setFillColor(HexColor("#666666"))
        today_long = datetime.now().strftime("%B %Y")
        canvas.drawString(LM, y - 14 * mm, today_long)

        # Footer bar
        canvas.setFillColor(CAN_NAVY)
        canvas.rect(0, 0, PAGE_W, 12 * mm, fill=1, stroke=0)
        canvas.setFillColor(white)
        canvas.setFont("Helvetica", 7)
        today_str = datetime.now().strftime("%Y-%m-%d")
        canvas.drawString(
            LM, 4.5 * mm,
            f"{L['generated']}: {today_str}  |  {L['source']}",
        )
        canvas.restoreState()

    def _draw_interior(canvas, doc):
        canvas.saveState()
        # Navy bar with |||||| stripe pattern
        canvas.setFillColor(CAN_NAVY)
        canvas.rect(0, PAGE_H - BAR_H, PAGE_W, BAR_H, fill=1, stroke=0)
        canvas.setFillColor(HexColor("#2A5499"))
        sw = 2.5 * mm
        x = LM * 1.5
        while x < PAGE_W - LM:
            canvas.rect(x, PAGE_H - BAR_H, sw, BAR_H, fill=1, stroke=0)
            x += sw * 2.4

        # CAN wordmark in bar
        canvas.setFillColor(white)
        canvas.setFont("Helvetica-Bold", 9)
        canvas.drawString(LM, PAGE_H - BAR_H + 3 * mm, "CAN Explorer")

        # Page number
        canvas.setFont("Helvetica", 8)
        pn = canvas.getPageNumber()
        canvas.drawRightString(
            PAGE_W - RM, PAGE_H - BAR_H + 3 * mm, f"{L['page']} {pn}"
        )

        # Bottom rule + source
        canvas.setStrokeColor(HexColor("#CCCCCC"))
        canvas.setLineWidth(0.5)
        canvas.line(LM, BM - 4 * mm, PAGE_W - RM, BM - 4 * mm)
        canvas.setFont("Helvetica", 7)
        canvas.setFillColor(HexColor("#888888"))
        canvas.drawString(LM, BM - 9 * mm, L["source"])
        canvas.restoreState()

    # ── Page templates ─────────────────────────────────────────
    navy_h = PAGE_H * 0.42
    cover_frame = Frame(
        LM, BM + 12 * mm,
        PAGE_W - LM - RM, PAGE_H - navy_h - BM - 14 * mm,
        id="cover",
    )
    interior_frame = Frame(
        LM, BM + 5 * mm,
        PAGE_W - LM - RM, PAGE_H - BAR_H - 12 * mm - BM - 5 * mm,
        id="interior",
    )

    doc = BaseDocTemplate(
        buf, pagesize=A4,
        leftMargin=LM, rightMargin=RM,
        topMargin=BAR_H + 4 * mm, bottomMargin=BM,
    )
    doc.addPageTemplates([
        PageTemplate(id="Cover", frames=[cover_frame], onPage=_draw_cover),
        PageTemplate(id="Interior", frames=[interior_frame], onPage=_draw_interior),
    ])

    # ── Story ──────────────────────────────────────────────────
    story = []

    # Cover page — canvas draws the design; just a spacer in the frame
    story.append(Spacer(1, 1))
    story.append(NextPageTemplate("Interior"))
    story.append(PageBreak())

    # Helper to add a section
    def _section(title, content_text):
        text = (content_text or "").strip()
        if not text:
            return
        story.append(Paragraph(title, section_style))
        story.append(HRFlowable(width="100%", thickness=1, color=CAN_NAVY, spaceAfter=5))
        for para in text.split("\n\n"):
            para = para.strip()
            if para:
                story.append(Paragraph(para, body_style))

    # Sammanfattning (highlighted box)
    summary_text = (sections.get("sammanfattning") or "").strip()
    if summary_text:
        story.append(Paragraph(L["summary"], section_style))
        story.append(HRFlowable(width="100%", thickness=1, color=CAN_NAVY, spaceAfter=5))
        story.append(Paragraph(summary_text, summary_box_style))
        story.append(Spacer(1, 4 * mm))

    _section(L["intro"], sections.get("inledning", ""))
    _section(L["results"], sections.get("resultat", ""))

    # Data table (up to 20 rows)
    if data is not None and len(data) > 0:
        story.append(Spacer(1, 4 * mm))
        story.append(Paragraph(L["data_table"], caption_style))
        df_show = data.head(20)
        tdata = [list(df_show.columns)] + [
            [
                str(c)[:25] if isinstance(c, str)
                else (round(c, 2) if isinstance(c, float) else c)
                for c in row
            ]
            for row in df_show.values.tolist()
        ]
        ncols = len(tdata[0])
        cw = (PAGE_W - LM - RM) / ncols
        tbl = Table(tdata, colWidths=[cw] * ncols, repeatRows=1)
        tbl.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), CAN_NAVY),
            ("TEXTCOLOR", (0, 0), (-1, 0), white),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("FONTSIZE", (0, 0), (-1, -1), 8),
            ("ALIGN", (0, 0), (-1, -1), "CENTER"),
            ("ROWBACKGROUNDS", (0, 1), (-1, -1), [CAN_BG, white]),
            ("GRID", (0, 0), (-1, -1), 0.25, HexColor("#CCCCCC")),
            ("TOPPADDING", (0, 0), (-1, -1), 3),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
        ]))
        story.append(tbl)
        story.append(Spacer(1, 4 * mm))

    _section(L["discussion"], sections.get("diskussion", ""))
    _section(L["conclusion"], sections.get("avslutande_kommentarer", ""))

    # External studies
    if studies:
        story.append(Paragraph(L["ext_studies"], section_style))
        story.append(HRFlowable(width="100%", thickness=1, color=CAN_NAVY, spaceAfter=5))
        commentary = (sections.get("external_studies_commentary") or "").strip()
        if commentary:
            story.append(Paragraph(commentary, body_style))
            story.append(Spacer(1, 3 * mm))

        for s in studies:
            au = ", ".join(s.get("authors", [])[:3])
            if len(s.get("authors", [])) > 3:
                au += " et al."
            pmid = s.get("pmid", "")
            url_txt = f"https://pubmed.ncbi.nlm.nih.gov/{pmid}/" if pmid else ""
            blurb = (
                f"<b>{s['title']}</b><br/>"
                f"{au} ({s.get('year','n.d.')}). <i>{s.get('journal','')}</i>.<br/>"
                f"{s['abstract'][:320]}…"
            )
            if url_txt:
                blurb += f"<br/><font color='#1B3A6B'>{url_txt}</font>"
            story.append(Paragraph(blurb, study_block_style))

    # References
    if studies:
        story.append(Paragraph(L["refs"], section_style))
        story.append(HRFlowable(width="100%", thickness=1, color=CAN_NAVY, spaceAfter=5))
        for i, s in enumerate(studies, 1):
            au = ", ".join(s.get("authors", [])[:3])
            if len(s.get("authors", [])) > 3:
                au += " et al."
            pmid = s.get("pmid", "")
            ref = (
                f"{i}. {au} ({s.get('year','n.d.')}). {s['title']}. "
                f"<i>{s.get('journal','')}</i>."
            )
            if pmid:
                ref += f" PMID: {pmid}."
            story.append(Paragraph(ref, ref_style))

    doc.build(story)
    return buf.getvalue()
