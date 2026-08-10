from bs4 import BeautifulSoup
from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER
from reportlab.lib.pagesizes import LETTER
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import BaseDocTemplate, PageTemplate, Frame, Paragraph, Spacer, PageBreak
from reportlab.pdfbase.pdfmetrics import stringWidth
from reportlab.pdfgen.canvas import Canvas
import html
import re


INPUT = "tmp/pdfs/greer.html"
OUTPUT = "output/pdf/greer-aspen-security-forum-auto-transcript.pdf"
SOURCE = "https://fight.fudgie.org/search/show/cspan/episode/20260718_CSPAN_1853-1940_EDT_US_Trade_Representative_Jamieson_Greer_Speaks_at_Security_Forum"


def clean(value):
    value = html.unescape(value or "")
    value = re.sub(r"\s+", " ", value).strip()
    return value


def extract_rows():
    soup = BeautifulSoup(open(INPUT, encoding="utf-8"), "html.parser")
    rows = []
    current_speaker = "Unidentified"
    for tr in soup.select("#lines tr"):
        chapter = tr.select_one(".chapter-marker-content")
        if chapter:
            title = clean(chapter.select_one(".chapter-title").get_text(" ", strip=True))
            time = clean(chapter.select_one(".chapter-time").get_text(" ", strip=True))
            rows.append(("chapter", title, time, ""))
            continue
        timestamp = tr.select_one(".text-timestamp")
        text_cell = tr.select_one(".text-cell")
        if not timestamp or not text_cell:
            continue
        speaker = timestamp.get("data-speaker-name")
        if speaker:
            current_speaker = clean(speaker)
        timestamp_text = clean(timestamp.get_text(" ", strip=True)).replace("▶", "")
        match = re.search(r"\b\d{2}:\d{2}:\d{2}\b", timestamp_text)
        time = match.group(0) if match else timestamp_text
        for tag in text_cell.select("a, button, svg"):
            tag.decompose()
        text = clean(text_cell.get_text(" ", strip=True))
        if text:
            rows.append(("line", current_speaker, time, text))
    return rows


class NumberedCanvas(Canvas):
    def __init__(self, *args, **kwargs):
        Canvas.__init__(self, *args, **kwargs)
        self._saved_page_states = []

    def save(self):
        page_count = len(self._saved_page_states)
        for state in self._saved_page_states:
            self.__dict__.update(state)
            self.draw_page_number(page_count)
            Canvas.showPage(self)
        Canvas.save(self)

    def showPage(self):
        self._saved_page_states.append(dict(self.__dict__))
        self._startPage()

    def draw_page_number(self, page_count):
        self.saveState()
        self.setStrokeColor(colors.HexColor("#D9E1E8"))
        self.line(0.65 * inch, 0.52 * inch, LETTER[0] - 0.65 * inch, 0.52 * inch)
        self.setFont("Helvetica", 8)
        self.setFillColor(colors.HexColor("#66737F"))
        self.drawString(0.65 * inch, 0.32 * inch, "Aspen Security Forum | Auto-generated transcript")
        label = f"Page {self._pageNumber} of {page_count}"
        self.drawRightString(LETTER[0] - 0.65 * inch, 0.32 * inch, label)
        self.restoreState()


def build():
    rows = extract_rows()
    styles = getSampleStyleSheet()
    styles.add(ParagraphStyle(name="CoverTitle", parent=styles["Title"], fontName="Helvetica-Bold", fontSize=23, leading=28, alignment=TA_CENTER, textColor=colors.HexColor("#123047"), spaceAfter=18))
    styles.add(ParagraphStyle(name="CoverMeta", parent=styles["Normal"], fontSize=10, leading=15, alignment=TA_CENTER, textColor=colors.HexColor("#4F5D68"), spaceAfter=8))
    styles.add(ParagraphStyle(name="Chapter", parent=styles["Heading2"], fontName="Helvetica-Bold", fontSize=14, leading=18, textColor=colors.HexColor("#0D5572"), spaceBefore=12, spaceAfter=8, keepWithNext=True))
    styles.add(ParagraphStyle(name="Transcript", parent=styles["Normal"], fontName="Helvetica", fontSize=9.3, leading=13.2, textColor=colors.HexColor("#1F2933"), spaceAfter=6))
    styles.add(ParagraphStyle(name="Speaker", parent=styles["Normal"], fontName="Helvetica-Bold", fontSize=8.4, leading=11, textColor=colors.HexColor("#0D5572"), spaceAfter=1))
    styles.add(ParagraphStyle(name="Note", parent=styles["Normal"], fontSize=8.5, leading=12, textColor=colors.HexColor("#596773"), borderColor=colors.HexColor("#D9E1E8"), borderWidth=0.6, borderPadding=8, backColor=colors.HexColor("#F5F8FA"), spaceBefore=12, spaceAfter=14))

    frame = Frame(0.72 * inch, 0.68 * inch, LETTER[0] - 1.44 * inch, LETTER[1] - 1.38 * inch, id="normal", leftPadding=0, rightPadding=0, topPadding=0, bottomPadding=0)
    doc = BaseDocTemplate(OUTPUT, pagesize=LETTER, leftMargin=0.72 * inch, rightMargin=0.72 * inch, topMargin=0.7 * inch, bottomMargin=0.7 * inch, title="Jamieson Greer at the Aspen Security Forum - Auto Transcript", author="Codex")
    doc.addPageTemplates([PageTemplate(id="main", frames=[frame])])
    story = []
    story.append(Spacer(1, 1.0 * inch))
    story.append(Paragraph("Fireside Chat with Ambassador Jamieson Greer", styles["CoverTitle"]))
    story.append(Paragraph("Aspen Security Forum | July 15, 2026", styles["CoverMeta"]))
    story.append(Paragraph("Jamieson Greer, 20th U.S. Trade Representative<br/>Moderator: Anja Manuel", styles["CoverMeta"]))
    story.append(Spacer(1, 0.2 * inch))
    story.append(Paragraph("Transcript source: C-SPAN program transcript mirror. This document preserves the automatically generated transcription and may contain inaccuracies. Please refer to the official video for authoritative wording.", styles["Note"]))
    story.append(Paragraph("Official event page: aspensecurityforum.org/fireside-chat-with-ambassador-jamieson-greer", styles["CoverMeta"]))
    story.append(PageBreak())
    for kind, speaker, time, text in rows:
        if kind == "chapter":
            story.append(Paragraph(f"{speaker} <font color='#66737F' size='9'>({time})</font>", styles["Chapter"]))
        else:
            story.append(Paragraph(f"{speaker}  |  {time}", styles["Speaker"]))
            story.append(Paragraph(text.replace("&", "&amp;"), styles["Transcript"]))
    doc.build(story, canvasmaker=NumberedCanvas)
    print(f"rows={len(rows)} output={OUTPUT}")


if __name__ == "__main__":
    build()
