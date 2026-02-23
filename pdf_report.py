"""
MODULE — PDF Report Generator
Generates a professional ops summary PDF using ReportLab.
"""
import io
from datetime import date
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.lib import colors
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
    HRFlowable, KeepTogether
)
from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_RIGHT

# ── Color palette ──────────────────────────────────────────
BG         = colors.HexColor('#0a0c0f')
SURFACE    = colors.HexColor('#111318')
BORDER     = colors.HexColor('#1e2330')
TEXT       = colors.HexColor('#e8eaf0')
MUTED      = colors.HexColor('#5a6070')
ACCENT     = colors.HexColor('#e8c547')
RED        = colors.HexColor('#ff4757')
AMBER      = colors.HexColor('#ffa502')
GREEN      = colors.HexColor('#2ed573')
BLUE       = colors.HexColor('#1e90ff')
WHITE      = colors.white
DARK_TEXT  = colors.HexColor('#1a1a2e')


def build_styles():
    base = getSampleStyleSheet()
    return {
        'title': ParagraphStyle('title', fontName='Helvetica-Bold', fontSize=22,
                                textColor=WHITE, spaceAfter=4, leading=26),
        'subtitle': ParagraphStyle('subtitle', fontName='Helvetica', fontSize=9,
                                   textColor=MUTED, spaceAfter=2, leading=12),
        'section': ParagraphStyle('section', fontName='Helvetica-Bold', fontSize=11,
                                  textColor=ACCENT, spaceBefore=14, spaceAfter=6,
                                  leading=14, letterSpacing=1),
        'body': ParagraphStyle('body', fontName='Helvetica', fontSize=8,
                               textColor=TEXT, leading=12),
        'mono': ParagraphStyle('mono', fontName='Courier', fontSize=7.5,
                               textColor=TEXT, leading=11),
        'muted': ParagraphStyle('muted', fontName='Helvetica', fontSize=7.5,
                                textColor=MUTED, leading=11),
        'kpi_value': ParagraphStyle('kpi_value', fontName='Helvetica-Bold', fontSize=20,
                                    textColor=ACCENT, leading=22, alignment=TA_CENTER),
        'kpi_label': ParagraphStyle('kpi_label', fontName='Helvetica', fontSize=7,
                                    textColor=MUTED, leading=9, alignment=TA_CENTER),
    }


def kpi_table(summary):
    s = build_styles()
    kpis = [
        ('TOTAL JOBS', summary['total_jobs'], WHITE),
        ('HIGH RISK', summary['high_risk_count'], RED),
        ('MEDIUM RISK', summary['medium_risk_count'], AMBER),
        ('LOW RISK', summary['low_risk_count'], GREEN),
        ('AVG DELAY', f"{summary['avg_delay_days']}d", ACCENT),
        ('COMPLETED', summary['completed_jobs'], MUTED),
    ]

    cells = []
    for label, value, color in kpis:
        val_style = ParagraphStyle('v', fontName='Helvetica-Bold', fontSize=18,
                                   textColor=color, leading=20, alignment=TA_CENTER)
        lbl_style = ParagraphStyle('l', fontName='Helvetica', fontSize=6.5,
                                   textColor=MUTED, leading=9, alignment=TA_CENTER)
        cells.append([Paragraph(str(value), val_style), Paragraph(label, lbl_style)])

    # 3 columns x 2 rows
    row1 = [[cells[i][0], cells[i][1]] for i in range(3)]
    row2 = [[cells[i][0], cells[i][1]] for i in range(3, 6)]

    data = [
        [cells[0][0], cells[1][0], cells[2][0]],
        [cells[0][1], cells[1][1], cells[2][1]],
        [Spacer(1, 6), Spacer(1, 6), Spacer(1, 6)],
        [cells[3][0], cells[4][0], cells[5][0]],
        [cells[3][1], cells[4][1], cells[5][1]],
    ]

    t = Table(data, colWidths=[2.1*inch, 2.1*inch, 2.1*inch])
    t.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,-1), SURFACE),
        ('LINEBELOW', (0,1), (-1,1), 0.5, BORDER),
        ('LINEBELOW', (0,4), (-1,4), 0.5, BORDER),
        ('TOPPADDING', (0,0), (-1,-1), 6),
        ('BOTTOMPADDING', (0,0), (-1,-1), 2),
        ('LEFTPADDING', (0,0), (-1,-1), 8),
        ('RIGHTPADDING', (0,0), (-1,-1), 8),
        ('ROWBACKGROUND', (0,0), (-1,1), SURFACE),
        ('ROWBACKGROUND', (0,3), (-1,4), SURFACE),
        ('BOX', (0,0), (-1,-1), 0.5, BORDER),
    ]))
    return t


def risk_table(scored_jobs):
    high = [j for j in scored_jobs if j['risk_level'] == 'HIGH'][:10]
    if not high:
        return None

    s = build_styles()
    header = ['JOB ID', 'CONTRACTOR', 'SCOPE', 'REGION', 'DAYS OPEN', 'DELAY', 'SCORE', 'REASONS']

    def cell(text, color=TEXT, bold=False):
        font = 'Helvetica-Bold' if bold else 'Helvetica'
        style = ParagraphStyle('c', fontName=font, fontSize=7, textColor=color,
                               leading=9, wordWrap='CJK')
        return Paragraph(str(text), style)

    rows = [[cell(h, MUTED, True) for h in header]]
    for j in high:
        rows.append([
            cell(j['job_id'], ACCENT),
            cell(j['contractor']),
            cell(j['scope_type'], MUTED),
            cell(j['region'], MUTED),
            cell(j.get('actual_duration_days', 0)),
            cell(f"{j.get('delay_days',0)}d", RED if j.get('delay_days',0) > 0 else MUTED),
            cell(j['risk_score'], RED, True),
            cell(j.get('risk_reasons','—'), MUTED),
        ])

    col_widths = [0.9*inch, 1.2*inch, 0.85*inch, 0.7*inch, 0.6*inch, 0.5*inch, 0.5*inch, 1.65*inch]
    t = Table(rows, colWidths=col_widths, repeatRows=1)
    t.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,0), SURFACE),
        ('LINEBELOW', (0,0), (-1,0), 0.5, BORDER),
        ('ROWBACKGROUND', (0,1), (-1,-1), BG),
        ('ROWBACKGROUND', (0,2), (-1,-1), SURFACE),
        ('LINEBELOW', (0,1), (-1,-2), 0.3, BORDER),
        ('TOPPADDING', (0,0), (-1,-1), 5),
        ('BOTTOMPADDING', (0,0), (-1,-1), 5),
        ('LEFTPADDING', (0,0), (-1,-1), 6),
        ('RIGHTPADDING', (0,0), (-1,-1), 6),
        ('BOX', (0,0), (-1,-1), 0.5, BORDER),
        ('VALIGN', (0,0), (-1,-1), 'TOP'),
    ]))
    return t


def contractor_table(ranked):
    s = build_styles()
    header = ['RANK', 'CONTRACTOR', 'JOBS', 'AVG DELAY', 'MARKOUT', 'INSP FAIL', 'RISK FACTOR']

    def cell(text, color=TEXT, bold=False):
        font = 'Helvetica-Bold' if bold else 'Helvetica'
        style = ParagraphStyle('c', fontName=font, fontSize=7.5, textColor=color, leading=10)
        return Paragraph(str(text), style)

    rows = [[cell(h, MUTED, True) for h in header]]
    for i, c in enumerate(ranked):
        risk_color = RED if i == 0 else AMBER if i == 1 else TEXT
        rows.append([
            cell(f"#{c['rank']}", MUTED),
            cell(c['contractor'], TEXT, i == 0),
            cell(c['job_count']),
            cell(f"{c['avg_delay_days']}d", RED if c['avg_delay_days'] > 10 else GREEN),
            cell(c['avg_markout_issues'], AMBER if c['avg_markout_issues'] > 1 else MUTED),
            cell(c['inspection_fail_rate'], AMBER if c['inspection_fail_rate'] > 1 else MUTED),
            cell(c['contractor_risk_factor'], risk_color, True),
        ])

    col_widths = [0.45*inch, 1.8*inch, 0.45*inch, 0.7*inch, 0.7*inch, 0.7*inch, 0.85*inch]
    t = Table(rows, colWidths=col_widths, repeatRows=1)
    t.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,0), SURFACE),
        ('LINEBELOW', (0,0), (-1,0), 0.5, BORDER),
        ('ROWBACKGROUND', (0,1), (-1,-1), BG),
        ('LINEBELOW', (0,1), (-1,-2), 0.3, BORDER),
        ('TOPPADDING', (0,0), (-1,-1), 6),
        ('BOTTOMPADDING', (0,0), (-1,-1), 6),
        ('LEFTPADDING', (0,0), (-1,-1), 6),
        ('RIGHTPADDING', (0,0), (-1,-1), 6),
        ('BOX', (0,0), (-1,-1), 0.5, BORDER),
    ]))
    return t


def generate_pdf(data: dict) -> bytes:
    """Generate PDF report from pipeline data. Returns PDF bytes."""
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(
        buffer,
        pagesize=letter,
        leftMargin=0.6*inch,
        rightMargin=0.6*inch,
        topMargin=0.6*inch,
        bottomMargin=0.6*inch,
    )

    s = build_styles()
    today = date.today().strftime('%B %d, %Y')
    story = []

    # ── Header ──────────────────────────────────────────────
    header_data = [[
        Paragraph('PRG RISK INTELLIGENCE', s['title']),
        Paragraph(f'Generated {today}', ParagraphStyle('d', fontName='Helvetica',
                  fontSize=8, textColor=MUTED, alignment=TA_RIGHT, leading=10))
    ]]
    header_t = Table(header_data, colWidths=[4.5*inch, 2.8*inch])
    header_t.setStyle(TableStyle([
        ('VALIGN', (0,0), (-1,-1), 'BOTTOM'),
        ('TOPPADDING', (0,0), (-1,-1), 0),
        ('BOTTOMPADDING', (0,0), (-1,-1), 0),
    ]))
    story.append(header_t)
    story.append(Paragraph('OPERATIONS SUMMARY REPORT', s['subtitle']))
    story.append(HRFlowable(width='100%', thickness=0.5, color=ACCENT, spaceAfter=12))

    # ── KPI Summary ─────────────────────────────────────────
    story.append(Paragraph('PORTFOLIO OVERVIEW', s['section']))
    story.append(kpi_table(data['summary']))
    story.append(Spacer(1, 12))

    # ── High Risk Jobs ───────────────────────────────────────
    rt = risk_table(data.get('scored_jobs', []))
    if rt:
        story.append(HRFlowable(width='100%', thickness=0.3, color=BORDER, spaceAfter=8))
        story.append(Paragraph('HIGH RISK JOBS', s['section']))
        story.append(rt)
        story.append(Spacer(1, 12))

    # ── Contractor Scorecards ────────────────────────────────
    ranked = data.get('ranked_contractors', [])
    if ranked:
        story.append(HRFlowable(width='100%', thickness=0.3, color=BORDER, spaceAfter=8))
        story.append(Paragraph('CONTRACTOR RISK RANKING', s['section']))
        story.append(contractor_table(ranked))
        story.append(Spacer(1, 12))

    # ── Permit Summary ───────────────────────────────────────
    ps = data.get('permit_summary')
    if ps:
        story.append(HRFlowable(width='100%', thickness=0.3, color=BORDER, spaceAfter=8))
        story.append(Paragraph('PERMIT STATUS', s['section']))
        permit_data = [
            [Paragraph('APPROVED', ParagraphStyle('p', fontName='Helvetica-Bold', fontSize=8, textColor=GREEN, leading=10)),
             Paragraph('PENDING', ParagraphStyle('p', fontName='Helvetica-Bold', fontSize=8, textColor=AMBER, leading=10)),
             Paragraph('BLOCKED', ParagraphStyle('p', fontName='Helvetica-Bold', fontSize=8, textColor=RED, leading=10)),
             Paragraph('EXPIRING SOON', ParagraphStyle('p', fontName='Helvetica-Bold', fontSize=8, textColor=AMBER, leading=10))],
            [Paragraph(str(ps['approved']), ParagraphStyle('v', fontName='Helvetica-Bold', fontSize=16, textColor=GREEN, leading=18, alignment=TA_CENTER)),
             Paragraph(str(ps['pending']), ParagraphStyle('v', fontName='Helvetica-Bold', fontSize=16, textColor=AMBER, leading=18, alignment=TA_CENTER)),
             Paragraph(str(ps['blocked']), ParagraphStyle('v', fontName='Helvetica-Bold', fontSize=16, textColor=RED, leading=18, alignment=TA_CENTER)),
             Paragraph(str(ps['expiring_soon']), ParagraphStyle('v', fontName='Helvetica-Bold', fontSize=16, textColor=AMBER, leading=18, alignment=TA_CENTER))],
        ]
        pt = Table(permit_data, colWidths=[1.65*inch]*4)
        pt.setStyle(TableStyle([
            ('BACKGROUND', (0,0), (-1,-1), SURFACE),
            ('BOX', (0,0), (-1,-1), 0.5, BORDER),
            ('INNERGRID', (0,0), (-1,-1), 0.3, BORDER),
            ('TOPPADDING', (0,0), (-1,-1), 8),
            ('BOTTOMPADDING', (0,0), (-1,-1), 8),
            ('LEFTPADDING', (0,0), (-1,-1), 8),
            ('ALIGN', (0,1), (-1,1), 'CENTER'),
        ]))
        story.append(pt)
        story.append(Spacer(1, 12))

    # ── RFI Summary ──────────────────────────────────────────
    rs = data.get('rfi_summary')
    if rs:
        story.append(HRFlowable(width='100%', thickness=0.3, color=BORDER, spaceAfter=8))
        story.append(Paragraph('RFI & SUBMITTAL STATUS', s['section']))
        ss = data.get('submittal_summary', {})
        rfi_data = [
            [Paragraph('OPEN RFIs', ParagraphStyle('p', fontName='Helvetica-Bold', fontSize=8, textColor=AMBER, leading=10)),
             Paragraph('OVERDUE RFIs', ParagraphStyle('p', fontName='Helvetica-Bold', fontSize=8, textColor=RED, leading=10)),
             Paragraph('AVG RESPONSE', ParagraphStyle('p', fontName='Helvetica-Bold', fontSize=8, textColor=TEXT, leading=10)),
             Paragraph('PENDING SUBS', ParagraphStyle('p', fontName='Helvetica-Bold', fontSize=8, textColor=AMBER, leading=10))],
            [Paragraph(str(rs['open']), ParagraphStyle('v', fontName='Helvetica-Bold', fontSize=16, textColor=AMBER, leading=18, alignment=TA_CENTER)),
             Paragraph(str(rs['overdue']), ParagraphStyle('v', fontName='Helvetica-Bold', fontSize=16, textColor=RED, leading=18, alignment=TA_CENTER)),
             Paragraph(f"{rs['avg_response_days']}d", ParagraphStyle('v', fontName='Helvetica-Bold', fontSize=16, textColor=TEXT, leading=18, alignment=TA_CENTER)),
             Paragraph(str(ss.get('pending', 0)), ParagraphStyle('v', fontName='Helvetica-Bold', fontSize=16, textColor=AMBER, leading=18, alignment=TA_CENTER))],
        ]
        rt2 = Table(rfi_data, colWidths=[1.65*inch]*4)
        rt2.setStyle(TableStyle([
            ('BACKGROUND', (0,0), (-1,-1), SURFACE),
            ('BOX', (0,0), (-1,-1), 0.5, BORDER),
            ('INNERGRID', (0,0), (-1,-1), 0.3, BORDER),
            ('TOPPADDING', (0,0), (-1,-1), 8),
            ('BOTTOMPADDING', (0,0), (-1,-1), 8),
            ('LEFTPADDING', (0,0), (-1,-1), 8),
            ('ALIGN', (0,1), (-1,1), 'CENTER'),
        ]))
        story.append(rt2)

    # ── Footer ───────────────────────────────────────────────
    story.append(Spacer(1, 16))
    story.append(HRFlowable(width='100%', thickness=0.3, color=BORDER, spaceAfter=6))
    story.append(Paragraph(
        f'PRG Group · Operations Intelligence · {today} · CONFIDENTIAL',
        ParagraphStyle('footer', fontName='Helvetica', fontSize=6.5,
                       textColor=MUTED, alignment=TA_CENTER, leading=9)
    ))

    def dark_bg(canvas, doc):
        canvas.saveState()
        canvas.setFillColor(BG)
        canvas.rect(0, 0, letter[0], letter[1], fill=1, stroke=0)
        canvas.restoreState()

    doc.build(story, onFirstPage=dark_bg, onLaterPages=dark_bg)
    buffer.seek(0)
    return buffer.read()
