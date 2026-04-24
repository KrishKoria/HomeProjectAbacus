from __future__ import annotations

import streamlit as st


def inject_base_styles() -> None:
    st.markdown(
        """
        <style>
        @import url('https://fonts.googleapis.com/css2?family=Fraunces:opsz,wght@9..144,600;9..144,700&family=Source+Sans+3:wght@400;600;700&display=swap');

        :root {
          --paper: #f6f1e7;
          --paper-deep: #ece2d0;
          --ink: #18333A;
          --muted: #5d6f74;
          --teal: #0F5C6E;
          --brass: #C9863B;
          --coral: #A34A37;
          --line: rgba(24, 51, 58, 0.12);
          --panel: rgba(255, 250, 242, 0.82);
        }

        .stApp {
          background:
            radial-gradient(circle at top left, rgba(15, 92, 110, 0.18), transparent 28%),
            radial-gradient(circle at bottom right, rgba(201, 134, 59, 0.18), transparent 24%),
            linear-gradient(180deg, var(--paper) 0%, var(--paper-deep) 100%);
          color: var(--ink);
          font-family: 'Source Sans 3', sans-serif;
        }

        [data-testid="stSidebar"] {
          background:
            linear-gradient(180deg, rgba(17, 44, 51, 0.94), rgba(15, 92, 110, 0.88));
          color: #f8efe2;
          border-right: 1px solid rgba(255,255,255,0.08);
        }

        [data-testid="stSidebar"] * {
          color: #f8efe2 !important;
        }

        .block-container {
          padding-top: 2rem;
          padding-bottom: 3rem;
        }

        h1, h2, h3 {
          font-family: 'Fraunces', serif !important;
          color: var(--ink);
          letter-spacing: -0.02em;
        }

        .atlas-hero {
          background: linear-gradient(135deg, rgba(255, 249, 238, 0.88), rgba(255, 244, 225, 0.78));
          border: 1px solid rgba(24, 51, 58, 0.10);
          border-radius: 24px;
          padding: 1.4rem 1.6rem 1.3rem;
          box-shadow: 0 24px 60px rgba(30, 38, 42, 0.08);
          margin-bottom: 1.4rem;
        }

        .atlas-eyebrow {
          text-transform: uppercase;
          letter-spacing: 0.18em;
          font-size: 0.78rem;
          color: var(--teal);
          font-weight: 700;
          margin-bottom: 0.4rem;
        }

        .atlas-subtitle {
          color: var(--muted);
          font-size: 1.03rem;
          margin-top: 0.35rem;
          max-width: 60rem;
        }

        .atlas-note {
          border-left: 4px solid var(--brass);
          background: rgba(255, 250, 242, 0.78);
          padding: 0.9rem 1rem;
          border-radius: 0 16px 16px 0;
          margin-bottom: 1rem;
          color: var(--ink);
        }

        .atlas-metric-card {
          background: var(--panel);
          border: 1px solid var(--line);
          border-radius: 20px;
          padding: 0.95rem 1.05rem 0.9rem;
          box-shadow: 0 18px 40px rgba(24, 51, 58, 0.06);
          min-height: 8.5rem;
          display: flex;
          flex-direction: column;
          justify-content: space-between;
        }

        .atlas-metric-label {
          color: var(--muted);
          text-transform: uppercase;
          letter-spacing: 0.08em;
          font-weight: 700;
          font-size: 0.82rem;
          line-height: 1.2;
          margin-bottom: 0.75rem;
        }

        .atlas-metric-value {
          color: var(--ink);
          font-family: 'Fraunces', serif;
          font-size: clamp(2.1rem, 4vw, 2.8rem);
          line-height: 1;
          letter-spacing: -0.03em;
        }

        .atlas-metric-delta {
          color: var(--teal);
          font-size: 0.9rem;
          font-weight: 700;
          margin-top: 0.65rem;
        }

        .atlas-status {
          display: inline-flex;
          align-items: center;
          gap: 0.45rem;
          padding: 0.45rem 0.7rem;
          border-radius: 999px;
          font-size: 0.9rem;
          font-weight: 700;
          border: 1px solid transparent;
          margin-bottom: 0.6rem;
        }

        .atlas-status.good {
          background: rgba(15, 92, 110, 0.12);
          color: var(--teal);
          border-color: rgba(15, 92, 110, 0.18);
        }

        .atlas-status.warn {
          background: rgba(201, 134, 59, 0.14);
          color: #8b5e22;
          border-color: rgba(201, 134, 59, 0.2);
        }

        .atlas-status.bad {
          background: rgba(163, 74, 55, 0.12);
          color: var(--coral);
          border-color: rgba(163, 74, 55, 0.22);
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def render_page_header(title: str, subtitle: str, eyebrow: str = "Claims Atlas") -> None:
    st.markdown(
        f"""
        <section class="atlas-hero">
          <div class="atlas-eyebrow">{eyebrow}</div>
          <h1>{title}</h1>
          <p class="atlas-subtitle">{subtitle}</p>
        </section>
        """,
        unsafe_allow_html=True,
    )


def render_status_banner(label: str, tone: str = "good") -> None:
    st.markdown(
        f'<div class="atlas-status {tone}">{label}</div>',
        unsafe_allow_html=True,
    )


def render_metric_card(label: str, value: str, delta: str | None = None) -> None:
    delta_markup = f'<div class="atlas-metric-delta">{delta}</div>' if delta else ""
    st.markdown(
        f"""
        <section class="atlas-metric-card">
          <div class="atlas-metric-label">{label}</div>
          <div>
            <div class="atlas-metric-value">{value}</div>
            {delta_markup}
          </div>
        </section>
        """,
        unsafe_allow_html=True,
    )


def render_missing_artifact(message: str) -> None:
    st.markdown(f'<div class="atlas-note">{message}</div>', unsafe_allow_html=True)


__all__ = [
    "inject_base_styles",
    "render_metric_card",
    "render_missing_artifact",
    "render_page_header",
    "render_status_banner",
]
