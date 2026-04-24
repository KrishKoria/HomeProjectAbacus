from streamlit_app.lib.ui import inject_base_styles, render_metric_card


def test_inject_base_styles_includes_custom_metric_card_styles(monkeypatch) -> None:
    captured: list[tuple[str, bool]] = []

    def fake_markdown(body: str, unsafe_allow_html: bool = False) -> None:
        captured.append((body, unsafe_allow_html))

    monkeypatch.setattr("streamlit_app.lib.ui.st.markdown", fake_markdown)

    inject_base_styles()

    assert captured
    css, unsafe = captured[0]
    assert unsafe is True
    assert ".atlas-metric-card" in css
    assert ".atlas-metric-label" in css
    assert ".atlas-metric-value" in css


def test_render_metric_card_outputs_label_value_and_delta(monkeypatch) -> None:
    captured: list[tuple[str, bool]] = []

    def fake_markdown(body: str, unsafe_allow_html: bool = False) -> None:
        captured.append((body, unsafe_allow_html))

    monkeypatch.setattr("streamlit_app.lib.ui.st.markdown", fake_markdown)

    render_metric_card("Warnings", "12", "Up from last run")

    assert captured
    markup, unsafe = captured[0]
    assert unsafe is True
    assert "atlas-metric-card" in markup
    assert "Warnings" in markup
    assert "12" in markup
    assert "Up from last run" in markup
