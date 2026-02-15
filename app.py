# app.py
from __future__ import annotations

from datetime import datetime

import streamlit as st

from src.topn import ALLOWED_DISTANCES, run_topn_query, to_csv_bytes

st.set_page_config(page_title="Speedskating TopN", layout="wide")

st.title("SpeedSkatingResults – TopN aikaväliltä (CSV)")

with st.sidebar:
    st.header("Hakuasetukset")

    ageclass = st.text_input("Ikäluokka (esim. FA2, MB2)", value="FA2").strip()

    distances = st.multiselect(
        "Matkat (m)",
        options=ALLOWED_DISTANCES,
        default=[500],
    )

    # Jos useampi matka -> TopN max 15
    multi = len(distances) > 1

    start_season = st.number_input("Alkukausi (season start year)", min_value=1800, max_value=2100, value=2007, step=1)
    current_year = datetime.now().year
    end_season = st.number_input("Loppukausi", min_value=1800, max_value=2100, value=int(current_year), step=1)

    if multi:
        top_n = st.number_input("TopN per matka (max 15)", min_value=1, max_value=15, value=15, step=1)
    else:
        top_n = st.number_input("TopN", min_value=1, max_value=200, value=30, step=1)

    per_season_top = st.number_input("Hae per kausi vähintään (buffer)", min_value=1, max_value=300, value=5, step=1)

    country = st.text_input("Maa (FIN / world / numero)", value="FIN").strip()

    st.caption("Vinkki: jos kirjoitat country=world, haetaan ilman country-rajausta.")

    run_btn = st.button("Hae TopN", type="primary")


@st.cache_data(show_spinner=False, ttl=60 * 30)
def cached_run(ageclass: str, distances: tuple[int, ...], start_season: int, end_season: int, top_n: int, per_season_top: int, country: str):
    return run_topn_query(
        ageclass=ageclass,
        distances=list(distances),
        start_season=int(start_season),
        end_season=int(end_season),
        top_n=int(top_n),
        per_season_top=int(per_season_top),
        country=country,
    )


if run_btn:
    if not ageclass:
        st.error("Ikäluokka puuttuu.")
        st.stop()
    if not distances:
        st.error("Valitse vähintään yksi matka.")
        st.stop()

    # enforce multi-distance TopN cap
    if len(distances) > 1 and top_n > 15:
        st.warning("Useamman matkan haussa TopN rajataan 15:een.")
        top_n = 15

    with st.spinner("Haetaan tuloksia SpeedSkatingResults API:sta..."):
        try:
            rows, summary = cached_run(
                ageclass=ageclass,
                distances=tuple(sorted(distances)),
                start_season=int(start_season),
                end_season=int(end_season),
                top_n=int(top_n),
                per_season_top=int(per_season_top),
                country=country or "FIN",
            )
        except Exception as e:
            st.exception(e)
            st.stop()

    # Summary
    st.subheader("Yhteenveto")
    cols = st.columns(min(4, len(distances)) if distances else 1)
    for i, d in enumerate(sorted(distances)):
        s = summary.get(d, {})
        with cols[i % len(cols)]:
            st.metric(label=f"{d}m – uniikit", value=int(s.get("unique_skaters", 0)))
            st.caption(f"Raakarivejä: {s.get('raw_rows', 0)} • Kirjoitetaan: {s.get('written', 0)}")
            errs = s.get("errors", [])
            if errs:
                with st.expander(f"Virheitä {d}m ({len(errs)})"):
                    for er in errs[:10]:
                        st.write(er)

    st.subheader("Tulokset")
    if not rows:
        st.warning("Ei yhtään tulosta annetuilla ehdoilla.")
    else:
        st.dataframe(rows, use_container_width=True, hide_index=True)

        # CSV download
        csv_bytes = to_csv_bytes(rows)
        dist_part = "-".join(str(d) for d in sorted(distances))
        fname = f"top_{ageclass}_{dist_part}_{int(min(start_season, end_season))}-{int(max(start_season, end_season))}_{(country or 'FIN')}.csv".replace(" ", "_")

        st.download_button(
            label="Lataa CSV",
            data=csv_bytes,
            file_name=fname,
            mime="text/csv",
        )

else:
    st.info("Valitse asetukset sivupalkista ja paina **Hae TopN**.")
