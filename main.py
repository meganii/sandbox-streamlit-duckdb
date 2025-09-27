# main.py
import streamlit as st
import duckdb
import urllib.parse


DATA_URL = "https://github.com/meganii/sandbox-github-actions-scheduler/releases/latest/download/pages.parquet"

@st.cache_data(show_spinner=False)
def run_query(data_url: str, search_word: str):
    # シングルクオートをエスケープして安全に埋め込む
    url_escaped = data_url.replace("'", "''")

    sql = f"""
    -- 一時テーブルに展開
    CREATE TEMP TABLE expanded_lines AS
    SELECT 
        t.id              AS page_id,
        t.title,
        t.created,
        u.ord             AS line_no,
        l.created         AS line_created,
        l.id              AS line_id,
        l.text,
        l.updated         AS line_updated,
        l.userId          AS line_userId
    FROM read_parquet('{url_escaped}') t
    CROSS JOIN UNNEST(t.lines) WITH ORDINALITY AS u(l, ord);

    WITH
    line_blocks AS (
        SELECT
        *,
        SUM(CASE WHEN "text" = '' THEN 1 ELSE 0 END) OVER (
            PARTITION BY
            page_id
            ORDER BY
            line_no
        ) AS block_id
        FROM
        expanded_lines
    ),
    target_blocks AS (
        SELECT DISTINCT
            page_id,
            block_id
        FROM
            line_blocks
        WHERE
            "text" LIKE '%' || ? || '%'
    ),
    -- 元のgrouped_text_blockを、よりシンプルな形に変更
    block_contents AS (
        SELECT
            lb.page_id,
            lb.title,
            lb.block_id, -- 集約のキーとして利用
            lb.line_no,
            lb.line_id,
            lb."text",
            lb.line_updated AS updated
        FROM
            line_blocks AS lb
            INNER JOIN target_blocks AS tb ON lb.page_id = tb.page_id
            AND lb.block_id = tb.block_id
        WHERE
            lb."text" != ''
    )

    -- 最終的なSELECT文で、ブロックごとに集約
    SELECT
        page_id,
        title,
        -- 各ブロックの先頭行のline_idを取得
        arg_min(line_id, line_no) AS first_line_id,
        -- ブロック内のテキストを改行で連結
        string_agg("text", '\n' ORDER BY line_no) AS block_text,
        max(updated) AS updated
    FROM
        block_contents
    GROUP BY
        page_id,
        title,
        block_id
    ORDER BY
        max(updated) DESC,
        page_id
    """

    con = duckdb.connect()
    try:
        # 検索ワードは最後のステートメント内でしかパラメータ化しないので安全
        df = con.execute(sql, [search_word]).fetchdf()
        return df
    finally:
        con.close()


def main():
    st.set_page_config(page_title="井戸端クライン検索 (DuckDB + Streamlit)")
    st.title("井戸端クライン検索")
    st.write("ctrl + iでアイコンを付けて言及したクライン（空行で区切られたテキストの塊）を検索する。")

    search_word = st.text_input(
        "検索ワード（部分一致）", value="[meganii.icon]", key="search_word")
    run_button = st.button("検索実行", key="run_button")

    # 検索結果と検索語をセッションに保持（key競合を避けるためlast_search_wordを使用）
    if 'search_df' not in st.session_state:
        st.session_state['search_df'] = None
    if 'last_search_word' not in st.session_state:
        st.session_state['last_search_word'] = search_word

    if run_button:
        with st.spinner("DuckDB を実行中..."):
            try:
                df = run_query(DATA_URL, search_word)
                st.session_state['search_df'] = df
                st.session_state['last_search_word'] = search_word
            except Exception as e:
                st.error(f"エラーが発生しました: {e}")
                st.markdown(
                    "**補足**: DuckDB が HTTP 経由で Parquet を読み込めない環境の場合は、ファイルをローカルにダウンロードして `DATA_URL` をローカルパスに置き換えてください。")

    df = st.session_state.get('search_df', None)
    if df is not None:
        st.success(f"検索完了（{len(df)} 行が見つかりました）")
        if len(df) == 0:
            st.info("該当するテキストは見つかりませんでした。検索ワードを変えて再試行してください。")
        else:
            filter_word = st.text_input(
                "テキスト内絞り込み（空欄で全件表示）", value="", key="filter_word")
            filtered_rows = []
            for _, row in df.iterrows():
                text_lines = row['block_text'].split('\n')
                if filter_word:
                    # 1行でもキーワードが含まれていればカードごと表示
                    if any(filter_word in line for line in text_lines):
                        filtered_rows.append((row['title'], row['first_line_id'], text_lines))
                else:
                    filtered_rows.append((row['title'], row['first_line_id'], text_lines))
            if len(filtered_rows) == 0:
                st.info("絞り込み条件に一致するテキストはありませんでした。キーワードを変えて再試行してください。")
            for title, first_line_id, all_lines in filtered_rows:
                with st.container():
                    st.markdown(
                        f"[{title}](https://scrapbox.io/villagepump/{urllib.parse.quote(title)}#{first_line_id})", unsafe_allow_html=True)
                    full_text = '\n'.join(all_lines)
                    if len(all_lines) > 20:
                        with st.expander(f"{len(all_lines)} 行（クリックで展開）", expanded=False):
                            st.markdown(
                                f"<div style='border:1px solid #ddd; border-radius:8px; padding:12px; margin-bottom:16px; background:#fafafa; white-space:pre-wrap;'>{full_text}</div>", unsafe_allow_html=True)
                    else:
                        st.markdown(
                            f"<div style='border:1px solid #ddd; border-radius:8px; padding:12px; margin-bottom:16px; background:#fafafa; white-space:pre-wrap;'>{full_text}</div>", unsafe_allow_html=True)

    st.markdown("---")
    st.markdown("**使い方メモ**")
    st.markdown("- 「検索ワード」に部分一致で探したい文字列を入力して「検索実行」を押してください。")
    st.markdown("- 初期値は `[meganii.icon]` です。")
    st.markdown("- 検索結果が多い場合は、下の「テキスト内絞り込み」にさらにキーワードを入れて絞り込めます。")

if __name__ == "__main__":
    main()
