import sqlite3
from kindra import constants 

import os 
import polars as pl
from polars import col
import json
from pathlib import Path



def query_db(query: str) -> pl.DataFrame:
    conn = sqlite3.connect(f"{os.getenv('KINDLE_VOCAB_PATH')}/vocab.db")
    return pl.read_database(query=query, connection=conn)


def dump_data(book: str):
    """dump the vocab.db data, create a parquet per book

    """

    # query vocab.db file
    info = query_db("select * from BOOK_INFO")
    words = query_db("select * from WORDS")
    lookup = query_db("select * from LOOKUPS")

    vocab = (info
    .join(lookup, left_on=["id"], right_on=["book_key"])
    .drop(["id_right", "asin", "guid", "dict_key", "pos", "authors", "id"])
    .join(words, left_on=["word_key"], right_on=["id"])
    .drop(["category", "lang_right", "timestamp_right", "profileid"])
    .unique(["word", "title"])  # could be same word with different context... but costs more money. 
    .with_columns(pl.struct(["word", "usage"]).map_elements(lambda x: utils.add_boldface(x["word"], x["usage"])).alias("usage"))
    )

    titles = vocab["title"].unique()
    all_titles = []

    for tit in titles:

        book_vocab = (vocab
            .filter(col("title") == tit)
            .sort("timestamp")
        )

        valid_title = utils.get_valid_filename(tit)  # remove spaces, weird punctuations, etc.. 
        
        Path(DUMPED_BOOK_FOLDER).mkdir(parents=True, exist_ok=True)
        book_vocab.write_parquet(file=DUMPED_BOOK_FILE_NAME.format(book_name=valid_title))

        all_titles.append(valid_title)

    # save the titles that are the names of the parquet files. Use later to read them back in
    with open(f"{META_DATA_FOLDER}/{TITLES_FILE_NAME}.json", 'w') as f:
        json.dump(all_titles, f)


if __name__ == "__main__":
    dump_data()