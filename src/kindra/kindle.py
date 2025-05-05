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


def dump_data(book_title: str):
    """
        dump the vocab.db data, create a parquet per book

    """

    # query vocab.db file
    info = query_db("select * from BOOK_INFO"
        ).select(["id", "title"]
        ).filter(col("title") == book_title
        )
        
    words = query_db("select * from WORDS")
    
    lookup = query_db("select * from LOOKUPS"
        ).select(["book_key", "timestamp", "usage", "word_key"]
        )

    vocab = (info
    .join(lookup, left_on="id", right_on="book_key")
    .join(words, left_on=["word_key"], right_on=["id"])
    .unique(["word", "title"])  # could be same word with different context... but costs more money. 
    .with_columns(pl.struct(["word", "usage"]).map_elements(lambda x: utils.add_boldface(x["word"], x["usage"])).alias("usage"))
    ).select(
        ["title", "usage", "timestamp", "stem", "lang"]
    ).sort(
        "timestamp", descending=False
    )

    valid_title = utils.get_valid_filename(book_title)  # remove spaces, weird punctuations, etc.. 
    
    Path(DUMPED_BOOK_FOLDER).mkdir(parents=True, exist_ok=True)
    vocab.write_parquet(file=DUMPED_BOOK_FILE_NAME.format(book_name=valid_title))


    # # save the titles that are the names of the parquet files. Use later to read them back in
    # with open(f"{META_DATA_FOLDER}/{TITLES_FILE_NAME}.json", 'w') as f:
    #     json.dump(all_titles, f)


if __name__ == "__main__":
    dump_data()