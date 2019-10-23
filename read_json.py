import sqlite3
import json
import os

DATA_DIR = "data"

def main():
    conn = sqlite3.connect("danbooru2018.db")
    c = conn.cursor()

    # note: keys do not exactly match columns in db
    images_keys = ("id",
                   "created_at",
                   "uploader_id",
                   "score",
                   "source",
                   "md5",
                   "last_commented_at",
                   "rating",
                   "image_width",
                   "image_height",
                   "is_note_locked",
                   "file_ext",
                   "last_noted_at",
                   "is_rating_locked",
                   "parent_id",
                   "has_children",
                   "approver_id",
                   "file_size",
                   "is_status_locked",
                   "up_score",
                   "down_score",
                   "is_pending",
                   "is_flagged",
                   "is_deleted",
                   "updated_at",
                   "is_banned",
                   "pixiv_id")

    # Create tables
    # In new database, id -> image_id
    c.execute('''CREATE TABLE images
                 (image_id INT PRIMARY KEY,
                  created_at TEXT,
                  uploaded_id INT,
                  score INT,
                  source TEXT,
                  md5 TEXT,
                  last_commented_at TEXT,
                  rating TEXT,
                  image_width INT,
                  image_height INT,
                  is_note_locked INT,
                  file_ext TEXT,
                  last_noted_at TEXT,
                  is_rating_locked INT,
                  parent_id INT,
                  has_children INT,
                  approver_id INT,
                  file_size INT,
                  is_status_locked INT,
                  up_score INT,
                  down_score INT,
                  is_pending INT,
                  is_flagged INT,
                  is_deleted INT,
                  updated_at TEXT,
                  is_banned INT,
                  pixiv_id INT)''')

    # id -> tag_id
    c.execute('''CREATE TABLE tags
                 (image_id INT,
                  tag_id INT,
                  name TEXT,
                  category INT,
                  PRIMARY KEY (image_id, tag_id))''')

    # list of values -> pool
    c.execute('''CREATE TABLE pools
                 (image_id INT,
                  pool TEXT,
                  PRIMARY KEY (image_id, pool))''')

    # list of values -> faver_id
    c.execute('''CREATE TABLE favs
                 (image_id INT,
                  faver_id TEXT,
                  PRIMARY KEY (image_id, faver_id))''')

    for json_file in os.listdir(DATA_DIR):
        json_path = os.path.join(DATA_DIR, json_file)
        print("Processing", json_path)

        with open(json_path) as f:
            raw_json_lines = f.readlines()


        for raw_json_line in raw_json_lines:
            json_line = json.loads(raw_json_line)
            image_id = json_line["id"]

            # table INSERTs
            images_values = list(json_line[key] for key in images_keys)
            # a little messy
            c.execute("INSERT INTO images VALUES (" + ','.join('?'*27) + ")",
                      images_values)

            tags_values = ((image_id, tag["id"], tag["name"], tag["category"])
                           for tag in json_line["tags"])
            # duplicate tags: ex. image_id 931532, tag_id 535673
            c.executemany("INSERT OR IGNORE INTO tags VALUES (?,?,?,?)",
                          tags_values)

            pools_values = ((image_id, pool) for pool in json_line["pools"])
            c.executemany("INSERT OR IGNORE INTO pools VALUES (?,?)",
                          pools_values)

            favs_values = \
                ((image_id, faver_id) for faver_id in json_line["favs"])
            c.executemany("INSERT OR IGNORE INTO favs VALUES (?,?)",
                          favs_values)

        conn.commit()

    conn.close()


if __name__ == "__main__":
    main()