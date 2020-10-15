import sqlite3
import json


def get_json_data(path):
    json_data = ''
    with open(path, encoding='utf-8') as f:
        json_data = f.read()

    data = json.loads(json_data)
    return data


def create_table(sql_con):
    cursor = sql_con.cursor()
    create_accommodation = """
    create table accommodation
    (
        id                 INTEGER
            primary key,
        name               text,
        summary            text,
        url                text,
        review_score_value INTEGER
    );
    """

    create_amenities = """
    create table amenities
    (
        accommodation_id INTEGER
            references accommodation(id),
        type             text,
        primary key (accommodation_id, type)
    );
    """

    create_host = """
    create table host
    (
        host_id       INTEGER
            primary key,
        host_url      TEXT,
        host_name     TEXT,
        host_about    TEXT,
        host_location TEXT
    );
    """

    create_host_accommodation = """
    create table host_accommodation
    (
        host_id          INTEGER
            references host(host_id),
        accommodation_id INTEGER
            references accommodation(id), 
        primary key (host_id, accommodation_id)
    );
    """

    create_review = """
    create table review
    (
        id               INTEGER
            primary key autoincrement,
        rid              INTEGER
            references reviewer(rid),
        comment          TEXT,
        datetime         TEXT,
        accommodation_id INTEGER
            references accommodation(id)
    );
    """

    create_reviewer = """
    create table reviewer
    (
        rid   INTEGER
            primary key,
        rname text
    );
    """

    cursor.execute(create_accommodation)
    cursor.execute(create_amenities)
    cursor.execute(create_host)
    cursor.execute(create_host_accommodation)
    cursor.execute(create_review)
    cursor.execute(create_reviewer)

    sql_con.commit()


def insert_accommodation(cursor, accommodation_id, name, summary, url, review_score_data):
    if 'review_scores_value' in review_score_data:
        cursor.execute('INSERT INTO accommodation VALUES (?, ?, ?, ?, ?);',
                       (accommodation_id, name, summary, url, review_score_data['review_scores_value']))
    else:
        cursor.execute('INSERT INTO accommodation VALUES (?, ?, ?, ?, NULL);',
                       (accommodation_id, name, summary, url))


def insert_amenities(cursor, accommodation_id, amenities_data):
    # 去重
    amenities_dict = {}
    for i in amenities_data:
        amenities_dict[i] = 0
    amenities = []

    for key in amenities_dict:
        amenities.append((accommodation_id, key))
    cursor.executemany('INSERT INTO amenities VALUES (?, ?)', amenities)


def insert_host(cursor, host_data):
    cursor.execute('SELECT * FROM host WHERE host.host_id = ?', (host_data['host_id'], ))
    output = cursor.fetchall()

    if len(output):
        return
    else:
        cursor.execute('INSERT INTO host VALUES (?, ?, ?, ?, ?)',
                       (host_data['host_id'], host_data['host_url'], host_data['host_name'],
                        host_data['host_about'], host_data['host_location']))


def insert_host_accommodation(cursor, host_id, accommodation_id):
    cursor.execute('INSERT INTO host_accommodation VALUES (?, ?)', (host_id, accommodation_id))


def insert_review(cursor, accommodation_id, review_data):
    reviews = []
    reviewers = []
    for review in review_data:
        reviews.append((review['reviewer_id'], review['comments'], review['date']['$date'], accommodation_id))
        reviewers.append((review['reviewer_id'], review['reviewer_name']))

    cursor.executemany('INSERT INTO review (rid, comment, datetime, accommodation_id) VALUES (?, ?, ?, ?)', reviews)
    insert_reviewer(cursor, reviewers)


def insert_reviewer(cursor, reviewers):
    reviewers_dict = {}
    for reviewer in reviewers:
        reviewers_dict[reviewer[0]] = reviewer[1]
    reviewers.clear()
    for key in reviewers_dict:
        reviewers.append((key, reviewers_dict[key]))

    cursor.executemany('INSERT INTO reviewer VALUES (?, ?)', reviewers)


def update_database(sql_con, single_data):
    cursor = sql_con.cursor()

    # 插入 accommodation
    insert_accommodation(cursor, single_data['_id'], single_data['name'], single_data['summary'], single_data['listing_url'], single_data['review_scores'])

    # 插入 amenities
    insert_amenities(cursor, single_data['_id'], single_data['amenities'])

    # 插入 host
    insert_host(cursor, single_data['host'])

    # 插入 host_accommodation
    insert_host_accommodation(cursor, single_data['host']['host_id'], single_data['_id'])

    # 插入 review 和 reviewer
    insert_review(cursor, single_data['_id'], single_data['reviews'])

    sql_con.commit()


def start():
    data = get_json_data('./airbnb.json')
    conn = sqlite3.connect('./airbnb.db')
    create_table(conn)

    for single_data in data:
        update_database(conn, single_data)

    conn.close()

    print()


if __name__ == '__main__':
    start()


