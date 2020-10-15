from flask import Flask, request, g
import sqlite3

app = Flask(__name__)


def get_db():
    if 'db' not in g:
        g.db = sqlite3.connect('./airbnb.db')

    return g.db


def close_db(e=None):
    db = g.pop('db', None)
    if db is not None:
        db.close()


@app.route('/mystudentID', methods=['GET'])
def get_student_id():
    return {
        # TODO 填入学生 ID
        'studentID': ''
    }


@app.route('/airbnb/reviews', methods=['GET'])
def get_all_reviews():
    result = dict()
    cursor = get_db().cursor()
    sql = """
    SELECT accommodation_id, comment, datetime, review.rid, reviewer.rname
    FROM review JOIN reviewer ON reviewer.rid = review.rid
    """

    if len(request.args) != 0:
        if 'start' in request.args:
            sql += " WHERE date(datetime) >= '%s'" % request.args['start']
        if 'end' in request.args:
            if 'start' in request.args:
                sql += ' AND'
            else:
                sql += ' WHERE'
            sql += " date(datetime) <= '%s'" % request.args['end']

    sql += ' ORDER BY date(datetime)'
    cursor.execute(sql)
    reviews_list = cursor.fetchall()
    cursor.close()
    reviews = []

    for review in reviews_list:
        review_dict = {'Accommodation ID': review[0], 'Comment': review[1],
                       'DateTime': review[2], 'Reviewer ID': review[3], 'Reviewer Name': review[4]}
        reviews.append(review_dict)

    result['Count'] = len(reviews)
    result['Reviews'] = reviews

    return result


@app.route('/airbnb/reviewers', methods=['GET'])
def get_all_reviewers():
    result = dict()
    cursor = get_db().cursor()
    sql = """
    SELECT count(*) AS Rcount, review.rid, rname
    FROM review JOIN reviewer on reviewer.rid = review.rid
    GROUP BY review.rid, rname
    ORDER BY Rcount
    """

    if len(request.args) != 0:
        if request.args['sort_by_review_count'] == 'descending':
            sql += ' DESC'

    cursor.execute(sql)
    reviewers_list = cursor.fetchall()
    cursor.close()

    reviewers = []
    for reviewer in reviewers_list:
        reviewer_dict = {'Review Count': reviewer[0], 'Reviewer ID': reviewer[1],
                         'Reviewer Name': reviewer[2]}
        reviewers.append(reviewer_dict)

    result['Count'] = len(reviewers)
    result['Reviewers'] = reviewers

    return result


@app.route('/airbnb/reviewers/<reviewer_id>')
def get_reviewer_and_reviews(reviewer_id):
    result = dict()
    cursor = get_db().cursor()
    reviewer_sql = """
    SELECT rid, rname
    FROM reviewer
    WHERE rid=%s
    """ % reviewer_id

    review_sql = """
    SELECT accommodation_id, comment, datetime
    FROM review
    WHERE rid=%s;
    """ % reviewer_id

    cursor.execute(reviewer_sql)
    reviewer = cursor.fetchone()

    if reviewer is None:
        result['Reasons'] = [{'Message': 'Reviewer not found'}]
        cursor.close()
        return result

    cursor.execute(review_sql)
    reviews_list = cursor.fetchall()
    cursor.close()

    reviews = []
    for review in reviews_list:
        review_dict = {'Accommodation ID': review[0], 'Comment': review[1],
                       'DateTime': review[2]}
        reviews.append(review_dict)

    result['Reviewer ID'] = reviewer[0]
    result['Review Name'] = reviewer[1]
    result['Reviews'] = reviews

    return result


@app.route('/airbnb/hosts', methods=['GET'])
def get_all_host():
    result = dict()
    cursor = get_db().cursor()
    sql = """
    SELECT count(*) AS Acount, host_about, host.host_id, host_location, host_name, host_url
    FROM host_accommodation JOIN host on host_accommodation.host_id = host.host_id
    GROUP BY host_accommodation.host_id
    ORDER BY Acount
    """

    if len(request.args) != 0:
        if request.args['sort_by_accommodation_count'] == 'descending':
            sql += ' DESC'

    cursor.execute(sql)
    hosts_list = cursor.fetchall()
    cursor.close()

    hosts = []
    for host in hosts_list:
        host_dict = {'Accommodation Count': host[0], 'Host About': host[1],
                     'Host ID': host[2], 'Host Location': host[3],
                     'Host Name': host[4], 'Host URL': host[5]}
        hosts.append(host_dict)

    result['Count'] = len(hosts)
    result['Hosts'] = hosts

    return result


@app.route('/airbnb/hosts/<host_id>')
def get_a_host_by_id(host_id):
    result = dict()
    cursor = get_db().cursor()
    host_sql = """
    SELECT host_about, host_id, host_location, host_name, host_url
    FROM host
    WHERE host_id=%s;
    """ % host_id

    accommodation_sql = """
    SELECT accommodation_id, name
    FROM host_accommodation JOIN accommodation on accommodation.id = host_accommodation.accommodation_id
    WHERE host_id=%s;
    """ % host_id

    cursor.execute(host_sql)
    host = cursor.fetchone()

    if host is None:
        result['Reasons'] = [{'Message': 'Host not found'}]
        cursor.close()
        return result

    cursor.execute(accommodation_sql)
    accommodations_list = cursor.fetchall()
    cursor.close()

    accommodations = []
    for accommodation in accommodations_list:
        accommodation_dict = {'Accommodation ID': accommodation[0], 'Accommodation Name': accommodation[1]}
        accommodations.append(accommodation_dict)

    result['Accommodation'] = accommodations
    result['Accommodation Count'] = len(accommodations)
    result['Host About'] = host[0]
    result['Host ID'] = host[1]
    result['Host Location'] = host[2]
    result['Host Name'] = host[3]
    result['Host URL'] = host[4]

    return result


@app.route('/airbnb/accommodations', methods=['GET'])
def get_all_accommodation():
    result = dict()
    cursor = get_db().cursor()

    acc_sql = """
    SELECT name, summary, url, accommodation.id, review_score_value, 
    count(*), host_about, host.host_id, host_location, host_name
    FROM review JOIN accommodation ON accommodation.id = review.accommodation_id 
            JOIN host_accommodation ON accommodation.id = host_accommodation.accommodation_id 
            JOIN host ON host.host_id = host_accommodation.host_id
    %s
    GROUP BY review.accommodation_id
    """

    where_sql = ''

    amenities_sql = """
    SELECT type
    FROM amenities
    WHERE accommodation_id=%s
    """

    if len(request.args) != 0:
        if 'min_review_score_value' in request.args:
            where_sql += ' WHERE review_score_value>=%s' % str(request.args['min_review_score_value'])
        if 'amenities' in request.args:
            if 'min_review_score_value' in request.args:
                where_sql += ' AND '
            else:
                where_sql += ' WHERE '
            where_sql += '''
            host_accommodation.accommodation_id IN (
            SELECT DISTINCT accommodation_id
            FROM amenities
            WHERE type='%s'
            )
            ''' % request.args['amenities']

    cursor.execute(acc_sql % where_sql)
    accommodations_list = cursor.fetchall()

    accommodations = []

    for accommodation in accommodations_list:
        cursor.execute(amenities_sql % accommodation[3])
        amenities_list = cursor.fetchall()
        amenities = []

        for amenity in amenities_list:
            amenities.append(amenity[0])

        accommodation_dict = {
            'Accommodation': {
                'Name': accommodation[0], 'Summary': accommodation[1],
                'URL': accommodation[2]
            },
            'Amenities': amenities,
            'Host': {
                'About': accommodation[6],
                'ID': accommodation[7],
                'Location': accommodation[8],
                'Name': accommodation[9]
            },
            'ID': accommodation[3],
            'Review Count': accommodation[5],
            'Review Score Value': accommodation[4]
        }

        accommodations.append(accommodation_dict)

    cursor.close()

    result['Accommodations'] = accommodations
    result['Count'] = len(accommodations)
    return result


@app.route('/airbnb/accommodations/<accommodation_id>')
def get_accommodation_by_id(accommodation_id):
    result = dict()
    cursor = get_db().cursor()

    accommodation_sql = """
    SELECT id, name, review_score_value, summary, url
    FROM accommodation
    WHERE id=%s;
    """ % accommodation_id

    amenities_sql = """
    SELECT type
    FROM amenities
    WHERE accommodation_id=%s;
    """ % accommodation_id

    review_sql = """
    SELECT comment, datetime, rname, review.rid
    FROM review JOIN reviewer r on r.rid = review.rid
    WHERE accommodation_id=%s;
    """ % accommodation_id

    cursor.execute(accommodation_sql)
    accommodation = cursor.fetchone()

    if accommodation is None:
        result['Reasons'] = [{'Message': 'Reviewer not found'}]
        cursor.close()
        return result

    cursor.execute(amenities_sql)
    amenities_list = cursor.fetchall()

    cursor.execute(review_sql)
    reviews_list = cursor.fetchall()
    cursor.close()

    amenities = []
    for amenity in amenities_list:
        amenities.append(amenity[0])

    reviews = []
    for review in reviews_list:
        review_dict = {
            'Comment': review[0],
            'DateTime': review[1],
            'Review Name': review[2],
            'Reviewer ID': review[3]
        }
        reviews.append(review_dict)

    result['Accommodation ID'] = accommodation[0]
    result['Accommodation Name'] = accommodation[1]
    result['Amenities'] = amenities
    result['Review Score Value'] = accommodation[2]
    result['Reviews'] = reviews
    result['Summary'] = accommodation[3]
    result['URL'] = accommodation[4]

    return result


if __name__ == '__main__':
    app.run()
