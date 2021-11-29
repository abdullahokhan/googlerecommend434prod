
import concurrent.futures

import flask
from google.cloud import bigquery


app = flask.Flask(__name__)
bigquery_client = bigquery.Client()

#for top 10 products
@app.route("/")
def main():
    query_job = bigquery_client.query(
        """
            CREATE OR REPLACE TABLE bqml.top_products AS (
            SELECT  p.v2ProductName,
                    p.v2ProductCategory,
                    SUM(p.ProductQuantity) AS Quantity,
                    SUM(p.localProductRevenue/1000000) AS Revenue
            FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
            UNNEST(hits) AS h,
            UNNEST (h.product) AS p
            GROUP BY 1,2
            ORDER BY Revenue DESC LIMIT 1000
            CREATE OR REPLACE TABLE bqml.aggregate_web_stats AS (
                  WITH
                    durations AS (
                      --calculate pageview durations
                      SELECT
                        CONCAT(fullVisitorId,'-',
                             CAST(visitNumber AS STRING),'-',
                             CAST(hitNumber AS STRING) ) AS visitorId_session_hit,
                        LEAD(time, 1) OVER (
                          PARTITION BY CONCAT(fullVisitorId,'-',CAST(visitNumber AS STRING))
                          ORDER BY
                          time ASC ) - time AS pageview_duration
                      FROM
                        `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
                        UNNEST(hits) AS hit
                    ),
                    prodview_durations AS (
                      --filter for product detail pages only
                     SELECT
                        CONCAT(fullVisitorId,'-',CAST(visitNumber AS STRING)) AS userId,
                        productSKU AS itemId,
                        IFNULL(dur.pageview_duration,
                         1) AS pageview_duration,
                      FROM
                        `bigquery-public-data.google_analytics_sample.ga_sessions_2017*` t,
                        UNNEST(hits) AS hits,
                        UNNEST(hits.product) AS hits_product
                      JOIN
                        durations dur
                      ON
                        CONCAT(fullVisitorId,'-',
                               CAST(visitNumber AS STRING),'-',
                               CAST(hitNumber AS STRING)) = dur.visitorId_session_hit
                      WHERE
                      eCommerceAction.action_type = "2"
                    ),
                    aggregate_web_stats AS(
                      --sum pageview durations by userId, itemId
                      SELECT
                        userId,
                        itemId,
                        SUM(pageview_duration) AS session_duration
                      FROM
                        prodview_durations
                      GROUP BY
                        userId,
                        itemId )
                    SELECT
                     *
                   FROM
                      aggregate_web_stats
                )
            CREATE OR REPLACE MODEL bqml.recommendaidev
            OPTIONS(model_type='matrix_factorization',
            user_col='userId',
            item_col='itemId',
            rating_col='session_duration',
            feedback_type='implicit'
            )
      AS
      SELECT * FROM bqml.aggregate_web_stats;
            SELECT * FROM `vivid-poet-333222.bqml.top_products` 
            order by Quantity desc  LIMIT 10
         """
    )

    return flask.redirect(
        flask.url_for(
            "results",
            project_id=query_job.project,
            job_id=query_job.job_id,
            location=query_job.location,
        )
    )

#for top 10 trending products
    
@app.route("/results")
def results():
    project_id = flask.request.args.get("project_id")
    job_id = flask.request.args.get("job_id")
    location = flask.request.args.get("location")

    query_job = bigquery_client.get_job(
        job_id,
        project=project_id,
        location=location,
    )

    try:
        # Set a timeout because queries could take longer than one minute.
        results = query_job.result(timeout=30)
    except concurrent.futures.TimeoutError:
        return flask.render_template("timeout.html", job_id=query_job.job_id)

    return flask.render_template("query_result.html", results=results)

@app.route("/recommendedproducts")
def top5prodsallcust():
    query_job = bigquery_client.query(
        """
            CREATE OR REPLACE TABLE bqml.top5prods AS (
            WITH predictions AS (
                SELECT
                  userId,
                  ARRAY_AGG(STRUCT(itemId,
                                   predicted_session_duration_confidence)
                            ORDER BY
                              predicted_session_duration_confidence DESC
                            LIMIT 5) as recommended
                FROM ML.RECOMMEND(MODEL bqml.recommendaidev)
                GROUP BY userId
            )

            SELECT
              userId,
              itemId,
              predicted_session_duration_confidence
            FROM
              predictions p,
              UNNEST(recommended)
            )
            SELECT * FROM `vivid-poet-333222.bqml.top5prods` 
       
        """
    )

    return flask.redirect(
        flask.url_for(
            "recommendedproducts",
            project_id=query_job.project,
            job_id=query_job.job_id,
            location=query_job.location,
        )
    )


@app.route("/recommendedproducts")
def bqmlresults():
    project_id = flask.request.args.get("project_id")
    job_id = flask.request.args.get("job_id")
    location = flask.request.args.get("location")

    query_job = bigquery_client.get_job(
        job_id,
        project=project_id,
        location=location,
    )

    try:
        bqmlresults = query_job.result(timeout=30)
    except concurrent.futures.TimeoutError:
        return flask.render_template("timeout.html", job_id=query_job.job_id)

    return flask.render_template("recommendedproducts.html", results=bqmlresults)
    
    try:
        # Set a timeout because queries could take longer than one minute.
        results = query_job.result(timeout=30)
    except concurrent.futures.TimeoutError:
        return flask.render_template("timeout.html", job_id=query_job.job_id)

    return flask.render_template("recommendedproducts.html", results=results)



if __name__ == "__main__":
    # This is used when running locally only. When deploying to Google App
    # Engine, a webserver process such as Gunicorn will serve the app. This
    # can be configured by adding an `entrypoint` to app.yaml.
    app.run(host="127.0.0.1", port=8080, debug=True)
# [END gae_python3_bigquery]
# [END gae_python38_bigquery]
