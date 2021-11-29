
"""`data_ingestion.py` is a Dataflow pipeline which reads a file and writes its
contents to a BigQuery table and rebuild model.

"""

import argparse
import logging
import re

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions



class DataIngestion:


    def parse_method(self, string_input):
      """
        CREATE OR REPLACE EXTERNAL TABLE mydataset.sales
                    OPTIONS (
                      format = 'CSV',
                      uris = ['gs://googlerecommend434prod/dailyfeed.csv']
                    )
        CREATE OR REPLACE TABLE bqml.top_products AS (
            SELECT  p.v2ProductName,
                    p.v2ProductCategory,
                    SUM(p.ProductQuantity) AS Quantity,
                    SUM(p.localProductRevenue/1000000) AS Revenue
            FROM `bqml.stagetbl`,
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
                        `bqml.stagetbl`,
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
                        `bqml.stagetbl` t,
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
            CREATE OR REPLACE MODEL bqml.recommendaiprod
            OPTIONS(model_type='matrix_factorization',
            user_col='userId',
            item_col='itemId',
            rating_col='session_duration',
            feedback_type='implicit'
            )
      AS
      SELECT * FROM bqml.aggregate_web_stats;
       CREATE OR REPLACE TABLE bqml.top5prods AS (
            WITH predictions AS (
                SELECT
                  userId,
                  ARRAY_AGG(STRUCT(itemId,
                                   predicted_session_duration_confidence)
                            ORDER BY
                              predicted_session_duration_confidence DESC
                            LIMIT 5) as recommended
                FROM ML.RECOMMEND(MODEL bqml.recommendaiprod)
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


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
