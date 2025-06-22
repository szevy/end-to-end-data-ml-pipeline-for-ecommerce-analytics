with base_reviews as (

  select
    review_id,
    order_id,
    review_score,
    coalesce(review_comment_message, '') as review_comment_message,
    length(coalesce(review_comment_message, '')) as review_length,
    case when review_comment_message is null or review_comment_message = '' then 0 else 1 end as has_comment,

    review_creation_date,
    cast(format_date('%Y%m%d', extract(date from review_creation_date)) as int64) as review_creation_date_id,

    review_answer_timestamp as review_answer_date,
    cast(format_date('%Y%m%d', extract(date from review_answer_timestamp)) as int64) as review_answer_date_id,

    case
      when review_creation_date is not null and review_answer_timestamp is not null
      then timestamp_diff(review_answer_timestamp, review_creation_date, hour)
      else null
    end as response_time_hours,

    case when review_score >= 4 then 1 else 0 end as is_positive_review,
    case when review_score <= 2 then 1 else 0 end as is_negative_review,

    load_timestamp

  from {{ ref('stg_olist_order_reviews') }}

  qualify row_number() over (
    partition by review_id 
    order by review_creation_date desc, review_answer_timestamp desc
  ) = 1

)

select
  br.*,
  o.customer_id

from base_reviews br
left join {{ ref('stg_olist_orders') }} o
  on br.order_id = o.order_id