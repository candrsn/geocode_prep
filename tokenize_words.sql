

-- word frequencies
CREATE TEMP TABLE word_stats as (
SELECT a.indexable_id, s as word, row_number() OVER (PARTITION BY a.indexable_id)
    FROM address_list_temp a,
        unnest(string_to_array(fulladdress, ' ')) as s
);


SELECT count(*) FROM address_list_temp;

SELECT count(*), word FROM word_stats GROUP BY 2 ORDER BY 1;

